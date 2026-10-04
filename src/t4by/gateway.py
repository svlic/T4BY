from __future__ import annotations

import asyncio
import hashlib
import logging
import random
import tempfile
import time
from collections import defaultdict
from collections.abc import Awaitable, Callable, Sequence
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Protocol, TypeVar

from telethon import TelegramClient
from telethon.errors import FileReferenceExpiredError, FloodWaitError, SlowModeWaitError
from telethon.tl.types import Message as TelethonMessage
from telethon.tl.types import PeerChannel, PeerChat, PeerUser

from .config import Chat
from .metrics import ACTIVE_DOWNLOADS, FLOOD_SECONDS, FLOOD_WAITS
from .models import LogicalMessage, Media, Message, Origin, collapse_messages

log = logging.getLogger(__name__)
T = TypeVar("T")


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


class RetryAfter(Exception):
    def __init__(self, seconds: float, method: str) -> None:
        self.seconds = seconds
        self.method = method
        super().__init__(f"{method} may retry in {seconds:.1f}s")


class TelegramGateway(Protocol):
    async def get_logical(self, chat: Chat, message_ids: Sequence[int]) -> LogicalMessage: ...

    async def resolve_forward_source(self, info: LogicalMessage) -> LogicalMessage | None: ...

    async def history_page(self, chat: Chat, anchor_id: int, *, newer: bool, limit: int = 60) -> list[Message]: ...

    async def forward(self, source: LogicalMessage, destination: Chat) -> LogicalMessage: ...

    async def hash_media(self, logical: LogicalMessage, origin: Origin) -> list[Media]: ...

    async def send_media(self, destination: Chat, media: Sequence[Media], caption: str) -> LogicalMessage: ...

    async def delete(self, messages: Sequence[tuple[int, int]]) -> None: ...


class AdaptiveTokenBucket:
    def __init__(self, rate: float, burst: float, minimum: float) -> None:
        self.baseline = rate
        self.rate = rate
        self.burst = burst
        self.minimum = minimum
        self.tokens = burst
        self.updated = time.monotonic()
        self.last_penalty: float | None = None
        self.lock = asyncio.Lock()

    async def acquire(self) -> None:
        while True:
            async with self.lock:
                now = time.monotonic()
                elapsed = now - self.updated
                self.tokens = min(self.burst, self.tokens + elapsed * self.rate)
                self.updated = now
                if self.last_penalty is not None and now - self.last_penalty >= 1800:
                    self.rate = min(self.baseline, self.rate * 1.25)
                    self.last_penalty = now if self.rate < self.baseline else None
                if self.tokens >= 1:
                    self.tokens -= 1
                    return
                delay = (1 - self.tokens) / self.rate
            await asyncio.sleep(delay)

    async def penalize(self) -> None:
        async with self.lock:
            self.rate = max(self.minimum, self.rate * 0.5)
            self.last_penalty = time.monotonic()


class AdaptiveConcurrency:
    def __init__(self, limit: int, minimum: int = 1) -> None:
        self.baseline = limit
        self.limit = limit
        self.minimum = minimum
        self.active = 0
        self.last_penalty: float | None = None
        self.condition = asyncio.Condition()

    async def __aenter__(self) -> None:
        async with self.condition:
            now = time.monotonic()
            if self.last_penalty is not None and now - self.last_penalty >= 1800:
                self.limit = min(self.baseline, self.limit + 1)
                self.last_penalty = now if self.limit < self.baseline else None
            await self.condition.wait_for(lambda: self.active < self.limit)
            self.active += 1

    async def __aexit__(self, *args: object) -> None:
        async with self.condition:
            self.active -= 1
            self.condition.notify_all()

    async def penalize(self) -> None:
        async with self.condition:
            self.limit = max(self.minimum, self.limit - 1)
            self.last_penalty = time.monotonic()


class RpcGates:
    def __init__(self) -> None:
        self.control = AdaptiveTokenBucket(5, 3, 1)
        self.history = AdaptiveTokenBucket(1, 1, 1)
        self.write = AdaptiveTokenBucket(2, 2, 0.5)
        self.control_inflight = asyncio.Semaphore(4)
        self.history_inflight = asyncio.Semaphore(1)
        self.download_global = asyncio.Semaphore(4)
        self.download_small = AdaptiveConcurrency(3)
        self.download_large = AdaptiveConcurrency(2)
        self.upload = asyncio.Semaphore(1)
        self.per_chat: dict[str, AdaptiveTokenBucket] = defaultdict(lambda: AdaptiveTokenBucket(1, 1, 0.5))
        self.cooldowns: dict[str, float] = {}

    async def _cooldown(self, key: str) -> None:
        available = self.cooldowns.get(key, 0)
        if available > time.monotonic():
            raise RetryAfter(available - time.monotonic(), key)

    @asynccontextmanager
    async def controlled(self, method: str):
        await self._cooldown(method)
        await self.control.acquire()
        async with self.control_inflight:
            yield

    @asynccontextmanager
    async def historical(self):
        await self._cooldown("history")
        await self.history.acquire()
        async with self.history_inflight:
            yield

    @asynccontextmanager
    async def writing(self, chat: Chat, method: str):
        key = f"{method}:{chat}"
        await self._cooldown(method)
        await self._cooldown(key)
        await self.write.acquire()
        await self.per_chat[str(chat)].acquire()
        yield

    async def flood(self, method: str, seconds: int, chat: Chat | None = None) -> RetryAfter:
        wait = seconds + 1 + random.random() * 0.5
        key = f"{method}:{chat}" if chat is not None else method
        self.cooldowns[key] = max(self.cooldowns.get(key, 0), time.monotonic() + wait)
        FLOOD_WAITS.labels(method=method).inc()
        FLOOD_SECONDS.labels(method=method).inc(seconds)
        if method in {"forward", "send_media", "delete"}:
            await self.write.penalize()
        elif method == "download_small":
            await self.download_small.penalize()
        elif method == "download_large":
            await self.download_large.penalize()
        else:
            await self.control.penalize()
        log.warning(
            "Telegram flood wait method=%s chat=%s server_seconds=%d retry_seconds=%.1f",
            method,
            chat,
            seconds,
            wait,
        )
        return RetryAfter(wait, key)


def _peer_id(peer: object | None) -> int | None:
    if isinstance(peer, PeerChannel):
        return -(10**12 + peer.channel_id)
    if isinstance(peer, PeerChat):
        return -peer.chat_id
    if isinstance(peer, PeerUser):
        return peer.user_id
    return None


def message_from_telethon(raw: TelethonMessage) -> Message:
    file = raw.file
    media_type = None
    if raw.photo:
        media_type = "photo"
    elif raw.video:
        media_type = "video"
    elif raw.document:
        media_type = "document"
    fwd = raw.fwd_from
    return Message(
        chat_id=raw.chat_id,
        message_id=raw.id,
        grouped_id=raw.grouped_id,
        text=raw.message or "",
        has_media=raw.media is not None,
        media_type=media_type,
        media_size=file.size if file else None,
        forward_chat_id=_peer_id(fwd.from_id) if fwd else None,
        forward_message_id=fwd.channel_post if fwd else None,
    )


class TelethonGateway:
    def __init__(
        self, client: TelegramClient, gates: RpcGates | None = None, *, forbidden_chat: Chat | None = None
    ) -> None:
        self.client = client
        self.gates = gates or RpcGates()
        self.forbidden_chat = forbidden_chat

    def _check_chat(self, chat: Chat) -> None:
        if self.forbidden_chat is not None and chat == self.forbidden_chat:
            raise ValueError("writer must not access SOURCE")

    async def _call(self, method: str, action: Callable[[], Awaitable[T]], *, chat: Chat | None = None) -> T:
        try:
            return await action()
        except (FloodWaitError, SlowModeWaitError) as error:
            raise await self.gates.flood(method, error.seconds, chat) from error

    async def get_logical(self, chat: Chat, message_ids: Sequence[int]) -> LogicalMessage:
        self._check_chat(chat)
        async with self.gates.controlled("get_messages"):
            raw = await self._call("get_messages", lambda: self.client.get_messages(chat, ids=list(message_ids)))
        values = [item for item in (raw if isinstance(raw, list) else [raw]) if item]
        if not values:
            raise LookupError(f"messages no longer available: {chat}/{list(message_ids)}")
        logical = collapse_messages(message_from_telethon(item) for item in values)
        if len(logical) != 1:
            raise ValueError("stored message ids no longer form one logical message")
        return logical[0]

    async def _whole_album(self, chat: Chat, message_id: int) -> LogicalMessage:
        self._check_chat(chat)
        async with self.gates.controlled("get_messages"):
            center = await self._call("get_messages", lambda: self.client.get_messages(chat, ids=message_id))
            if center is None:
                raise LookupError(f"source message not found: {chat}/{message_id}")
            if center.grouped_id is None:
                return LogicalMessage(center.chat_id, (message_from_telethon(center),))
            raw = await self._call(
                "get_messages",
                lambda: self.client.get_messages(
                    chat,
                    limit=20,
                    min_id=max(0, message_id - 11),
                    max_id=message_id + 11,
                ),
            )
        members = [item for item in raw if item.grouped_id == center.grouped_id]
        return LogicalMessage(
            center.chat_id,
            tuple(
                sorted(
                    (message_from_telethon(item) for item in members),
                    key=lambda item: item.message_id,
                )
            ),
        )

    async def resolve_forward_source(self, info: LogicalMessage) -> LogicalMessage | None:
        ref = next(
            (
                (message.forward_chat_id, message.forward_message_id)
                for message in info.messages
                if message.forward_chat_id is not None and message.forward_message_id is not None
            ),
            None,
        )
        return await self._whole_album(*ref) if ref else None

    async def history_page(self, chat: Chat, anchor_id: int, *, newer: bool, limit: int = 60) -> list[Message]:
        self._check_chat(chat)
        async with self.gates.historical():

            async def collect() -> list[TelethonMessage]:
                kwargs = {"min_id": anchor_id, "reverse": True} if newer else {"max_id": anchor_id}
                return [item async for item in self.client.iter_messages(chat, limit=limit, **kwargs)]

            raw = await self._call("history", collect)
        return [message_from_telethon(item) for item in raw]

    async def forward(self, source: LogicalMessage, destination: Chat) -> LogicalMessage:
        self._check_chat(source.chat_id)
        self._check_chat(destination)
        async with self.gates.writing(destination, "forward"):
            raw = await self._call(
                "forward",
                lambda: self.client.forward_messages(destination, list(source.message_ids), from_peer=source.chat_id),
                chat=destination,
            )
        values = raw if isinstance(raw, list) else [raw]
        return LogicalMessage(values[0].chat_id, tuple(message_from_telethon(item) for item in values))

    async def hash_media(self, logical: LogicalMessage, origin: Origin) -> list[Media]:
        self._check_chat(logical.chat_id)
        result: list[Media] = []
        for order, message in enumerate(logical.media_messages):
            large = message.media_size is not None and message.media_size >= 20 * 1024 * 1024
            size = "large" if large else "small"
            size_gate = self.gates.download_large if large else self.gates.download_small
            async with self.gates.download_global, size_gate:
                ACTIVE_DOWNLOADS.labels(size=size).inc()
                try:
                    with tempfile.TemporaryDirectory(prefix="t4by-") as directory:
                        path = Path(directory) / str(message.message_id)
                        async with self.gates.controlled("get_messages"):
                            raw = await self._call(
                                "get_messages",
                                lambda message=message: self.client.get_messages(
                                    logical.chat_id, ids=message.message_id
                                ),
                            )
                        downloaded = await self._call(
                            f"download_{size}",
                            lambda raw=raw, path=path: self.client.download_media(raw, file=path),
                        )
                        if not downloaded:
                            raise OSError(f"failed to download media {logical.chat_id}/{message.message_id}")
                        digest = await asyncio.to_thread(_sha256, downloaded)
                finally:
                    ACTIVE_DOWNLOADS.labels(size=size).dec()
            result.append(
                Media(
                    digest,
                    origin,
                    logical.chat_id,
                    message.message_id,
                    order,
                    message.media_type or "media",
                )
            )
        return result

    async def send_media(self, destination: Chat, media: Sequence[Media], caption: str) -> LogicalMessage:
        self._check_chat(destination)
        for item in media:
            self._check_chat(item.chat_id)
        if not media:
            raise ValueError("cannot send an empty media group")
        references = []
        for item in media:
            async with self.gates.controlled("get_messages"):
                message = await self._call(
                    "get_messages",
                    lambda item=item: self.client.get_messages(item.chat_id, ids=item.message_id),
                )
            if message is None or message.media is None:
                raise LookupError(f"media reference unavailable: {item.chat_id}/{item.message_id}")
            references.append(message.media)
        try:
            async with self.gates.writing(destination, "send_media"):
                raw = await self._call(
                    "send_media",
                    lambda: self.client.send_file(destination, references, caption=caption),
                    chat=destination,
                )
        except FileReferenceExpiredError:
            log.warning(
                "Telegram media reference expired; using upload fallback destination=%s media=%d",
                destination,
                len(media),
            )
            raw = await self._upload_fallback(destination, media, caption)
        values = raw if isinstance(raw, list) else [raw]
        return LogicalMessage(values[0].chat_id, tuple(message_from_telethon(item) for item in values))

    async def _upload_fallback(
        self, destination: Chat, media: Sequence[Media], caption: str
    ) -> TelethonMessage | list[TelethonMessage]:
        self._check_chat(destination)
        for item in media:
            self._check_chat(item.chat_id)
        with tempfile.TemporaryDirectory(prefix="t4by-upload-") as directory:
            paths: list[str] = []
            for index, item in enumerate(media):
                async with self.gates.controlled("get_messages"):
                    source = await self._call(
                        "get_messages",
                        lambda item=item: self.client.get_messages(item.chat_id, ids=item.message_id),
                    )
                if source is None or source.media is None:
                    raise LookupError(f"upload fallback source unavailable: {item.chat_id}/{item.message_id}")
                media_size = source.file.size if source.file else None
                large = media_size is not None and media_size >= 20 * 1024 * 1024
                size = "large" if large else "small"
                size_gate = self.gates.download_large if large else self.gates.download_small
                async with self.gates.download_global, size_gate:
                    path = Path(directory) / str(index)
                    downloaded = await self._call(
                        f"download_{size}",
                        lambda source=source, path=path: self.client.download_media(source, file=path),
                    )
                if not downloaded:
                    raise OSError(f"upload fallback download failed: {item.chat_id}/{item.message_id}")
                paths.append(downloaded)
            async with self.gates.upload, self.gates.writing(destination, "send_media"):
                return await self._call(
                    "send_media",
                    lambda: self.client.send_file(destination, paths, caption=caption),
                    chat=destination,
                )

    async def delete(self, messages: Sequence[tuple[int, int]]) -> None:
        by_chat: dict[int, list[int]] = defaultdict(list)
        for chat_id, message_id in messages:
            self._check_chat(chat_id)
            by_chat[chat_id].append(message_id)
        for chat_id, message_ids in by_chat.items():
            async with self.gates.writing(chat_id, "delete"):
                await self._call(
                    "delete",
                    lambda chat_id=chat_id, message_ids=message_ids: self.client.delete_messages(chat_id, message_ids),
                    chat=chat_id,
                )
