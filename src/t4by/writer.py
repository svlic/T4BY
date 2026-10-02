from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Sequence

from .config import Chats
from .gateway import RetryAfter, TelegramGateway
from .models import Bucket, LogicalMessage, Media, Origin
from .storage import Occurrence, Store

log = logging.getLogger(__name__)


def choose_bucket(all_hashes: set[str], historical: set[str], up: set[str], blacklist: set[str]) -> Bucket:
    if all_hashes & blacklist:
        return Bucket.BLACKLIST
    if all_hashes & up:
        return Bucket.UP
    if all_hashes.isdisjoint(historical):
        return Bucket.ONESHOT
    return Bucket.REPEAT


class WriterService:
    def __init__(self, store: Store, gateway: TelegramGateway, chats: Chats) -> None:
        self.store = store
        self.gateway = gateway
        self.chats = chats
        self.classification_commit_lock = asyncio.Lock()

    async def enqueue_ver(self, logical: LogicalMessage, received_at: float | None = None) -> None:
        key = f"{logical.chat_id}:{logical.grouped_id or logical.min_id}"
        await self.store.enqueue_job(
            "classify",
            key,
            {"chat_id": logical.chat_id, "message_ids": list(logical.message_ids)},
            not_before=(received_at or time.time()) + 10,
        )

    async def _to_man(self, ver: LogicalMessage, occurrence: Occurrence | None = None) -> None:
        await self.gateway.forward(ver, self.chats.man)
        if occurrence:
            try:
                info = await self.gateway.get_logical(occurrence.info_chat_id, occurrence.info_message_ids)
                await self.gateway.forward(info, self.chats.man)
            except Exception:
                log.exception("failed to include INFO in writer MAN report")

    async def process(self, chat_id: int, message_ids: Sequence[int]) -> None:
        ver = await self.gateway.get_logical(chat_id, message_ids)
        occurrence = await self.store.occurrence_for_ver(chat_id, message_ids)
        if occurrence is None:
            await self._to_man(ver)
            return
        try:
            info = await self.gateway.get_logical(occurrence.info_chat_id, occurrence.info_message_ids)
            info_media, ver_media = await asyncio.gather(
                self.gateway.hash_media(info, Origin.INFO),
                self.gateway.hash_media(ver, Origin.VER),
            )
            media = [*info_media, *ver_media]
            if not media:
                raise ValueError("INFO and VER contain no downloadable media")
            await self._classify(occurrence, info, ver, media)
        except RetryAfter:
            raise
        except Exception:
            log.exception("writer classification failed", extra={"occurrence_id": occurrence.occurrence_id})
            await self._to_man(ver, occurrence)
            raise

    async def _classify(
        self,
        occurrence: Occurrence,
        info: LogicalMessage,
        ver: LogicalMessage,
        media: Sequence[Media],
    ) -> Bucket:
        all_hashes = {item.media_hash for item in media}
        # The decision, destination write, and history commit are serialized. Downloads still run
        # concurrently; this closes the duplicate-first-seen race without a distributed lock.
        async with self.classification_commit_lock:
            historical, up, blacklist = await self.store.known_hashes(all_hashes)
            bucket = choose_bucket(all_hashes, historical, up, blacklist)
            if bucket == Bucket.BLACKLIST:
                await self.gateway.forward(ver, self.chats.blacklist)
                await self.gateway.forward(info, self.chats.blacklist)
                await self.store.save_classification(occurrence, media, bucket, None)
                return bucket

            if bucket == Bucket.UP:
                up_info = await self.gateway.forward(info, self.chats.up)
                up_ver = await self.gateway.forward(ver, self.chats.up)
                group_id = await self.store.save_classification(occurrence, media, bucket, up_ver)
                if group_id:
                    await self.store.append_group_messages(group_id, bucket, [up_info])
                await self.store.mark_merge_dirty(bucket, all_hashes)
                return bucket

            destination = self.chats.oneshot if bucket == Bucket.ONESHOT else self.chats.repeat
            target = await self.gateway.forward(ver, destination)
            await self.store.save_classification(occurrence, media, bucket, target)
            if bucket == Bucket.REPEAT:
                await self.store.mark_merge_dirty(bucket, all_hashes)
            return bucket
