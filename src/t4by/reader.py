from __future__ import annotations

import logging
from collections.abc import Sequence

from .config import Chats
from .gateway import RetryAfter, TelegramGateway
from .models import (
    LogicalMessage,
    ReaderStatus,
    collapse_messages,
    contains_code,
    extract_code,
)
from .storage import Store

log = logging.getLogger(__name__)


async def find_source_match(
    gateway: TelegramGateway,
    source: LogicalMessage,
    code: str,
    *,
    window: int = 5,
    page_size: int = 60,
) -> LogicalMessage | None:
    """Search newer logical messages first, then older logical messages."""
    source_ids = set(source.message_ids)
    for newer, start in ((True, source.max_id), (False, source.min_id)):
        logical: list[LogicalMessage] = []
        anchor = start
        seen: set[tuple[int, ...]] = set()
        while len(logical) < window:
            raw = await gateway.history_page(source.chat_id, anchor, newer=newer, limit=page_size)
            if not raw:
                break
            page = collapse_messages(raw, newest_first=not newer)
            for candidate in page:
                identity = candidate.message_ids
                if identity in seen or not source_ids.isdisjoint(identity):
                    continue
                seen.add(identity)
                logical.append(candidate)
                if len(logical) == window:
                    break
            next_anchor = max(item.message_id for item in raw) if newer else min(item.message_id for item in raw)
            if next_anchor == anchor or len(raw) < page_size:
                break
            anchor = next_anchor
        for candidate in logical[:window]:
            if contains_code(candidate.effective_text, code):
                return candidate
    return None


class ReaderService:
    def __init__(self, store: Store, gateway: TelegramGateway, chats: Chats) -> None:
        self.store = store
        self.gateway = gateway
        self.chats = chats

    async def enqueue(self, logical: LogicalMessage) -> None:
        key = f"{logical.chat_id}:{logical.grouped_id or logical.min_id}"
        await self.store.enqueue_job(
            "reader",
            key,
            {"chat_id": logical.chat_id, "message_ids": list(logical.message_ids)},
        )

    async def process(self, chat_id: int, message_ids: Sequence[int]) -> None:
        info = await self.gateway.get_logical(chat_id, message_ids)
        code = extract_code(info.effective_text)
        occurrence_id = await self.store.create_occurrence(info, code)
        source: LogicalMessage | None = None
        try:
            if code is None:
                man = await self.gateway.forward(info, self.chats.man)
                await self.store.finish_reader(occurrence_id, ReaderStatus.ERROR, man=man)
                return
            source = await self.gateway.resolve_forward_source(info)
            if source is None or source.chat_id != self.chats.source:
                man = await self.gateway.forward(info, self.chats.man)
                await self.store.finish_reader(occurrence_id, ReaderStatus.ERROR, man=man)
                return
            target = await find_source_match(self.gateway, source, code)
            if target is None:
                man = await self.gateway.forward(info, self.chats.man)
                await self.store.finish_reader(occurrence_id, ReaderStatus.UNMATCHED, source=source, man=man)
                return
            ver = await self.gateway.forward(target, self.chats.ver)
            await self.store.finish_reader(occurrence_id, ReaderStatus.MATCHED, source=source, ver=ver)
        except RetryAfter:
            raise
        except Exception:
            log.exception("reader occurrence failed", extra={"occurrence_id": occurrence_id})
            try:
                man = await self.gateway.forward(info, self.chats.man)
                await self.store.finish_reader(occurrence_id, ReaderStatus.ERROR, source=source, man=man)
            except Exception:
                log.exception("failed to forward reader error to man")
            raise
