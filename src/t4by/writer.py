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

    async def enqueue_ver(self, logical: LogicalMessage) -> None:
        key = f"{logical.chat_id}:{logical.grouped_id or logical.min_id}"
        enqueued = await self.store.enqueue_job(
            "classify",
            key,
            {"chat_id": logical.chat_id, "message_ids": list(logical.message_ids)},
            not_before=time.time() + 10,
        )
        log.info(
            "classification message %s chat_id=%s message_ids=%s delay_seconds=10",
            "enqueued" if enqueued else "deduplicated",
            logical.chat_id,
            logical.message_ids,
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
        log.info("classification started chat_id=%s message_ids=%s", chat_id, tuple(message_ids))
        ver = await self.gateway.get_logical(chat_id, message_ids)
        occurrence = await self.store.occurrence_for_ver(chat_id, message_ids)
        if occurrence is None:
            log.warning(
                "classification routed to man reason=occurrence_not_found ver=%s/%s", chat_id, tuple(message_ids)
            )
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
            log.info(
                "classification media hashed occurrence_id=%s info_media=%d ver_media=%d unique_hashes=%d",
                occurrence.occurrence_id,
                len(info_media),
                len(ver_media),
                len({item.media_hash for item in media}),
            )
            await self._classify(occurrence, info, ver, media)
        except RetryAfter:
            raise
        except Exception:
            log.exception("writer classification failed occurrence_id=%s", occurrence.occurrence_id)
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
            log.info(
                "classification decided occurrence_id=%s code=%s bucket=%s hashes=%d historical=%d up=%d blacklist=%d",
                occurrence.occurrence_id,
                occurrence.code,
                bucket,
                len(all_hashes),
                len(historical),
                len(up),
                len(blacklist),
            )
            if bucket == Bucket.BLACKLIST:
                await self.gateway.forward(ver, self.chats.blacklist)
                await self.gateway.forward(info, self.chats.blacklist)
                await self.store.save_classification(occurrence, media, bucket, None)
                log.info("classification completed occurrence_id=%s bucket=%s", occurrence.occurrence_id, bucket)
                return bucket

            if bucket == Bucket.UP:
                up_info = await self.gateway.forward(info, self.chats.up)
                up_ver = await self.gateway.forward(ver, self.chats.up)
                group_id = await self.store.save_classification(occurrence, media, bucket, up_ver)
                if group_id:
                    await self.store.append_group_messages(group_id, bucket, [up_info])
                await self.store.mark_merge_dirty(bucket, all_hashes)
                log.info(
                    "classification completed occurrence_id=%s bucket=%s group_id=%s merge_dirty=true",
                    occurrence.occurrence_id,
                    bucket,
                    group_id,
                )
                return bucket

            destination = self.chats.oneshot if bucket == Bucket.ONESHOT else self.chats.repeat
            target = await self.gateway.forward(ver, destination)
            await self.store.save_classification(occurrence, media, bucket, target)
            if bucket == Bucket.REPEAT:
                await self.store.mark_merge_dirty(bucket, all_hashes)
            log.info(
                "classification completed occurrence_id=%s bucket=%s target=%s/%s merge_dirty=%s",
                occurrence.occurrence_id,
                bucket,
                target.chat_id,
                target.message_ids,
                bucket == Bucket.REPEAT,
            )
            return bucket
