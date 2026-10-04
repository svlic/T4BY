from __future__ import annotations

import logging
from collections.abc import Sequence

from .config import Chats
from .gateway import RetryAfter, TelegramGateway
from .models import Bucket, LogicalMessage
from .storage import GroupData, Store

log = logging.getLogger(__name__)


class ManualService:
    def __init__(self, store: Store, gateway: TelegramGateway, chats: Chats) -> None:
        self.store = store
        self.gateway = gateway
        self.chats = chats

    async def enqueue(self, logical: LogicalMessage, target: Bucket) -> None:
        source = next(
            (
                (message.forward_chat_id, message.forward_message_id)
                for message in logical.messages
                if message.forward_chat_id is not None and message.forward_message_id is not None
            ),
            None,
        )
        key = f"{target}:{logical.chat_id}:{logical.grouped_id or logical.min_id}"
        enqueued = await self.store.enqueue_job(
            "manual",
            key,
            {
                "target": target,
                "trigger_chat_id": logical.chat_id,
                "trigger_message_ids": list(logical.message_ids),
                "source_chat_id": source[0] if source else None,
                "source_message_id": source[1] if source else None,
            },
        )
        log.info(
            "manual trigger %s target=%s trigger=%s/%s source=%s/%s",
            "enqueued" if enqueued else "deduplicated",
            target,
            logical.chat_id,
            logical.message_ids,
            source[0] if source else None,
            source[1] if source else None,
        )

    async def _originals(self, group: GroupData) -> list[LogicalMessage]:
        originals: list[LogicalMessage] = []
        for occurrence_id in group.occurrence_ids:
            occurrence = await self.store.get_occurrence(occurrence_id)
            if occurrence is None or occurrence.ver_chat_id is None:
                raise LookupError(f"mapping unavailable for {occurrence_id}")
            originals.append(await self.gateway.get_logical(occurrence.info_chat_id, occurrence.info_message_ids))
            originals.append(await self.gateway.get_logical(occurrence.ver_chat_id, occurrence.ver_message_ids))
        return originals

    async def process(
        self,
        target: Bucket,
        trigger_chat_id: int,
        trigger_message_ids: Sequence[int],
        source_chat_id: int | None,
        source_message_id: int | None,
    ) -> None:
        log.info(
            "manual migration started target=%s trigger=%s/%s source=%s/%s",
            target,
            trigger_chat_id,
            tuple(trigger_message_ids),
            source_chat_id,
            source_message_id,
        )
        trigger = await self.gateway.get_logical(trigger_chat_id, trigger_message_ids)
        if source_chat_id is None or source_message_id is None:
            log.warning(
                "manual trigger routed to man reason=missing_source target=%s trigger=%s/%s",
                target,
                trigger.chat_id,
                trigger.message_ids,
            )
            await self.gateway.forward(trigger, self.chats.man)
            return
        group = await self.store.active_group(source_chat_id, source_message_id)
        if group is None or group.bucket not in (Bucket.ONESHOT, Bucket.REPEAT):
            log.warning(
                "manual trigger routed to man reason=inactive_source target=%s trigger=%s/%s "
                "source=%s/%s group_bucket=%s",
                target,
                trigger.chat_id,
                trigger.message_ids,
                source_chat_id,
                source_message_id,
                group.bucket if group else None,
            )
            await self.gateway.forward(trigger, self.chats.man)
            return
        try:
            originals = await self._originals(group)
            destination = self.chats.up if target == Bucket.UP else self.chats.blacklist
            log.info(
                "manual source resolved target=%s group_id=%s source_bucket=%s occurrences=%d originals=%d hashes=%d",
                target,
                group.group_id,
                group.bucket,
                len(group.occurrence_ids),
                len(originals),
                len(group.hashes),
            )
            forwarded = [await self.gateway.forward(item, destination) for item in originals]
            if target == Bucket.UP:
                await self.store.promote_manual_up(group)
                await self.store.append_group_messages(group.group_id, Bucket.UP, [trigger, *forwarded])
                await self.store.mark_merge_dirty(Bucket.UP, group.hashes)
                log.info(
                    "manual migration completed target=%s group_id=%s forwarded=%d merge_dirty=true",
                    target,
                    group.group_id,
                    len(forwarded),
                )
                return
            await self.store.prepare_manual_blacklist(group.hashes)
            old_messages = await self.store.messages_for_groups({group.group_id})
            trigger_messages = [(trigger.chat_id, message_id) for message_id in trigger.message_ids]
            await self.gateway.delete([*old_messages, *trigger_messages])
            await self.store.finish_manual_blacklist(group)
            log.info(
                "manual migration completed target=%s group_id=%s forwarded=%d deleted=%d",
                target,
                group.group_id,
                len(forwarded),
                len(old_messages) + len(trigger_messages),
            )
        except RetryAfter:
            raise
        except Exception:
            log.exception("manual migration failed target=%s group_id=%s", target, group.group_id)
            await self.gateway.forward(trigger, self.chats.man)
            raise
