from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Any

from .config import Chats
from .gateway import RetryAfter, TelegramGateway
from .models import (
    Bucket,
    LogicalMessage,
    Media,
    Origin,
    caption_for_codes,
    chunks,
    connected_component,
    stable_unique_media,
)
from .storage import Store


def partition_components(seed_hashes: set[str], nodes: dict[str, set[str]]) -> list[set[str]]:
    pending = set(seed_hashes)
    result: list[set[str]] = []
    while pending:
        seed = pending.pop()
        members = connected_component({seed}, nodes)
        if not members:
            continue
        hashes = set().union(*(nodes[node] for node in members))
        pending.difference_update(hashes)
        result.append(members)
    return result


class MergeEngine:
    def __init__(self, store: Store, gateway: TelegramGateway, chats: Chats) -> None:
        self.store = store
        self.gateway = gateway
        self.chats = chats

    async def merge(self, bucket: Bucket, seed_hashes: set[str]) -> None:
        if bucket == Bucket.REPEAT:
            nodes = await self.store.repeat_nodes()
        elif bucket == Bucket.UP:
            nodes = await self.store.up_nodes()
        else:
            raise ValueError(f"bucket is not mergeable: {bucket}")
        for members in partition_components(seed_hashes, nodes):
            if bucket == Bucket.REPEAT:
                await self._repeat_component(members, nodes)
            else:
                await self._up_component(members, nodes)

    async def _send_batches(
        self, destination: int | str, media: Sequence[Media], codes: Iterable[str]
    ) -> list[tuple[LogicalMessage, Sequence[Media]]]:
        caption = caption_for_codes(codes)
        sent: list[tuple[LogicalMessage, Sequence[Media]]] = []
        for batch in chunks(media):
            logical = await self.gateway.send_media(destination, batch, caption)
            sent.append((logical, batch))
        return sent

    async def _report_failure(self, occurrence_ids: set[str], old_messages: Sequence[tuple[int, int]]) -> None:
        references: list[dict[str, Any]] = []
        for occurrence_id in occurrence_ids:
            occurrence = await self.store.get_occurrence(occurrence_id)
            if occurrence is None:
                continue
            references.append(
                {
                    "chat_id": occurrence.info_chat_id,
                    "message_ids": list(occurrence.info_message_ids),
                }
            )
            if occurrence.ver_chat_id and occurrence.ver_message_ids:
                references.append(
                    {
                        "chat_id": occurrence.ver_chat_id,
                        "message_ids": list(occurrence.ver_message_ids),
                    }
                )
        if not references:
            references = [{"chat_id": chat_id, "message_ids": [message_id]} for chat_id, message_id in old_messages]
        identity = ":".join(f"{item['chat_id']}-{'-'.join(map(str, item['message_ids']))}" for item in references)
        await self.store.enqueue_job(
            "man",
            f"merge:{identity}",
            {"references": references},
        )

    async def _repeat_component(self, members: set[str], nodes: dict[str, set[str]]) -> None:
        occurrence_ids = set(members)
        codes = await self.store.codes_for_occurrences(occurrence_ids)
        media = stable_unique_media([await self.store.media_for_occurrences(occurrence_ids, Origin.VER)])
        old_groups = await self.store.groups_for_occurrences(occurrence_ids)
        old_messages = await self.store.messages_for_groups(old_groups)
        try:
            sent = await self._send_batches(self.chats.repeat, media, codes)
            await self.gateway.delete(old_messages)
            hashes = set().union(*(nodes[node] for node in members))
            await self.store.replace_group(
                Bucket.REPEAT,
                old_groups,
                occurrence_ids,
                codes,
                hashes,
                sent,
            )
        except RetryAfter:
            raise
        except Exception:
            await self._report_failure(occurrence_ids, old_messages)
            raise

    async def _up_component(self, members: set[str], nodes: dict[str, set[str]]) -> None:
        old_groups = {node.removeprefix("group:") for node in members if node.startswith("group:")}
        occurrence_ids = {node.removeprefix("occ:") for node in members if node.startswith("occ:")}
        old_groups.update(await self.store.groups_for_occurrences(occurrence_ids))
        codes = await self.store.codes_for_occurrences(occurrence_ids)
        for group_id in old_groups:
            codes.update((await self.store.group_data(group_id)).codes)
        hashes = set().union(*(nodes[node] for node in members))
        hidden = await self.store.hidden_hashes(hashes)
        previous_media = await self.store.up_group_media(old_groups)
        new_media = await self.store.media_for_occurrences(occurrence_ids)
        display = [item for item in stable_unique_media([previous_media, new_media]) if item.media_hash not in hidden]
        old_messages = await self.store.messages_for_groups(old_groups)
        try:
            sent = await self._send_batches(self.chats.up, display, codes) if display else []
            await self.store.add_expected_deletions(old_messages)
            await self.gateway.delete(old_messages)
            await self.store.replace_group(
                Bucket.UP,
                old_groups,
                occurrence_ids,
                codes,
                hashes,
                sent,
            )
        except RetryAfter:
            raise
        except Exception:
            await self._report_failure(occurrence_ids, old_messages)
            raise
