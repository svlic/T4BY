import logging
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, call

import pytest
from conftest import FakeGateway, logical
from telethon.tl.types import Message as TelegramMessage
from telethon.tl.types import MessageFwdHeader, MessageMediaPhoto, PeerChannel, PhotoEmpty

from t4by.app import Application, _logical
from t4by.manual import ManualService
from t4by.models import Bucket, Media, Origin, ReaderStatus


async def _oneshot_group(store, gateway, *, offset=0, hashes=("HASH",), origin=Origin.VER):
    info = logical(2, 10 + offset, text="123456")
    ver = logical(3, 110 + offset, text="123456")
    occurrence_id = await store.create_occurrence(info, "123456")
    await store.finish_reader(occurrence_id, ReaderStatus.MATCHED, source=info, ver=ver)
    occurrence = await store.get_occurrence(occurrence_id)
    assert occurrence
    gateway.add(info)
    gateway.add(ver)
    target = await gateway.forward(ver, 5)
    group_id = await store.save_classification(
        occurrence,
        [Media(value, origin, ver.chat_id, ver.min_id + index, index, "video") for index, value in enumerate(hashes)],
        Bucket.ONESHOT,
        target,
    )
    assert group_id
    return occurrence_id, target


async def test_manual_up_promotes_group_and_marks_merge_dirty(store, chats, caplog) -> None:
    gateway = FakeGateway()
    occurrence_id, target = await _oneshot_group(store, gateway)
    trigger = logical(chats.up, 700)
    gateway.add(trigger)
    service = ManualService(store, gateway, chats)
    caplog.set_level(logging.INFO, logger="t4by.manual")

    await service.process(Bucket.UP, trigger.chat_id, trigger.message_ids, target.chat_id, target.min_id)

    assert [destination for _, destination in gateway.forwarded[-2:]] == [chats.up, chats.up]
    occurrence = await store.get_occurrence(occurrence_id)
    assert occurrence and occurrence.current_bucket == "up_pending"
    assert await store.claim_due_merge(0, 0) == (Bucket.UP, {"HASH"})
    messages = [record.getMessage() for record in caplog.records]
    assert any(
        f"manual migration started target=up trigger={trigger.chat_id}/{trigger.message_ids}" in message
        for message in messages
    )
    assert any(
        "manual migration completed target=up" in message and "merge_dirty=true" in message for message in messages
    )


async def test_manual_blacklist_forwards_originals_and_removes_group(store, chats) -> None:
    gateway = FakeGateway()
    occurrence_id, target = await _oneshot_group(store, gateway)
    trigger = logical(chats.blacklist, 800)
    gateway.add(trigger)
    service = ManualService(store, gateway, chats)

    await service.process(Bucket.BLACKLIST, trigger.chat_id, trigger.message_ids, chats.ver, 110)

    forwarded, destination = gateway.forwarded[-1]
    assert (forwarded.chat_id, destination) == (chats.info, chats.blacklist)
    assert gateway.deleted == [(target.chat_id, target.min_id)]
    assert await store.get_occurrence(occurrence_id) is None
    _, _, blacklist = await store.known_hashes({"HASH"})
    assert blacklist == {"HASH"}


async def test_blacklist_ignores_internal_ver_without_active_group(store, chats) -> None:
    gateway = FakeGateway()
    trigger = logical(chats.blacklist, 800)
    gateway.add(trigger)
    service = ManualService(store, gateway, chats)

    await service.process(Bucket.BLACKLIST, trigger.chat_id, trigger.message_ids, chats.ver, 110)

    assert gateway.forwarded == []
    assert gateway.deleted == []


async def test_blacklist_source_header_reads_info_without_source_access(store, chats) -> None:
    chats = replace(chats, source=-(10**12 + 1), blacklist=-(10**12 + 8))
    gateway = FakeGateway()
    occurrence_id, target = await _oneshot_group(store, gateway)
    raw = TelegramMessage(
        id=800,
        peer_id=PeerChannel(8),
        message="123456",
        media=MessageMediaPhoto(photo=PhotoEmpty(123)),
        fwd_from=MessageFwdHeader(date=None, from_id=PeerChannel(1), channel_post=999),
    )
    trigger = _logical([raw])
    gateway.add(trigger)
    gateway.get_logical = AsyncMock(wraps=gateway.get_logical)
    gateway.hash_media = AsyncMock(return_value=[Media("HASH", Origin.VER, trigger.chat_id, 800, 0, "video")])
    gateway.resolve_forward_source = AsyncMock(side_effect=AssertionError("must not access SOURCE"))
    gateway.history_page = AsyncMock(side_effect=AssertionError("must not search SOURCE"))
    service = ManualService(store, gateway, chats)

    class Client:
        def __init__(self):
            self.handlers = {}

        def on(self, event):
            def register(handler):
                self.handlers[handler.__name__] = handler
                return handler

            return register

    app = object.__new__(Application)
    app.reader_client = Client()
    app.writer_client = Client()
    app.manual = service
    app._register_handlers(chats)
    await app.writer_client.handlers["manual_single"](SimpleNamespace(message=raw, chat_id=trigger.chat_id))
    job = await store.claim_job(("manual",))
    assert job and job.payload["source_chat_id"] == chats.source
    await service.process(**job.payload)

    assert gateway.get_logical.await_args_list == [call(trigger.chat_id, [800]), call(chats.info, (10,))]
    gateway.hash_media.assert_awaited_once_with(trigger, Origin.VER)
    gateway.resolve_forward_source.assert_not_awaited()
    gateway.history_page.assert_not_awaited()
    assert gateway.forwarded[-1] == (logical(chats.info, 10, text="123456"), chats.blacklist)
    assert gateway.deleted == [(target.chat_id, target.min_id)]
    assert await store.get_occurrence(occurrence_id) is None
    assert (await store.known_hashes({"HASH"}))[2] == {"HASH"}

    # A program-generated forward with the same retained SOURCE header must not loop into MAN.
    forwarded_count = len(gateway.forwarded)
    await service.process(**job.payload)
    assert len(gateway.forwarded) == forwarded_count


@pytest.mark.parametrize("hashes", [{"HASH"}, {"HASH", "OTHER"}, {"MISSING"}, set()])
async def test_blacklist_hash_lookup_requires_unique_complete_ver_match(store, chats, hashes) -> None:
    gateway = FakeGateway()
    first, _ = await _oneshot_group(store, gateway, hashes=("HASH", "OTHER"))
    await _oneshot_group(store, gateway, offset=20)
    await _oneshot_group(store, gateway, offset=40, hashes=("HASH", "OTHER"), origin=Origin.INFO)

    group = await store.active_group_for_ver_hashes(hashes)

    if hashes == {"HASH", "OTHER"}:
        assert group and group.occurrence_ids == (first,)
    else:
        assert group is None


async def test_blacklist_ambiguous_source_resource_routes_to_man_without_deleting(store, chats) -> None:
    gateway = FakeGateway()
    first, _ = await _oneshot_group(store, gateway)
    second, _ = await _oneshot_group(store, gateway, offset=20)
    trigger = logical(chats.blacklist, 800)
    gateway.add(trigger)
    gateway.hash_media = AsyncMock(return_value=[Media("HASH", Origin.VER, trigger.chat_id, 800, 0, "video")])

    await ManualService(store, gateway, chats).process(
        Bucket.BLACKLIST,
        trigger.chat_id,
        trigger.message_ids,
        chats.source,
        999,
    )

    assert gateway.forwarded[-1] == (trigger, chats.man)
    assert gateway.deleted == []
    assert await store.get_occurrence(first)
    assert await store.get_occurrence(second)
    assert (await store.known_hashes({"HASH"}))[2] == set()
