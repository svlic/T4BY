from conftest import FakeGateway, logical

from t4by.manual import ManualService
from t4by.models import Bucket, Media, Origin, ReaderStatus


async def _oneshot_group(store, gateway):
    info = logical(2, 10, text="123456")
    ver = logical(3, 110, text="123456")
    occurrence_id = await store.create_occurrence(info, "123456")
    await store.finish_reader(occurrence_id, ReaderStatus.MATCHED, source=info, ver=ver)
    occurrence = await store.get_occurrence(occurrence_id)
    assert occurrence
    gateway.add(info)
    gateway.add(ver)
    target = await gateway.forward(ver, 5)
    group_id = await store.save_classification(
        occurrence,
        [Media("HASH", Origin.VER, ver.chat_id, ver.min_id, 0, "video")],
        Bucket.ONESHOT,
        target,
    )
    assert group_id
    return occurrence_id, target


async def test_manual_up_promotes_group_and_marks_merge_dirty(store, chats) -> None:
    gateway = FakeGateway()
    occurrence_id, target = await _oneshot_group(store, gateway)
    trigger = logical(chats.up, 700)
    gateway.add(trigger)
    service = ManualService(store, gateway, chats)

    await service.process(Bucket.UP, trigger.chat_id, trigger.message_ids, target.chat_id, target.min_id)

    assert [destination for _, destination in gateway.forwarded[-2:]] == [chats.up, chats.up]
    occurrence = await store.get_occurrence(occurrence_id)
    assert occurrence and occurrence.current_bucket == "up_pending"
    assert await store.claim_due_merge(0, 0) == (Bucket.UP, {"HASH"})


async def test_manual_blacklist_forwards_originals_and_removes_group(store, chats) -> None:
    gateway = FakeGateway()
    occurrence_id, target = await _oneshot_group(store, gateway)
    trigger = logical(chats.blacklist, 800)
    gateway.add(trigger)
    service = ManualService(store, gateway, chats)

    await service.process(Bucket.BLACKLIST, trigger.chat_id, trigger.message_ids, target.chat_id, target.min_id)

    assert [destination for _, destination in gateway.forwarded[-2:]] == [chats.blacklist, chats.blacklist]
    assert set(gateway.deleted) == {(target.chat_id, target.min_id), (trigger.chat_id, trigger.min_id)}
    assert await store.get_occurrence(occurrence_id) is None
    _, _, blacklist = await store.known_hashes({"HASH"})
    assert blacklist == {"HASH"}
