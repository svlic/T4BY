from conftest import FakeGateway, logical

from t4by.merger import MergeEngine, partition_components
from t4by.models import Bucket, Media, Origin, ReaderStatus


def test_partition_does_not_merge_unrelated_dirty_seeds() -> None:
    nodes = {"a": {"A", "B"}, "b": {"B", "C"}, "x": {"X"}}
    assert {frozenset(value) for value in partition_components({"A", "X"}, nodes)} == {
        frozenset({"a", "b"}),
        frozenset({"x"}),
    }


async def _save(store, gateway, code, suffix, bucket, media):
    info = logical(2, suffix, text=code)
    ver = logical(3, suffix + 100, text=code)
    occurrence_id = await store.create_occurrence(info, code)
    await store.finish_reader(occurrence_id, ReaderStatus.MATCHED, source=info, ver=ver)
    occurrence = await store.get_occurrence(occurrence_id)
    assert occurrence
    target = await gateway.forward(ver, 5 if bucket == Bucket.ONESHOT else 6)
    await store.save_classification(occurrence, media, bucket, target)
    return occurrence_id


async def test_repeat_relates_by_info_but_displays_only_ver(store, chats) -> None:
    gateway = FakeGateway()
    first = await _save(
        store,
        gateway,
        "111111",
        1,
        Bucket.ONESHOT,
        [Media("H", Origin.INFO, 2, 1, 0, "photo"), Media("V1", Origin.VER, 3, 101, 0, "video")],
    )
    second = await _save(
        store,
        gateway,
        "222222",
        2,
        Bucket.REPEAT,
        [Media("H", Origin.INFO, 2, 2, 0, "photo"), Media("V2", Origin.VER, 3, 102, 0, "video")],
    )
    engine = MergeEngine(store, gateway, chats)

    await engine.merge(Bucket.REPEAT, {"H"})

    assert first != second
    assert len(gateway.sent) == 1
    destination, displayed, caption = gateway.sent[0]
    assert destination == chats.repeat
    assert {item.media_hash for item in displayed} == {"V1", "V2"}
    assert caption == "111111\n222222"


async def test_hidden_up_hash_still_connects_but_never_reappears(store, chats) -> None:
    gateway = FakeGateway()
    first = await _save(
        store,
        gateway,
        "111111",
        10,
        Bucket.UP,
        [
            Media("HIDDEN", Origin.INFO, 2, 10, 0, "photo"),
            Media("V1", Origin.VER, 3, 110, 0, "video"),
        ],
    )
    engine = MergeEngine(store, gateway, chats)
    await engine.merge(Bucket.UP, {"HIDDEN"})
    row = await (
        await store._database().execute(
            """SELECT chat_id,message_id FROM group_messages
               WHERE media_hash='HIDDEN' AND is_active=1"""
        )
    ).fetchone()
    assert row is not None
    assert await store.hide_up_message(row["chat_id"], row["message_id"]) == "HIDDEN"

    second = await _save(
        store,
        gateway,
        "222222",
        20,
        Bucket.UP,
        [
            Media("HIDDEN", Origin.INFO, 2, 20, 0, "photo"),
            Media("V2", Origin.VER, 3, 120, 0, "video"),
        ],
    )
    await engine.merge(Bucket.UP, {"HIDDEN"})

    assert first != second
    _, displayed, caption = gateway.sent[-1]
    assert {item.media_hash for item in displayed} == {"V1", "V2"}
    assert caption == "111111\n222222"
    _, up, _ = await store.known_hashes({"HIDDEN"})
    assert up == {"HIDDEN"}
