from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, call

import pytest
from conftest import FakeGateway, logical

from t4by.gateway import RetryAfter
from t4by.models import Bucket, Media, Origin, ReaderStatus
from t4by.writer import WriterService, choose_bucket


def test_classification_priority() -> None:
    hashes = {"A", "B"}
    assert choose_bucket(hashes, set(), {"A"}, {"B"}) == Bucket.BLACKLIST
    assert choose_bucket(hashes, set(), {"A"}, set()) == Bucket.UP
    assert choose_bucket(hashes, set(), set(), set()) == Bucket.ONESHOT
    assert choose_bucket(hashes, {"A"}, set(), set()) == Bucket.REPEAT


async def test_ver_job_is_delayed_ten_seconds(store, chats, monkeypatch) -> None:
    monkeypatch.setattr("t4by.writer.time", SimpleNamespace(time=lambda: 100.0))
    writer = WriterService(store, FakeGateway(), chats)

    await writer.enqueue_ver(logical(chats.ver, 10))

    row = await (await store._database().execute("SELECT not_before FROM jobs")).fetchone()
    assert row["not_before"] == 110.0


async def _occurrence(store, code: str, suffix: int):
    info = logical(2, suffix, text=code)
    ver = logical(3, suffix + 100, text=code)
    occurrence_id = await store.create_occurrence(info, code)
    await store.finish_reader(occurrence_id, ReaderStatus.MATCHED, source=info, ver=ver)
    return await store.get_occurrence(occurrence_id), info, ver


async def test_repeated_code_does_not_create_relation_without_shared_hash(store, chats) -> None:
    gateway = FakeGateway()
    writer = WriterService(store, gateway, chats)
    first, info1, ver1 = await _occurrence(store, "123456", 1)
    second, info2, ver2 = await _occurrence(store, "123456", 2)
    assert first and second

    bucket1 = await writer._classify(
        first,
        info1,
        ver1,
        [Media("A", Origin.INFO, 2, 1, 0, "photo"), Media("V1", Origin.VER, 3, 101, 0, "video")],
    )
    bucket2 = await writer._classify(
        second,
        info2,
        ver2,
        [Media("B", Origin.INFO, 2, 2, 0, "photo"), Media("V2", Origin.VER, 3, 102, 0, "video")],
    )

    assert bucket1 == Bucket.ONESHOT
    assert bucket2 == Bucket.ONESHOT
    assert first.occurrence_id != second.occurrence_id


async def test_shared_info_hash_causes_repeat(store, chats) -> None:
    gateway = FakeGateway()
    writer = WriterService(store, gateway, chats)
    first, info1, ver1 = await _occurrence(store, "111111", 10)
    second, info2, ver2 = await _occurrence(store, "222222", 20)
    assert first and second
    await writer._classify(
        first,
        info1,
        ver1,
        [Media("SHARED", Origin.INFO, 2, 10, 0, "photo"), Media("V1", Origin.VER, 3, 110, 0, "video")],
    )

    result = await writer._classify(
        second,
        info2,
        ver2,
        [Media("SHARED", Origin.INFO, 2, 20, 0, "photo"), Media("V2", Origin.VER, 3, 120, 0, "video")],
    )

    assert result == Bucket.REPEAT


async def test_flood_wait_is_retried_without_forwarding_to_man(store, chats) -> None:
    class FloodingGateway(FakeGateway):
        async def hash_media(self, value, origin):
            raise RetryAfter(12, "download_media")

    gateway = FloodingGateway()
    writer = WriterService(store, gateway, chats)
    occurrence, info, ver = await _occurrence(store, "111111", 30)
    assert occurrence
    gateway.add(info)
    gateway.add(ver)

    try:
        await writer.process(ver.chat_id, ver.message_ids)
    except RetryAfter:
        pass
    else:
        raise AssertionError("RetryAfter was not propagated to the delayed queue")

    assert gateway.forwarded == []


@pytest.mark.parametrize("bucket", [Bucket.ONESHOT, Bucket.REPEAT, Bucket.UP, Bucket.BLACKLIST])
async def test_classification_reads_only_info_ver_despite_source_headers(store, chats, bucket) -> None:
    gateway = FakeGateway()
    occurrence, info, ver = await _occurrence(store, "111111", 30)
    assert occurrence
    for value in (info, ver):
        gateway.add(
            replace(
                value,
                messages=tuple(
                    replace(message, forward_chat_id=chats.source, forward_message_id=999) for message in value.messages
                ),
            )
        )
    gateway.get_logical = AsyncMock(wraps=gateway.get_logical)
    gateway.resolve_forward_source = AsyncMock(side_effect=AssertionError("SOURCE access"))
    gateway.history_page = AsyncMock(side_effect=AssertionError("SOURCE history"))
    known = {f"h-{chats.ver}-{ver.min_id}"}
    store.known_hashes = AsyncMock(
        return_value=(
            known if bucket != Bucket.ONESHOT else set(),
            known if bucket == Bucket.UP else set(),
            known if bucket == Bucket.BLACKLIST else set(),
        )
    )

    await WriterService(store, gateway, chats).process(ver.chat_id, ver.message_ids)

    assert gateway.get_logical.await_args_list == [
        call(chats.ver, ver.message_ids),
        call(chats.info, info.message_ids),
    ]
    gateway.resolve_forward_source.assert_not_awaited()
    gateway.history_page.assert_not_awaited()
    assert {item.chat_id for item, _ in gateway.forwarded} <= {chats.info, chats.ver}
    assert {destination for _, destination in gateway.forwarded} == {getattr(chats, bucket.value)}
