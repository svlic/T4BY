from dataclasses import replace
from types import SimpleNamespace

import pytest
from conftest import FakeGateway
from telethon.tl.types import Message, MessageFwdHeader, PeerChannel

from t4by.app import Application, _logical, _resolve_chats
from t4by.manual import ManualService
from t4by.models import Bucket


class Client:
    def __init__(self, offset: int) -> None:
        self.offset = offset
        self.values: list[int | str] = []

    async def get_peer_id(self, value: int | str) -> int:
        self.values.append(value)
        return int(value) + self.offset


async def test_resolve_chats_uses_reader_only_for_source(chats) -> None:
    reader = Client(100)
    writer = Client(200)

    resolved = await _resolve_chats(reader, writer, chats)

    assert resolved.source == chats.source + 100
    assert reader.values == [chats.source]
    assert writer.values == [
        chats.info,
        chats.ver,
        chats.man,
        chats.oneshot,
        chats.repeat,
        chats.up,
        chats.blacklist,
    ]


@pytest.mark.parametrize("sources", [None, (2, 2), (2, 1), (1, None)])
async def test_manual_albums_ignore_outputs_and_reject_mixed_origins(store, chats, sources) -> None:
    chats = replace(chats, info=-(10**12 + 2), up=-(10**12 + 7))

    class EventClient:
        def __init__(self):
            self.handlers = {}

        def on(self, event):
            def register(handler):
                self.handlers[handler.__name__] = handler
                return handler

            return register

    raw = [
        Message(
            id=800 + index,
            peer_id=PeerChannel(7),
            message="123456",
            grouped_id=700,
            fwd_from=MessageFwdHeader(date=None, from_id=PeerChannel(source), channel_post=100 + index)
            if source
            else None,
        )
        for index, source in enumerate(sources or (None, None))
    ]
    gateway = FakeGateway()
    trigger = _logical(raw)
    gateway.add(trigger)
    app = object.__new__(Application)
    app.reader_client = EventClient()
    app.writer_client = EventClient()
    app.manual = ManualService(store, gateway, chats)
    app._register_handlers(chats)
    await app.writer_client.handlers["manual_album"](SimpleNamespace(messages=raw, chat_id=chats.up))
    job = await store.claim_job(("manual",))
    if sources in (None, (2, 2)):
        assert job is None
    else:
        assert job and job.payload["source_chat_id"] is None
        await app.manual.process(**job.payload)
        assert gateway.forwarded == [(trigger, chats.man)]
        assert gateway.deleted == []


@pytest.mark.parametrize("source", ["oneshot", "ver", "source"])
async def test_repeat_listener_enqueues_retained_forward_origins(store, chats, source) -> None:
    chats = replace(
        chats,
        source=-(10**12 + 1),
        ver=-(10**12 + 3),
        oneshot=-(10**12 + 5),
        repeat=-(10**12 + 6),
    )

    class EventClient:
        def __init__(self):
            self.handlers = {}

        def on(self, event):
            def register(handler):
                self.handlers[handler.__name__] = handler
                return handler

            return register

    source_channel = {"source": 1, "ver": 3, "oneshot": 5}[source]
    raw = Message(
        id=800,
        peer_id=PeerChannel(6),
        message="123456",
        fwd_from=MessageFwdHeader(date=None, from_id=PeerChannel(source_channel), channel_post=100),
    )
    app = object.__new__(Application)
    app.reader_client = EventClient()
    app.writer_client = EventClient()
    app.manual = ManualService(store, FakeGateway(), chats)
    app._register_handlers(chats)

    await app.writer_client.handlers["manual_single"](SimpleNamespace(message=raw, chat_id=chats.repeat))

    job = await store.claim_job(("manual",))
    assert job and job.payload == {
        "target": Bucket.REPEAT,
        "trigger_chat_id": chats.repeat,
        "trigger_message_ids": [800],
        "source_chat_id": getattr(chats, source),
        "source_message_id": 100,
    }
