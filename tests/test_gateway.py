from dataclasses import replace
from unittest.mock import AsyncMock

import pytest
from conftest import logical
from telethon.tl.types import Message, MessageFwdHeader, PeerChannel

from t4by.gateway import TelethonGateway
from t4by.models import Media, Origin


@pytest.mark.parametrize(
    "operation",
    ["read", "resolve", "history", "forward_from", "forward_to", "hash", "send", "upload", "delete"],
)
async def test_writer_gateway_rejects_source_before_any_rpc(chats, operation) -> None:
    client = AsyncMock()
    gateway = TelethonGateway(client, forbidden_chat=chats.source)
    source = logical(chats.source, 100)
    info = logical(chats.info, 200)
    forwarded = replace(
        info,
        messages=(replace(info.messages[0], forward_chat_id=chats.source, forward_message_id=100),),
    )
    media = [
        Media("INFO", Origin.INFO, chats.info, 200, 0, "video"),
        Media("SOURCE", Origin.VER, chats.source, 100, 1, "video"),
    ]
    with pytest.raises(ValueError, match="writer must not access SOURCE"):
        if operation == "read":
            await gateway.get_logical(chats.source, [100])
        elif operation == "resolve":
            await gateway.resolve_forward_source(forwarded)
        elif operation == "history":
            await gateway.history_page(chats.source, 100, newer=True)
        elif operation == "forward_from":
            await gateway.forward(source, chats.ver)
        elif operation == "forward_to":
            await gateway.forward(info, chats.source)
        elif operation == "hash":
            await gateway.hash_media(source, Origin.VER)
        elif operation == "send":
            await gateway.send_media(chats.up, media, "")
        elif operation == "upload":
            await gateway._upload_fallback(chats.up, media, "")
        elif operation == "delete":
            await gateway.delete([(chats.up, 300), (chats.source, 100)])
    assert client.mock_calls == []


@pytest.mark.parametrize("role,channel", [("reader", 1), ("writer", 2), ("writer", 3)])
async def test_gateway_allows_reader_source_and_writer_copies(role, channel) -> None:
    client = AsyncMock()
    client.get_messages.return_value = [
        Message(
            id=100,
            peer_id=PeerChannel(channel),
            message="123456",
            fwd_from=MessageFwdHeader(date=None, from_id=PeerChannel(1), channel_post=999),
        )
    ]
    gateway = TelethonGateway(client, forbidden_chat=-(10**12 + 1) if role == "writer" else None)

    result = await gateway.get_logical(-(10**12 + channel), [100])

    assert result.chat_id == -(10**12 + channel)
    assert result.messages[0].forward_chat_id == -(10**12 + 1)
    client.get_messages.assert_awaited_once_with(result.chat_id, ids=[100])
