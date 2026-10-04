from t4by.app import _resolve_chats


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
