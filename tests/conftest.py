from __future__ import annotations

from collections.abc import Sequence

import pytest

from t4by.config import Chats
from t4by.models import LogicalMessage, Media, Message, Origin
from t4by.storage import Store


def logical(
    chat_id: int,
    *message_ids: int,
    text: str = "",
    grouped_id: int | None = None,
    media: bool = True,
) -> LogicalMessage:
    return LogicalMessage(
        chat_id,
        tuple(
            Message(
                chat_id,
                message_id,
                grouped_id=grouped_id,
                text=text if index == 0 else "",
                has_media=media,
                media_type="video" if media else None,
            )
            for index, message_id in enumerate(message_ids)
        ),
    )


@pytest.fixture
def chats() -> Chats:
    return Chats(source=1, info=2, ver=3, man=4, oneshot=5, repeat=6, up=7, blacklist=8)


@pytest.fixture
async def store(tmp_path):
    value = Store(tmp_path / "test.sqlite3")
    await value.open()
    yield value
    await value.close()


class FakeGateway:
    def __init__(self) -> None:
        self.messages: dict[tuple[int, tuple[int, ...]], LogicalMessage] = {}
        self.history: dict[tuple[bool, int], list[Message]] = {}
        self.source: LogicalMessage | None = None
        self.forwarded: list[tuple[LogicalMessage, int]] = []
        self.sent: list[tuple[int, list[Media], str]] = []
        self.deleted: list[tuple[int, int]] = []
        self.next_id = 1000

    def add(self, value: LogicalMessage) -> None:
        self.messages[(value.chat_id, value.message_ids)] = value

    async def get_logical(self, chat, message_ids: Sequence[int]) -> LogicalMessage:
        return self.messages[(int(chat), tuple(message_ids))]

    async def resolve_forward_source(self, info: LogicalMessage) -> LogicalMessage | None:
        return self.source

    async def history_page(self, chat, anchor_id: int, *, newer: bool, limit: int = 60):
        return self.history.get((newer, anchor_id), [])[:limit]

    async def forward(self, source: LogicalMessage, destination) -> LogicalMessage:
        self.forwarded.append((source, int(destination)))
        result = logical(
            int(destination),
            *range(self.next_id, self.next_id + len(source.messages)),
            text=source.effective_text,
            grouped_id=self.next_id if len(source.messages) > 1 else None,
            media=bool(source.media_messages),
        )
        self.next_id += len(source.messages)
        self.add(result)
        return result

    async def hash_media(self, value: LogicalMessage, origin: Origin) -> list[Media]:
        return [
            Media(f"h-{value.chat_id}-{message.message_id}", origin, value.chat_id, message.message_id, index, "video")
            for index, message in enumerate(value.media_messages)
        ]

    async def send_media(self, destination, media: Sequence[Media], caption: str) -> LogicalMessage:
        self.sent.append((int(destination), list(media), caption))
        result = logical(
            int(destination),
            *range(self.next_id, self.next_id + len(media)),
            text=caption,
            grouped_id=self.next_id if len(media) > 1 else None,
        )
        self.next_id += len(media)
        self.add(result)
        return result

    async def delete(self, messages: Sequence[tuple[int, int]]) -> None:
        self.deleted.extend(messages)
