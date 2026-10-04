from __future__ import annotations

import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from enum import StrEnum
from itertools import chain

CODE_RE = re.compile(r"(?<!\d)\d{6}(?!\d)")


class Bucket(StrEnum):
    ONESHOT = "oneshot"
    REPEAT = "repeat"
    UP = "up"
    BLACKLIST = "blacklist"


class Origin(StrEnum):
    INFO = "info"
    VER = "ver"


class ReaderStatus(StrEnum):
    MATCHED = "matched"
    UNMATCHED = "unmatched"
    ERROR = "error"


@dataclass(frozen=True, slots=True)
class Message:
    chat_id: int
    message_id: int
    grouped_id: int | None = None
    text: str = ""
    has_media: bool = False
    media_type: str | None = None
    media_size: int | None = None
    forward_chat_id: int | None = None
    forward_message_id: int | None = None


@dataclass(frozen=True, slots=True)
class LogicalMessage:
    chat_id: int
    messages: tuple[Message, ...]

    def __post_init__(self) -> None:
        if not self.messages:
            raise ValueError("a logical message cannot be empty")
        if any(message.chat_id != self.chat_id for message in self.messages):
            raise ValueError("all logical message members must belong to the same chat")

    @property
    def message_ids(self) -> tuple[int, ...]:
        return tuple(message.message_id for message in self.messages)

    @property
    def grouped_id(self) -> int | None:
        return next((message.grouped_id for message in self.messages if message.grouped_id), None)

    @property
    def min_id(self) -> int:
        return min(self.message_ids)

    @property
    def max_id(self) -> int:
        return max(self.message_ids)

    @property
    def effective_text(self) -> str:
        return "\n".join(message.text for message in self.messages if message.text)

    @property
    def media_messages(self) -> tuple[Message, ...]:
        return tuple(message for message in self.messages if message.has_media)


@dataclass(frozen=True, slots=True)
class Media:
    media_hash: str
    origin: Origin
    chat_id: int
    message_id: int
    media_order: int
    media_type: str


def extract_code(text: str) -> str | None:
    match = CODE_RE.search(text)
    return match.group(0) if match else None


def contains_code(text: str, code: str) -> bool:
    if len(code) != 6 or not code.isdigit():
        raise ValueError("code must contain exactly six digits")
    return re.search(rf"(?<!\d){re.escape(code)}(?!\d)", text) is not None


def collapse_messages(messages: Iterable[Message], *, newest_first: bool = False) -> list[LogicalMessage]:
    """Fold raw Telegram messages into logical messages while preserving direction order."""
    groups: dict[tuple[str, int], list[Message]] = {}
    keys: list[tuple[str, int]] = []
    for message in messages:
        key = ("group", message.grouped_id) if message.grouped_id else ("single", message.message_id)
        if key not in groups:
            groups[key] = []
            keys.append(key)
        groups[key].append(message)
    result = []
    for key in keys:
        members = tuple(sorted(groups[key], key=lambda item: item.message_id))
        result.append(LogicalMessage(chat_id=members[0].chat_id, messages=members))
    if newest_first:
        result.sort(key=lambda item: item.max_id, reverse=True)
    else:
        result.sort(key=lambda item: item.min_id)
    return result


def stable_unique_media(media_sets: Sequence[Sequence[Media]]) -> list[Media]:
    seen: set[str] = set()
    result: list[Media] = []
    for media in chain.from_iterable(media_sets):
        if media.media_hash not in seen:
            seen.add(media.media_hash)
            result.append(media)
    return result


def caption_for_codes(codes: Iterable[str]) -> str:
    return "\n".join(sorted(set(codes), key=int))


def chunks(items: Sequence[Media], size: int = 10) -> list[list[Media]]:
    if size < 1:
        raise ValueError("chunk size must be positive")
    return [list(items[index : index + size]) for index in range(0, len(items), size)]


def connected_component(seed_hashes: set[str], nodes: dict[str, set[str]]) -> set[str]:
    """Return node ids transitively connected to any seed hash."""
    selected: set[str] = set()
    frontier = set(seed_hashes)
    while frontier:
        matching = {
            node_id for node_id, hashes in nodes.items() if node_id not in selected and hashes.intersection(frontier)
        }
        if not matching:
            break
        selected.update(matching)
        frontier.update(*(nodes[node_id] for node_id in matching))
    return selected
