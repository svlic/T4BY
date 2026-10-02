from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

Chat = int | str


def _required(name: str) -> str:
    value = os.getenv(name)
    if value is None or not value.strip():
        raise ValueError(f"missing required environment variable: {name}")
    return value.strip()


def _chat(name: str) -> Chat:
    value = _required(name)
    try:
        return int(value)
    except ValueError:
        return value


@dataclass(frozen=True, slots=True)
class Chats:
    source: Chat
    info: Chat
    ver: Chat
    man: Chat
    oneshot: Chat
    repeat: Chat
    up: Chat
    blacklist: Chat


@dataclass(frozen=True, slots=True)
class Settings:
    reader_api_id: int
    reader_api_hash: str
    writer_api_id: int
    writer_api_hash: str
    reader_session: str
    writer_session: str
    database: Path
    chats: Chats
    metrics_host: str = "127.0.0.1"
    metrics_port: int = 9464
    log_level: str = "INFO"
    reader_workers: int = 4
    classification_workers: int = 8
    manual_workers: int = 2
    quiet_period: float = 5.0
    merge_max_wait: float = 20.0

    @classmethod
    def from_env(cls) -> Settings:
        settings = cls(
            reader_api_id=int(_required("T4BY_READER_API_ID")),
            reader_api_hash=_required("T4BY_READER_API_HASH"),
            writer_api_id=int(_required("T4BY_WRITER_API_ID")),
            writer_api_hash=_required("T4BY_WRITER_API_HASH"),
            reader_session=os.getenv("T4BY_READER_SESSION", "data/reader"),
            writer_session=os.getenv("T4BY_WRITER_SESSION", "data/writer"),
            database=Path(os.getenv("T4BY_DATABASE", "data/t4by.sqlite3")),
            chats=Chats(
                source=_chat("T4BY_SOURCE_CHAT"),
                info=_chat("T4BY_INFO_CHAT"),
                ver=_chat("T4BY_VER_CHAT"),
                man=_chat("T4BY_MAN_CHAT"),
                oneshot=_chat("T4BY_ONESHOT_CHAT"),
                repeat=_chat("T4BY_REPEAT_CHAT"),
                up=_chat("T4BY_UP_CHAT"),
                blacklist=_chat("T4BY_BLACKLIST_CHAT"),
            ),
            metrics_host=os.getenv("T4BY_METRICS_HOST", "127.0.0.1"),
            metrics_port=int(os.getenv("T4BY_METRICS_PORT", "9464")),
            log_level=os.getenv("T4BY_LOG_LEVEL", "INFO").upper(),
        )
        settings.database.parent.mkdir(parents=True, exist_ok=True)
        Path(settings.reader_session).parent.mkdir(parents=True, exist_ok=True)
        Path(settings.writer_session).parent.mkdir(parents=True, exist_ok=True)
        return settings
