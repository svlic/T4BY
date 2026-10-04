from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import aiosqlite

from .models import Bucket, LogicalMessage, Media, Origin, ReaderStatus

log = logging.getLogger(__name__)

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS occurrence_records (
    occurrence_id TEXT PRIMARY KEY,
    code TEXT,
    info_chat_id INTEGER NOT NULL,
    info_message_ids TEXT NOT NULL,
    info_grouped_id INTEGER,
    source_chat_id INTEGER,
    source_message_ids TEXT,
    source_grouped_id INTEGER,
    ver_chat_id INTEGER,
    ver_message_ids TEXT,
    ver_grouped_id INTEGER,
    man_chat_id INTEGER,
    man_message_ids TEXT,
    reader_status TEXT NOT NULL,
    current_bucket TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS occurrence_code_idx ON occurrence_records(code);

CREATE TABLE IF NOT EXISTS occurrence_info_messages (
    occurrence_id TEXT NOT NULL REFERENCES occurrence_records(occurrence_id) ON DELETE CASCADE,
    chat_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    PRIMARY KEY(chat_id, message_id)
);

CREATE TABLE IF NOT EXISTS occurrence_ver_messages (
    occurrence_id TEXT NOT NULL REFERENCES occurrence_records(occurrence_id) ON DELETE CASCADE,
    chat_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    PRIMARY KEY(chat_id, message_id)
);

CREATE TABLE IF NOT EXISTS media_hash_registry (
    media_hash TEXT PRIMARY KEY,
    first_seen_occurrence_id TEXT NOT NULL,
    first_seen_code TEXT NOT NULL,
    first_seen_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS occurrence_media (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    occurrence_id TEXT NOT NULL REFERENCES occurrence_records(occurrence_id) ON DELETE CASCADE,
    code TEXT NOT NULL,
    media_hash TEXT NOT NULL,
    origin TEXT NOT NULL,
    origin_chat_id INTEGER NOT NULL,
    origin_message_id INTEGER NOT NULL,
    media_order INTEGER NOT NULL,
    media_type TEXT NOT NULL,
    created_at REAL NOT NULL,
    UNIQUE(occurrence_id, origin, origin_message_id, media_hash)
);
CREATE INDEX IF NOT EXISTS occurrence_media_hash_idx ON occurrence_media(media_hash);

CREATE TABLE IF NOT EXISTS bucket_hashes (
    bucket TEXT NOT NULL CHECK(bucket IN ('up', 'blacklist')),
    media_hash TEXT NOT NULL,
    added_at REAL NOT NULL,
    PRIMARY KEY(bucket, media_hash)
);

CREATE TABLE IF NOT EXISTS bucket_groups (
    group_id TEXT PRIMARY KEY,
    bucket TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('active', 'superseded')),
    superseded_by TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS active_group_bucket_idx ON bucket_groups(bucket, status);

CREATE TABLE IF NOT EXISTS group_occurrences (
    group_id TEXT NOT NULL REFERENCES bucket_groups(group_id) ON DELETE CASCADE,
    occurrence_id TEXT NOT NULL,
    code TEXT NOT NULL,
    PRIMARY KEY(group_id, occurrence_id)
);

CREATE TABLE IF NOT EXISTS group_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    group_id TEXT NOT NULL REFERENCES bucket_groups(group_id) ON DELETE CASCADE,
    channel_role TEXT NOT NULL,
    chat_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    album_batch_index INTEGER NOT NULL,
    media_hash TEXT,
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at REAL NOT NULL,
    UNIQUE(chat_id, message_id)
);
CREATE INDEX IF NOT EXISTS group_message_lookup_idx ON group_messages(chat_id, message_id, is_active);

CREATE TABLE IF NOT EXISTS up_group_codes (
    group_id TEXT NOT NULL REFERENCES bucket_groups(group_id) ON DELETE CASCADE,
    code TEXT NOT NULL,
    PRIMARY KEY(group_id, code)
);

CREATE TABLE IF NOT EXISTS up_group_hashes (
    group_id TEXT NOT NULL REFERENCES bucket_groups(group_id) ON DELETE CASCADE,
    media_hash TEXT NOT NULL,
    PRIMARY KEY(group_id, media_hash)
);
CREATE INDEX IF NOT EXISTS up_group_hash_idx ON up_group_hashes(media_hash);

CREATE TABLE IF NOT EXISTS up_hidden_hashes (
    media_hash TEXT PRIMARY KEY,
    hidden_at REAL NOT NULL,
    source_group_id TEXT,
    source_message_id INTEGER
);

CREATE TABLE IF NOT EXISTS expected_deletions (
    chat_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    expires_at REAL NOT NULL,
    PRIMARY KEY(chat_id, message_id)
);

CREATE TABLE IF NOT EXISTS jobs (
    job_id INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL,
    dedupe_key TEXT NOT NULL,
    payload TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('pending', 'running', 'retry_wait', 'done', 'failed')),
    not_before REAL NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    UNIQUE(kind, dedupe_key)
);
CREATE INDEX IF NOT EXISTS due_jobs_idx ON jobs(kind, status, not_before);

CREATE TABLE IF NOT EXISTS merge_dirty (
    bucket TEXT PRIMARY KEY CHECK(bucket IN ('repeat', 'up')),
    seed_hashes TEXT NOT NULL,
    first_dirty_at REAL NOT NULL,
    last_dirty_at REAL NOT NULL,
    dirty INTEGER NOT NULL,
    running INTEGER NOT NULL DEFAULT 0
);
"""


def _ids(value: Sequence[int] | None) -> str | None:
    return json.dumps(list(value)) if value is not None else None


def _loads_ids(value: str | None) -> tuple[int, ...]:
    return tuple(json.loads(value)) if value else ()


@dataclass(frozen=True, slots=True)
class Occurrence:
    occurrence_id: str
    code: str | None
    info_chat_id: int
    info_message_ids: tuple[int, ...]
    info_grouped_id: int | None
    source_chat_id: int | None
    source_message_ids: tuple[int, ...]
    source_grouped_id: int | None
    ver_chat_id: int | None
    ver_message_ids: tuple[int, ...]
    ver_grouped_id: int | None
    current_bucket: str | None
    reader_status: str


@dataclass(frozen=True, slots=True)
class Job:
    job_id: int
    kind: str
    payload: dict[str, Any]


@dataclass(frozen=True, slots=True)
class GroupData:
    group_id: str
    bucket: str
    occurrence_ids: tuple[str, ...]
    codes: tuple[str, ...]
    hashes: frozenset[str]


class Store:
    def __init__(self, path: Path | str) -> None:
        self.path = path
        self.db: aiosqlite.Connection | None = None
        self.lock = asyncio.Lock()

    async def open(self) -> None:
        self.db = await aiosqlite.connect(self.path, isolation_level=None)
        self.db.row_factory = aiosqlite.Row
        await self.db.executescript(SCHEMA)
        await self.db.execute("PRAGMA busy_timeout=5000")
        await self.recover_running_jobs()
        log.info("storage opened path=%s", self.path)

    async def close(self) -> None:
        if self.db:
            await self.db.close()
            self.db = None
            log.info("storage closed path=%s", self.path)

    def _database(self) -> aiosqlite.Connection:
        if self.db is None:
            raise RuntimeError("store is not open")
        return self.db

    async def _begin(self) -> aiosqlite.Connection:
        db = self._database()
        await db.execute("BEGIN IMMEDIATE")
        return db

    async def recover_running_jobs(self) -> None:
        now = time.time()
        jobs = await self._database().execute(
            "UPDATE jobs SET status='retry_wait', not_before=?, updated_at=? WHERE status='running'",
            (now, now),
        )
        merges = await self._database().execute("UPDATE merge_dirty SET running=0, dirty=1 WHERE running=1")
        if jobs.rowcount or merges.rowcount:
            log.warning("storage recovered interrupted work jobs=%d merges=%d", jobs.rowcount, merges.rowcount)

    async def enqueue_job(
        self, kind: str, dedupe_key: str, payload: dict[str, Any], not_before: float | None = None
    ) -> bool:
        now = time.time()
        cursor = await self._database().execute(
            """INSERT OR IGNORE INTO jobs
               (kind, dedupe_key, payload, status, not_before, created_at, updated_at)
               VALUES(?, ?, ?, 'pending', ?, ?, ?)""",
            (kind, dedupe_key, json.dumps(payload), not_before or now, now, now),
        )
        return cursor.rowcount == 1

    async def claim_job(self, kinds: Sequence[str]) -> Job | None:
        placeholders = ",".join("?" for _ in kinds)
        async with self.lock:
            db = await self._begin()
            try:
                row = await (
                    await db.execute(
                        f"""SELECT * FROM jobs
                            WHERE kind IN ({placeholders})
                              AND status IN ('pending', 'retry_wait') AND not_before <= ?
                            ORDER BY not_before, job_id LIMIT 1""",
                        (*kinds, time.time()),
                    )
                ).fetchone()
                if row is None:
                    await db.commit()
                    return None
                await db.execute(
                    "UPDATE jobs SET status='running', attempts=attempts+1, updated_at=? WHERE job_id=?",
                    (time.time(), row["job_id"]),
                )
                await db.commit()
                return Job(row["job_id"], row["kind"], json.loads(row["payload"]))
            except BaseException:
                await db.rollback()
                raise

    async def finish_job(self, job_id: int) -> None:
        await self._database().execute(
            "UPDATE jobs SET status='done', updated_at=? WHERE job_id=?", (time.time(), job_id)
        )

    async def retry_job(self, job_id: int, not_before: float, error: str) -> None:
        await self._database().execute(
            """UPDATE jobs SET status='retry_wait', not_before=?, last_error=?, updated_at=?
               WHERE job_id=?""",
            (not_before, error[:1000], time.time(), job_id),
        )

    async def fail_job(self, job_id: int, error: str) -> None:
        await self._database().execute(
            "UPDATE jobs SET status='failed', last_error=?, updated_at=? WHERE job_id=?",
            (error[:1000], time.time(), job_id),
        )

    async def queue_depth(self, kinds: Sequence[str]) -> int:
        placeholders = ",".join("?" for _ in kinds)
        row = await (
            await self._database().execute(
                f"""SELECT count(*) n FROM jobs WHERE kind IN ({placeholders})
                    AND status IN ('pending','retry_wait','running')""",
                tuple(kinds),
            )
        ).fetchone()
        return int(row["n"])

    async def create_occurrence(self, info: LogicalMessage, code: str | None) -> str:
        async with self.lock:
            db = await self._begin()
            try:
                placeholders = ",".join("?" for _ in info.message_ids)
                existing = await (
                    await db.execute(
                        f"""SELECT occurrence_id FROM occurrence_info_messages
                            WHERE chat_id=? AND message_id IN ({placeholders}) LIMIT 1""",
                        (info.chat_id, *info.message_ids),
                    )
                ).fetchone()
                if existing:
                    await db.commit()
                    return existing["occurrence_id"]
                occurrence_id = str(uuid.uuid4())
                now = time.time()
                await db.execute(
                    """INSERT INTO occurrence_records
                       (occurrence_id, code, info_chat_id, info_message_ids, info_grouped_id,
                        reader_status, created_at, updated_at)
                       VALUES(?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        occurrence_id,
                        code,
                        info.chat_id,
                        _ids(info.message_ids),
                        info.grouped_id,
                        ReaderStatus.ERROR,
                        now,
                        now,
                    ),
                )
                await db.executemany(
                    "INSERT INTO occurrence_info_messages VALUES(?,?,?)",
                    [(occurrence_id, info.chat_id, message_id) for message_id in info.message_ids],
                )
                await db.commit()
                return occurrence_id
            except BaseException:
                await db.rollback()
                raise

    async def operational_metrics(self) -> tuple[int, dict[str, float]]:
        db = self._database()
        dirty = await (await db.execute("SELECT count(*) n FROM merge_dirty WHERE dirty=1 OR running=1")).fetchone()
        rows = await (
            await db.execute(
                """SELECT kind, min(created_at) oldest FROM jobs
                   WHERE status IN ('pending','retry_wait','running') GROUP BY kind"""
            )
        ).fetchall()
        now = time.time()
        ages = {row["kind"]: max(0, now - row["oldest"]) for row in rows}
        return int(dirty["n"]), ages

    async def finish_reader(
        self,
        occurrence_id: str,
        status: ReaderStatus,
        *,
        source: LogicalMessage | None = None,
        ver: LogicalMessage | None = None,
        man: LogicalMessage | None = None,
    ) -> None:
        async with self.lock:
            db = await self._begin()
            try:
                await db.execute(
                    """UPDATE occurrence_records SET
                       source_chat_id=?, source_message_ids=?, source_grouped_id=?,
                       ver_chat_id=?, ver_message_ids=?, ver_grouped_id=?,
                       man_chat_id=?, man_message_ids=?, reader_status=?, updated_at=?
                       WHERE occurrence_id=?""",
                    (
                        source.chat_id if source else None,
                        _ids(source.message_ids) if source else None,
                        source.grouped_id if source else None,
                        ver.chat_id if ver else None,
                        _ids(ver.message_ids) if ver else None,
                        ver.grouped_id if ver else None,
                        man.chat_id if man else None,
                        _ids(man.message_ids) if man else None,
                        status,
                        time.time(),
                        occurrence_id,
                    ),
                )
                if ver:
                    await db.executemany(
                        "INSERT INTO occurrence_ver_messages(occurrence_id, chat_id, message_id) VALUES(?,?,?)",
                        [(occurrence_id, ver.chat_id, message_id) for message_id in ver.message_ids],
                    )
                await db.commit()
            except BaseException:
                await db.rollback()
                raise

    @staticmethod
    def _occurrence(row: aiosqlite.Row) -> Occurrence:
        return Occurrence(
            occurrence_id=row["occurrence_id"],
            code=row["code"],
            info_chat_id=row["info_chat_id"],
            info_message_ids=_loads_ids(row["info_message_ids"]),
            info_grouped_id=row["info_grouped_id"],
            source_chat_id=row["source_chat_id"],
            source_message_ids=_loads_ids(row["source_message_ids"]),
            source_grouped_id=row["source_grouped_id"],
            ver_chat_id=row["ver_chat_id"],
            ver_message_ids=_loads_ids(row["ver_message_ids"]),
            ver_grouped_id=row["ver_grouped_id"],
            current_bucket=row["current_bucket"],
            reader_status=row["reader_status"],
        )

    async def occurrence_for_ver(self, chat_id: int, message_ids: Iterable[int]) -> Occurrence | None:
        ids = list(message_ids)
        if not ids:
            return None
        placeholders = ",".join("?" for _ in ids)
        row = await (
            await self._database().execute(
                f"""SELECT o.* FROM occurrence_records o
                    JOIN occurrence_ver_messages v ON v.occurrence_id=o.occurrence_id
                    WHERE v.chat_id=? AND v.message_id IN ({placeholders}) LIMIT 1""",
                (chat_id, *ids),
            )
        ).fetchone()
        return self._occurrence(row) if row else None

    async def get_occurrence(self, occurrence_id: str) -> Occurrence | None:
        row = await (
            await self._database().execute("SELECT * FROM occurrence_records WHERE occurrence_id=?", (occurrence_id,))
        ).fetchone()
        return self._occurrence(row) if row else None

    async def known_hashes(self, hashes: Iterable[str]) -> tuple[set[str], set[str], set[str]]:
        values = list(set(hashes))
        if not values:
            return set(), set(), set()
        placeholders = ",".join("?" for _ in values)
        db = self._database()
        historical = {
            row[0]
            for row in await (
                await db.execute(
                    f"SELECT media_hash FROM media_hash_registry WHERE media_hash IN ({placeholders})",
                    values,
                )
            ).fetchall()
        }
        rows = await (
            await db.execute(
                f"SELECT bucket, media_hash FROM bucket_hashes WHERE media_hash IN ({placeholders})",
                values,
            )
        ).fetchall()
        up = {row["media_hash"] for row in rows if row["bucket"] == Bucket.UP}
        blacklist = {row["media_hash"] for row in rows if row["bucket"] == Bucket.BLACKLIST}
        return historical, up, blacklist

    async def save_classification(
        self,
        occurrence: Occurrence,
        media: Sequence[Media],
        bucket: Bucket,
        target: LogicalMessage | None,
    ) -> str | None:
        """Atomically persist hash history and initial bucket state after Telegram write succeeds."""
        if occurrence.code is None:
            raise ValueError("classified occurrence has no code")
        now = time.time()
        hashes = {item.media_hash for item in media}
        async with self.lock:
            db = await self._begin()
            try:
                await db.executemany(
                    """INSERT OR IGNORE INTO media_hash_registry
                       (media_hash, first_seen_occurrence_id, first_seen_code, first_seen_at)
                       VALUES(?,?,?,?)""",
                    [(value, occurrence.occurrence_id, occurrence.code, now) for value in hashes],
                )
                await db.executemany(
                    """INSERT OR IGNORE INTO occurrence_media
                       (occurrence_id, code, media_hash, origin, origin_chat_id, origin_message_id,
                        media_order, media_type, created_at) VALUES(?,?,?,?,?,?,?,?,?)""",
                    [
                        (
                            occurrence.occurrence_id,
                            occurrence.code,
                            item.media_hash,
                            item.origin,
                            item.chat_id,
                            item.message_id,
                            item.media_order,
                            item.media_type,
                            now,
                        )
                        for item in media
                    ],
                )
                if bucket in (Bucket.UP, Bucket.BLACKLIST):
                    await db.executemany(
                        "INSERT OR IGNORE INTO bucket_hashes(bucket, media_hash, added_at) VALUES(?,?,?)",
                        [(bucket, value, now) for value in hashes],
                    )
                if bucket == Bucket.BLACKLIST:
                    await db.execute(
                        "DELETE FROM occurrence_records WHERE occurrence_id=?",
                        (occurrence.occurrence_id,),
                    )
                    await db.commit()
                    return None

                group_id = str(uuid.uuid4())
                await db.execute(
                    "INSERT INTO bucket_groups VALUES(?, ?, 'active', NULL, ?, ?)",
                    (group_id, bucket, now, now),
                )
                await db.execute(
                    "INSERT INTO group_occurrences VALUES(?,?,?)",
                    (group_id, occurrence.occurrence_id, occurrence.code),
                )
                if target:
                    target_media = [item for item in media if item.origin == Origin.VER]
                    for index, message_id in enumerate(target.message_ids):
                        media_hash = target_media[index].media_hash if index < len(target_media) else None
                        await db.execute(
                            """INSERT INTO group_messages
                               (group_id, channel_role, chat_id, message_id, album_batch_index,
                                media_hash, is_active, created_at) VALUES(?,?,?,?,?,?,1,?)""",
                            (group_id, bucket, target.chat_id, message_id, 0, media_hash, now),
                        )
                current = "up_pending" if bucket == Bucket.UP else bucket
                await db.execute(
                    "UPDATE occurrence_records SET current_bucket=?, updated_at=? WHERE occurrence_id=?",
                    (current, now, occurrence.occurrence_id),
                )
                await db.commit()
                return group_id
            except BaseException:
                await db.rollback()
                raise

    async def mark_merge_dirty(self, bucket: Bucket, hashes: Iterable[str]) -> None:
        if bucket not in (Bucket.REPEAT, Bucket.UP):
            raise ValueError("only repeat and up are mergeable")
        values = set(hashes)
        now = time.time()
        async with self.lock:
            db = await self._begin()
            try:
                row = await (await db.execute("SELECT * FROM merge_dirty WHERE bucket=?", (bucket,))).fetchone()
                if row is None:
                    await db.execute(
                        "INSERT INTO merge_dirty VALUES(?,?,?,?,1,0)",
                        (bucket, json.dumps(sorted(values)), now, now),
                    )
                else:
                    existing = set(json.loads(row["seed_hashes"]))
                    first = row["first_dirty_at"] if row["dirty"] else now
                    await db.execute(
                        """UPDATE merge_dirty SET seed_hashes=?, first_dirty_at=?, last_dirty_at=?, dirty=1
                           WHERE bucket=?""",
                        (json.dumps(sorted(existing | values)), first, now, bucket),
                    )
                await db.commit()
            except BaseException:
                await db.rollback()
                raise

    async def claim_due_merge(self, quiet_period: float, max_wait: float) -> tuple[Bucket, set[str]] | None:
        now = time.time()
        async with self.lock:
            db = await self._begin()
            try:
                row = await (
                    await db.execute(
                        """SELECT * FROM merge_dirty WHERE dirty=1 AND running=0
                           AND (? - last_dirty_at >= ? OR ? - first_dirty_at >= ?)
                           ORDER BY first_dirty_at LIMIT 1""",
                        (now, quiet_period, now, max_wait),
                    )
                ).fetchone()
                if row is None:
                    await db.commit()
                    return None
                await db.execute(
                    "UPDATE merge_dirty SET dirty=0, running=1, seed_hashes='[]' WHERE bucket=?",
                    (row["bucket"],),
                )
                await db.commit()
                return Bucket(row["bucket"]), set(json.loads(row["seed_hashes"]))
            except BaseException:
                await db.rollback()
                raise

    async def finish_merge(self, bucket: Bucket, *, failed_hashes: set[str] | None = None) -> None:
        async with self.lock:
            db = await self._begin()
            try:
                row = await (await db.execute("SELECT * FROM merge_dirty WHERE bucket=?", (bucket,))).fetchone()
                if row and failed_hashes:
                    seeds = set(json.loads(row["seed_hashes"])) | failed_hashes
                    now = time.time()
                    await db.execute(
                        """UPDATE merge_dirty SET running=0, dirty=1, seed_hashes=?,
                           first_dirty_at=?, last_dirty_at=? WHERE bucket=?""",
                        (json.dumps(sorted(seeds)), now, now, bucket),
                    )
                else:
                    await db.execute("UPDATE merge_dirty SET running=0 WHERE bucket=?", (bucket,))
                await db.commit()
            except BaseException:
                await db.rollback()
                raise

    async def repeat_nodes(self) -> dict[str, set[str]]:
        rows = await (
            await self._database().execute(
                """SELECT o.occurrence_id, m.media_hash FROM occurrence_records o
                   JOIN occurrence_media m ON m.occurrence_id=o.occurrence_id
                   WHERE o.current_bucket IN ('oneshot','repeat')"""
            )
        ).fetchall()
        nodes: dict[str, set[str]] = {}
        for row in rows:
            nodes.setdefault(row["occurrence_id"], set()).add(row["media_hash"])
        return nodes

    async def up_nodes(self) -> dict[str, set[str]]:
        rows = await (
            await self._database().execute(
                """SELECT 'group:' || h.group_id node_id, h.media_hash
                   FROM up_group_hashes h JOIN bucket_groups g ON g.group_id=h.group_id
                   WHERE g.status='active'
                   UNION ALL
                   SELECT 'occ:' || o.occurrence_id, m.media_hash
                   FROM occurrence_records o JOIN occurrence_media m USING(occurrence_id)
                   WHERE o.current_bucket='up_pending'"""
            )
        ).fetchall()
        nodes: dict[str, set[str]] = {}
        for row in rows:
            nodes.setdefault(row["node_id"], set()).add(row["media_hash"])
        return nodes

    async def media_for_occurrences(self, occurrence_ids: Iterable[str], origin: Origin | None = None) -> list[Media]:
        ids = list(occurrence_ids)
        if not ids:
            return []
        placeholders = ",".join("?" for _ in ids)
        clause = " AND origin=?" if origin else ""
        params: tuple[Any, ...] = (*ids, origin) if origin else tuple(ids)
        rows = await (
            await self._database().execute(
                f"""SELECT * FROM occurrence_media WHERE occurrence_id IN ({placeholders}){clause}
                    ORDER BY occurrence_id, origin, media_order""",
                params,
            )
        ).fetchall()
        return [
            Media(
                row["media_hash"],
                Origin(row["origin"]),
                row["origin_chat_id"],
                row["origin_message_id"],
                row["media_order"],
                row["media_type"],
            )
            for row in rows
        ]

    async def codes_for_occurrences(self, occurrence_ids: Iterable[str]) -> set[str]:
        ids = list(occurrence_ids)
        if not ids:
            return set()
        placeholders = ",".join("?" for _ in ids)
        rows = await (
            await self._database().execute(
                f"SELECT code FROM occurrence_records WHERE occurrence_id IN ({placeholders})", ids
            )
        ).fetchall()
        return {row["code"] for row in rows if row["code"]}

    async def groups_for_occurrences(self, occurrence_ids: Iterable[str]) -> set[str]:
        ids = list(occurrence_ids)
        if not ids:
            return set()
        placeholders = ",".join("?" for _ in ids)
        rows = await (
            await self._database().execute(
                f"""SELECT DISTINCT go.group_id FROM group_occurrences go
                    JOIN bucket_groups g USING(group_id)
                    WHERE go.occurrence_id IN ({placeholders}) AND g.status='active'""",
                ids,
            )
        ).fetchall()
        return {row["group_id"] for row in rows}

    async def active_group(self, chat_id: int, message_id: int) -> GroupData | None:
        row = await (
            await self._database().execute(
                """SELECT g.group_id, g.bucket FROM group_messages m
                   JOIN bucket_groups g USING(group_id)
                   WHERE m.chat_id=? AND m.message_id=? AND m.is_active=1 AND g.status='active'""",
                (chat_id, message_id),
            )
        ).fetchone()
        return await self.group_data(row["group_id"]) if row else None

    async def active_group_for_occurrence(self, occurrence_id: str) -> GroupData | None:
        row = await (
            await self._database().execute(
                """SELECT g.group_id FROM group_occurrences o
                   JOIN bucket_groups g USING(group_id)
                   WHERE o.occurrence_id=? AND g.status='active'""",
                (occurrence_id,),
            )
        ).fetchone()
        return await self.group_data(row["group_id"]) if row else None

    async def active_group_for_ver_hashes(self, hashes: set[str]) -> GroupData | None:
        if not hashes:
            return None
        placeholders = ",".join("?" for _ in hashes)
        rows = await (
            await self._database().execute(
                f"""SELECT g.group_id FROM bucket_groups g
                    JOIN group_occurrences o USING(group_id)
                    JOIN occurrence_media m USING(occurrence_id)
                    WHERE g.status='active' AND g.bucket IN ('oneshot','repeat')
                      AND m.origin='ver' AND m.media_hash IN ({placeholders})
                    GROUP BY g.group_id HAVING count(DISTINCT m.media_hash)=?""",
                (*hashes, len(hashes)),
            )
        ).fetchall()
        # A forwarded resource can occur in multiple groups. Never choose an arbitrary one.
        return await self.group_data(rows[0]["group_id"]) if len(rows) == 1 else None

    async def group_data(self, group_id: str) -> GroupData:
        db = self._database()
        group = await (await db.execute("SELECT * FROM bucket_groups WHERE group_id=?", (group_id,))).fetchone()
        if group is None:
            raise KeyError(group_id)
        occurrences = await (
            await db.execute("SELECT * FROM group_occurrences WHERE group_id=?", (group_id,))
        ).fetchall()
        hashes = await (
            await db.execute("SELECT media_hash FROM up_group_hashes WHERE group_id=?", (group_id,))
        ).fetchall()
        if not hashes and occurrences:
            occurrence_ids = [row["occurrence_id"] for row in occurrences]
            placeholders = ",".join("?" for _ in occurrence_ids)
            hashes = await (
                await db.execute(
                    f"""SELECT DISTINCT media_hash FROM occurrence_media
                        WHERE occurrence_id IN ({placeholders})""",
                    occurrence_ids,
                )
            ).fetchall()
        codes = await (await db.execute("SELECT code FROM up_group_codes WHERE group_id=?", (group_id,))).fetchall()
        occurrence_codes = {row["code"] for row in occurrences}
        return GroupData(
            group_id,
            group["bucket"],
            tuple(row["occurrence_id"] for row in occurrences),
            tuple(sorted({row["code"] for row in codes} | occurrence_codes)),
            frozenset(row["media_hash"] for row in hashes),
        )

    async def up_group_media(self, group_ids: Iterable[str]) -> list[Media]:
        ids = list(group_ids)
        if not ids:
            return []
        placeholders = ",".join("?" for _ in ids)
        rows = await (
            await self._database().execute(
                f"""SELECT media_hash,chat_id,message_id,id FROM group_messages
                    WHERE group_id IN ({placeholders}) AND is_active=1 AND media_hash IS NOT NULL
                    ORDER BY id""",
                ids,
            )
        ).fetchall()
        return [
            Media(
                row["media_hash"],
                Origin.INFO,
                row["chat_id"],
                row["message_id"],
                index,
                "media",
            )
            for index, row in enumerate(rows)
        ]

    async def hidden_hashes(self, hashes: Iterable[str]) -> set[str]:
        values = list(hashes)
        if not values:
            return set()
        placeholders = ",".join("?" for _ in values)
        rows = await (
            await self._database().execute(
                f"SELECT media_hash FROM up_hidden_hashes WHERE media_hash IN ({placeholders})", values
            )
        ).fetchall()
        return {row["media_hash"] for row in rows}

    async def replace_group(
        self,
        bucket: Bucket,
        old_group_ids: set[str],
        occurrence_ids: set[str],
        codes: set[str],
        hashes: set[str],
        sent: Sequence[tuple[LogicalMessage, Sequence[Media]]],
    ) -> str:
        group_id = str(uuid.uuid4())
        now = time.time()
        async with self.lock:
            db = await self._begin()
            try:
                await db.execute(
                    "INSERT INTO bucket_groups VALUES(?, ?, 'active', NULL, ?, ?)",
                    (group_id, bucket, now, now),
                )
                if bucket == Bucket.UP:
                    await db.executemany("INSERT INTO up_group_codes VALUES(?,?)", [(group_id, code) for code in codes])
                    await db.executemany(
                        "INSERT INTO up_group_hashes VALUES(?,?)", [(group_id, value) for value in hashes]
                    )
                else:
                    await db.executemany(
                        "INSERT INTO group_occurrences VALUES(?,?,?)",
                        [
                            (group_id, occurrence_id, code)
                            for occurrence_id in occurrence_ids
                            for code in [next(iter(await self.codes_for_occurrences([occurrence_id])))]
                        ],
                    )
                for batch_index, (logical, media) in enumerate(sent):
                    for index, message_id in enumerate(logical.message_ids):
                        media_hash = media[index].media_hash if index < len(media) else None
                        await db.execute(
                            """INSERT INTO group_messages
                               (group_id,channel_role,chat_id,message_id,album_batch_index,
                                media_hash,is_active,created_at) VALUES(?,?,?,?,?,?,1,?)""",
                            (group_id, bucket, logical.chat_id, message_id, batch_index, media_hash, now),
                        )
                if old_group_ids:
                    placeholders = ",".join("?" for _ in old_group_ids)
                    await db.execute(
                        f"""UPDATE bucket_groups SET status='superseded',superseded_by=?,updated_at=?
                            WHERE group_id IN ({placeholders})""",
                        (group_id, now, *old_group_ids),
                    )
                    await db.execute(
                        f"UPDATE group_messages SET is_active=0 WHERE group_id IN ({placeholders})",
                        tuple(old_group_ids),
                    )
                if occurrence_ids:
                    placeholders = ",".join("?" for _ in occurrence_ids)
                    if bucket == Bucket.UP:
                        await db.execute(
                            f"DELETE FROM occurrence_records WHERE occurrence_id IN ({placeholders})",
                            tuple(occurrence_ids),
                        )
                    else:
                        await db.execute(
                            f"""UPDATE occurrence_records SET current_bucket='repeat',updated_at=?
                                WHERE occurrence_id IN ({placeholders})""",
                            (now, *occurrence_ids),
                        )
                await db.commit()
                return group_id
            except BaseException:
                await db.rollback()
                raise

    async def add_expected_deletions(self, messages: Iterable[tuple[int, int]], ttl: float = 3600) -> None:
        expires = time.time() + ttl
        await self._database().executemany(
            "INSERT OR REPLACE INTO expected_deletions VALUES(?,?,?)",
            [(chat_id, message_id, expires) for chat_id, message_id in messages],
        )

    async def consume_expected_deletion(self, chat_id: int, message_id: int) -> bool:
        async with self.lock:
            db = await self._begin()
            try:
                await db.execute("DELETE FROM expected_deletions WHERE expires_at < ?", (time.time(),))
                cursor = await db.execute(
                    "DELETE FROM expected_deletions WHERE chat_id=? AND message_id=?",
                    (chat_id, message_id),
                )
                await db.commit()
                return cursor.rowcount == 1
            except BaseException:
                await db.rollback()
                raise

    async def hide_up_message(self, chat_id: int, message_id: int) -> str | None:
        row = await (
            await self._database().execute(
                """SELECT m.group_id,m.media_hash FROM group_messages m JOIN bucket_groups g USING(group_id)
                   WHERE m.chat_id=? AND m.message_id=? AND m.is_active=1
                     AND m.channel_role='up' AND g.status='active'""",
                (chat_id, message_id),
            )
        ).fetchone()
        if row is None or row["media_hash"] is None:
            return None
        await self._database().execute(
            "INSERT OR REPLACE INTO up_hidden_hashes VALUES(?,?,?,?)",
            (row["media_hash"], time.time(), row["group_id"], message_id),
        )
        return row["media_hash"]

    async def messages_for_groups(self, group_ids: Iterable[str]) -> list[tuple[int, int]]:
        ids = list(group_ids)
        if not ids:
            return []
        placeholders = ",".join("?" for _ in ids)
        rows = await (
            await self._database().execute(
                f"SELECT chat_id,message_id FROM group_messages WHERE group_id IN ({placeholders}) AND is_active=1",
                ids,
            )
        ).fetchall()
        return [(row["chat_id"], row["message_id"]) for row in rows]

    async def append_group_messages(
        self, group_id: str, channel_role: Bucket, logical_messages: Sequence[LogicalMessage]
    ) -> None:
        now = time.time()
        rows = [
            (group_id, channel_role, logical.chat_id, message_id, batch, None, now)
            for batch, logical in enumerate(logical_messages)
            for message_id in logical.message_ids
        ]
        await self._database().executemany(
            """INSERT OR IGNORE INTO group_messages
               (group_id,channel_role,chat_id,message_id,album_batch_index,
                media_hash,is_active,created_at) VALUES(?,?,?,?,?,?,1,?)""",
            rows,
        )

    async def finish_manual_repeat(self, group: GroupData, trigger: LogicalMessage) -> None:
        now = time.time()
        async with self.lock:
            db = await self._begin()
            try:
                old_media = await (
                    await db.execute(
                        """SELECT media_hash FROM group_messages
                           WHERE group_id=? AND is_active=1 ORDER BY id""",
                        (group.group_id,),
                    )
                ).fetchall()
                await db.execute(
                    "UPDATE bucket_groups SET bucket='repeat',updated_at=? WHERE group_id=?",
                    (now, group.group_id),
                )
                await db.execute("UPDATE group_messages SET is_active=0 WHERE group_id=?", (group.group_id,))
                await db.executemany(
                    """INSERT OR IGNORE INTO group_messages
                       (group_id,channel_role,chat_id,message_id,album_batch_index,
                        media_hash,is_active,created_at) VALUES(?,?,?,?,?,?,1,?)""",
                    [
                        (
                            group.group_id,
                            Bucket.REPEAT,
                            trigger.chat_id,
                            message_id,
                            0,
                            old_media[index]["media_hash"] if index < len(old_media) else None,
                            now,
                        )
                        for index, message_id in enumerate(trigger.message_ids)
                    ],
                )
                if group.occurrence_ids:
                    placeholders = ",".join("?" for _ in group.occurrence_ids)
                    await db.execute(
                        f"""UPDATE occurrence_records SET current_bucket='repeat',updated_at=?
                            WHERE occurrence_id IN ({placeholders})""",
                        (now, *group.occurrence_ids),
                    )
                await db.commit()
            except BaseException:
                await db.rollback()
                raise

    async def promote_manual_up(self, group: GroupData) -> None:
        now = time.time()
        async with self.lock:
            db = await self._begin()
            try:
                await db.executemany(
                    "INSERT OR IGNORE INTO bucket_hashes(bucket,media_hash,added_at) VALUES('up',?,?)",
                    [(value, now) for value in group.hashes],
                )
                if group.occurrence_ids:
                    placeholders = ",".join("?" for _ in group.occurrence_ids)
                    await db.execute(
                        f"""UPDATE occurrence_records SET current_bucket='up_pending',updated_at=?
                            WHERE occurrence_id IN ({placeholders})""",
                        (now, *group.occurrence_ids),
                    )
                await db.commit()
            except BaseException:
                await db.rollback()
                raise

    async def finish_manual_blacklist(self, group: GroupData) -> None:
        now = time.time()
        async with self.lock:
            db = await self._begin()
            try:
                await db.execute(
                    """UPDATE bucket_groups SET status='superseded',updated_at=? WHERE group_id=?""",
                    (now, group.group_id),
                )
                await db.execute("UPDATE group_messages SET is_active=0 WHERE group_id=?", (group.group_id,))
                if group.occurrence_ids:
                    placeholders = ",".join("?" for _ in group.occurrence_ids)
                    await db.execute(
                        f"DELETE FROM occurrence_records WHERE occurrence_id IN ({placeholders})",
                        group.occurrence_ids,
                    )
                await db.commit()
            except BaseException:
                await db.rollback()
                raise

    async def prepare_manual_blacklist(self, hashes: Iterable[str]) -> None:
        now = time.time()
        await self._database().executemany(
            """INSERT OR IGNORE INTO bucket_hashes(bucket,media_hash,added_at)
               VALUES('blacklist',?,?)""",
            [(value, now) for value in hashes],
        )
