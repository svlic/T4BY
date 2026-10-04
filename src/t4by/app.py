from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
import time
from dataclasses import replace
from typing import Any

from prometheus_client import start_http_server
from telethon import TelegramClient, events

from .config import Chats, Settings
from .gateway import RetryAfter, RpcGates, TelethonGateway, message_from_telethon
from .manual import ManualService
from .merger import MergeEngine
from .metrics import JOB_AGE, MERGE_DIRTY, MERGE_DURATION, QUEUE_DEPTH, RPC_RATE
from .models import Bucket, LogicalMessage
from .reader import ReaderService
from .storage import Job, Store
from .writer import WriterService

log = logging.getLogger(__name__)


def _logical(raw_messages: list[Any]) -> LogicalMessage:
    messages = tuple(message_from_telethon(item) for item in raw_messages)
    return LogicalMessage(messages[0].chat_id, messages)


async def _resolve_chats(reader: TelegramClient, writer: TelegramClient, chats: Chats) -> Chats:
    return Chats(
        source=await reader.get_peer_id(chats.source),
        info=await writer.get_peer_id(chats.info),
        ver=await writer.get_peer_id(chats.ver),
        man=await writer.get_peer_id(chats.man),
        oneshot=await writer.get_peer_id(chats.oneshot),
        repeat=await writer.get_peer_id(chats.repeat),
        up=await writer.get_peer_id(chats.up),
        blacklist=await writer.get_peer_id(chats.blacklist),
    )


class Application:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.store = Store(settings.database)
        self.reader_client = TelegramClient(
            settings.reader_session,
            settings.reader_api_id,
            settings.reader_api_hash,
            flood_sleep_threshold=0,
        )
        self.writer_client = TelegramClient(
            settings.writer_session,
            settings.writer_api_id,
            settings.writer_api_hash,
            flood_sleep_threshold=0,
        )
        self.stop = asyncio.Event()
        self.tasks: list[asyncio.Task[None]] = []

    async def start(self) -> None:
        log.info("T4BY starting database=%s", self.settings.database)
        await self.store.open()
        await self.reader_client.start()
        await self.writer_client.start()
        chats = await _resolve_chats(self.reader_client, self.writer_client, self.settings.chats)
        self.settings = replace(self.settings, chats=chats)
        log.info(
            "Telegram chats resolved source=%s info=%s ver=%s man=%s oneshot=%s repeat=%s up=%s blacklist=%s",
            chats.source,
            chats.info,
            chats.ver,
            chats.man,
            chats.oneshot,
            chats.repeat,
            chats.up,
            chats.blacklist,
        )
        reader_gateway = TelethonGateway(self.reader_client, RpcGates())
        writer_gateway = TelethonGateway(self.writer_client, RpcGates())
        self.reader = ReaderService(self.store, reader_gateway, chats)
        self.writer = WriterService(self.store, writer_gateway, chats)
        self.manual = ManualService(self.store, writer_gateway, chats)
        self.merger = MergeEngine(self.store, writer_gateway, chats)
        self._register_handlers(chats)
        self.tasks.extend(
            asyncio.create_task(self._job_worker(("reader",)), name=f"reader-{index}")
            for index in range(self.settings.reader_workers)
        )
        self.tasks.extend(
            asyncio.create_task(self._job_worker(("classify",)), name=f"classify-{index}")
            for index in range(self.settings.classification_workers)
        )
        self.tasks.extend(
            asyncio.create_task(self._job_worker(("manual", "man")), name=f"manual-{index}")
            for index in range(self.settings.manual_workers)
        )
        self.tasks.append(asyncio.create_task(self._merge_scheduler(), name="merge-scheduler"))
        self.tasks.append(asyncio.create_task(self._observe(), name="metrics-observer"))
        log.info(
            "T4BY started reader_workers=%d classification_workers=%d manual_workers=%d",
            self.settings.reader_workers,
            self.settings.classification_workers,
            self.settings.manual_workers,
        )

    def _register_handlers(self, chats: Chats) -> None:
        @self.reader_client.on(events.Album(chats=chats.info))
        async def info_album(event: events.Album.Event) -> None:
            await self.reader.enqueue(_logical(event.messages))

        @self.reader_client.on(events.NewMessage(chats=chats.info))
        async def info_single(event: events.NewMessage.Event) -> None:
            if event.message.grouped_id is None:
                await self.reader.enqueue(_logical([event.message]))

        @self.writer_client.on(events.Album(chats=chats.ver))
        async def ver_album(event: events.Album.Event) -> None:
            await self.writer.enqueue_ver(_logical(event.messages))

        @self.writer_client.on(events.NewMessage(chats=chats.ver))
        async def ver_single(event: events.NewMessage.Event) -> None:
            if event.message.grouped_id is None:
                await self.writer.enqueue_ver(_logical([event.message]))

        async def manual_event(raw_messages: list[Any], target: Bucket) -> None:
            logical = _logical(raw_messages)
            source_chats = {
                message.forward_chat_id for message in logical.messages if message.forward_chat_id is not None
            }
            if source_chats.intersection({chats.info, chats.ver}):
                log.info(
                    "manual event ignored reason=internal_forward target=%s trigger=%s/%s source_chats=%s",
                    target,
                    logical.chat_id,
                    logical.message_ids,
                    sorted(source_chats),
                )
                return
            await self.manual.enqueue(logical, target)

        @self.writer_client.on(events.Album(chats=[chats.up, chats.blacklist]))
        async def manual_album(event: events.Album.Event) -> None:
            target = Bucket.UP if event.chat_id == chats.up else Bucket.BLACKLIST
            await manual_event(event.messages, target)

        @self.writer_client.on(events.NewMessage(chats=[chats.up, chats.blacklist]))
        async def manual_single(event: events.NewMessage.Event) -> None:
            if event.message.grouped_id is None and event.message.fwd_from:
                target = Bucket.UP if event.chat_id == chats.up else Bucket.BLACKLIST
                await manual_event([event.message], target)

        @self.writer_client.on(events.MessageDeleted(chats=chats.up))
        async def up_deleted(event: events.MessageDeleted.Event) -> None:
            if event.chat_id is None:
                return
            for message_id in event.deleted_ids:
                if await self.store.consume_expected_deletion(event.chat_id, message_id):
                    log.info(
                        "up deletion acknowledged chat_id=%s message_id=%s expected=true", event.chat_id, message_id
                    )
                    continue
                media_hash = await self.store.hide_up_message(event.chat_id, message_id)
                if media_hash:
                    await self.store.mark_merge_dirty(Bucket.UP, {media_hash})
                    log.info(
                        "up message hidden chat_id=%s message_id=%s merge_dirty=true",
                        event.chat_id,
                        message_id,
                    )
                else:
                    log.warning("up deletion not mapped chat_id=%s message_id=%s", event.chat_id, message_id)

    async def _job_worker(self, kinds: tuple[str, ...]) -> None:
        while not self.stop.is_set():
            job = await self.store.claim_job(kinds)
            if job is None:
                await asyncio.sleep(0.2)
                continue
            started = time.monotonic()
            log.info("job started job_id=%s kind=%s", job.job_id, job.kind)
            try:
                await self._run_job(job)
                await self.store.finish_job(job.job_id)
                log.info(
                    "job completed job_id=%s kind=%s duration_seconds=%.3f",
                    job.job_id,
                    job.kind,
                    time.monotonic() - started,
                )
            except RetryAfter as error:
                await self.store.retry_job(job.job_id, time.time() + error.seconds, str(error))
                log.warning(
                    "job retry scheduled job_id=%s kind=%s method=%s delay_seconds=%.1f",
                    job.job_id,
                    job.kind,
                    error.method,
                    error.seconds,
                )
            except Exception as error:
                log.exception("job failed job_id=%s kind=%s", job.job_id, job.kind)
                await self.store.fail_job(job.job_id, repr(error))

    async def _run_job(self, job: Job) -> None:
        if job.kind == "reader":
            await self.reader.process(**job.payload)
        elif job.kind == "classify":
            await self.writer.process(**job.payload)
        elif job.kind == "manual":
            payload = dict(job.payload)
            payload["target"] = Bucket(payload["target"])
            await self.manual.process(**payload)
        elif job.kind == "man":
            log.info("man report started job_id=%s references=%d", job.job_id, len(job.payload["references"]))
            for reference in job.payload["references"]:
                logical = await self.writer.gateway.get_logical(reference["chat_id"], reference["message_ids"])
                await self.writer.gateway.forward(logical, self.settings.chats.man)
        else:
            raise ValueError(f"unknown job kind: {job.kind}")

    async def _merge_scheduler(self) -> None:
        while not self.stop.is_set():
            due = await self.store.claim_due_merge(self.settings.quiet_period, self.settings.merge_max_wait)
            if due is None:
                await asyncio.sleep(0.25)
                continue
            bucket, hashes = due
            started = time.monotonic()
            log.info("merge job started bucket=%s seed_hashes=%d", bucket, len(hashes))
            try:
                await self.merger.merge(bucket, hashes)
                await self.store.finish_merge(bucket)
                log.info(
                    "merge job completed bucket=%s seed_hashes=%d duration_seconds=%.3f",
                    bucket,
                    len(hashes),
                    time.monotonic() - started,
                )
            except RetryAfter as error:
                await self.store.finish_merge(bucket, failed_hashes=hashes)
                log.warning(
                    "merge retry scheduled bucket=%s seed_hashes=%d method=%s delay_seconds=%.1f",
                    bucket,
                    len(hashes),
                    error.method,
                    error.seconds,
                )
            except Exception:
                log.exception("merge failed bucket=%s seed_hashes=%d", bucket, len(hashes))
                await self.store.finish_merge(bucket)
            finally:
                MERGE_DURATION.labels(bucket=bucket).observe(time.monotonic() - started)

    async def _observe(self) -> None:
        while not self.stop.is_set():
            QUEUE_DEPTH.labels(queue="reader").set(await self.store.queue_depth(["reader"]))
            QUEUE_DEPTH.labels(queue="classification").set(await self.store.queue_depth(["classify"]))
            QUEUE_DEPTH.labels(queue="manual").set(await self.store.queue_depth(["manual", "man"]))
            dirty, ages = await self.store.operational_metrics()
            MERGE_DIRTY.set(dirty)
            QUEUE_DEPTH.labels(queue="merge").set(dirty)
            for queue, kind in (
                ("reader", "reader"),
                ("classification", "classify"),
            ):
                JOB_AGE.labels(queue=queue).set(ages.get(kind, 0))
            JOB_AGE.labels(queue="manual").set(max(ages.get("manual", 0), ages.get("man", 0)))
            RPC_RATE.labels(gate="reader_control").set(
                self.reader.gateway.gates.control.rate  # type: ignore[attr-defined]
            )
            RPC_RATE.labels(gate="reader_write").set(
                self.reader.gateway.gates.write.rate  # type: ignore[attr-defined]
            )
            RPC_RATE.labels(gate="writer_control").set(
                self.writer.gateway.gates.control.rate  # type: ignore[attr-defined]
            )
            RPC_RATE.labels(gate="writer_write").set(
                self.writer.gateway.gates.write.rate  # type: ignore[attr-defined]
            )
            await asyncio.sleep(5)

    async def run(self) -> None:
        await self.start()
        await self.stop.wait()

    async def close(self) -> None:
        log.info("T4BY stopping active_tasks=%d", len(self.tasks))
        self.stop.set()
        for task in self.tasks:
            task.cancel()
        for task in self.tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        await asyncio.gather(self.reader_client.disconnect(), self.writer_client.disconnect())
        await self.store.close()
        log.info("T4BY stopped")


async def async_main() -> None:
    settings = Settings.from_env()
    logging.basicConfig(
        level=getattr(logging, settings.log_level),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    start_http_server(settings.metrics_port, addr=settings.metrics_host)
    app = Application(settings)
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(signum, app.stop.set)
    try:
        await app.run()
    finally:
        await app.close()


def main() -> None:
    asyncio.run(async_main())
