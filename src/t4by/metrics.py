from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

QUEUE_DEPTH = Gauge("t4by_queue_depth", "Queued and running jobs", ["queue"])
MERGE_DIRTY = Gauge("t4by_merge_dirty_count", "Dirty merge buckets")
ACTIVE_DOWNLOADS = Gauge("t4by_active_downloads", "Active media downloads", ["size"])
RPC_RATE = Gauge("t4by_rpc_rate_current", "Current adaptive RPC rate", ["gate"])
FLOOD_WAITS = Counter("t4by_flood_wait_total", "Telegram flood waits", ["method"])
FLOOD_SECONDS = Counter("t4by_flood_wait_seconds_total", "Telegram requested wait seconds", ["method"])
MERGE_DURATION = Histogram("t4by_merge_duration_seconds", "Merge execution duration", ["bucket"])
JOB_AGE = Gauge("t4by_oldest_job_age_seconds", "Age of oldest outstanding job", ["queue"])
