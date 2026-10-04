import time

from conftest import logical

from t4by.models import Bucket


async def test_delayed_job_and_restart_recovery(store) -> None:
    await store.enqueue_job("classify", "one", {"x": 1}, not_before=time.time() + 60)
    assert await store.claim_job(["classify"]) is None
    await store.enqueue_job("reader", "two", {"x": 2})
    job = await store.claim_job(["reader"])
    assert job is not None
    await store.recover_running_jobs()
    recovered = await store.claim_job(["reader"])
    assert recovered is not None
    assert recovered.job_id == job.job_id


async def test_merge_dirty_coalesces_and_respects_quiet_period(store) -> None:
    await store.mark_merge_dirty(Bucket.REPEAT, {"A"})
    await store.mark_merge_dirty(Bucket.REPEAT, {"B"})
    assert await store.claim_due_merge(quiet_period=10, max_wait=20) is None
    due = await store.claim_due_merge(quiet_period=0, max_wait=20)
    assert due == (Bucket.REPEAT, {"A", "B"})


async def test_expected_program_deletion_is_consumed_once(store) -> None:
    await store.add_expected_deletions([(7, 5002)])
    assert await store.consume_expected_deletion(7, 5002)
    assert not await store.consume_expected_deletion(7, 5002)


async def test_occurrences_with_same_code_remain_distinct(store) -> None:
    first = await store.create_occurrence(logical(2, 1, text="123456"), "123456")
    second = await store.create_occurrence(logical(2, 2, text="123456"), "123456")
    assert first != second


async def test_retried_info_reuses_occurrence_identity(store) -> None:
    info = logical(2, 10, 11, text="123456", grouped_id=90)
    first = await store.create_occurrence(info, "123456")
    second = await store.create_occurrence(info, "123456")
    assert first == second


async def test_operational_metrics_reports_merge_depth_from_dirty_buckets(store) -> None:
    await store.mark_merge_dirty(Bucket.REPEAT, {"A"})

    dirty, ages = await store.operational_metrics()

    assert dirty == 1
    assert ages == {}
