import pytest
import asyncio
import time
import statistics
from utils import TaskScheduler

async def _wait_until_complete(scheduler, ids, timeout: float, poll: float = 0.05):
    start = time.perf_counter()
    while True:
        completed = 0
        for task_id in ids:
            status = await scheduler.get_task_status(task_id)
            if status and status["status"] == "completed":
                completed += 1
        if completed == len(ids):
            return True, time.perf_counter() - start
        if time.perf_counter() - start > timeout:
            return False, time.perf_counter() - start
        await asyncio.sleep(poll)

class TestScalability:
    @pytest.mark.asyncio
    async def test_high_task_volume(self):
        scheduler = TaskScheduler(max_workers=4, queue_size=50)
        await scheduler.start()
        try:
            task_count = 100
            batch_size = 20
            all_ids = []

            submit_start = time.perf_counter()
            for batch_start in range(0, task_count, batch_size):
                for i in range(batch_start, min(batch_start + batch_size, task_count)):
                    task_data = {
                        "id": f"volume-task-{i}",
                        "name": "compute",
                        "payload": {"iterations": 1000 + i * 100},
                        "priority": "normal",
                        "max_retries": 1,
                        "timeout": 30,
                        "created_at": time.time(),
                        "status": "pending",
                    }
                    if await scheduler.submit_task(task_data):
                        all_ids.append(task_data["id"])
                await asyncio.sleep(0.05)

            ok, elapsed = await _wait_until_complete(
                scheduler, all_ids, timeout=20.0, poll=0.05
            )

            completed = 0
            failed = 0
            for task_id in all_ids:
                status = await scheduler.get_task_status(task_id)
                if status:
                    if status["status"] == "completed":
                        completed += 1
                    elif status["status"] == "failed":
                        failed += 1

            completion_rate = completed / len(all_ids) if all_ids else 0.0
            assert completion_rate >= 0.8, f"Low completion rate: {completion_rate:.2%}"

            throughput = completed / max(elapsed, 1e-6)
            assert throughput > 5.0, f"Low throughput: {throughput:.2f} tasks/sec"

            print(
                f"High volume: {completed}/{len(all_ids)} completed in {elapsed:.2f}s "
                f"({throughput:.2f} tasks/sec)"
            )
        finally:
            await scheduler.shutdown()

    @pytest.mark.asyncio
    async def test_worker_scaling_efficiency(self):
        test_results = {}
        worker_counts = [1, 2, 4, 8]
        task_count = 20

        # Heavier compute so CPU dominates IPC/dispatch overhead
        BASE_ITER = 200_000

        for workers in worker_counts:
            scheduler = TaskScheduler(max_workers=workers, queue_size=30)
            await scheduler.start()
            try:
                # --- warm up the process pool BEFORE timing ---
                warm_ids = []
                for i in range(min(4, task_count)):
                    warm_task = {
                        "id": f"warm-{workers}-{i}",
                        "name": "compute",
                        "payload": {"iterations": 10_000},
                        "priority": "normal",
                        "max_retries": 1,
                        "timeout": 30,
                        "created_at": time.time(),
                        "status": "pending",
                    }
                    await scheduler.submit_task(warm_task)
                    warm_ids.append(warm_task["id"])
                # wait for warmups to finish (short deadline)
                await _wait_until_complete(scheduler, warm_ids, timeout=8.0, poll=0.02)

                # --- measured run ---
                ids = []
                for i in range(task_count):
                    task_data = {
                        "id": f"scaling-{workers}-{i}",
                        "name": "compute",
                        "payload": {"iterations": BASE_ITER},
                        "priority": "normal",
                        "max_retries": 1,
                        "timeout": 60,
                        "created_at": time.time(),
                        "status": "pending",
                    }
                    if await scheduler.submit_task(task_data):
                        ids.append(task_data["id"])

                # allow enough time for heavier CPU tasks
                timeout = 40.0 if workers >= 4 else 50.0
                ok, elapsed = await _wait_until_complete(scheduler, ids, timeout=timeout, poll=0.02)

                final_completed = 0
                for task_id in ids:
                    status = await scheduler.get_task_status(task_id)
                    if status and status["status"] == "completed":
                        final_completed += 1

                test_results[workers] = {
                    "time": elapsed,
                    "completed": final_completed,
                    "throughput": final_completed / elapsed if elapsed > 0 else 0.0,
                }

                print(
                    f"Workers: {workers}, Time: {elapsed:.2f}s, "
                    f"Completed: {final_completed}/{len(ids)}"
                )
            finally:
                await scheduler.shutdown()

        # sanity: collected results
        assert len(test_results) >= 3

        # Compare 1 worker vs 4 workers after warmup with heavier load
        single_worker_time = test_results[1]["time"]
        multi_worker_time = test_results[4]["time"]
        speedup = single_worker_time / multi_worker_time if multi_worker_time > 0 else 0.0

        # With heavier compute, 4 workers should be much faster.
        assert speedup >= 1.5, f"4 workers not much faster than 1: {speedup:.2f}x speedup"
        
    @pytest.mark.asyncio
    async def test_queue_size_impact(self):
        test_results = {}
        queue_sizes = [5, 20, 50]

        for queue_size in queue_sizes:
            scheduler = TaskScheduler(max_workers=3, queue_size=queue_size)
            await scheduler.start()
            try:
                tasks_to_submit = queue_size + 10
                accepted_ids = []
                rejected_count = 0

                for i in range(tasks_to_submit):
                    task_data = {
                        "id": f"queue-{queue_size}-{i}",
                        "name": "compute",
                        "payload": {"iterations": 2000},
                        "priority": "normal",
                        "max_retries": 1,
                        "timeout": 30,
                        "created_at": time.time(),
                        "status": "pending",
                    }
                    if await scheduler.submit_task(task_data):
                        accepted_ids.append(task_data["id"])
                    else:
                        rejected_count += 1

                await _wait_until_complete(
                    scheduler, accepted_ids, timeout=12.0, poll=0.05
                )

                completed = 0
                for task_id in accepted_ids:
                    status = await scheduler.get_task_status(task_id)
                    if status and status["status"] == "completed":
                        completed += 1

                test_results[queue_size] = {
                    "accepted": len(accepted_ids),
                    "rejected": rejected_count,
                    "completed": completed,
                }
            finally:
                await scheduler.shutdown()

        for queue_size, results in test_results.items():
            max_expected = queue_size + 3
            assert results["accepted"] <= max_expected, \
                f"Queue {queue_size} accepted too many: {results['accepted']}"

        if 5 in test_results and 20 in test_results:
            assert test_results[20]["accepted"] > test_results[5]["accepted"]
        if 20 in test_results and 50 in test_results:
            assert test_results[50]["accepted"] >= test_results[20]["accepted"]

    @pytest.mark.asyncio
    async def test_mixed_workload_performance(self):
        scheduler = TaskScheduler(max_workers=4, queue_size=30)
        await scheduler.start()
        try:
            task_types = [
                ("compute", {"iterations": 5000}),          # CPU-bound → processes
                ("io_operation", {"duration": 0.5}),        # I/O-bound → threads
                ("data_processing", {"data": list(range(100))}),
            ]

            all_pairs = []
            for i in range(60):  # 20 of each type
                tname, payload = task_types[i % 3]
                task_data = {
                    "id": f"mixed-{tname}-{i}",
                    "name": tname,
                    "payload": payload,
                    "priority": "normal",
                    "max_retries": 1,
                    "timeout": 30,
                    "created_at": time.time(),
                    "status": "pending",
                }
                if await scheduler.submit_task(task_data):
                    all_pairs.append((tname, task_data["id"]))

            ids = [tid for _, tid in all_pairs]
            # Give more headroom for process pool scheduling
            ok, elapsed = await _wait_until_complete(scheduler, ids, timeout=30.0, poll=0.05)

            # Count completions by type
            per_type_total = len(all_pairs) // 3  # 20 each
            counts = {"compute": 0, "io_operation": 0, "data_processing": 0}
            for tname, tid in all_pairs:
                status = await scheduler.get_task_status(tid)
                if status and status["status"] == "completed":
                    counts[tname] += 1

            total_completed = sum(counts.values())
            completion_rate = total_completed / len(all_pairs) if all_pairs else 0.0
            assert completion_rate >= 0.8, \
                f"Low completion rate for mixed workload: {completion_rate:.2%}"

            # Per-type proportional floor (60% of submitted per type)
            min_per_type = int(0.60 * per_type_total)  # 12 for 20 submitted
            for tname, cnt in counts.items():
                assert cnt >= min_per_type, \
                    f"Low completion for {tname}: {cnt}/{per_type_total} (min {min_per_type})"

            throughput = total_completed / max(elapsed, 1e-6)
            assert throughput > 8.0, \
                f"Low mixed workload throughput: {throughput:.2f} tasks/sec"

        finally:
            await scheduler.shutdown()

    @pytest.mark.asyncio
    async def test_burst_load_handling(self):
        scheduler = TaskScheduler(max_workers=3, queue_size=25)
        await scheduler.start()
        try:
            for i in range(5):
                task_data = {
                    "id": f"normal-{i}",
                    "name": "compute",
                    "payload": {"iterations": 1000},
                    "priority": "normal",
                    "max_retries": 1,
                    "timeout": 30,
                    "created_at": time.time(),
                    "status": "pending",
                }
                await scheduler.submit_task(task_data)

            await asyncio.sleep(0.5)

            burst_ids = []
            burst_start = time.perf_counter()
            for i in range(20):
                task_data = {
                    "id": f"burst-{i}",
                    "name": "compute",
                    "payload": {"iterations": 2000},
                    "priority": "normal",
                    "max_retries": 1,
                    "timeout": 30,
                    "created_at": time.time(),
                    "status": "pending",
                }
                if await scheduler.submit_task(task_data):
                    burst_ids.append(task_data["id"])
            burst_submit_time = time.perf_counter() - burst_start

            assert len(burst_ids) > 15, f"Should accept most burst tasks: {len(burst_ids)}/20"
            assert burst_submit_time < 2.0, f"Burst submission too slow: {burst_submit_time:.2f}s"

            ok, _ = await _wait_until_complete(
                scheduler, burst_ids, timeout=12.0, poll=0.05
            )

            burst_completed = 0
            for tid in burst_ids:
                status = await scheduler.get_task_status(tid)
                if status and status["status"] == "completed":
                    burst_completed += 1

            rate = burst_completed / len(burst_ids) if burst_ids else 0.0
            assert rate >= 0.7, f"Low burst completion rate: {rate:.2%}"

            health = await scheduler.health_check()
            assert health
        finally:
            await scheduler.shutdown()

    @pytest.mark.asyncio
    async def test_sustained_load_stability(self):
        scheduler = TaskScheduler(max_workers=3, queue_size=20)
        await scheduler.start()
        try:
            total_submitted = 0
            total_completed = 0

            for round_num in range(5):
                round_ids = []
                for i in range(10):
                    task_data = {
                        "id": f"sustained-{round_num}-{i}",
                        "name": "compute",
                        "payload": {"iterations": 1500},
                        "priority": "normal",
                        "max_retries": 1,
                        "timeout": 30,
                        "created_at": time.time(),
                        "status": "pending",
                    }
                    if await scheduler.submit_task(task_data):
                        round_ids.append(task_data["id"])
                        total_submitted += 1

                await _wait_until_complete(
                    scheduler, round_ids, timeout=10.0, poll=0.05
                )

                round_completed = 0
                for tid in round_ids:
                    status = await scheduler.get_task_status(tid)
                    if status and status["status"] == "completed":
                        round_completed += 1
                        total_completed += 1

                print(f"Round {round_num + 1}: {round_completed}/{len(round_ids)} completed")
                round_rate = round_completed / len(round_ids) if round_ids else 0.0
                assert round_rate >= 0.7, f"Round {round_num + 1} rate too low: {round_rate:.2%}"

            overall_rate = total_completed / total_submitted if total_submitted else 0.0
            assert overall_rate >= 0.8, f"Overall completion rate too low: {overall_rate:.2%}"

            health = await scheduler.health_check()
            assert health

            final_stats = await scheduler.get_worker_stats()
            assert final_stats["completed_tasks"] >= total_completed
        finally:
            await scheduler.shutdown()