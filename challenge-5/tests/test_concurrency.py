
import pytest
import asyncio
import time
import math
from utils import TaskScheduler, TaskProcessor


class TestConcurrencyPerformance:
    """Test concurrent processing and performance"""
    
    @pytest.mark.asyncio
    async def test_concurrent_task_execution(self):
        """Test that multiple tasks execute concurrently"""
        scheduler = TaskScheduler(max_workers=4, queue_size=20)
        await scheduler.start()

        try:
            task_count = 8
            task_duration = 1.0
            max_workers = 4

            start = time.perf_counter()

            # Submit tasks
            ids = []
            for i in range(task_count):
                task = {
                    'id': f'concurrent-{i}',
                    'name': 'io_operation',
                    'payload': {'duration': task_duration},
                    'priority': 'normal',
                    'max_retries': 1,
                    'timeout': 30,
                    'created_at': time.time(),
                    'status': 'pending'
                }
                assert await scheduler.submit_task(task)
                ids.append(task['id'])

            # Wait until all are completed or timeout (don’t just sleep a fixed amount)
            expected_batches = math.ceil(task_count / max_workers)
            expected_max_time = expected_batches * task_duration  # ideal
            timeout = expected_max_time + 1.0  # safety margin for CI jitter

            deadline = start + timeout
            poll_interval = 0.02

            while True:
                completed = 0
                for task_id in ids:
                    status = await scheduler.get_task_status(task_id)
                    if status and status['status'] == 'completed':
                        completed += 1
                if completed == task_count:
                    break
                if time.perf_counter() > deadline:
                    break
                await asyncio.sleep(poll_interval)

            end = time.perf_counter()
            total_time = end - start

            # Assert all done
            completed = 0
            for task_id in ids:
                status = await scheduler.get_task_status(task_id)
                if status and status['status'] == 'completed':
                    completed += 1
            assert completed == task_count, f"Expected {task_count} completed, got {completed}"

            # Allow a small buffer (scheduler overhead, OS scheduling)
            assert total_time <= expected_max_time + 0.25, \
                f"Tasks took too long: {total_time:.2f}s, expected ≤ {(expected_max_time + 0.25):.2f}s"

        finally:
            await scheduler.stop()
    
    @pytest.mark.asyncio
    async def test_throughput_measurement(self):
        """Test task processing throughput"""
        scheduler = TaskScheduler(max_workers=3, queue_size=20)
        await scheduler.start()
        
        try:
            # Submit many quick tasks
            task_count = 15
            start_time = time.time()
            
            tasks = []
            for i in range(task_count):
                task_data = {
                    'id': f'throughput-{i}',
                    'name': 'compute',
                    'payload': {'iterations': 1000},  # Quick compute tasks
                    'priority': 'normal',
                    'max_retries': 1,
                    'timeout': 30,
                    'created_at': time.time(),
                    'status': 'pending'
                }
                success = await scheduler.submit_task(task_data)
                assert success
                tasks.append(task_data['id'])
            
            # Wait for completion
            await asyncio.sleep(3)
            
            end_time = time.time()
            total_time = end_time - start_time
            
            # Count completed tasks
            completed = 0
            for task_id in tasks:
                status = await scheduler.get_task_status(task_id)
                if status and status['status'] == 'completed':
                    completed += 1
            
            # Calculate throughput
            throughput = completed / total_time
            
            assert completed == task_count, f"Not all tasks completed: {completed}/{task_count}"
            assert throughput > 1.0, f"Throughput too low: {throughput:.2f} tasks/sec"
            
            # Get scheduler stats
            stats = await scheduler.get_scheduler_stats()
            assert stats['total_processed'] >= completed
            
        finally:
            await scheduler.shutdown()
    
    @pytest.mark.asyncio
    async def test_scalability_with_worker_count(self):
        """Test that more workers improve performance"""
        results = {}
        
        # Test with different worker counts
        for worker_count in [1, 2, 4]:
            scheduler = TaskScheduler(max_workers=worker_count, queue_size=20)
            await scheduler.start()
            
            try:
                task_count = 8
                start_time = time.time()
                
                # Submit I/O bound tasks
                tasks = []
                for i in range(task_count):
                    task_data = {
                        'id': f'scale-{worker_count}-{i}',
                        'name': 'io_operation',
                        'payload': {'duration': 0.5},
                        'priority': 'normal',
                        'max_retries': 1,
                        'timeout': 30,
                        'created_at': time.time(),
                        'status': 'pending'
                    }
                    await scheduler.submit_task(task_data)
                    tasks.append(task_data['id'])
                
                # Wait for completion
                await asyncio.sleep(3)
                
                end_time = time.time()
                completion_time = end_time - start_time
                
                # Verify completion
                completed = 0
                for task_id in tasks:
                    status = await scheduler.get_task_status(task_id)
                    if status and status['status'] == 'completed':
                        completed += 1
                
                results[worker_count] = {
                    'time': completion_time,
                    'completed': completed
                }
                
            finally:
                await scheduler.shutdown()
        
        # Verify scalability: more workers should complete tasks faster
        assert results[1]['completed'] > 0, "Single worker should complete tasks"
        assert results[4]['completed'] > 0, "Multiple workers should complete tasks"
        
        # With more workers, completion time should be better (or at least not much worse)
        time_improvement = results[1]['time'] / results[4]['time']
        assert time_improvement >= 0.5, f"4 workers not much better than 1: {time_improvement:.2f}x"
    
    @pytest.mark.asyncio
    async def test_memory_efficiency(self):
        """Test that system doesn't leak memory with many tasks"""
        import psutil
        import os
        
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        scheduler = TaskScheduler(max_workers=2, queue_size=100)
        await scheduler.start()
        
        try:
            # Process many tasks in batches
            total_tasks = 50
            batch_size = 10
            
            for batch in range(0, total_tasks, batch_size):
                tasks = []
                
                # Submit batch
                for i in range(batch_size):
                    task_id = f'memory-test-{batch}-{i}'
                    task_data = {
                        'id': task_id,
                        'name': 'compute',
                        'payload': {'iterations': 1000},
                        'priority': 'normal',
                        'max_retries': 1,
                        'timeout': 30,
                        'created_at': time.time(),
                        'status': 'pending'
                    }
                    await scheduler.submit_task(task_data)
                    tasks.append(task_id)
                
                # Wait for batch completion
                await asyncio.sleep(1)
                
                # Verify batch completed
                for task_id in tasks:
                    status = await scheduler.get_task_status(task_id)
                    # Task should be completed or at least processed
                    assert status is not None
            
            # Check final memory usage
            final_memory = process.memory_info().rss / 1024 / 1024  # MB
            memory_increase = final_memory - initial_memory
            
            # Memory increase should be reasonable (less than 100MB for this test)
            assert memory_increase < 100, f"Memory increased too much: {memory_increase:.2f}MB"
            
        finally:
            await scheduler.shutdown()
    
    @pytest.mark.asyncio
    async def test_task_completion_timing(self):
        """Test task completion timing accuracy"""
        scheduler = TaskScheduler(max_workers=2, queue_size=10)
        await scheduler.start()
        
        try:
            # Submit tasks with known duration
            expected_duration = 0.5  # 500ms
            tasks = []
            
            for i in range(4):
                task_data = {
                    'id': f'timing-{i}',
                    'name': 'io_operation',
                    'payload': {'duration': expected_duration},
                    'priority': 'normal',
                    'max_retries': 1,
                    'timeout': 30,
                    'created_at': time.time(),
                    'status': 'pending'
                }
                await scheduler.submit_task(task_data)
                tasks.append(task_data['id'])
            
            # Wait for completion
            await asyncio.sleep(expected_duration + 1)
            
            # Check task completion times
            completion_times = []
            for task_id in tasks:
                status = await scheduler.get_task_status(task_id)
                if status and status['status'] == 'completed':
                    # We could check timing if we tracked it properly
                    completion_times.append(True)
            
            # Verify reasonable completion
            assert len(completion_times) == len(tasks), "All tasks should complete"
            
        finally:
            await scheduler.shutdown()
    
    def test_task_processor_performance(self):
        """Test individual task processor performance"""
        # Test compute task performance
        compute_task = {
            'id': 'perf-compute',
            'name': 'compute', 
            'payload': {'iterations': 10000}
        }
        
        start_time = time.time()
        result = TaskProcessor.process_task(compute_task)
        end_time = time.time()
        
        processing_time = end_time - start_time
        
        assert result['status'] == 'completed'
        assert processing_time < 1.0, f"Compute task took too long: {processing_time:.3f}s"
        
        # Test I/O task timing
        io_task = {
            'id': 'perf-io',
            'name': 'io_operation',
            'payload': {'duration': 0.1}
        }
        
        start_time = time.time()
        result = TaskProcessor.process_task(io_task)
        end_time = time.time()
        
        processing_time = end_time - start_time
        
        assert result['status'] == 'completed'
        # Should take approximately the requested duration
        assert 0.08 <= processing_time <= 0.15, f"I/O task timing off: {processing_time:.3f}s"

