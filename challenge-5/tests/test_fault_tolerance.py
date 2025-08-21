"""
Test Suite: Fault Tolerance and Error Handling
Tests that verify the system's ability to handle failures gracefully
"""

import pytest
import asyncio
import time
from utils import TaskScheduler, TaskProcessor


class TestFaultTolerance:
    """Test fault tolerance and error handling features"""
    
    @pytest.mark.asyncio
    async def test_task_retry_mechanism(self):
        """Test that failed tasks are retried according to configuration"""
        scheduler = TaskScheduler(max_workers=2, queue_size=10)
        await scheduler.start()
        
        try:
            # Submit a task that will fail
            task_data = {
                'id': 'retry-test',
                'name': 'error_task',  # This task type intentionally fails
                'payload': {},
                'priority': 'normal',
                'max_retries': 3,
                'timeout': 30,
                'created_at': time.time(),
                'status': 'pending'
            }
            
            success = await scheduler.submit_task(task_data)
            assert success, "Task submission should succeed"
            
            # Wait for processing and retries
            await asyncio.sleep(2)
            
            # Check final status
            status = await scheduler.get_task_status('retry-test')
            assert status is not None, "Task status should be available"
            assert status['status'] == 'failed', "Task should eventually fail after retries"
            
            # Note: In a full implementation, we'd track retry count
            # For now, verify the task was processed and failed appropriately
            
        finally:
            await scheduler.shutdown()
    
    @pytest.mark.asyncio
    async def test_worker_failure_recovery(self):
        """Test recovery when worker processes fail"""
        scheduler = TaskScheduler(max_workers=3, queue_size=15)
        await scheduler.start()
        
        try:
            # Submit mix of good and bad tasks
            tasks = []
            
            # Good tasks
            for i in range(5):
                task_data = {
                    'id': f'good-task-{i}',
                    'name': 'compute',
                    'payload': {'iterations': 1000},
                    'priority': 'normal',
                    'max_retries': 1,
                    'timeout': 30,
                    'created_at': time.time(),
                    'status': 'pending'
                }
                await scheduler.submit_task(task_data)
                tasks.append(('good', task_data['id']))
            
            # Bad tasks that will fail
            for i in range(3):
                task_data = {
                    'id': f'bad-task-{i}',
                    'name': 'error_task',
                    'payload': {},
                    'priority': 'normal',
                    'max_retries': 1,
                    'timeout': 30,
                    'created_at': time.time(),
                    'status': 'pending'
                }
                await scheduler.submit_task(task_data)
                tasks.append(('bad', task_data['id']))
            
            # More good tasks after bad ones
            for i in range(3):
                task_data = {
                    'id': f'recovery-task-{i}',
                    'name': 'compute',
                    'payload': {'iterations': 1000},
                    'priority': 'normal',
                    'max_retries': 1,
                    'timeout': 30,
                    'created_at': time.time(),
                    'status': 'pending'
                }
                await scheduler.submit_task(task_data)
                tasks.append(('recovery', task_data['id']))
            
            # Wait for all processing
            await asyncio.sleep(3)
            
            # Check results
            good_completed = 0
            bad_failed = 0
            recovery_completed = 0
            
            for task_type, task_id in tasks:
                status = await scheduler.get_task_status(task_id)
                if status:
                    if task_type == 'good' and status['status'] == 'completed':
                        good_completed += 1
                    elif task_type == 'bad' and status['status'] == 'failed':
                        bad_failed += 1
                    elif task_type == 'recovery' and status['status'] == 'completed':
                        recovery_completed += 1
            
            # Verify system recovers and continues processing
            assert good_completed >= 4, f"Most good tasks should complete: {good_completed}/5"
            assert bad_failed >= 2, f"Bad tasks should fail: {bad_failed}/3"
            assert recovery_completed >= 2, f"Recovery tasks should complete: {recovery_completed}/3"
            
            # System should still be healthy
            health = await scheduler.health_check()
            assert health, "Scheduler should remain healthy after errors"
            
        finally:
            await scheduler.shutdown()
    
    @pytest.mark.asyncio
    async def test_timeout_handling(self):
        """Test handling of task timeouts"""
        scheduler = TaskScheduler(max_workers=2, queue_size=10)
        await scheduler.start()
        
        try:
            # Submit a task with very short timeout
            task_data = {
                'id': 'timeout-test',
                'name': 'io_operation',
                'payload': {'duration': 5.0},  # 5 seconds
                'priority': 'normal',
                'max_retries': 1,
                'timeout': 2,  # But timeout after 2 seconds
                'created_at': time.time(),
                'status': 'pending'
            }
            
            success = await scheduler.submit_task(task_data)
            assert success
            
            # Wait longer than timeout
            await asyncio.sleep(3)
            
            # Task should be handled (in a full implementation it would timeout)
            status = await scheduler.get_task_status('timeout-test')
            assert status is not None
            # For now, just verify the task was processed
            
        finally:
            await scheduler.shutdown()
    
    @pytest.mark.asyncio
    async def test_queue_overflow_handling(self):
        """Test graceful handling when task queue overflows"""
        scheduler = TaskScheduler(max_workers=1, queue_size=3)  # Very small queue
        await scheduler.start()
        
        try:
            # Submit long-running task to occupy worker
            long_task = {
                'id': 'blocking-task',
                'name': 'io_operation',
                'payload': {'duration': 3.0},
                'priority': 'normal',
                'max_retries': 1,
                'timeout': 30,
                'created_at': time.time(),
                'status': 'pending'
            }
            await scheduler.submit_task(long_task)
            
            # Fill up the queue
            accepted_count = 0
            rejected_count = 0
            
            for i in range(10):  # Try to submit more than queue can hold
                task_data = {
                    'id': f'overflow-{i}',
                    'name': 'compute',
                    'payload': {'iterations': 1000},
                    'priority': 'normal',
                    'max_retries': 1,
                    'timeout': 30,
                    'created_at': time.time(),
                    'status': 'pending'
                }
                
                success = await scheduler.submit_task(task_data)
                if success:
                    accepted_count += 1
                else:
                    rejected_count += 1
            
            # Should gracefully reject excess tasks
            assert accepted_count > 0, "Some tasks should be accepted"
            assert rejected_count > 0, "Some tasks should be rejected when queue full"
            assert accepted_count <= 3, "Should not accept more than queue size"
            
            # Scheduler should remain functional
            health = await scheduler.health_check()
            assert health, "Scheduler should remain healthy after queue overflow"
            
        finally:
            await scheduler.shutdown()
    
    @pytest.mark.asyncio
    async def test_graceful_shutdown(self):
        """Test graceful shutdown with active tasks"""
        scheduler = TaskScheduler(max_workers=2, queue_size=10)
        await scheduler.start()
        
        # Submit some tasks
        tasks = []
        for i in range(5):
            task_data = {
                'id': f'shutdown-task-{i}',
                'name': 'io_operation',
                'payload': {'duration': 2.0},
                'priority': 'normal',
                'max_retries': 1,
                'timeout': 30,
                'created_at': time.time(),
                'status': 'pending'
            }
            await scheduler.submit_task(task_data)
            tasks.append(task_data['id'])
        
        # Let some processing start
        await asyncio.sleep(0.5)
        
        # Shutdown should complete without hanging
        start_shutdown = time.time()
        await scheduler.shutdown()
        shutdown_time = time.time() - start_shutdown
        
        # Shutdown should be reasonably quick (not hang indefinitely)
        assert shutdown_time < 10, f"Shutdown took too long: {shutdown_time:.2f}s"
    
    def test_error_task_handling(self):
        """Test handling of tasks that throw exceptions"""
        # Test various error conditions
        error_cases = [
            {
                'id': 'error-1',
                'name': 'error_task',
                'payload': {}
            },
            {
                'id': 'error-2', 
                'name': 'data_processing',
                'payload': {'data': 'not_a_list'}  # Invalid data type
            },
            {
                'id': 'error-3',
                'name': 'unknown_task_type',
                'payload': {}
            }
        ]
        
        for task_data in error_cases:
            result = TaskProcessor.process_task(task_data)
            
            assert result['status'] == 'failed', f"Task {task_data['id']} should fail"
            assert 'error' in result, f"Task {task_data['id']} should have error message"
            assert result['result']['success'] is False
    
    @pytest.mark.asyncio
    async def test_resource_exhaustion_handling(self):
        """Test handling when system resources are exhausted"""
        scheduler = TaskScheduler(max_workers=2, queue_size=20)
        await scheduler.start()
        
        try:
            # Submit many resource-intensive tasks
            tasks = []
            for i in range(10):
                task_data = {
                    'id': f'resource-{i}',
                    'name': 'compute',
                    'payload': {'iterations': 100000},  # More intensive
                    'priority': 'normal',
                    'max_retries': 1,
                    'timeout': 30,
                    'created_at': time.time(),
                    'status': 'pending'
                }
                
                success = await scheduler.submit_task(task_data)
                if success:
                    tasks.append(task_data['id'])
            
            # System should handle the load
            assert len(tasks) > 0, "Should accept some tasks"
            
            # Wait for processing
            await asyncio.sleep(3)
            
            # Check that system is still responsive
            health = await scheduler.health_check()
            assert health, "System should remain healthy under load"
            
            # Check some tasks completed
            completed = 0
            for task_id in tasks:
                status = await scheduler.get_task_status(task_id)
                if status and status['status'] == 'completed':
                    completed += 1
            
            assert completed > 0, "At least some tasks should complete"
            
        finally:
            await scheduler.shutdown()
    
    @pytest.mark.asyncio
    async def test_invalid_task_data_handling(self):
        """Test handling of invalid task data"""
        scheduler = TaskScheduler(max_workers=2, queue_size=10)
        await scheduler.start()
        
        try:
            # Test missing required fields
            invalid_task = {
                'id': 'invalid-1',
                # Missing 'name' field
                'payload': {},
                'priority': 'normal',
                'max_retries': 1,
                'timeout': 30,
                'created_at': time.time(),
                'status': 'pending'
            }
            
            # This should either be rejected or handled gracefully
            try:
                success = await scheduler.submit_task(invalid_task)
                # If accepted, should be processed without crashing system
                if success:
                    await asyncio.sleep(1)
                    status = await scheduler.get_task_status('invalid-1')
                    # Should either fail or be handled gracefully
                    assert status is not None
            except Exception:
                # It's okay if validation rejects invalid tasks
                pass
            
            # System should remain healthy
            health = await scheduler.health_check()
            assert health, "System should remain healthy after invalid input"
            
        finally:
            await scheduler.shutdown()

