"""
Unit tests for the distributed task scheduler
"""

import pytest
import asyncio
import time
from utils import TaskScheduler, TaskProcessor


@pytest.mark.asyncio
async def test_task_processor():
    """Test the basic task processor functionality"""
    # Test compute task
    task_data = {
        'id': 'test-compute',
        'name': 'compute',
        'payload': {'iterations': 1000}
    }
    
    result = TaskProcessor.process_task(task_data)
    
    assert result['task_id'] == 'test-compute'
    assert result['status'] == 'completed'
    assert 'result' in result
    assert result['result']['success'] is True
    assert 'result_data' in result['result']
    assert result['result']['result_data']['iterations'] == 1000


@pytest.mark.asyncio
async def test_scheduler_basic():
    """Test basic scheduler functionality"""
    scheduler = TaskScheduler(max_workers=2, queue_size=5)
    await scheduler.start()
    
    try:
        # Submit a task
        task_data = {
            'id': 'test-task-1',
            'name': 'compute',
            'payload': {'iterations': 100},
            'priority': 'normal',
            'max_retries': 1,
            'timeout': 30,
            'created_at': time.time(),
            'status': 'pending'
        }
        
        success = await scheduler.submit_task(task_data)
        assert success is True
        
        # Wait for processing
        await asyncio.sleep(2)
        
        # Check status
        status = await scheduler.get_task_status('test-task-1')
        assert status is not None
        assert status['task_id'] == 'test-task-1'
        
    finally:
        await scheduler.shutdown()


@pytest.mark.asyncio
async def test_different_task_types():
    """Test different types of tasks"""
    scheduler = TaskScheduler(max_workers=2, queue_size=10)
    await scheduler.start()
    
    try:
        tasks = [
            {
                'id': 'compute-test',
                'name': 'compute',
                'payload': {'iterations': 500},
                'priority': 'normal',
                'max_retries': 1,
                'timeout': 30,
                'created_at': time.time(),
                'status': 'pending'
            },
            {
                'id': 'io-test',
                'name': 'io_operation',
                'payload': {'duration': 0.1},
                'priority': 'high',
                'max_retries': 1,
                'timeout': 30,
                'created_at': time.time(),
                'status': 'pending'
            },
            {
                'id': 'data-test',
                'name': 'data_processing',
                'payload': {'data': [1, 2, 3]},
                'priority': 'normal',
                'max_retries': 1,
                'timeout': 30,
                'created_at': time.time(),
                'status': 'pending'
            }
        ]
        
        # Submit all tasks
        for task in tasks:
            success = await scheduler.submit_task(task)
            assert success is True
        
        # Wait for processing
        await asyncio.sleep(3)
        
        # Check all completed
        for task in tasks:
            status = await scheduler.get_task_status(task['id'])
            assert status is not None
            # Status should be completed or at least not pending
            assert status['status'] in ['completed', 'running', 'failed']
            
    finally:
        await scheduler.shutdown()


def test_task_processor_error_handling():
    """Test error handling in task processor"""
    task_data = {
        'id': 'error-task',
        'name': 'error_task',
        'payload': {}
    }
    
    result = TaskProcessor.process_task(task_data)
    
    assert result['task_id'] == 'error-task'
    assert result['status'] == 'failed'
    assert 'error' in result
    assert result['result']['success'] is False


@pytest.mark.asyncio
async def test_worker_stats():
    """Test worker statistics functionality"""
    scheduler = TaskScheduler(max_workers=2, queue_size=5)
    await scheduler.start()
    
    try:
        stats = await scheduler.get_worker_stats()
        
        assert 'total_workers' in stats
        assert 'active_workers' in stats
        assert 'queue_size' in stats
        assert stats['total_workers'] == 2
        
    finally:
        await scheduler.shutdown()


@pytest.mark.asyncio 
async def test_scheduler_stats():
    """Test scheduler statistics"""
    scheduler = TaskScheduler(max_workers=2, queue_size=5)
    await scheduler.start()
    
    try:
        stats = await scheduler.get_scheduler_stats()
        
        assert 'total_processed' in stats
        assert 'throughput' in stats
        assert 'worker_utilization' in stats
        assert 'queue_utilization' in stats
        
    finally:
        await scheduler.shutdown()
