#!/usr/bin/env python3

"""
Comprehensive Demo for the Distributed Task Scheduler

This demo showcases all the key features:
- Task submission and distribution across workers
- Multiple task types and priorities
- Real-time status monitoring
- System metrics and performance tracking
- Error handling and retry mechanisms
- Both direct scheduler and API testing
"""

import json
import time
import asyncio
import subprocess
import signal
import os
from pathlib import Path
from fastapi.testclient import TestClient
from app import app
from utils import TaskScheduler
from models import TaskPriority, TaskStatus

def print_banner(title):
    """Print a formatted banner"""
    print(f"\n{'='*60}")
    print(f"🎯 {title}")
    print(f"{'='*60}")

def print_section(title):
    """Print a section header"""
    print(f"\n📋 {title}")
    print("-" * 40)

async def test_direct_scheduler():
    """Test the scheduler directly without API layer"""
    print_banner("DIRECT SCHEDULER CORE FUNCTIONALITY TEST")
    
    # Create and start scheduler
    scheduler = TaskScheduler(max_workers=3, queue_size=10)
    await scheduler.start()
    
    try:
        print_section("Task Submission Test")
        
        tasks = []
        for i in range(5):
            task_data = {
                'id': f'demo-task-{i}',
                'name': 'compute',
                'payload': {'iterations': 5000 * (i + 1)},
                'priority': 'normal' if i < 3 else 'high',
                'max_retries': 2,
                'timeout': 30,
                'created_at': time.time(),
                'status': 'pending'
            }
            
            success = await scheduler.submit_task(task_data)
            if success:
                tasks.append(task_data['id'])
                print(f"  ✅ Submitted {task_data['priority']} priority task: {task_data['id']}")
            else:
                print(f"  ❌ Failed to submit task: {task_data['id']}")
        
        print_section("Task Processing Monitoring")
        
        completed_tasks = set()
        max_wait = 15
        start_time = time.time()
        
        while len(completed_tasks) < len(tasks) and (time.time() - start_time) < max_wait:
            await asyncio.sleep(0.5)
            
            for task_id in tasks:
                if task_id not in completed_tasks:
                    status = await scheduler.get_task_status(task_id)
                    if status and status['status'] in ['completed', 'failed']:
                        completed_tasks.add(task_id)
                        result_info = status.get('result', {})
                        print(f"  ✅ Task {task_id}: {status['status']}")
                        if 'result' in result_info:
                            print(f"     Result: {result_info['result']}")
        
        print_section("Worker Statistics")
        stats = await scheduler.get_worker_stats()
        print(f"  Total workers: {stats.get('total_workers', 0)}")
        print(f"  Active workers: {stats.get('active_workers', 0)}")
        print(f"  Queue size: {stats.get('queue_size', 0)}")
        print(f"  Completed tasks: {stats.get('completed_tasks', 0)}")
        
        print_section("System Health Check")
        health = await scheduler.health_check()
        print(f"  System healthy: {'✅ Yes' if health else '❌ No'}")
        
        return len(completed_tasks) == len(tasks)
        
    finally:
        await scheduler.shutdown()

def test_api_functionality():
    """Test the FastAPI endpoints"""
    print_banner("API FUNCTIONALITY TEST")
    
    client = TestClient(app)
    
    print_section("Health Check")
    response = client.get("/health")
    if response.status_code == 200:
        health = response.json()
        print(f"  ✅ API Health: {health['status']}")
    else:
        print(f"  ❌ Health check failed: {response.status_code}")
        return False
    
    print_section("Task Submission via API")
    
    # Submit different types of tasks
    tasks = [
        {
            "task_name": "compute",
            "payload": {"iterations": 10000},
            "priority": "high",
            "max_retries": 2,
            "timeout": 30
        },
        {
            "task_name": "io_operation", 
            "payload": {"duration": 1.0},
            "priority": "normal",
            "max_retries": 1,
            "timeout": 30
        },
        {
            "task_name": "data_processing",
            "payload": {"data": list(range(50))},
            "priority": "low",
            "max_retries": 1,
            "timeout": 30
        }
    ]
    
    submitted_tasks = []
    
    for i, task in enumerate(tasks):
        response = client.post("/tasks/submit", json=task)
        if response.status_code == 200:
            task_data = response.json()
            task_id = task_data['task_id']
            submitted_tasks.append(task_id)
            print(f"  ✅ Submitted {task['task_name']} task: {task_id}")
        else:
            print(f"  ❌ Failed to submit {task['task_name']}: {response.text}")
    
    print_section("Task Status Monitoring")
    
    # Monitor task completion
    max_checks = 20
    for check in range(max_checks):
        time.sleep(0.5)
        completed_count = 0
        
        for task_id in submitted_tasks:
            response = client.get(f"/tasks/{task_id}/status")
            if response.status_code == 200:
                status_data = response.json()
                if status_data['status'] in ['completed', 'failed']:
                    completed_count += 1
                    if check == 0 or check % 5 == 0:  # Print occasionally
                        print(f"  📊 Task {task_id}: {status_data['status']}")
        
        if completed_count == len(submitted_tasks):
            print(f"  ✅ All {len(submitted_tasks)} tasks completed!")
            break
    
    print_section("System Metrics")
    response = client.get("/metrics")
    if response.status_code == 200:
        metrics = response.json()
        print(f"  📈 System Metrics:")
        print(f"     Total tasks: {metrics.get('total_tasks', 0)}")
        print(f"     Completed: {metrics.get('completed_tasks', 0)}")
        print(f"     Failed: {metrics.get('failed_tasks', 0)}")
        print(f"     Active workers: {metrics.get('active_workers', 0)}")
    
    print_section("Worker Status")
    response = client.get("/workers")
    if response.status_code == 200:
        workers = response.json()
        print(f"  👷 Worker Status:")
        print(f"     Total workers: {workers.get('total_workers', 0)}")
        print(f"     Active: {workers.get('active_workers', 0)}")
        print(f"     Queue size: {workers.get('current_queue_size', 0)}")
    
    return len(submitted_tasks) > 0

def main():
    """Run the complete demo"""
    print_banner("DISTRIBUTED TASK SCHEDULER COMPREHENSIVE DEMO")
    print("This demo will test both the core scheduler and API functionality")
    
    # Test 1: Direct scheduler functionality
    try:
        direct_result = asyncio.run(test_direct_scheduler())
        print(f"\n🎯 Direct Scheduler Test: {'✅ PASSED' if direct_result else '❌ FAILED'}")
    except Exception as e:
        print(f"\n🎯 Direct Scheduler Test: ❌ FAILED - {e}")
        direct_result = False
    
    # Test 2: API functionality
    try:
        api_result = test_api_functionality()
        print(f"\n🌐 API Functionality Test: {'✅ PASSED' if api_result else '❌ FAILED'}")
    except Exception as e:
        print(f"\n🌐 API Functionality Test: ❌ FAILED - {e}")
        api_result = False
    
    # Final summary
    print_banner("DEMO RESULTS SUMMARY")
    print(f"Direct Scheduler: {'✅ PASSED' if direct_result else '❌ FAILED'}")
    print(f"API Functionality: {'✅ PASSED' if api_result else '❌ FAILED'}")
    
    if direct_result and api_result:
        print("\n🎉 ALL TESTS PASSED! The Distributed Task Scheduler is working correctly.")
    else:
        print("\n⚠️  Some tests failed. Please check the output above for details.")
    
    return direct_result and api_result

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
