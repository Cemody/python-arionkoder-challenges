import asyncio
import multiprocessing as mp
import sqlite3
import json
import time
import psutil
import os
from typing import Any, Dict, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from concurrent.futures import Future, ThreadPoolExecutor, ProcessPoolExecutor, wait, ALL_COMPLETED
import logging

from models import TaskStatus, TaskPriority, WorkerInfo

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class Task:
    """In-memory task record."""
    id: str
    name: str
    payload: Dict[str, Any]
    priority: TaskPriority
    max_retries: int
    timeout: int
    created_at: str
    status: TaskStatus
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    worker_id: Optional[str] = None
    retry_count: int = 0
    error_message: Optional[str] = None
    result: Optional[Dict[str, Any]] = None


class TaskProcessor:
    """Executes task logic (run in worker process/thread)."""

    @staticmethod
    def process_task(task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Execute one task; return result dict with string status."""
        start_time = time.time()
        task_id = task_data['id']
        task_name = task_data['name']
        payload = task_data.get('payload', {}) or {}

        try:
            result = TaskProcessor._execute_task(task_name, payload)
            processing_time = (time.time() - start_time) * 1000.0

            return {
                'task_id': task_id,
                'status': TaskStatus.COMPLETED.value,  # <-- string 'completed'
                'result': {
                    'success': True,
                    'result_data': result,
                    'metrics': {'processing_time_ms': processing_time},
                },
                'processing_time_ms': processing_time,
                'worker_pid': os.getpid(),
                'completed_at': datetime.now().isoformat(),
            }

        except Exception as e:
            processing_time = (time.time() - start_time) * 1000.0

            return {
                'task_id': task_id,
                'status': TaskStatus.FAILED.value,  # <-- string 'failed'
                'result': {
                    'success': False,
                    'error_message': str(e),
                    'metrics': {'processing_time_ms': processing_time},
                },
                'error': str(e),
                'processing_time_ms': processing_time,
                'worker_pid': os.getpid(),
                'completed_at': datetime.now().isoformat(),
            }

    @staticmethod
    def _execute_task(task_name: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Dispatch concrete task types (compute|io_operation|data_processing|error_task)."""
        if task_name == "compute":
            iterations = int(payload.get('iterations', 1_000_000))
            result = sum(i * i for i in range(iterations))
            return {'result': result, 'iterations': iterations}

        elif task_name == "io_operation":
            duration = float(payload.get('duration', 1.0))
            time.sleep(duration)  # runs in worker thread
            return {'slept_for': duration, 'timestamp': datetime.now().isoformat()}

        elif task_name == "data_processing":
            data = payload.get('data', [])
            if not isinstance(data, list):
                raise ValueError("Data must be a list")
            processed = [
                (item * 2 if isinstance(item, (int, float)) else str(item).upper())
                for item in data
            ]
            return {'original_count': len(data), 'processed_data': processed}

        elif task_name == "error_task":
            raise Exception("Intentional task failure for testing")

        # ⬇️ Unknown task types should FAIL (so tests see status == 'failed')
        raise ValueError(f"Unknown task type: {task_name}")





class WorkerPool:
    """Hybrid execution pool (processes for CPU-bound, threads for others)."""
    CPU_BOUND_TASKS = {"compute"}  # route these to ProcessPoolExecutor

    def __init__(self, max_workers: int = 4):
        self.max_workers = max_workers
        self.thread_executor: Optional[ThreadPoolExecutor] = None
        self.process_executor: Optional[ProcessPoolExecutor] = None
        self.active_tasks: Dict[str, Future] = {}
        self._task_exec_map: Dict[str, str] = {}  # task_id -> "thread" | "process"
        self.worker_stats: Dict[int, Dict] = {}
        self.start_time = time.time()

    async def start(self):
        self.thread_executor = ThreadPoolExecutor(max_workers=self.max_workers)
        self.process_executor = ProcessPoolExecutor(max_workers=self.max_workers)
        logger.info(f"Started worker pool with {self.max_workers} workers (threads + processes)")

    async def stop(self):
        futures = list(self.active_tasks.values())
        if futures:
            logger.info(f"Waiting for {len(futures)} active tasks to complete...")
            loop = asyncio.get_running_loop()
            try:
                await loop.run_in_executor(
                    None, lambda: wait(futures, timeout=30.0, return_when=ALL_COMPLETED)
                )
            except Exception as e:
                logger.error(f"Error waiting for tasks to complete: {e}")
                for f in futures:
                    if not f.done():
                        f.cancel()

        if self.thread_executor:
            self.thread_executor.shutdown(wait=True)
            self.thread_executor = None
        if self.process_executor:
            self.process_executor.shutdown(wait=True)
            self.process_executor = None

        self.active_tasks.clear()
        self._task_exec_map.clear()
        logger.info("Worker pool stopped")

    async def submit_task(self, task: Task) -> str:
        if not (self.thread_executor and self.process_executor):
            raise RuntimeError("Worker pool not started")

        task_data = asdict(task)
        # Route CPU-bound tasks to processes, others to threads
        use_process = task.name in self.CPU_BOUND_TASKS
        if use_process:
            future = self.process_executor.submit(TaskProcessor.process_task, task_data)
            exec_kind = "process"
        else:
            future = self.thread_executor.submit(TaskProcessor.process_task, task_data)
            exec_kind = "thread"

        self.active_tasks[task.id] = future
        self._task_exec_map[task.id] = exec_kind
        return task.id

    async def get_task_result(self, task_id: str) -> Optional[Dict[str, Any]]:
        future = self.active_tasks.get(task_id)
        if future is None:
            return None

        if future.done():
            try:
                result = future.result()
            except Exception as e:
                result = {'task_id': task_id, 'status': TaskStatus.FAILED.value if hasattr(TaskStatus, "FAILED") else TaskStatus.FAILED, 'error': str(e)}
            finally:
                self.active_tasks.pop(task_id, None)
                self._task_exec_map.pop(task_id, None)
            return result

        return None

    def get_active_task_count(self) -> int:
        return len(self.active_tasks)

    def get_worker_stats(self) -> Dict[str, Any]:
        return {
            'max_workers': self.max_workers,
            'active_tasks': len(self.active_tasks),
            'uptime_seconds': time.time() - self.start_time,
        }


class TaskScheduler:
    """Coordinates queueing, dispatch, completion tracking, and stats."""

    def __init__(self, max_workers: int = 4, queue_size: int = 100):
        self.max_workers = max_workers
        self.queue_size = queue_size
        self.task_queue: asyncio.Queue = None
        self.worker_pool = WorkerPool(max_workers)
        self.active_tasks: Dict[str, Task] = {}
        self.completed_tasks: Dict[str, Task] = {}
        self.running = False
        self.processing_task = None
        self.stats = {
            'total_submitted': 0,
            'total_processed': 0,
            'total_failed': 0,
            'total_cancelled': 0
        }
        self.start_time = time.time()

    async def start(self):
        self.task_queue = asyncio.Queue(maxsize=self.queue_size)
        await self.worker_pool.start()
        self.running = True
        self.processing_task = asyncio.create_task(self._process_tasks())
        logger.info("Task scheduler started")

    async def shutdown(self):
        """Gracefully stop processing and worker pool."""
        # If already stopped, return quickly (works for both pool styles)
        if not self.running and not any((
            getattr(self.worker_pool, "executor", None),
            getattr(self.worker_pool, "thread_executor", None),
            getattr(self.worker_pool, "process_executor", None),
        )):
            logger.info("Task scheduler already stopped")
            return

        # Stop accepting new tasks
        self.running = False

        # Let the processing loop flush the queue / active tasks
        max_shutdown_wait = 10  # seconds
        shutdown_start = time.time()
        while (time.time() - shutdown_start) < max_shutdown_wait:
            if self.task_queue.empty() and not self.active_tasks:
                break
            await self._check_completed_tasks()
            await asyncio.sleep(0.1)

        # Cancel the processing loop task if still running
        if self.processing_task and not self.processing_task.done():
            self.processing_task.cancel()
            try:
                await self.processing_task
            except asyncio.CancelledError:
                pass

        # Stop the worker pool (waits for in-flight tasks as implemented in WorkerPool.stop)
        await self.worker_pool.stop()

        # Final sweep of completions
        await self._check_completed_tasks()

        logger.info("Task scheduler shutdown complete")

    async def stop(self) -> None:
        """Alias for shutdown (compat)."""
        await self.shutdown()

    async def submit_task(self, task_data: Dict[str, Any]) -> bool:
        if not self.running:
            return False
        try:
            task = Task(
                id=task_data['id'],
                name=task_data['name'],
                payload=task_data['payload'],
                priority=TaskPriority(task_data['priority']),
                max_retries=task_data['max_retries'],
                timeout=task_data['timeout'],
                created_at=task_data['created_at'],
                status=TaskStatus.PENDING
            )
            self.task_queue.put_nowait(task)
            self.stats['total_submitted'] += 1
            return True
        except asyncio.QueueFull:
            return False

    async def _process_tasks(self):
        logger.info("Starting task processing loop")
        while self.running:
            try:
                task = await asyncio.wait_for(self.task_queue.get(), timeout=0.5)
                logger.info(f"Processing task: {task.id}")

                task.status = TaskStatus.RUNNING
                task.started_at = datetime.now().isoformat()
                self.active_tasks[task.id] = task

                await self.worker_pool.submit_task(task)

                await self._check_completed_tasks()
            except asyncio.TimeoutError:
                await self._check_completed_tasks()
                continue
            except Exception as e:
                logger.error(f"Error in task processing loop: {e}")
                await asyncio.sleep(1)

    async def _check_completed_tasks(self):
        completed_task_ids = []

        for task_id, task in list(self.active_tasks.items()):
            result = await self.worker_pool.get_task_result(task_id)
            if result:
                status_raw = result.get('status')
                # Normalize to string for comparisons
                status_str = status_raw.value if hasattr(status_raw, "value") else str(status_raw)
                logger.info(f"Task {task_id} completed with status: {status_str}")

                # Store enum on the task (if you prefer), but use string for logic
                try:
                    task.status = TaskStatus(status_str)
                except Exception:
                    # Fallback if status_str already is enum name or unrecognized
                    task.status = TaskStatus.COMPLETED if status_str == "completed" else TaskStatus.FAILED

                task.completed_at = result.get('completed_at')

                if status_str == "completed":
                    task.result = result.get('result')
                    self.stats['total_processed'] += 1
                else:
                    task.error_message = result.get('error')
                    self.stats['total_failed'] += 1

                self.completed_tasks[task_id] = task
                completed_task_ids.append(task_id)

        for task_id in completed_task_ids:
            self.active_tasks.pop(task_id, None)

    async def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        # Keep statuses fresh
        if self.running:
            await self._check_completed_tasks()

        if task_id in self.active_tasks:
            task = self.active_tasks[task_id]
            # Return string status for tests
            status_str = task.status.value if hasattr(task.status, "value") else str(task.status)
            return {
                'task_id': task.id,
                'status': status_str,
                'started_at': task.started_at,
                'worker_id': task.worker_id,
                'progress': 50 if status_str == "running" else 0,
            }

        if task_id in self.completed_tasks:
            task = self.completed_tasks[task_id]
            status_str = task.status.value if hasattr(task.status, "value") else str(task.status)
            return {
                'task_id': task.id,
                'status': status_str,
                'started_at': task.started_at,
                'completed_at': task.completed_at,
                'result': task.result,
                'error': task.error_message,
                'progress': 100 if status_str == "completed" else 0,
            }

        return None

    async def cancel_task(self, task_id: str) -> bool:
        if task_id in self.active_tasks:
            task = self.active_tasks[task_id]
            task.status = TaskStatus.CANCELLED
            self.stats['total_cancelled'] += 1
            self.completed_tasks[task_id] = task
            del self.active_tasks[task_id]
            return True
        return False

    async def get_queue_position(self, task_id: str) -> int:
        return self.task_queue.qsize()

    async def estimate_start_time(self, task_id: str) -> Optional[str]:
        queue_size = self.task_queue.qsize()
        if queue_size == 0:
            return datetime.now().isoformat()
        avg_time = 30  # seconds (simple heuristic)
        estimated_delay = (queue_size * avg_time) / self.max_workers
        return (datetime.now() + timedelta(seconds=estimated_delay)).isoformat()

    async def get_worker_stats(self) -> Dict[str, Any]:
        pool = self.worker_pool.get_worker_stats()
        return {
            'total_workers': self.max_workers,
            'active_workers': min(pool['active_tasks'], self.max_workers),
            'idle_workers': max(0, self.max_workers - pool['active_tasks']),
            'workers': [],
            'queue_size': self.task_queue.qsize(),
            'completed_tasks': self.stats['total_processed'],
            'failed_tasks': self.stats['total_failed'],
        }

    async def get_scheduler_stats(self) -> Dict[str, Any]:
        uptime = time.time() - self.start_time
        total_processed = self.stats['total_processed']
        return {
            'total_processed': total_processed,
            'throughput': total_processed / max(uptime, 1),
            'avg_processing_time': 1000,  # Placeholder
            'worker_utilization': (len(self.active_tasks) / self.max_workers) * 100,
            'queue_utilization': (self.task_queue.qsize() / self.queue_size) * 100,
        }

    async def health_check(self) -> bool:
        """Return True if running and pool active."""
        return self.running and any((
            getattr(self.worker_pool, "executor", None),         # old single-executor
            getattr(self.worker_pool, "thread_executor", None),  # new hybrid thread pool
            getattr(self.worker_pool, "process_executor", None), # new hybrid process pool
        ))

    async def worker_pool_active(self) -> bool:
        """Return True if any executor alive."""
        return any((
            getattr(self.worker_pool, "executor", None),
            getattr(self.worker_pool, "thread_executor", None),
            getattr(self.worker_pool, "process_executor", None),
        ))


# Database operations
async def save_task_to_database(task: Dict[str, Any]):
    """Persist task row (upsert)."""
    try:
        conn = sqlite3.connect('task_scheduler.db')
        cursor = conn.cursor()
        
        # Create table if it doesn't exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tasks (
                id TEXT PRIMARY KEY,
                name TEXT,
                payload TEXT,
                priority TEXT,
                status TEXT,
                created_at TEXT,
                started_at TEXT,
                completed_at TEXT,
                result TEXT,
                error_message TEXT,
                processing_time_ms REAL,
                retry_count INTEGER
            )
        ''')
        
        cursor.execute('''
            INSERT OR REPLACE INTO tasks 
            (id, name, payload, priority, status, created_at, started_at, completed_at, 
             result, error_message, processing_time_ms, retry_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            task['id'],
            task['name'],
            json.dumps(task['payload']),
            task['priority'],
            task['status'],
            task['created_at'],
            task.get('started_at'),
            task.get('completed_at'),
            json.dumps(task.get('result')) if task.get('result') else None,
            task.get('error_message'),
            task.get('processing_time_ms'),
            task.get('retry_count', 0)
        ))
        
        conn.commit()
        conn.close()
        
    except Exception as e:
        logger.error(f"Failed to save task to database: {e}")


async def get_task_status(task_id: str) -> Optional[Dict[str, Any]]:
    """Fetch persisted task row -> status dict."""
    try:
        conn = sqlite3.connect('task_scheduler.db')
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM tasks WHERE id = ?', (task_id,))
        row = cursor.fetchone()
        conn.close()
        
        if row:
            return {
                'task_id': row[0],
                'status': row[4],
                'started_at': row[6],
                'completed_at': row[7],
                'result': json.loads(row[8]) if row[8] else None,
                'error_message': row[9],
                'processing_time_ms': row[10],
                'retry_count': row[11]
            }
        
        return None
        
    except Exception as e:
        logger.error(f"Failed to get task status from database: {e}")
        return None


async def get_worker_stats() -> Dict[str, Any]:
    """Return minimal process-level worker stats."""
    # This is a simplified version - in a real system you'd track more details
    return {
        'active_workers': mp.active_children().__len__(),
        'system_load': psutil.getloadavg()[0] if hasattr(psutil, 'getloadavg') else 0
    }


async def cleanup_completed_tasks(older_than_hours: int = 24):
    """Delete completed/failed/cancelled tasks older than cutoff."""
    try:
        conn = sqlite3.connect('task_scheduler.db')
        cursor = conn.cursor()
        
        cutoff_time = (datetime.now() - timedelta(hours=older_than_hours)).isoformat()
        
        cursor.execute('''
            DELETE FROM tasks 
            WHERE status IN ('completed', 'failed', 'cancelled') 
            AND completed_at < ?
        ''', (cutoff_time,))
        
        deleted_count = cursor.rowcount
        conn.commit()
        conn.close()
        
        logger.info(f"Cleaned up {deleted_count} old tasks")
        
    except Exception as e:
        logger.error(f"Failed to cleanup tasks: {e}")


async def get_system_metrics() -> Dict[str, Any]:
    """Return basic system performance metrics (CPU, memory, uptime)."""
    try:
        # Get system metrics
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        
        # Calculate uptime (simplified)
        uptime = time.time() - psutil.boot_time()
        
        return {
            'uptime_seconds': uptime,
            'cpu_usage_percent': cpu_percent,
            'memory_usage_mb': memory.used / (1024 * 1024),
            'memory_total_mb': memory.total / (1024 * 1024)
        }
        
    except Exception as e:
        logger.error(f"Failed to get system metrics: {e}")
        return {
            'uptime_seconds': 0,
            'cpu_usage_percent': 0,
            'memory_usage_mb': 0,
            'memory_total_mb': 0
        }
