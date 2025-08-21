import pytest
import asyncio
import tempfile
import os
import sys
from pathlib import Path
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).parent.parent))

from app import app
from utils import TaskScheduler


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def client():
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
async def test_scheduler():
    scheduler = TaskScheduler(max_workers=2, queue_size=10)
    await scheduler.start()
    yield scheduler
    await scheduler.shutdown()


@pytest.fixture
def temp_db():
    temp_file = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
    temp_file.close()
    original_path = os.environ.get('DB_PATH')
    os.environ['DB_PATH'] = temp_file.name
    yield temp_file.name
    if original_path:
        os.environ['DB_PATH'] = original_path
    else:
        os.environ.pop('DB_PATH', None)
    try:
        os.unlink(temp_file.name)
    except OSError:
        pass


@pytest.fixture
def sample_task_data():
    return {
        "task_name": "test_task",
        "payload": {"test": "data"},
        "priority": "normal",
        "max_retries": 2,
        "timeout": 30
    }


@pytest.fixture
def sample_compute_task():
    return {
        "task_name": "compute",
        "payload": {"iterations": 1000},
        "priority": "normal",
        "max_retries": 1,
        "timeout": 30
    }


@pytest.fixture
def sample_io_task():
    return {
        "task_name": "io_operation",
        "payload": {"duration": 0.1},
        "priority": "high",
        "max_retries": 1,
        "timeout": 30
    }


@pytest.fixture
def sample_data_task():
    return {
        "task_name": "data_processing",
        "payload": {"data": [1, 2, 3, 4, 5]},
        "priority": "normal",
        "max_retries": 2,
        "timeout": 45
    }
