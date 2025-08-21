"""
Pytest configuration file for Challenge 1 tests.

This file ensures that the parent directory is in the Python path
so that test files can import app, utils, and models modules.
"""

import sys
from pathlib import Path

# Add the parent directory to the Python path
parent_dir = Path(__file__).parent.parent
if str(parent_dir) not in sys.path:
    sys.path.insert(0, str(parent_dir))

import pytest
from pathlib import Path


def cleanup_test_files():
    """Clean up test database and message queue files"""
    try:
        db_path = Path("webhook_results.db")
        if db_path.exists():
            db_path.unlink()
        
        queue_dir = Path("message_queue")
        if queue_dir.exists():
            for file in queue_dir.glob("*.json"):
                file.unlink()
            if not any(queue_dir.iterdir()):
                queue_dir.rmdir()
    except Exception as e:
        print(f"Cleanup warning: {e}")


@pytest.fixture(autouse=True, scope="session")
def session_setup_and_cleanup():
    """Setup and cleanup for the entire test session"""
    cleanup_test_files()
    yield
    # Uncomment to cleanup after all tests
    # cleanup_test_files()
