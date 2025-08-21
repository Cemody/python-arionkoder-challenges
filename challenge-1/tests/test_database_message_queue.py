"""
Challenge 1 - Database and Message Queue Integration Tests

Tests for the core feature: Outputs results to both a database and a message queue.
"""

from pathlib import Path
from fastapi.testclient import TestClient
from app import app
import pytest


class TestDatabaseAndMessageQueueIntegration:
    """Test database and message queue output functionality"""
    
    def test_database_storage(self):
        """Test that webhook results are stored in database"""
        with TestClient(app) as client:
            payload = {"events": [{"category": "test_db"}, {"category": "test_db"}]}
            
            response = client.post(
                "/webhook?group_by=category",
                headers={"Content-Type": "application/json"},
                json=payload
            )
            
            assert response.status_code == 200
            
            # Verify database was created and contains data
            db_path = Path("webhook_results.db")
            assert db_path.exists()
            
            # Check results endpoint
            results_response = client.get("/results?limit=5")
            assert results_response.status_code == 200
            results_data = results_response.json()
            assert results_data["ok"] is True
            assert len(results_data["results"]) > 0

    def test_message_queue_publishing(self):
        """Test that webhook results are published to message queue"""
        with TestClient(app) as client:
            payload = {"events": [{"category": "test_queue"}, {"category": "test_queue"}]}
            
            response = client.post(
                "/webhook?group_by=category",
                headers={"Content-Type": "application/json"},
                json=payload
            )
            
            assert response.status_code == 200
            
            # Verify message queue directory exists with messages
            queue_dir = Path("message_queue")
            assert queue_dir.exists()
            message_files = list(queue_dir.glob("*.json"))
            assert len(message_files) > 0
            
            # Check messages endpoint
            messages_response = client.get("/messages?limit=5")
            assert messages_response.status_code == 200
            messages_data = messages_response.json()
            assert messages_data["ok"] is True
            assert len(messages_data["messages"]) > 0

    def test_concurrent_database_and_queue_operations(self):
        """Test that database and queue operations work concurrently"""
        with TestClient(app) as client:
            # Send multiple requests to test concurrent operations
            for i in range(3):
                payload = {"events": [{"batch": f"batch_{i}"}, {"batch": f"batch_{i}"}]}
                response = client.post(
                    "/webhook?group_by=batch",
                    headers={"Content-Type": "application/json"},
                    json=payload
                )
                assert response.status_code == 200
            
            # Verify both storage mechanisms have data
            results_response = client.get("/results?limit=10")
            messages_response = client.get("/messages?limit=10")
            
            assert results_response.status_code == 200
            assert messages_response.status_code == 200
            assert len(results_response.json()["results"]) >= 3
            assert len(messages_response.json()["messages"]) >= 3

    def test_data_persistence_across_requests(self):
        """Test that data persists correctly across multiple webhook requests"""
        with TestClient(app) as client:
            # Send first batch
            payload1 = {"events": [{"source": "batch1", "count": 5}]}
            response1 = client.post(
                "/webhook?group_by=source",
                headers={"Content-Type": "application/json"},
                json=payload1
            )
            assert response1.status_code == 200
            
            # Send second batch
            payload2 = {"events": [{"source": "batch2", "count": 3}]}
            response2 = client.post(
                "/webhook?group_by=source",
                headers={"Content-Type": "application/json"},
                json=payload2
            )
            assert response2.status_code == 200
            
            # Verify both are stored
            results_response = client.get("/results?limit=10")
            assert results_response.status_code == 200
            results = results_response.json()["results"]
            
            # Should have at least 2 entries
            assert len(results) >= 2
            
            # Verify different source values exist
            sources = [r["aggregation"] for r in results if r["aggregation"]]
            assert any("batch1" in str(s) for s in sources)
            assert any("batch2" in str(s) for s in sources)

    def test_database_result_structure(self):
        """Test that database results have correct structure"""
        with TestClient(app) as client:
            payload = {"events": [{"category": "structure_test"}]}
            
            response = client.post(
                "/webhook?group_by=category",
                headers={"Content-Type": "application/json"},
                json=payload
            )
            assert response.status_code == 200
            
            # Check database result structure
            results_response = client.get("/results?limit=1")
            assert results_response.status_code == 200
            
            result = results_response.json()["results"][0]
            
            # Verify all required fields exist
            required_fields = ["timestamp", "group_by_field", "aggregation", "processed_records", "created_at"]
            for field in required_fields:
                assert field in result
            
            # Verify data types
            assert isinstance(result["processed_records"], int)
            assert isinstance(result["aggregation"], dict)

    def test_message_queue_result_structure(self):
        """Test that message queue results have correct structure"""
        with TestClient(app) as client:
            payload = {"events": [{"category": "queue_structure_test"}]}
            
            response = client.post(
                "/webhook?group_by=category",
                headers={"Content-Type": "application/json"},
                json=payload
            )
            assert response.status_code == 200
            
            # Check message queue result structure
            messages_response = client.get("/messages?limit=1")
            assert messages_response.status_code == 200
            
            message = messages_response.json()["messages"][0]
            
            # Verify all required fields exist
            required_fields = ["id", "timestamp", "payload", "status"]
            for field in required_fields:
                assert field in message
            
            # Verify data types and values
            assert isinstance(message["payload"], dict)
            assert message["status"] == "published"
            assert message["id"].startswith("msg_")


# Cleanup function for test artifacts
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


# Pytest fixture for setup and cleanup
@pytest.fixture(autouse=True)
def setup_and_cleanup():
    """Setup and cleanup for each test"""
    cleanup_test_files()
    yield
    # Uncomment to cleanup after each test
    # cleanup_test_files()
