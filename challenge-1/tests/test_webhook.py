import json
from pathlib import Path
from fastapi.testclient import TestClient
from app import app  # import your FastAPI app

def test_ndjson_sum():
    """Test NDJSON endpoint with sum aggregation"""
    with TestClient(app) as client:
        lines = [
            {"category":"a","amount":10},
            {"category":"b","amount":5},
            {"category":"a","amount":7},
        ]
        
        # Create NDJSON content
        ndjson_content = ""
        for line in lines:
            ndjson_content += json.dumps(line) + "\n"
        
        r = client.post(
            "/webhook?group_by=category&sum_field=amount",
            headers={"Content-Type": "application/x-ndjson"},
            content=ndjson_content
        )
        assert r.status_code == 200
        assert r.json()["aggregation"] == {"a": 17.0, "b": 5.0}
        # Check new fields
        assert "timestamp" in r.json()
        assert r.json()["processed_records"] == 2

def test_json_array_count():
    """Test JSON endpoint with count aggregation"""
    with TestClient(app) as client:
        payload = {"events":[{"category":"x"},{"category":"x"},{"category":"y"}]}
        r = client.post("/webhook?group_by=category",
                              headers={"Content-Type":"application/json"},
                              json=payload)
        assert r.status_code == 200
        assert r.json()["aggregation"] == {"x": 2.0, "y": 1.0}
        # Check new fields
        assert "timestamp" in r.json()
        assert r.json()["processed_records"] == 2

def test_database_and_queue_integration():
    """Test that results are saved to database and message queue"""
    with TestClient(app) as client:
        # Send a webhook request
        payload = {"events":[{"category":"test"},{"category":"test"}]}
        r = client.post("/webhook?group_by=category",
                              headers={"Content-Type":"application/json"},
                              json=payload)
        assert r.status_code == 200
        
        # Check that database was created and contains data
        db_path = Path("webhook_results.db")
        assert db_path.exists()
        
        # Check that message queue directory was created
        queue_dir = Path("message_queue")
        assert queue_dir.exists()
        assert len(list(queue_dir.glob("*.json"))) > 0

def test_results_endpoint():
    """Test the /results endpoint"""
    with TestClient(app) as client:
        r = client.get("/results")
        assert r.status_code == 200
        assert "results" in r.json()
        assert "count" in r.json()

def test_messages_endpoint():
    """Test the /messages endpoint"""
    with TestClient(app) as client:
        r = client.get("/messages")
        assert r.status_code == 200
        assert "messages" in r.json()
        assert "count" in r.json()

def test_status_endpoint():
    """Test the /status endpoint"""
    with TestClient(app) as client:
        r = client.get("/status")
        assert r.status_code == 200
        assert r.json()["status"] == "running"
        assert "recent_activity" in r.json()

# Cleanup function to remove test files
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
            queue_dir.rmdir()
    except Exception as e:
        print(f"Cleanup warning: {e}")

# Run cleanup after tests (you might want to call this manually if needed)
# cleanup_test_files()
