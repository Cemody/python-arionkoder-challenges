"""
Challenge 1 - Streaming Data Processing Tests

Tests for the core feature: Processes a stream of incoming JSON data from a webhook
and transforms/aggregates the data using generators and iterators.
"""

import json
from pathlib import Path
from fastapi.testclient import TestClient
from app import app
import pytest


class TestStreamingDataProcessing:
    """Test streaming JSON data processing capabilities"""
    
    def test_ndjson_streaming_aggregation(self):
        """Test NDJSON streaming with sum aggregation"""
        with TestClient(app) as client:
            lines = [
                {"category": "electronics", "amount": 100},
                {"category": "books", "amount": 25}, 
                {"category": "electronics", "amount": 75},
                {"category": "books", "amount": 15}
            ]
            
            ndjson_content = '\n'.join(json.dumps(line) for line in lines) + '\n'
            
            response = client.post(
                "/webhook?group_by=category&sum_field=amount",
                headers={"Content-Type": "application/x-ndjson"},
                content=ndjson_content
            )
            
            assert response.status_code == 200
            result = response.json()
            assert result["ok"] is True
            assert result["aggregation"] == {"electronics": 175.0, "books": 40.0}
            assert result["processed_records"] == 2
            assert "timestamp" in result
            assert "processing_time_ms" in result

    def test_json_streaming_count_aggregation(self):
        """Test JSON streaming with count aggregation"""
        with TestClient(app) as client:
            payload = {
                "events": [
                    {"department": "engineering"},
                    {"department": "sales"},
                    {"department": "engineering"},
                    {"department": "marketing"},
                    {"department": "engineering"}
                ]
            }
            
            response = client.post(
                "/webhook?group_by=department",
                headers={"Content-Type": "application/json"},
                json=payload
            )
            
            assert response.status_code == 200
            result = response.json()
            assert result["aggregation"] == {"engineering": 3.0, "sales": 1.0, "marketing": 1.0}
            assert result["processed_records"] == 3

    def test_data_transformation_with_projection(self):
        """Test data transformation using field projection"""
        with TestClient(app) as client:
            lines = [
                {"name": "John", "age": 30, "department": "engineering", "salary": 80000, "secret": "hidden"},
                {"name": "Jane", "age": 25, "department": "sales", "salary": 70000, "secret": "hidden"},
                {"name": "Bob", "age": 35, "department": "engineering", "salary": 90000, "secret": "hidden"}
            ]
            
            ndjson_content = '\n'.join(json.dumps(line) for line in lines) + '\n'
            
            response = client.post(
                "/webhook?group_by=department&sum_field=salary&include=name,department,salary",
                headers={"Content-Type": "application/x-ndjson"},
                content=ndjson_content
            )
            
            assert response.status_code == 200
            result = response.json()
            assert result["aggregation"] == {"engineering": 170000.0, "sales": 70000.0}

    def test_nested_json_record_extraction(self):
        """Test extraction of records from nested JSON structures"""
        with TestClient(app) as client:
            payload = {
                "metadata": {"source": "api", "version": "1.0"},
                "data": {
                    "items": [
                        {"type": "order", "value": 100},
                        {"type": "refund", "value": 25},
                        {"type": "order", "value": 150}
                    ]
                }
            }
            
            response = client.post(
                "/webhook?group_by=type&sum_field=value",
                headers={"Content-Type": "application/json"},
                json=payload
            )
            
            assert response.status_code == 200
            result = response.json()
            assert result["aggregation"] == {"order": 250.0, "refund": 25.0}

    def test_multiple_nested_collections(self):
        """Test processing multiple nested collection types"""
        with TestClient(app) as client:
            payload = {
                "events": [
                    {"category": "A", "value": 10},
                    {"category": "B", "value": 20}
                ],
                "data": {
                    "records": [
                        {"category": "A", "value": 15},
                        {"category": "C", "value": 30}
                    ]
                },
                "items": [
                    {"category": "B", "value": 25}
                ]
            }
            
            response = client.post(
                "/webhook?group_by=category&sum_field=value",
                headers={"Content-Type": "application/json"},
                json=payload
            )
            
            assert response.status_code == 200
            result = response.json()
            # Should aggregate across all nested collections
            expected = {"A": 25.0, "B": 45.0, "C": 30.0}  # A: 10+15, B: 20+25, C: 30
            assert result["aggregation"] == expected


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
