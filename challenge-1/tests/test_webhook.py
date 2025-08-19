import json
import pytest
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

def test_json_array_count():
    """Test JSON endpoint with count aggregation"""
    with TestClient(app) as client:
        payload = {"events":[{"category":"x"},{"category":"x"},{"category":"y"}]}
        r = client.post("/webhook?group_by=category",
                              headers={"Content-Type":"application/json"},
                              json=payload)
        assert r.status_code == 200
        assert r.json()["aggregation"] == {"x": 2.0, "y": 1.0}
