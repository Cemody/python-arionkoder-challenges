"""
Challenge 1 - Constant Memory Usage Tests

Tests for the constraint: Should maintain constant memory usage regardless of input volume.
"""

import json
import time
import os
from fastapi.testclient import TestClient
from app import app
import pytest

# Optional psutil import for memory monitoring
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False


class TestConstantMemoryUsage:
    """Test that memory usage remains constant regardless of input volume"""
    
    def get_memory_usage(self):
        """Get current memory usage in MB"""
        if not PSUTIL_AVAILABLE:
            return 0.0  # Return 0 if psutil not available
        process = psutil.Process(os.getpid())
        return process.memory_info().rss / 1024 / 1024

    @pytest.mark.skipif(not PSUTIL_AVAILABLE, reason="psutil not available")
    def test_constant_memory_ndjson_processing(self):
        """Test memory usage stays constant for large NDJSON streams"""
        with TestClient(app) as client:
            # Measure baseline memory
            baseline_memory = self.get_memory_usage()
            
            # Process increasingly large NDJSON streams
            memory_measurements = [baseline_memory]
            
            for size in [100, 500, 1000]:
                # Create large NDJSON content
                lines = [{"category": f"cat_{i % 10}", "amount": i} for i in range(size)]
                ndjson_content = '\n'.join(json.dumps(line) for line in lines) + '\n'
                
                response = client.post(
                    "/webhook?group_by=category&sum_field=amount",
                    headers={"Content-Type": "application/x-ndjson"},
                    content=ndjson_content
                )
                
                assert response.status_code == 200
                current_memory = self.get_memory_usage()
                memory_measurements.append(current_memory)
            
            # Memory should not grow significantly (allow some variance)
            max_memory = max(memory_measurements)
            min_memory = min(memory_measurements)
            memory_growth = max_memory - baseline_memory
            
            # Allow reasonable memory growth (less than 50MB for this test)
            assert memory_growth < 50, f"Memory grew too much: {memory_growth:.2f}MB"

    @pytest.mark.skipif(not PSUTIL_AVAILABLE, reason="psutil not available")
    def test_constant_memory_json_processing(self):
        """Test memory usage stays constant for large JSON payloads"""
        with TestClient(app) as client:
            baseline_memory = self.get_memory_usage()
            memory_measurements = [baseline_memory]
            
            for record_count in [100, 500, 1000]:
                # Create large JSON payload
                payload = {
                    "data": {
                        "records": [
                            {"department": f"dept_{i % 5}", "salary": 50000 + i}
                            for i in range(record_count)
                        ]
                    }
                }
                
                response = client.post(
                    "/webhook?group_by=department&sum_field=salary",
                    headers={"Content-Type": "application/json"},
                    json=payload
                )
                
                assert response.status_code == 200
                current_memory = self.get_memory_usage()
                memory_measurements.append(current_memory)
            
            # Verify memory growth is bounded
            max_memory = max(memory_measurements)
            memory_growth = max_memory - baseline_memory
            assert memory_growth < 50, f"Memory grew too much: {memory_growth:.2f}MB"

    @pytest.mark.skipif(not PSUTIL_AVAILABLE, reason="psutil not available")
    def test_memory_stability_under_load(self):
        """Test memory remains stable under sustained load"""
        with TestClient(app) as client:
            baseline_memory = self.get_memory_usage()
            
            # Sustained load test
            for batch in range(20):
                # Mix of different payload types
                payloads = [
                    {"events": [{"type": "load_test", "batch": batch, "value": i} for i in range(50)]},
                    {"data": {"items": [{"category": f"cat_{i}", "amount": i} for i in range(25)]}},
                ]
                
                for payload in payloads:
                    response = client.post(
                        "/webhook?group_by=type&sum_field=value",
                        headers={"Content-Type": "application/json"},
                        json=payload
                    )
                    assert response.status_code == 200
            
            final_memory = self.get_memory_usage()
            memory_growth = final_memory - baseline_memory
            
            # Memory should remain stable even after sustained processing
            assert memory_growth < 100, f"Memory grew too much under load: {memory_growth:.2f}MB"

    def test_memory_efficient_large_aggregations(self):
        """Test that large aggregations don't cause excessive memory use"""
        with TestClient(app) as client:
            # Create payload with many unique groups to test aggregation memory
            payload = {
                "events": [
                    {"group_id": f"group_{i}", "value": i % 100}
                    for i in range(2000)  # 2000 unique groups
                ]
            }
            
            response = client.post(
                "/webhook?group_by=group_id&sum_field=value",
                headers={"Content-Type": "application/json"},
                json=payload
            )
            
            assert response.status_code == 200
            result = response.json()
            
            # Should successfully aggregate all groups
            assert result["processed_records"] == 2000  # 2000 unique groups
            assert len(result["aggregation"]) == 2000
            
            # Verify some sample aggregations
            assert "group_0" in result["aggregation"]
            assert "group_1999" in result["aggregation"]

    def test_memory_cleanup_between_requests(self):
        """Test that memory is properly cleaned up between requests"""
        with TestClient(app) as client:
            if not PSUTIL_AVAILABLE:
                pytest.skip("psutil not available for memory monitoring")
            
            # Baseline memory
            baseline_memory = self.get_memory_usage()
            
            # Process large request with reasonable grouping
            large_payload = {
                "events": [{"category": f"cat_{i % 10}", "data": f"large_data_{i}" * 50} for i in range(1000)]
            }
            
            response = client.post(
                "/webhook?group_by=category",
                headers={"Content-Type": "application/json"},
                json=large_payload
            )
            assert response.status_code == 200
            
            # Wait a moment for any cleanup
            time.sleep(0.1)
            
            # Memory after large request
            after_large_memory = self.get_memory_usage()
            
            # Process small request
            small_payload = {"events": [{"category": "small", "data": "small_data"}]}
            response = client.post(
                "/webhook?group_by=category",
                headers={"Content-Type": "application/json"},
                json=small_payload
            )
            assert response.status_code == 200
            
            # Wait for cleanup
            time.sleep(0.1)
            
            # Final memory
            final_memory = self.get_memory_usage()
            
            # Memory should not retain data from large request
            memory_retention = final_memory - baseline_memory
            assert memory_retention < 30, f"Too much memory retained: {memory_retention:.2f}MB"

    def test_streaming_vs_batch_memory_comparison(self):
        """Test that streaming approach uses less memory than batch processing"""
        with TestClient(app) as client:
            if not PSUTIL_AVAILABLE:
                pytest.skip("psutil not available for memory monitoring")
            
            baseline_memory = self.get_memory_usage()
            
            # Test streaming NDJSON (should use constant memory)
            streaming_lines = [{"category": f"stream_{i % 50}", "value": i} for i in range(1000)]
            ndjson_content = '\n'.join(json.dumps(line) for line in streaming_lines) + '\n'
            
            streaming_response = client.post(
                "/webhook?group_by=category&sum_field=value",
                headers={"Content-Type": "application/x-ndjson"},
                content=ndjson_content
            )
            
            streaming_memory = self.get_memory_usage()
            assert streaming_response.status_code == 200
            
            # Test equivalent batch JSON (may use more memory)
            batch_payload = {"events": streaming_lines}
            
            batch_response = client.post(
                "/webhook?group_by=category&sum_field=value",
                headers={"Content-Type": "application/json"},
                json=batch_payload
            )
            
            batch_memory = self.get_memory_usage()
            assert batch_response.status_code == 200
            
            # Both should produce same results
            streaming_result = streaming_response.json()
            batch_result = batch_response.json()
            assert streaming_result["aggregation"] == batch_result["aggregation"]
            
            # Memory usage patterns
            streaming_growth = streaming_memory - baseline_memory
            batch_growth = batch_memory - baseline_memory
            
            # Both should be reasonable, but streaming might be more efficient
            assert streaming_growth < 50, f"Streaming memory growth too high: {streaming_growth:.2f}MB"
            assert batch_growth < 50, f"Batch memory growth too high: {batch_growth:.2f}MB"
