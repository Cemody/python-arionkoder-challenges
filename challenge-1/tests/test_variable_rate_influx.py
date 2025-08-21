"""
Challenge 1 - Variable Rate Data Influx Tests

Tests for the constraint: Must handle variable-rate data influx efficiently.
"""

import time
from fastapi.testclient import TestClient
from app import app


class TestVariableRateDataInflux:
    """Test handling of variable-rate data influx efficiently"""
    
    def test_high_frequency_small_payloads(self):
        """Test processing many small payloads rapidly"""
        with TestClient(app) as client:
            start_time = time.time()
            
            # Send 50 small requests rapidly
            for i in range(50):
                payload = {"events": [{"request_id": i, "type": "small"}]}
                response = client.post(
                    "/webhook?group_by=type",
                    headers={"Content-Type": "application/json"},
                    json=payload
                )
                assert response.status_code == 200
            
            end_time = time.time()
            processing_time = end_time - start_time
            
            # Should complete within reasonable time (adjust threshold as needed)
            assert processing_time < 10.0, f"Processing took too long: {processing_time}s"

    def test_burst_data_processing(self):
        """Test handling burst of data after quiet period"""
        with TestClient(app) as client:
            # Simulate burst: send many requests in quick succession
            burst_size = 20
            responses = []
            
            start_time = time.time()
            for i in range(burst_size):
                payload = {
                    "data": {
                        "records": [
                            {"event_type": "burst", "value": i},
                            {"event_type": "burst", "value": i + 100}
                        ]
                    }
                }
                response = client.post(
                    "/webhook?group_by=event_type&sum_field=value",
                    headers={"Content-Type": "application/json"},
                    json=payload
                )
                responses.append(response)
            
            end_time = time.time()
            
            # Verify all requests succeeded
            for response in responses:
                assert response.status_code == 200
                assert response.json()["ok"] is True
            
            # Check that burst was handled efficiently
            burst_time = end_time - start_time
            assert burst_time < 5.0, f"Burst processing took too long: {burst_time}s"

    def test_mixed_payload_sizes(self):
        """Test handling variable payload sizes efficiently"""
        with TestClient(app) as client:
            test_cases = [
                # Small payload
                {"events": [{"size": "small", "value": 1}]},
                # Medium payload
                {"events": [{"size": "medium", "value": i} for i in range(100)]},
                # Large payload  
                {"events": [{"size": "large", "value": i} for i in range(1000)]},
                # Small again
                {"events": [{"size": "small", "value": 2}]}
            ]
            
            processing_times = []
            for payload in test_cases:
                start_time = time.time()
                response = client.post(
                    "/webhook?group_by=size&sum_field=value",
                    headers={"Content-Type": "application/json"},
                    json=payload
                )
                end_time = time.time()
                
                assert response.status_code == 200
                processing_times.append(end_time - start_time)
            
            # Verify processing times scale reasonably with payload size
            # Small payloads should be much faster than large ones
            assert processing_times[0] < processing_times[2]  # small < large
            assert processing_times[3] < processing_times[2]  # small < large

    def test_interleaved_request_patterns(self):
        """Test handling interleaved patterns of different request types"""
        with TestClient(app) as client:
            # Pattern: small, large, small, medium, small, large
            patterns = [
                ("small", [{"type": "pattern", "size": "small", "id": 1}]),
                ("large", [{"type": "pattern", "size": "large", "id": i} for i in range(500)]),
                ("small", [{"type": "pattern", "size": "small", "id": 2}]),
                ("medium", [{"type": "pattern", "size": "medium", "id": i} for i in range(100)]),
                ("small", [{"type": "pattern", "size": "small", "id": 3}]),
                ("large", [{"type": "pattern", "size": "large", "id": i} for i in range(800)])
            ]
            
            results = []
            for pattern_name, events in patterns:
                payload = {"events": events}
                
                start_time = time.time()
                response = client.post(
                    "/webhook?group_by=size",
                    headers={"Content-Type": "application/json"},
                    json=payload
                )
                end_time = time.time()
                
                assert response.status_code == 200
                results.append({
                    "pattern": pattern_name,
                    "time": end_time - start_time,
                    "records": response.json()["processed_records"]
                })
            
            # Verify all patterns processed successfully
            assert len(results) == 6
            
            # Check that small requests consistently fast
            small_times = [r["time"] for r in results if r["pattern"] == "small"]
            assert all(t < 0.1 for t in small_times), "Small requests should be consistently fast"

    def test_sustained_load_performance(self):
        """Test performance under sustained variable load"""
        with TestClient(app) as client:
            # Sustained load with varying sizes
            total_requests = 30
            size_cycle = ["small", "medium", "large"]
            
            start_time = time.time()
            successful_requests = 0
            
            for i in range(total_requests):
                size_type = size_cycle[i % 3]
                
                if size_type == "small":
                    events = [{"load": "sustained", "request": i, "size": "small"}]
                elif size_type == "medium":
                    events = [{"load": "sustained", "request": i, "size": "medium"} for _ in range(20)]
                else:  # large
                    events = [{"load": "sustained", "request": i, "size": "large"} for _ in range(100)]
                
                payload = {"events": events}
                response = client.post(
                    "/webhook?group_by=size",
                    headers={"Content-Type": "application/json"},
                    json=payload
                )
                
                if response.status_code == 200:
                    successful_requests += 1
            
            end_time = time.time()
            total_time = end_time - start_time
            
            # All requests should succeed
            assert successful_requests == total_requests
            
            # Average time per request should be reasonable
            avg_time_per_request = total_time / total_requests
            assert avg_time_per_request < 0.5, f"Average time per request too high: {avg_time_per_request}s"

    def test_rapid_fire_requests(self):
        """Test handling rapid-fire requests without throttling"""
        with TestClient(app) as client:
            rapid_requests = 25
            responses = []
            
            # Send requests as fast as possible
            start_time = time.time()
            for i in range(rapid_requests):
                payload = {"events": [{"rapid": True, "sequence": i}]}
                response = client.post(
                    "/webhook?group_by=rapid",
                    headers={"Content-Type": "application/json"},
                    json=payload
                )
                responses.append((i, response))
            
            end_time = time.time()
            
            # All should succeed
            for seq, response in responses:
                assert response.status_code == 200, f"Request {seq} failed"
            
            # Should complete rapidly
            total_time = end_time - start_time
            assert total_time < 3.0, f"Rapid requests took too long: {total_time}s"
            
            # Average response time should be low
            avg_response_time = total_time / rapid_requests
            assert avg_response_time < 0.12, f"Average response time too high: {avg_response_time}s"
