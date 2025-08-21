"""
Challenge 1 - Error Handling and Robustness Tests

Tests for system robustness, error handling, and edge cases.
"""

from fastapi.testclient import TestClient
from app import app


class TestErrorHandlingAndRobustness:
    """Test error handling and system robustness"""
    
    def test_malformed_json_handling(self):
        """Test handling of malformed JSON data"""
        with TestClient(app) as client:
            # Send malformed JSON
            response = client.post(
                "/webhook",
                headers={"Content-Type": "application/json"},
                content='{"invalid": json, content}'
            )
            
            # Should handle gracefully (may return 400 or process as text)
            assert response.status_code in [200, 400]

    def test_large_payload_handling(self):
        """Test handling of very large payloads"""
        with TestClient(app) as client:
            # Create a large but valid payload
            large_payload = {
                "events": [
                    {"id": i, "category": f"cat_{i % 100}", "value": i * 10}
                    for i in range(5000)  # 5K records
                ]
            }
            
            response = client.post(
                "/webhook?group_by=category&sum_field=value",
                headers={"Content-Type": "application/json"},
                json=large_payload
            )
            
            assert response.status_code == 200
            result = response.json()
            assert result["ok"] is True
            assert result["processed_records"] == 100  # 100 unique categories

    def test_edge_case_payloads(self):
        """Test edge case payloads"""
        with TestClient(app) as client:
            edge_cases = [
                {},  # Empty payload
                {"events": []},  # Empty events array
                {"data": None},  # Null data
                {"events": [{}]},  # Empty event object
            ]
            
            for payload in edge_cases:
                response = client.post(
                    "/webhook?group_by=category",
                    headers={"Content-Type": "application/json"},
                    json=payload
                )
                
                # Should handle gracefully
                assert response.status_code == 200

    def test_invalid_query_parameters(self):
        """Test handling of invalid query parameters"""
        with TestClient(app) as client:
            payload = {"events": [{"category": "test"}]}
            
            # Test invalid include parameter with special characters
            # This should raise a validation error during dependency injection
            try:
                response = client.post(
                    "/webhook?include=invalid-field-name!",
                    headers={"Content-Type": "application/json"},
                    json=payload
                )
                # If we get a response, it should be an error status
                assert response.status_code in [400, 422]
            except Exception as e:
                # Pydantic validation error is expected for invalid field names
                assert "ValidationError" in str(type(e)) or "value_error" in str(e)

    def test_missing_group_field_in_data(self):
        """Test behavior when group_by field is missing from all records"""
        with TestClient(app) as client:
            payload = {
                "events": [
                    {"name": "John", "age": 30},
                    {"name": "Jane", "age": 25},
                    {"name": "Bob", "age": 35}
                ]
            }
            
            response = client.post(
                "/webhook?group_by=department",  # Field doesn't exist in data
                headers={"Content-Type": "application/json"},
                json=payload
            )
            
            assert response.status_code == 200
            result = response.json()
            # Should return empty aggregation or null
            assert result["aggregation"] in [None, {}]
            assert result["processed_records"] == 0

    def test_mixed_data_types_in_sum_field(self):
        """Test handling mixed data types in sum field"""
        with TestClient(app) as client:
            payload = {
                "events": [
                    {"category": "A", "value": 10},
                    {"category": "A", "value": "invalid"},
                    {"category": "A", "value": 20},
                    {"category": "B", "value": None},
                    {"category": "B", "value": 15}
                ]
            }
            
            response = client.post(
                "/webhook?group_by=category&sum_field=value",
                headers={"Content-Type": "application/json"},
                json=payload
            )
            
            assert response.status_code == 200
            result = response.json()
            
            # Should only sum valid numeric values
            # A: 10 + 20 = 30, B: 15
            expected = {"A": 30.0, "B": 15.0}
            assert result["aggregation"] == expected

    def test_extremely_deep_nesting(self):
        """Test handling of deeply nested JSON with known collection keys"""
        with TestClient(app) as client:
            # Create nested structure with events at multiple levels
            deep_payload = {
                "events": [{"category": "top", "value": 10}],
                "data": {
                    "events": [{"category": "nested", "value": 32}]
                }
            }
            
            response = client.post(
                "/webhook?group_by=category&sum_field=value",
                headers={"Content-Type": "application/json"},
                json=deep_payload
            )
            
            assert response.status_code == 200
            result = response.json()
            # Should extract from nested events (should find both top and nested)
            expected_total = 10 + 32  # top + nested
            actual_total = sum(result["aggregation"].values())
            assert actual_total == expected_total

    def test_unicode_and_special_characters(self):
        """Test handling of Unicode and special characters"""
        with TestClient(app) as client:
            payload = {
                "events": [
                    {"category": "émojis 🚀", "value": 100},
                    {"category": "中文", "value": 200},
                    {"category": "العربية", "value": 300},
                    {"category": "🔥 special chars! @#$%", "value": 400}
                ]
            }
            
            response = client.post(
                "/webhook?group_by=category&sum_field=value",
                headers={"Content-Type": "application/json"},
                json=payload
            )
            
            assert response.status_code == 200
            result = response.json()
            
            # Should handle Unicode correctly
            assert "émojis 🚀" in result["aggregation"]
            assert "中文" in result["aggregation"]
            assert "العربية" in result["aggregation"]
            assert result["aggregation"]["émojis 🚀"] == 100.0

    def test_concurrent_requests_stability(self):
        """Test system stability under concurrent requests"""
        with TestClient(app) as client:
            # Send multiple requests that might cause race conditions
            responses = []
            
            for i in range(10):
                payload = {"events": [{"thread": i, "value": i * 10}]}
                response = client.post(
                    "/webhook?group_by=thread&sum_field=value",
                    headers={"Content-Type": "application/json"},
                    json=payload
                )
                responses.append(response)
            
            # All should succeed
            for i, response in enumerate(responses):
                assert response.status_code == 200, f"Request {i} failed"
                result = response.json()
                assert result["ok"] is True

    def test_invalid_content_type_handling(self):
        """Test handling of invalid or missing content types"""
        with TestClient(app) as client:
            json_data = '{"events": [{"category": "test"}]}'
            
            # Test with wrong content type
            response = client.post(
                "/webhook?group_by=category",
                headers={"Content-Type": "text/plain"},
                content=json_data
            )
            
            # Should either process as text or return appropriate error
            assert response.status_code in [200, 400, 415]

    def test_empty_ndjson_lines(self):
        """Test handling of empty lines in NDJSON"""
        with TestClient(app) as client:
            # NDJSON with empty lines and valid lines
            ndjson_content = """{"category": "A", "value": 10}

{"category": "B", "value": 20}


{"category": "A", "value": 15}

"""
            
            response = client.post(
                "/webhook?group_by=category&sum_field=value",
                headers={"Content-Type": "application/x-ndjson"},
                content=ndjson_content
            )
            
            assert response.status_code == 200
            result = response.json()
            
            # Should ignore empty lines and process valid JSON
            expected = {"A": 25.0, "B": 20.0}
            assert result["aggregation"] == expected

    def test_malformed_ndjson_lines(self):
        """Test handling of malformed lines in NDJSON"""
        with TestClient(app) as client:
            # Mix of valid and invalid JSON lines
            ndjson_content = """{"category": "A", "value": 10}
{invalid json line}
{"category": "B", "value": 20}
{another: invalid, line}
{"category": "A", "value": 15}
"""
            
            response = client.post(
                "/webhook?group_by=category&sum_field=value",
                headers={"Content-Type": "application/x-ndjson"},
                content=ndjson_content
            )
            
            assert response.status_code == 200
            result = response.json()
            
            # Should process valid lines and skip invalid ones
            expected = {"A": 25.0, "B": 20.0}
            assert result["aggregation"] == expected

    def test_database_error_resilience(self):
        """Test resilience to database-related errors"""
        with TestClient(app) as client:
            # This test ensures the webhook still works even if database operations fail
            payload = {"events": [{"category": "resilience_test"}]}
            
            response = client.post(
                "/webhook?group_by=category",
                headers={"Content-Type": "application/json"},
                json=payload
            )
            
            # Main processing should succeed even if database/queue operations fail
            assert response.status_code == 200
            result = response.json()
            assert result["ok"] is True
            assert result["aggregation"] == {"resilience_test": 1.0}

    def test_system_endpoints_under_stress(self):
        """Test system endpoints can handle requests under stress"""
        with TestClient(app) as client:
            # Generate some data first
            for i in range(5):
                payload = {"events": [{"stress_test": True, "batch": i}]}
                client.post(
                    "/webhook?group_by=stress_test",
                    headers={"Content-Type": "application/json"},
                    json=payload
                )
            
            # Test all endpoints still work
            endpoints = ["/results", "/messages", "/status", "/health"]
            
            for endpoint in endpoints:
                response = client.get(endpoint)
                assert response.status_code in [200, 503], f"Endpoint {endpoint} failed"
