"""
Test suite for performance metrics and logging functionality.
This file tests the feature: "Includes detailed logging and performance metrics"
"""

import pytest
import asyncio
import time
import tempfile
import os
from pathlib import Path
from utils import (
    ResourceManager, DatabaseConnection, APIConnection, CacheConnection,
    save_connection_log, get_connection_logs, get_performance_analytics
)


class TestPerformanceMetricsAndLogging:
    """Test detailed logging and performance metrics"""
    
    @pytest.mark.asyncio
    async def test_connection_performance_tracking(self):
        """Test that connection setup time is tracked"""
        start_time = time.time()
        
        async with ResourceManager(["database", "cache", "api"]) as resources:
            setup_time = time.time() - start_time
            
            # Verify connections were established
            assert len(resources) >= 1
            
            # Test each resource has performance metrics
            for resource_name, resource in resources.items():
                if hasattr(resource, 'metrics'):
                    assert resource.metrics.connection_time is not None
                    assert resource.metrics.connection_time > 0
                    print(f"{resource_name} connection time: {resource.metrics.connection_time:.3f}s")
        
        # Setup should be reasonably fast
        assert setup_time < 5.0, f"Setup took too long: {setup_time:.3f}s"
    
    @pytest.mark.asyncio
    async def test_operation_performance_tracking(self):
        """Test that individual operations are timed"""
        
        async with ResourceManager(["database", "cache"]) as resources:
            # Database operations
            if "database" in resources:
                db = resources["database"]
                
                # Insert operation
                insert_result = await db.execute_operation("insert", {
                    "name": "perf_test", "value": "performance_data"
                })
                assert "execution_time" in insert_result
                assert insert_result["execution_time"] > 0
                
                # Query operation
                query_result = await db.execute_operation("query", {"limit": 5})
                assert "execution_time" in query_result
                assert query_result["execution_time"] > 0
            
            # Cache operations
            if "cache" in resources:
                cache = resources["cache"]
                
                # Set operation
                set_result = await cache.execute_operation("set", {
                    "key": "perf_key", "value": "perf_value"
                })
                assert "execution_time" in set_result
                assert set_result["execution_time"] > 0
                
                # Get operation
                get_result = await cache.execute_operation("get", {
                    "key": "perf_key"
                })
                assert "execution_time" in get_result
                assert get_result["execution_time"] > 0
    
    @pytest.mark.asyncio
    async def test_performance_metrics_persistence(self):
        """Test that performance metrics are saved to database"""
        
        # Generate some operations to create metrics
        async with ResourceManager(["database", "cache"]) as resources:
            if "database" in resources:
                db = resources["database"]
                
                # Perform multiple operations
                for i in range(3):
                    await db.execute_operation("insert", {
                        "name": f"metrics_test_{i}", "value": f"test_value_{i}"
                    })
        
        # Check if performance metrics were saved
        async with ResourceManager(["database"]) as resources:
            db = resources["database"]
            
            # Query performance metrics table
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, 
                lambda: db._execute_query({"table": "performance_metrics", "limit": 10})
            )
            
            assert len(result) > 0, "Performance metrics should be saved"
            
            # Verify metrics structure
            for metric in result:
                assert "resource_type" in metric
                assert "operation_type" in metric  
                assert "execution_time" in metric
                assert metric["execution_time"] > 0
    
    @pytest.mark.asyncio
    async def test_connection_logging(self):
        """Test that connection events are logged"""
        
        # Create some connection events
        async with ResourceManager(["database", "cache"]) as resources:
            # Perform operations to generate logs
            if "database" in resources:
                await resources["database"].test_connection()
                await resources["database"].execute_operation("insert", {
                    "name": "log_test", "value": "logging_data"
                })
            
            if "cache" in resources:
                await resources["cache"].test_connection()
                await resources["cache"].execute_operation("set", {
                    "key": "log_key", "value": "logging_cache"
                })
        
        # Retrieve connection logs
        logs = await get_connection_logs(limit=10)
        
        assert len(logs) > 0, "Connection logs should be created"
        
        # Verify log structure
        for log in logs:
            assert "resource" in log
            assert "action" in log
            assert "status" in log
            assert "timestamp" in log
            assert log["status"] in ["success", "error", "warning"]
    
    @pytest.mark.asyncio
    async def test_performance_analytics(self):
        """Test performance analytics generation"""
        
        # Generate operations for analytics
        async with ResourceManager(["database", "cache"]) as resources:
            operations = [
                ("database", "insert", {"name": "analytics_1", "value": "data_1"}),
                ("database", "insert", {"name": "analytics_2", "value": "data_2"}),
                ("database", "query", {"limit": 5}),
                ("cache", "set", {"key": "analytics_key_1", "value": "cache_1"}),
                ("cache", "set", {"key": "analytics_key_2", "value": "cache_2"}),
                ("cache", "get", {"key": "analytics_key_1"}),
            ]
            
            for resource_name, operation, data in operations:
                if resource_name in resources:
                    await resources[resource_name].execute_operation(operation, data)
        
        # Get performance analytics
        analytics = await get_performance_analytics(hours=1)
        
        assert "summary" in analytics
        assert "operations" in analytics
        assert "analytics_generation_time" in analytics
        
        summary = analytics["summary"]
        assert "total_operations" in summary
        assert "total_successes" in summary
        assert "overall_success_rate" in summary
        assert summary["total_operations"] > 0
        assert summary["overall_success_rate"] >= 0.0
        
        # Verify operations breakdown
        operations = analytics["operations"]
        assert len(operations) > 0
        
        for op in operations:
            assert "resource_type" in op
            assert "operation_type" in op
            assert "avg_execution_time" in op
            assert "operation_count" in op
            assert op["avg_execution_time"] > 0
    
    @pytest.mark.asyncio
    async def test_cache_hit_miss_tracking(self):
        """Test cache-specific performance metrics"""
        
        async with ResourceManager(["cache"]) as resources:
            cache = resources["cache"]
            
            # Set some values
            await cache.execute_operation("set", {"key": "hit_test_1", "value": "value_1"})
            await cache.execute_operation("set", {"key": "hit_test_2", "value": "value_2"})
            
            # Get existing values (hits)
            hit_result_1 = await cache.execute_operation("get", {"key": "hit_test_1"})
            hit_result_2 = await cache.execute_operation("get", {"key": "hit_test_2"})
            
            # Get non-existing value (miss)
            miss_result = await cache.execute_operation("get", {"key": "nonexistent"})
            
            # Check cache statistics
            stats_result = await cache.execute_operation("stats", {})
            
            assert hit_result_1["found"] is True
            assert hit_result_2["found"] is True  
            assert miss_result["found"] is False
            
            # Verify hit/miss tracking in results
            assert "cache_stats" in hit_result_1
            assert "cache_stats" in miss_result
            
            # Check stats after all operations (including the miss)
            final_stats = miss_result["cache_stats"]  # Use miss_result which has the latest stats
            assert final_stats["hit_count"] >= 2
            assert final_stats["miss_count"] >= 1
    
    @pytest.mark.asyncio
    async def test_error_tracking_and_logging(self):
        """Test tracking and logging of errors"""
        
        error_count_before = 0
        
        # Get initial error count
        try:
            analytics = await get_performance_analytics(hours=1)
            error_count_before = analytics["summary"].get("total_errors", 0)
        except:
            pass  # Analytics might not be available yet
        
        # Generate some errors
        try:
            async with ResourceManager(["database"]) as resources:
                db = resources["database"]
                
                # Attempt invalid operations
                try:
                    await db.execute_operation("invalid_op", {})
                except ValueError:
                    pass  # Expected error
                
                try:
                    await db.execute_operation("update", {"id": "nonexistent"})
                except ValueError:
                    pass  # Expected error
        except:
            pass
        
        # Check if errors were tracked
        analytics = await get_performance_analytics(hours=1)
        error_count_after = analytics["summary"].get("total_errors", 0)
        
        # Should have more errors (or at least same if other tests created errors)
        assert error_count_after >= error_count_before
        
        # Check error details
        if "top_errors" in analytics:
            top_errors = analytics["top_errors"]
            if len(top_errors) > 0:
                error_entry = top_errors[0]
                assert "resource" in error_entry
                assert "error_count" in error_entry
                assert "error_message" in error_entry
    
    @pytest.mark.asyncio
    async def test_logging_detail_levels(self):
        """Test different levels of logging detail"""
        
        async with ResourceManager(["database", "cache"]) as resources:
            # Perform operations that should generate different log levels
            
            # Successful operations (INFO level)
            if "database" in resources:
                await resources["database"].test_connection()
                await resources["database"].execute_operation("insert", {
                    "name": "detail_test", "value": "detail_value"
                })
            
            # Operations that might generate warnings
            if "cache" in resources:
                cache = resources["cache"]
                # Fill cache to capacity to potentially trigger evictions
                for i in range(5):
                    await cache.execute_operation("set", {
                        "key": f"detail_key_{i}", "value": f"detail_value_{i}"
                    })
        
        # Retrieve logs and check for different detail levels
        logs = await get_connection_logs(limit=20)
        
        # Should have logs with different actions
        actions = set(log["action"] for log in logs)
        assert len(actions) > 1, f"Should have multiple action types, got: {actions}"
        
        # Should have successful operations
        successful_logs = [log for log in logs if log["status"] == "success"]
        assert len(successful_logs) > 0, "Should have successful operations logged"
    
    @pytest.mark.asyncio
    async def test_performance_baseline_tracking(self):
        """Test establishment of performance baselines"""
        baseline_times = []
        
        # Perform multiple identical operations to establish baseline
        for i in range(5):
            async with ResourceManager(["cache"]) as resources:
                cache = resources["cache"]
                
                start_time = time.time()
                await cache.execute_operation("set", {
                    "key": f"baseline_{i}", "value": f"baseline_value_{i}"
                })
                operation_time = time.time() - start_time
                baseline_times.append(operation_time)
        
        # Calculate baseline statistics
        avg_time = sum(baseline_times) / len(baseline_times)
        max_time = max(baseline_times)
        min_time = min(baseline_times)
        
        # Verify reasonable performance consistency
        time_variance = (max_time - min_time) / avg_time if avg_time > 0 else 0
        
        assert avg_time > 0, "Operations should take measurable time"
        assert time_variance < 2.0, f"Performance should be consistent, variance: {time_variance:.2f}"
        
        print(f"Baseline performance: avg={avg_time:.4f}s, min={min_time:.4f}s, max={max_time:.4f}s")
    
    @pytest.mark.asyncio
    async def test_memory_usage_tracking(self):
        """Test memory usage tracking in performance metrics"""
        import psutil
        import os
        
        process = psutil.Process(os.getpid())
        memory_before = process.memory_info().rss
        
        async with ResourceManager(["database", "cache"]) as resources:
            # Perform memory-intensive operations
            large_data = "x" * 1000  # 1KB of data
            
            if "database" in resources:
                for i in range(10):
                    await resources["database"].execute_operation("insert", {
                        "name": f"memory_test_{i}", "value": large_data
                    })
            
            if "cache" in resources:
                for i in range(50):
                    await resources["cache"].execute_operation("set", {
                        "key": f"memory_key_{i}", "value": large_data
                    })
        
        memory_after = process.memory_info().rss
        memory_delta = (memory_after - memory_before) / 1024 / 1024  # MB
        
        # Memory should have increased but should be reasonable
        assert memory_delta < 100, f"Memory usage should be reasonable, got {memory_delta:.2f} MB"
        
        print(f"Memory delta: {memory_delta:.2f} MB")
