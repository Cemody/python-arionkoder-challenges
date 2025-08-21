"""
Test suite for nested context managers functionality.
This file tests the constraint: "Must support nested context managers"
"""

import pytest
import asyncio
import time
from utils import ResourceManager, DatabaseConnection, APIConnection, CacheConnection


class TestNestedContextManagers:
    """Test nested context manager support"""
    
    @pytest.mark.asyncio
    async def test_basic_nested_contexts(self):
        """Test basic nested context manager functionality"""
        outer_resources = ["database", "cache"]
        inner_resources = ["api"]

        # Track context states
        outer_connections = None
        inner_connections = None

        async with ResourceManager(outer_resources) as outer_ctx:
            outer_connections = list(outer_ctx.keys())
            assert len(outer_connections) >= 1, "Outer context should establish connections"

            # Test outer context operations
            if "database" in outer_ctx:
                db_result = await outer_ctx["database"].test_connection()
                assert "database_file" in db_result

                # Create nested context
            async with ResourceManager(inner_resources) as inner_ctx:
                inner_connections = list(inner_ctx.keys())
                assert len(inner_connections) >= 1, "Inner context should establish connections"

                # Test inner context operations (API is stubbed)
                if "api" in inner_ctx:
                    api_result = await inner_ctx["api"].test_connection()
                    assert "status_code" in api_result

                # Test that both contexts work simultaneously
                if "database" in outer_ctx and "api" in inner_ctx:
                    db_task = outer_ctx["database"].execute_operation("insert", {
                        "name": "nested_test", "value": "outer_context"
                    })
                    api_task = inner_ctx["api"].execute_operation("get", {
                        "endpoint": "/json"
                    })

                    db_result, api_result = await asyncio.gather(db_task, api_task)

                    assert "rows_affected" in db_result
                    assert api_result["status_code"] == 200

            # Verify outer context still works after inner context cleanup
            if "cache" in outer_ctx:
                cache_result = await outer_ctx["cache"].test_connection()
                assert "max_size" in cache_result


    @pytest.mark.asyncio
    async def test_multiple_nesting_levels(self):
        """Test multiple levels of nested context managers"""
        contexts_created = []
        
        # Level 1: Database
        async with ResourceManager(["database"]) as ctx1:
            contexts_created.append("level1")
            assert "database" in ctx1
            
            # Level 2: Cache
            async with ResourceManager(["cache"]) as ctx2:
                contexts_created.append("level2")
                assert "cache" in ctx2
                
                # Level 3: API
                async with ResourceManager(["api"]) as ctx3:
                    contexts_created.append("level3")
                    assert "api" in ctx3
                    
                    # Test all three levels work together
                    db_op = ctx1["database"].execute_operation("insert", {
                        "name": "triple_nested", "value": "level3"
                    })
                    cache_op = ctx2["cache"].execute_operation("set", {
                        "key": "triple_test", "value": "nested_value"
                    })
                    api_op = ctx3["api"].execute_operation("get", {
                        "endpoint": "/uuid"
                    })
                    
                    results = await asyncio.gather(db_op, cache_op, api_op)
                    
                    # Verify all operations succeeded
                    assert "rows_affected" in results[0]
                    assert "stored" in results[1]
                    assert results[2]["status_code"] == 200
        
        assert len(contexts_created) == 3, "All three nesting levels should be created"
    
    @pytest.mark.asyncio
    async def test_nested_context_exception_handling(self):
        """Test exception handling in nested contexts"""
        cleanup_verified = []
        
        try:
            async with ResourceManager(["database", "cache"]) as outer_ctx:
                cleanup_verified.append("outer_entered")
                
                # Insert some data in outer context
                await outer_ctx["database"].execute_operation("insert", {
                    "name": "before_nested_failure", "value": "outer_data"
                })
                
                try:
                    async with ResourceManager(["api"]) as inner_ctx:
                        cleanup_verified.append("inner_entered")
                        
                        # Successful operation in inner context
                        await inner_ctx["api"].test_connection()
                        
                        # Force an exception in inner context
                        raise ValueError("Simulated inner context failure")
                        
                except ValueError as e:
                    if "inner context failure" in str(e):
                        cleanup_verified.append("inner_exception_caught")
                
                # Verify outer context still works after inner context exception
                test_result = await outer_ctx["database"].test_connection()
                assert "database_file" in test_result
                cleanup_verified.append("outer_still_functional")
                
        except Exception as e:
            pytest.fail(f"Outer context should not fail due to inner context exception: {e}")
        
        # Verify proper cleanup sequence
        expected_cleanup = ["outer_entered", "inner_entered", "inner_exception_caught", "outer_still_functional"]
        assert cleanup_verified == expected_cleanup, f"Expected {expected_cleanup}, got {cleanup_verified}"
    
    @pytest.mark.asyncio
    async def test_nested_context_resource_isolation(self):
        """Test that nested contexts don't interfere with each other's resources"""
        
        async with ResourceManager(["database"]) as outer_ctx:
            # Set up data in outer context
            await outer_ctx["database"].execute_operation("insert", {
                "name": "outer_data", "value": "from_outer"
            })
            
            async with ResourceManager(["database"]) as inner_ctx:
                # This creates a new database connection in inner context
                await inner_ctx["database"].execute_operation("insert", {
                    "name": "inner_data", "value": "from_inner"
                })
                
                # Test that both contexts have their own connection instances
                outer_db = outer_ctx["database"]
                inner_db = inner_ctx["database"]
                
                # They should be different connection objects
                assert outer_db is not inner_db, "Nested contexts should have separate connection instances"
                
                # Both should be functional
                outer_test = await outer_db.test_connection()
                inner_test = await inner_db.test_connection()
                
                assert outer_test["database_file"] == inner_test["database_file"]  # Same file
                assert outer_db.connected and inner_db.connected  # Both connected
    
    @pytest.mark.asyncio
    async def test_concurrent_nested_contexts(self):
        """Test multiple nested contexts running concurrently"""
        
        async def nested_task(task_id: int):
            """Create a nested context and perform operations"""
            async with ResourceManager(["cache"]) as outer_ctx:
                await outer_ctx["cache"].execute_operation("set", {
                    "key": f"outer_{task_id}", "value": f"outer_value_{task_id}"
                })
                
                async with ResourceManager(["database"]) as inner_ctx:
                    result = await inner_ctx["database"].execute_operation("insert", {
                        "name": f"concurrent_nested_{task_id}", 
                        "value": f"task_{task_id}_value"
                    })
                    return result["last_insert_id"]
        
        # Run multiple nested contexts concurrently
        tasks = [nested_task(i) for i in range(3)]
        results = await asyncio.gather(*tasks)
        
        # All tasks should complete successfully
        assert len(results) == 3
        assert all(isinstance(result, int) for result in results)
        assert len(set(results)) == 3, "All tasks should have unique inserted IDs"
    
    @pytest.mark.asyncio
    async def test_nested_context_performance(self):
        """Test performance characteristics of nested contexts"""
        setup_times = []
        
        # Measure setup time for nested contexts
        for i in range(3):
            start_time = time.time()
            
            async with ResourceManager(["database"]) as outer_ctx:
                async with ResourceManager(["cache"]) as inner_ctx:
                    # Perform minimal operations to test setup overhead
                    await outer_ctx["database"].test_connection()
                    await inner_ctx["cache"].test_connection()
            
            setup_time = time.time() - start_time
            setup_times.append(setup_time)
        
        # Verify reasonable performance
        avg_setup_time = sum(setup_times) / len(setup_times)
        assert avg_setup_time < 2.0, f"Nested context setup should be fast, got {avg_setup_time:.3f}s"
        
        # Verify consistency (no significant degradation)
        max_time = max(setup_times)
        min_time = min(setup_times)
        assert (max_time - min_time) / avg_setup_time < 0.5, "Setup times should be consistent"
