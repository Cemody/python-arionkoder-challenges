"""
Test suite for resource acquisition and release API.
This file tests the constraint: "Should provide a clear API for resource acquisition and release"
"""

import pytest
import asyncio
import time
from typing import Dict, Any
from utils import ResourceManager, DatabaseConnection, APIConnection, CacheConnection


class TestResourceAcquisitionAPI:
    """Test clear API for resource acquisition and release"""
    
    @pytest.mark.asyncio
    async def test_individual_resource_lifecycle(self):
        """Test individual resource acquisition and release lifecycle"""
        
        # Test Database Connection
        db = DatabaseConnection()
        assert not db.connected, "Database should start disconnected"
        
        await db.connect()
        assert db.connected, "Database should be connected after connect()"
        assert db.connection is not None, "Database connection object should exist"
        
        # Test database functionality
        test_result = await db.test_connection()
        assert "database_file" in test_result
        
        await db.disconnect()
        assert not db.connected, "Database should be disconnected after disconnect()"
        
        # Test Cache Connection
        cache = CacheConnection(max_size=100)
        assert not cache.connected, "Cache should start disconnected"
        
        await cache.connect()
        assert cache.connected, "Cache should be connected after connect()"
        
        # Test cache functionality
        test_result = await cache.test_connection()
        assert "max_size" in test_result
        assert test_result["max_size"] == 100
        
        await cache.disconnect()
        assert not cache.connected, "Cache should be disconnected after disconnect()"
        
        # Test API Connection
        api = APIConnection()
        assert not api.connected, "API should start disconnected"
        
        await api.connect()
        assert api.connected, "API should be connected after connect()"
        assert api.session is not None, "API session should exist"
        
        # Test API functionality
        test_result = await api.test_connection()
        assert "status_code" in test_result
        
        await api.disconnect()
        assert not api.connected, "API should be disconnected after disconnect()"
    
    @pytest.mark.asyncio
    async def test_resource_manager_acquisition_api(self):
        """Test ResourceManager as a clear acquisition API"""
        
        # Test resource acquisition through context manager
        resources = None
        
        async with ResourceManager(["database", "cache"]) as ctx_resources:
            resources = ctx_resources
            
            # Verify API clarity
            assert isinstance(resources, dict), "Resources should be returned as a dictionary"
            assert len(resources) >= 1, "At least one resource should be acquired"
            
            # Verify resource types
            for resource_name, resource in resources.items():
                assert resource_name in ["database", "cache", "api"], f"Unexpected resource type: {resource_name}"
                assert hasattr(resource, "connect"), "Resource should have connect method"
                assert hasattr(resource, "disconnect"), "Resource should have disconnect method"
                assert hasattr(resource, "test_connection"), "Resource should have test_connection method"
                assert hasattr(resource, "execute_operation"), "Resource should have execute_operation method"
        
        # Resources should be automatically released after context exit
        # We can't directly test this, but we can verify new connections work
        async with ResourceManager(["database"]) as new_resources:
            assert len(new_resources) >= 1, "Should be able to acquire resources again after release"
    
    @pytest.mark.asyncio
    async def test_resource_operation_api_consistency(self):
        """Test that all resources provide consistent operation API"""
        
        async with ResourceManager(["database", "cache", "api"]) as resources:
            
            for resource_name, resource in resources.items():
                # Test connection testing API
                test_result = await resource.test_connection()
                assert isinstance(test_result, dict), f"{resource_name} test_connection should return dict"
                assert len(test_result) > 0, f"{resource_name} test_connection should return data"
                
                # Test operation execution API consistency
                if resource_name == "database":
                    # Database operations
                    operations = [
                        ("insert", {"name": "api_test", "value": "test_value"}),
                        ("query", {"limit": 5}),
                    ]
                    
                    for op_type, op_data in operations:
                        result = await resource.execute_operation(op_type, op_data)
                        assert isinstance(result, dict), f"Database {op_type} should return dict"
                        assert "execution_time" in result, f"Database {op_type} should include execution_time"
                
                elif resource_name == "cache":
                    # Cache operations
                    operations = [
                        ("set", {"key": "api_test_key", "value": "test_cache_value"}),
                        ("get", {"key": "api_test_key"}),
                        ("stats", {}),
                    ]
                    
                    for op_type, op_data in operations:
                        result = await resource.execute_operation(op_type, op_data)
                        assert isinstance(result, dict), f"Cache {op_type} should return dict"
                        assert "execution_time" in result, f"Cache {op_type} should include execution_time"
                
                elif resource_name == "api":
                    # API operations
                    operations = [
                        ("get", {"endpoint": "/json", "params": {"test": "api_consistency"}}),
                        ("post", {"endpoint": "/post", "payload": {"test": "post_data"}}),
                    ]
                    
                    for op_type, op_data in operations:
                        result = await resource.execute_operation(op_type, op_data)
                        assert isinstance(result, dict), f"API {op_type} should return dict"
                        assert "status_code" in result, f"API {op_type} should include status_code"
    
    @pytest.mark.asyncio
    async def test_error_handling_in_api(self):
        """Test error handling in resource acquisition API"""
        
        # Test invalid resource types
        with pytest.raises(Exception):
            async with ResourceManager(["invalid_resource_type"]) as resources:
                pass
        
        # Test operations on disconnected resources
        db = DatabaseConnection()
        
        with pytest.raises(RuntimeError, match="not connected"):
            await db.test_connection()
        
        with pytest.raises(RuntimeError, match="not connected"):
            await db.execute_operation("insert", {"name": "test", "value": "test"})
        
        # Test invalid operations
        async with ResourceManager(["database"]) as resources:
            db = resources["database"]
            
            with pytest.raises(ValueError, match="Unsupported.*operation"):
                await db.execute_operation("invalid_operation", {})
        
        async with ResourceManager(["cache"]) as resources:
            cache = resources["cache"]
            
            with pytest.raises(ValueError, match="Unsupported.*operation"):
                await cache.execute_operation("invalid_operation", {})
    
    @pytest.mark.asyncio
    async def test_resource_configuration_api(self):
        """Test resource configuration through clear API"""
        
        # Test database with custom path
        custom_db = DatabaseConnection("custom_test.db")
        await custom_db.connect()
        
        test_result = await custom_db.test_connection()
        assert "custom_test.db" in test_result["database_file"]
        
        await custom_db.disconnect()
        
        # Test cache with custom size
        custom_cache = CacheConnection(max_size=50)
        await custom_cache.connect()
        
        test_result = await custom_cache.test_connection()
        assert test_result["max_size"] == 50
        
        await custom_cache.disconnect()
        
        # Test API with custom base URL
        custom_api = APIConnection("https://httpbin.org")
        await custom_api.connect()
        
        test_result = await custom_api.test_connection()
        assert "httpbin.org" in test_result["base_url"]
        
        await custom_api.disconnect()
    
    @pytest.mark.asyncio
    async def test_concurrent_resource_acquisition(self):
        """Test concurrent resource acquisition through API"""
        
        async def acquire_and_use_resources(task_id: int):
            async with ResourceManager(["database", "cache"]) as resources:
                # Use resources
                operations = []
                
                if "database" in resources:
                    operations.append(
                        resources["database"].execute_operation("insert", {
                            "name": f"concurrent_api_{task_id}",
                            "value": f"task_{task_id}_data"
                        })
                    )
                
                if "cache" in resources:
                    operations.append(
                        resources["cache"].execute_operation("set", {
                            "key": f"concurrent_key_{task_id}",
                            "value": f"task_{task_id}_cache"
                        })
                    )
                
                results = await asyncio.gather(*operations)
                return len(results)
        
        # Run multiple concurrent resource acquisitions
        tasks = [acquire_and_use_resources(i) for i in range(5)]
        results = await asyncio.gather(*tasks)
        
        # All tasks should complete successfully
        assert all(result >= 1 for result in results), "All concurrent acquisitions should succeed"
        assert len(results) == 5, "All tasks should complete"
    
    @pytest.mark.asyncio
    async def test_resource_state_isolation(self):
        """Test that resource instances are properly isolated"""
        
        # Create multiple database connections
        db1 = DatabaseConnection("test_db1.db")
        db2 = DatabaseConnection("test_db2.db")
        
        await db1.connect()
        await db2.connect()
        
        # They should be independent
        assert db1.connection is not db2.connection
        assert db1.db_path != db2.db_path
        
        # Operations on one shouldn't affect the other
        await db1.execute_operation("insert", {"name": "db1_test", "value": "db1_value"})
        await db2.execute_operation("insert", {"name": "db2_test", "value": "db2_value"})
        
        db1_result = await db1.execute_operation("query", {"limit": 10})
        db2_result = await db2.execute_operation("query", {"limit": 10})
        
        # Results should be different
        db1_data = db1_result["data"]
        db2_data = db2_result["data"]
        
        db1_names = [record["name"] for record in db1_data]
        db2_names = [record["name"] for record in db2_data]
        
        assert "db1_test" in db1_names
        assert "db2_test" in db2_names
        assert "db2_test" not in db1_names
        assert "db1_test" not in db2_names
        
        await db1.disconnect()
        await db2.disconnect()
    
    @pytest.mark.asyncio
    async def test_api_discoverability(self):
        """Test that API methods are discoverable and well-documented"""
        
        # Test ResourceManager API discoverability
        rm = ResourceManager(["database"])
        
        # Should have clear context manager methods
        assert hasattr(rm, "__aenter__"), "ResourceManager should support async context management"
        assert hasattr(rm, "__aexit__"), "ResourceManager should support async context management"
        
        # Test individual resource API discoverability
        db = DatabaseConnection()
        
        # Core methods should be available
        required_methods = ["connect", "disconnect", "test_connection", "execute_operation"]
        for method in required_methods:
            assert hasattr(db, method), f"DatabaseConnection should have {method} method"
            assert callable(getattr(db, method)), f"{method} should be callable"
        
        cache = CacheConnection()
        for method in required_methods:
            assert hasattr(cache, method), f"CacheConnection should have {method} method"
            assert callable(getattr(cache, method)), f"{method} should be callable"
        
        api = APIConnection()
        for method in required_methods:
            assert hasattr(api, method), f"APIConnection should have {method} method"
            assert callable(getattr(api, method)), f"{method} should be callable"
    
    @pytest.mark.asyncio
    async def test_resource_acquisition_performance(self):
        """Test that resource acquisition API performs well"""
        
        # Test single resource acquisition speed
        single_times = []
        for i in range(3):
            start_time = time.time()
            async with ResourceManager(["cache"]) as resources:
                await resources["cache"].test_connection()
            acquisition_time = time.time() - start_time
            single_times.append(acquisition_time)
        
        avg_single_time = sum(single_times) / len(single_times)
        assert avg_single_time < 1.0, f"Single resource acquisition should be fast, got {avg_single_time:.3f}s"
        
        # Test multiple resource acquisition speed
        multi_times = []
        for i in range(3):
            start_time = time.time()
            async with ResourceManager(["database", "cache", "api"]) as resources:
                # Test all resources
                tasks = [resource.test_connection() for resource in resources.values()]
                await asyncio.gather(*tasks)
            acquisition_time = time.time() - start_time
            multi_times.append(acquisition_time)
        
        avg_multi_time = sum(multi_times) / len(multi_times)
        assert avg_multi_time < 3.0, f"Multiple resource acquisition should be reasonable, got {avg_multi_time:.3f}s"
        
        print(f"API Performance - Single: {avg_single_time:.3f}s, Multiple: {avg_multi_time:.3f}s")
