import json
import pytest
import asyncio
from pathlib import Path
from fastapi.testclient import TestClient
from app import app
from utils import ResourceManager, DatabaseConnection, APIConnection, CacheConnection

def test_resource_manager_context():
    """Test the basic context manager functionality"""
    with TestClient(app) as client:
        # Test with all resource types
        r = client.post("/resources/test?resource_types=database,cache")
        assert r.status_code == 200
        result = r.json()
        assert result["ok"] is True
        assert "results" in result
        assert len(result["results"]) >= 1  # At least database and cache should work

def test_resource_manager_with_operations():
    """Test executing operations through the resource manager"""
    with TestClient(app) as client:
        # Test database operations
        operations = [
            {
                "resource": "database",
                "operation": "insert",
                "data": {"name": "test_item", "value": "test_value"}
            },
            {
                "resource": "cache",
                "operation": "set",
                "data": {"key": "test_key", "value": "test_cache_value"}
            }
        ]
        
        r = client.post(f"/resources/execute?operations={json.dumps(operations)}")
        assert r.status_code == 200
        result = r.json()
        assert result["ok"] is True
        assert result["executed_operations"] == 2

def test_database_connection():
    """Test database connection independently"""
    async def test_db():
        db = DatabaseConnection("test_db.db")
        
        await db.connect()
        assert db.connected is True
        
        # Test connection
        test_result = await db.test_connection()
        assert "database_file" in test_result
        
        # Test insert operation
        insert_result = await db.execute_operation("insert", {
            "name": "test_record",
            "value": "test_value"
        })
        assert "inserted_id" in insert_result
        
        # Test query operation
        query_result = await db.execute_operation("query", {"limit": 5})
        assert isinstance(query_result, dict)
        assert "data" in query_result
        assert isinstance(query_result["data"], list)
        assert len(query_result["data"]) >= 1
        assert "execution_time" in query_result
        
        await db.disconnect()
        assert db.connected is False
        
        # Cleanup
        Path("test_db.db").unlink(missing_ok=True)
    
    asyncio.run(test_db())

def test_cache_connection():
    """Test cache connection independently"""
    async def test_cache():
        cache = CacheConnection(max_size=100)
        
        await cache.connect()
        assert cache.connected is True
        
        # Test connection
        test_result = await cache.test_connection()
        assert "max_size" in test_result
        assert test_result["max_size"] == 100
        
        # Test set operation
        set_result = await cache.execute_operation("set", {
            "key": "test_key",
            "value": "test_value"
        })
        assert set_result["key"] == "test_key"
        assert set_result["value"] == "test_value"
        
        # Test get operation
        get_result = await cache.execute_operation("get", {"key": "test_key"})
        assert get_result["found"] is True
        assert get_result["value"] == "test_value"
        
        # Test stats operation
        stats_result = await cache.execute_operation("stats", {})
        assert "current_size" in stats_result
        assert stats_result["current_size"] >= 1
        
        await cache.disconnect()
        assert cache.connected is False
    
    asyncio.run(test_cache())

def test_resource_manager_context_manager():
    """Test the context manager directly"""
    async def test_context():
        # Test with multiple resources
        async with ResourceManager(["database", "cache"]) as resources:
            assert "database" in resources
            assert "cache" in resources
            
            # Test that connections are established
            db_test = await resources["database"].test_connection()
            assert "database_file" in db_test
            
            cache_test = await resources["cache"].test_connection()
            assert "max_size" in cache_test
        
        # Test with invalid resource type (should handle gracefully)
        try:
            async with ResourceManager(["invalid_resource"]) as resources:
                pass  # Should raise an error
        except RuntimeError:
            pass  # Expected behavior
        
        # Cleanup
        Path("resource_manager.db").unlink(missing_ok=True)
    
    asyncio.run(test_context())

def test_resource_status_endpoint():
    """Test the resource status endpoint"""
    with TestClient(app) as client:
        r = client.get("/resources/status")
        assert r.status_code == 200
        result = r.json()
        assert result["ok"] is True
        assert "resources" in result
        
        # Check that we have status for known resource types
        resources = result["resources"]
        assert "database" in resources
        assert "cache" in resources

def test_logs_endpoint():
    """Test the logs endpoint"""
    with TestClient(app) as client:
        # First, generate some logs by testing resources
        client.post("/resources/test?resource_types=database,cache")
        
        # Then check logs
        r = client.get("/resources/logs?limit=5")
        assert r.status_code == 200
        result = r.json()
        assert result["ok"] is True
        assert "logs" in result
        assert isinstance(result["logs"], list)

def test_health_check():
    """Test the health check endpoint"""
    with TestClient(app) as client:
        r = client.get("/resources/health")
        assert r.status_code == 200
        result = r.json()
        assert result["ok"] is True
        # Status could be healthy, degraded, or unhealthy depending on resource availability
        assert result["status"] in ["healthy", "degraded", "unhealthy"]
        assert result["service"] == "resource_manager"
        assert "resources" in result
        assert "healthy_resources" in result
        assert "total_resources" in result
        
        # At least database should be healthy
        resources = result["resources"]
        assert "database" in resources
        assert resources["database"]["status"] == "healthy"

def test_resource_cleanup():
    """Test that resources are properly cleaned up"""
    async def test_cleanup():
        # Test normal cleanup
        async with ResourceManager(["database", "cache"]) as resources:
            db = resources["database"]
            cache = resources["cache"]
            assert db.connected is True
            assert cache.connected is True
        
        # After context exit, connections should be closed
        assert db.connected is False
        assert cache.connected is False
        
        # Test cleanup with exception
        try:
            async with ResourceManager(["database", "cache"]) as resources:
                db = resources["database"]
                cache = resources["cache"]
                assert db.connected is True
                assert cache.connected is True
                raise ValueError("Test exception")
        except ValueError:
            pass  # Expected
        
        # Even with exception, connections should be closed
        assert db.connected is False
        assert cache.connected is False
        
        # Cleanup
        Path("resource_manager.db").unlink(missing_ok=True)
    
    asyncio.run(test_cleanup())

def test_error_handling():
    """Test error handling in resource operations"""
    with TestClient(app) as client:
        # Test with invalid operations JSON
        r = client.post("/resources/execute?operations=invalid_json")
        assert r.status_code == 400
        
        # Test with missing operations parameter
        r = client.post("/resources/execute")
        assert r.status_code == 400
        
        # Test with invalid operation
        operations = [{"resource": "database", "operation": "invalid_op", "data": {}}]
        r = client.post(f"/resources/execute?operations={json.dumps(operations)}")
        assert r.status_code == 200  # Should handle gracefully
        result = r.json()
        # Check that the error is captured in results
        assert "operation_0" in result["results"]
        assert result["results"]["operation_0"]["status"] == "error"

# Cleanup function to remove test files
def cleanup_test_files():
    """Clean up test database files"""
    try:
        Path("resource_manager.db").unlink(missing_ok=True)
        Path("test_db.db").unlink(missing_ok=True)
    except Exception as e:
        print(f"Cleanup warning: {e}")

# Run cleanup after tests (you might want to call this manually if needed)
# cleanup_test_files()
