"""
Configuration for pytest to set up the proper import paths and mocked fixtures.
"""

import sys
import os
from pathlib import Path
import pytest
from unittest.mock import AsyncMock, MagicMock
from typing import Dict, Any, Optional


# Add the parent directory to Python path so we can import from utils, app, etc.
parent_dir = Path(__file__).parent.parent
sys.path.insert(0, str(parent_dir))

# Import after path setup
from utils import APIConnection, ResourceManager, DatabaseConnection, CacheConnection


@pytest.fixture
def mock_api_response():
    """Fixture providing configurable API response data."""
    return {
        "default": {"status_code": 200, "data": {"message": "success"}},
        "test_connection": {"status_code": 200, "connection_ok": True},
        "get": {"status_code": 200, "data": {"endpoint": "test"}},
        "post": {"status_code": 201, "data": {"created": True}},
        "put": {"status_code": 200, "data": {"updated": True}},
        "delete": {"status_code": 204},
        "error": {"status_code": 500, "error": "Internal Server Error"}
    }


@pytest.fixture
def mock_api_connection(mock_api_response):
    """Fixture providing a fully mocked APIConnection."""
    
    async def _fake_execute_operation(op: str, payload: Optional[Dict[str, Any]] = None):
        """Mock execute_operation method with configurable responses."""
        payload = payload or {}
        
        # Return specific responses based on operation type
        if op in mock_api_response:
            response = mock_api_response[op].copy()
            if "data" in response and payload:
                response["data"].update(payload)
            return response
        
        # Default response
        return mock_api_response["default"]
    
    async def _fake_test_connection():
        """Mock test_connection method."""
        return mock_api_response["test_connection"]
    
    # Create mock instance
    mock_api = MagicMock(spec=APIConnection)
    mock_api.test_connection = AsyncMock(side_effect=_fake_test_connection)
    mock_api.execute_operation = AsyncMock(side_effect=_fake_execute_operation)
    mock_api.connected = True
    mock_api.session = MagicMock()
    
    return mock_api


@pytest.fixture
def mock_database_connection():
    """Fixture providing a fully mocked DatabaseConnection."""
    
    async def _fake_test_connection():
        """Mock database test_connection method."""
        return {
            "database_file": "test.db",
            "connection_ok": True,
            "tables_count": 5
        }
    
    async def _fake_execute_operation(op: str, payload: Optional[Dict[str, Any]] = None):
        """Mock database execute_operation method."""
        payload = payload or {}
        
        if op == "select":
            return {"rows": [{"id": 1, "name": "test"}], "count": 1}
        elif op == "query":
            return {"data": [{"id": 1, "name": "test"}], "count": 1}
        elif op == "insert":
            return {"rows_affected": 1, "last_insert_id": 123}
        elif op == "update":
            return {"rows_affected": 1}
        elif op == "delete":
            return {"rows_affected": 1}
        elif op == "invalid_operation":
            raise ValueError("Unsupported database operation: invalid_operation")
        
        return {"success": True}
    
    mock_db = MagicMock(spec=DatabaseConnection)
    mock_db.test_connection = AsyncMock(side_effect=_fake_test_connection)
    mock_db.execute_operation = AsyncMock(side_effect=_fake_execute_operation)
    mock_db.connected = True
    mock_db.connection = MagicMock()
    
    return mock_db


@pytest.fixture
def mock_cache_connection():
    """Fixture providing a fully mocked CacheConnection."""
    
    async def _fake_test_connection():
        """Mock cache test_connection method."""
        # Get cache data from the mock instance
        cache_data = getattr(mock_cache, '_cache_data', {})
        return {
            "max_size": 1000,
            "current_size": len(cache_data),
            "connection_ok": True
        }
    
    async def _fake_execute_operation(op: str, payload: Optional[Dict[str, Any]] = None):
        """Mock cache execute_operation method."""
        payload = payload or {}
        
        # Get or initialize cache data for this instance
        if not hasattr(mock_cache, '_cache_data'):
            mock_cache._cache_data = {}
        cache_data = mock_cache._cache_data
        
        if op == "get":
            key = payload.get('key', 'default')
            if key in cache_data:
                return {"value": cache_data[key], "hit": True}
            else:
                return {"value": None, "hit": False}
        elif op == "set":
            key = payload.get("key")
            value = payload.get("value")
            if key:
                cache_data[key] = value
            return {"stored": True, "key": key}
        elif op == "delete":
            key = payload.get("key")
            if key in cache_data:
                del cache_data[key]
            return {"deleted": True}
        elif op == "clear":
            items_removed = len(cache_data)
            cache_data.clear()
            return {"cleared": True, "items_removed": items_removed}
        
        return {"success": True}
    
    mock_cache = MagicMock(spec=CacheConnection)
    mock_cache.test_connection = AsyncMock(side_effect=_fake_test_connection)
    mock_cache.execute_operation = AsyncMock(side_effect=_fake_execute_operation)
    mock_cache.connected = True
    mock_cache._cache_data = {}  # Initialize empty cache data for this instance
    mock_cache.max_size = 1000
    
    return mock_cache


@pytest.fixture(autouse=True)
def mock_all_connections(monkeypatch, mock_api_response):
    """Auto-applied fixture that mocks all connection types to prevent real network/IO calls."""
    
    # API Connection mocks
    async def _fake_api_execute_operation(self, op: str, payload: Optional[Dict[str, Any]] = None):
        # Check if the API is connected
        if not getattr(self, 'connected', False):
            raise RuntimeError("API is not connected")
            
        payload = payload or {}
        if op in mock_api_response:
            response = mock_api_response[op].copy()
            if "data" in response and payload:
                response["data"].update(payload)
            return response
        return mock_api_response["default"]
    
    async def _fake_api_test_connection(self):
        # Check if the API is connected
        if not getattr(self, 'connected', False):
            raise RuntimeError("API is not connected")
        
        # Get the base_url from the instance, fallback to default
        base_url = getattr(self, 'base_url', 'https://httpbin.org')
        
        response = mock_api_response["test_connection"].copy()
        response["base_url"] = base_url
        return response
    
    # Database Connection mocks
    _insert_id_counter = 0  # Counter for unique insert IDs
    
    async def _fake_db_test_connection(self):
        # Check if the database is connected
        if not getattr(self, 'connected', False):
            raise RuntimeError("Database is not connected")
        
        # Use the actual db_path from the instance, fallback to default
        db_file = getattr(self, 'db_path', 'test.db')
        # Convert Path object to string to match real implementation
        if hasattr(db_file, '__fspath__'):  # Check if it's a Path object
            db_file = str(db_file)
        return {
            "database_file": db_file,
            "connection_ok": True,
            "tables_count": 5
        }
    
    async def _fake_db_execute_operation(self, op: str, payload: Optional[Dict[str, Any]] = None):
        # Check if the database is connected
        if not getattr(self, 'connected', False):
            raise RuntimeError("Database is not connected")
            
        # Each instance gets its own data storage
        if not hasattr(self, '_db_data'):
            self._db_data = []
            
        nonlocal _insert_id_counter
        payload = payload or {}
        
        # Simulate realistic execution time
        import random
        execution_time = random.uniform(0.001, 0.005)  # 1-5ms
        
        if op == "select":
            # Return stored data or default
            data = self._db_data if self._db_data else [{"id": 1, "name": "test"}]
            result = {"rows": data, "count": len(data)}
        elif op == "query":
            # Return stored data or default
            data = self._db_data if self._db_data else [{"id": 1, "name": "test"}]
            limit = payload.get("limit", 100)
            limited_data = data[:limit] if data else []
            result = {"data": limited_data, "count": len(limited_data)}
        elif op == "insert":
            _insert_id_counter += 1
            # Store the inserted data
            new_record = {
                "id": _insert_id_counter,
                "name": payload.get("name", f"record_{_insert_id_counter}"),
                "value": payload.get("value", "")
            }
            self._db_data.append(new_record)
            result = {"rows_affected": 1, "last_insert_id": _insert_id_counter}
        elif op == "update":
            result = {"rows_affected": 1}
        elif op == "delete":
            result = {"rows_affected": 1}
        elif op == "invalid_operation":
            raise ValueError("Unsupported database operation: invalid_operation")
        else:
            result = {"success": True}
        
        # Add execution time to result
        result["execution_time"] = execution_time
        
        return result
    
    # Cache Connection mocks
    async def _fake_cache_test_connection(self):
        # Check if the cache is connected
        if not getattr(self, 'connected', False):
            raise RuntimeError("Cache is not connected")
        # Each instance gets its own cache data
        if not hasattr(self, '_cache_data'):
            self._cache_data = {}
        # Use the actual max_size from the instance, fallback to 1000
        max_size = getattr(self, 'max_size', 1000)
        return {
            "max_size": max_size,
            "current_size": len(self._cache_data),
            "connection_ok": True
        }
    
    async def _fake_cache_execute_operation(self, op: str, payload: Optional[Dict[str, Any]] = None):
        # Check if the cache is connected
        if not getattr(self, 'connected', False):
            raise RuntimeError("Cache is not connected")
            
        payload = payload or {}
        # Each instance gets its own cache data
        if not hasattr(self, '_cache_data'):
            self._cache_data = {}
        if not hasattr(self, '_cache_stats'):
            self._cache_stats = {"hit_count": 0, "miss_count": 0, "total_operations": 0}
        
        cache_data = self._cache_data
        cache_stats = self._cache_stats
        
        # Simulate realistic execution time
        import random
        execution_time = random.uniform(0.0005, 0.002)  # 0.5-2ms
        
        if op == "get":
            key = payload.get('key', 'default')
            cache_stats["total_operations"] += 1
            if key in cache_data:
                cache_stats["hit_count"] += 1
                result = {
                    "value": cache_data[key], 
                    "hit": True, 
                    "found": True,
                    "cache_stats": {
                        "hit_count": cache_stats["hit_count"],
                        "miss_count": cache_stats["miss_count"],
                        "total_operations": cache_stats["total_operations"],
                        "hit_rate": cache_stats["hit_count"] / cache_stats["total_operations"]
                    }
                }
            else:
                cache_stats["miss_count"] += 1
                result = {
                    "value": None, 
                    "hit": False, 
                    "found": False,
                    "cache_stats": {
                        "hit_count": cache_stats["hit_count"],
                        "miss_count": cache_stats["miss_count"],
                        "total_operations": cache_stats["total_operations"],
                        "hit_rate": cache_stats["hit_count"] / cache_stats["total_operations"]
                    }
                }
        elif op == "set":
            key = payload.get("key")
            value = payload.get("value")
            if key:
                cache_data[key] = value
            result = {"stored": True, "key": key}
        elif op == "delete":
            key = payload.get("key")
            if key in cache_data:
                del cache_data[key]
            result = {"deleted": True}
        elif op == "clear":
            items_removed = len(cache_data)
            cache_data.clear()
            result = {"cleared": True, "items_removed": items_removed}
        elif op == "stats":
            result = {
                "hit_count": cache_stats["hit_count"],
                "miss_count": cache_stats["miss_count"],
                "total_operations": cache_stats["total_operations"],
                "hit_rate": cache_stats["hit_count"] / max(cache_stats["total_operations"], 1),
                "cache_size": len(cache_data)
            }
        else:
            raise ValueError(f"Unsupported cache operation: {op}")
        
        # Add execution time to result
        result["execution_time"] = execution_time
        
        return result
    
    # Apply the patches
    monkeypatch.setattr(APIConnection, "test_connection", _fake_api_test_connection)
    monkeypatch.setattr(APIConnection, "execute_operation", _fake_api_execute_operation)
    
    monkeypatch.setattr(DatabaseConnection, "test_connection", _fake_db_test_connection)
    monkeypatch.setattr(DatabaseConnection, "execute_operation", _fake_db_execute_operation)
    
    monkeypatch.setattr(CacheConnection, "test_connection", _fake_cache_test_connection)
    monkeypatch.setattr(CacheConnection, "execute_operation", _fake_cache_execute_operation)
    
    yield


@pytest.fixture
def api_error_scenario(monkeypatch, mock_api_response):
    """Fixture to simulate API error scenarios."""
    
    async def _error_execute_operation(op: str, payload: Optional[Dict[str, Any]] = None):
        """Mock that always returns error responses."""
        return mock_api_response["error"]
    
    async def _error_test_connection():
        """Mock that simulates connection failures."""
        raise ConnectionError("Failed to connect to API")
    
    def apply_error_scenario():
        monkeypatch.setattr(APIConnection, "test_connection", AsyncMock(side_effect=_error_test_connection))
        monkeypatch.setattr(APIConnection, "execute_operation", AsyncMock(side_effect=_error_execute_operation))
    
    return apply_error_scenario