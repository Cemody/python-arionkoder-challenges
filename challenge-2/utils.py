import asyncio
import datetime
import sqlite3
import json
import sys
import os
import aiohttp
import time
import logging
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional, AsyncContextManager, Protocol
from contextlib import asynccontextmanager
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from collections.abc import MutableMapping

# ---------- Performance Metrics and Logging Setup ----------

@dataclass
class PerformanceMetrics:
    """Track performance metrics for resource operations"""
    operation_start: float = field(default_factory=time.time)
    operation_end: Optional[float] = None
    connection_time: Optional[float] = None
    execution_time: Optional[float] = None
    memory_usage: Optional[int] = None
    error_count: int = 0
    success_count: int = 0
    
    def start_operation(self):
        """Start timing an operation"""
        self.operation_start = time.time()
    
    def end_operation(self, success: bool = True):
        """End timing an operation"""
        self.operation_end = time.time()
        self.execution_time = self.operation_end - self.operation_start
        if success:
            self.success_count += 1
        else:
            self.error_count += 1
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary"""
        return {
            "connection_time": self.connection_time,
            "execution_time": self.execution_time,
            "memory_usage": self.memory_usage,
            "error_count": self.error_count,
            "success_count": self.success_count,
            "total_operations": self.error_count + self.success_count
        }

# Configure structured logging
def setup_logging():
    """Setup structured logging for the resource manager"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('resource_manager.log')
        ]
    )
    return logging.getLogger('resource_manager')

logger = setup_logging()

# ---------- Resource Connection Protocols ----------

class ResourceConnection(Protocol):
    """Protocol defining the interface for all resource connections"""
    
    async def connect(self) -> None:
        """Establish connection to the resource"""
        ...
    
    async def disconnect(self) -> None:
        """Close connection to the resource"""
        ...
    
    async def test_connection(self) -> Dict[str, Any]:
        """Test the connection and return status information"""
        ...
    
    async def execute_operation(self, operation: str, data: Dict[str, Any]) -> Any:
        """Execute an operation on the resource"""
        ...

# ---------- Concrete Resource Connection Classes ----------

class DatabaseConnection:
    """Manages SQLite database connections with performance tracking"""
    
    def __init__(self, db_path: str = "resource_manager.db"):
        self.db_path = Path(db_path)
        self.connection = None
        self.connected = False
        self.connection_time = None
        self.metrics = PerformanceMetrics()
        self.logger = logging.getLogger(f'resource_manager.database')
    
    async def connect(self) -> None:
        """Establish database connection with performance tracking"""
        connect_start = time.time()
        self.logger.info(f"Attempting to connect to database: {self.db_path}")
        
        try:
            # Run database connection in thread pool
            loop = asyncio.get_event_loop()
            self.connection = await loop.run_in_executor(None, self._connect_sync)
            
            connect_end = time.time()
            self.connection_time = datetime.datetime.now(datetime.timezone.utc)
            self.metrics.connection_time = connect_end - connect_start
            self.connected = True
            
            self.logger.info(f"Database connected successfully in {self.metrics.connection_time:.3f}s: {self.db_path}")
            print(f"✓ Database connected: {self.db_path} ({self.metrics.connection_time:.3f}s)")
            
        except Exception as e:
            self.metrics.end_operation(success=False)
            self.logger.error(f"Database connection failed: {e}", exc_info=True)
            print(f"✗ Database connection failed: {e}")
            raise
    
    def _connect_sync(self):
        """Synchronous database connection with initialization"""
        try:
            # Use check_same_thread=False to allow use from different threads
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            # Initialize tables if they don't exist
            cursor = conn.cursor()
            
            # Enhanced schema with performance tracking
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS resource_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    resource TEXT NOT NULL,
                    action TEXT NOT NULL,
                    status TEXT NOT NULL,
                    error TEXT,
                    execution_time REAL,
                    memory_usage INTEGER,
                    timestamp TEXT NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS performance_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    resource_type TEXT NOT NULL,
                    operation_type TEXT NOT NULL,
                    execution_time REAL NOT NULL,
                    success_count INTEGER DEFAULT 0,
                    error_count INTEGER DEFAULT 0,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS test_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    value TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()
            return conn
            
        except Exception as e:
            self.logger.error(f"Database initialization failed: {e}", exc_info=True)
            raise
    
    async def disconnect(self) -> None:
        """Close database connection with cleanup tracking"""
        disconnect_start = time.time()
        self.logger.info("Disconnecting from database")
        
        if self.connection:
            try:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, self.connection.close)
                
                disconnect_time = time.time() - disconnect_start
                self.connected = False
                self.connection = None  # Release the connection reference
                
                self.logger.info(f"Database disconnected successfully in {disconnect_time:.3f}s")
                print(f"✓ Database disconnected: {self.db_path} ({disconnect_time:.3f}s)")
                
                # Clear references to help with garbage collection
                self.metrics = None
                self.logger = None
                
            except Exception as e:
                self.logger.error(f"Error during database disconnection: {e}", exc_info=True)
                raise
    
    async def test_connection(self) -> Dict[str, Any]:
        """Test database connection with performance metrics"""
        if not self.connected:
            raise RuntimeError("Database not connected")
        
        test_start = time.time()
        self.logger.debug("Testing database connection")
        
        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, self._test_connection_sync)
            
            test_time = time.time() - test_start
            result["test_execution_time"] = test_time
            result["performance_metrics"] = self.metrics.to_dict()
            
            self.logger.info(f"Database test completed successfully in {test_time:.3f}s")
            return result
            
        except Exception as e:
            self.metrics.end_operation(success=False)
            self.logger.error(f"Database test failed: {e}", exc_info=True)
            raise
    
    def _test_connection_sync(self) -> Dict[str, Any]:
        """Synchronous database test with detailed metrics"""
        try:
            cursor = self.connection.cursor()
            
            # Get various counts and statistics
            cursor.execute("SELECT COUNT(*) FROM resource_logs")
            log_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM test_data")
            data_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM performance_metrics")
            metrics_count = cursor.fetchone()[0]
            
            # Get database file size
            db_size = self.db_path.stat().st_size if self.db_path.exists() else 0
            
            return {
                "database_file": str(self.db_path),
                "database_size_bytes": db_size,
                "log_records": log_count,
                "test_records": data_count,
                "performance_records": metrics_count,
                "connection_time": self.connection_time.isoformat() if self.connection_time else None
            }
            
        except Exception as e:
            self.logger.error(f"Database test sync failed: {e}", exc_info=True)
            raise
    
    async def execute_operation(self, operation: str, data: Dict[str, Any]) -> Any:
        """Execute database operations with performance tracking"""
        if not self.connected:
            raise RuntimeError("Database not connected")
        
        op_start = time.time()
        self.logger.info(f"Executing database operation: {operation}")
        
        try:
            loop = asyncio.get_event_loop()
            
            if operation == "query":
                result = await loop.run_in_executor(None, self._execute_query, data)
            elif operation == "insert":
                result = await loop.run_in_executor(None, self._execute_insert, data)
            elif operation == "update":
                result = await loop.run_in_executor(None, self._execute_update, data)
            else:
                raise ValueError(f"Unsupported database operation: {operation}")
            
            op_time = time.time() - op_start
            self.metrics.end_operation(success=True)
            
            # Save performance metrics
            await self._save_performance_metrics(operation, op_time, True)
            
            # Add execution time to result based on result type
            if isinstance(result, dict):
                result["execution_time"] = op_time
            elif isinstance(result, list):
                # For list results (like query results), wrap in a dict with metadata
                result = {
                    "data": result,
                    "execution_time": op_time,
                    "operation": operation
                }
            
            self.logger.info(f"Database operation '{operation}' completed successfully in {op_time:.3f}s")
            
            return result
            
        except Exception as e:
            op_time = time.time() - op_start
            self.metrics.end_operation(success=False)
            
            # Save error metrics
            await self._save_performance_metrics(operation, op_time, False)
            
            self.logger.error(f"Database operation '{operation}' failed after {op_time:.3f}s: {e}", exc_info=True)
            raise
    
    async def _save_performance_metrics(self, operation: str, execution_time: float, success: bool):
        """Save performance metrics to database"""
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._save_metrics_sync, operation, execution_time, success)
        except Exception as e:
            self.logger.warning(f"Failed to save performance metrics: {e}")
    
    def _save_metrics_sync(self, operation: str, execution_time: float, success: bool):
        """Synchronous performance metrics saving"""
        try:
            cursor = self.connection.cursor()
            cursor.execute("""
                INSERT INTO performance_metrics (resource_type, operation_type, execution_time, success_count, error_count)
                VALUES (?, ?, ?, ?, ?)
            """, ("database", operation, execution_time, 1 if success else 0, 0 if success else 1))
            self.connection.commit()
        except Exception as e:
            self.logger.warning(f"Performance metrics save failed: {e}")
    
    def _execute_query(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Execute a query operation"""
        table = data.get("table", "test_data")
        limit = data.get("limit", 10)
        
        cursor = self.connection.cursor()
        cursor.execute(f"SELECT * FROM {table} ORDER BY created_at DESC LIMIT ?", (limit,))
        
        columns = [description[0] for description in cursor.description]
        rows = cursor.fetchall()
        
        return [dict(zip(columns, row)) for row in rows]
    
    def _execute_insert(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Execute an insert operation"""
        name = data.get("name", f"test_record_{int(time.time())}")
        value = data.get("value", "test_value")
        
        cursor = self.connection.cursor()
        cursor.execute("INSERT INTO test_data (name, value) VALUES (?, ?)", (name, value))
        self.connection.commit()
        
        return {"inserted_id": cursor.lastrowid, "name": name, "value": value}
    
    def _execute_update(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Execute an update operation"""
        record_id = data.get("id")
        if not record_id:
            raise ValueError("ID is required for update operation")
        
        name = data.get("name")
        value = data.get("value")
        
        cursor = self.connection.cursor()
        if name and value:
            cursor.execute("UPDATE test_data SET name = ?, value = ? WHERE id = ?", (name, value, record_id))
        elif name:
            cursor.execute("UPDATE test_data SET name = ? WHERE id = ?", (name, record_id))
        elif value:
            cursor.execute("UPDATE test_data SET value = ? WHERE id = ?", (value, record_id))
        else:
            raise ValueError("Either name or value must be provided for update")
        
        self.connection.commit()
        return {"updated_id": record_id, "affected_rows": cursor.rowcount}

class APIConnection:
    """Manages HTTP API connections"""
    
    def __init__(self, base_url: str = "https://httpbin.org"):
        self.base_url = base_url
        self.session = None
        self.connected = False
        self.connection_time = None
    
    async def connect(self) -> None:
        """Establish HTTP session"""
        try:
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
                connector=aiohttp.TCPConnector(limit=10)
            )
            self.connected = True
            self.connection_time = datetime.datetime.now(datetime.timezone.utc)
            print(f"✓ API session created: {self.base_url}")
        except Exception as e:
            print(f"✗ API session creation failed: {e}")
            raise
    
    async def disconnect(self) -> None:
        """Close HTTP session"""
        if self.session:
            await self.session.close()
            self.connected = False
            print(f"✓ API session closed: {self.base_url}")
    
    async def test_connection(self) -> Dict[str, Any]:
        """Test API connection"""
        if not self.connected:
            raise RuntimeError("API session not connected")
        
        try:
            async with self.session.get(f"{self.base_url}/status/200") as response:
                status_code = response.status
                response_time_ms = response.headers.get("X-Response-Time", "unknown")
                
                return {
                    "base_url": self.base_url,
                    "status_code": status_code,
                    "response_time": response_time_ms,
                    "connection_time": self.connection_time.isoformat() if self.connection_time else None,
                    "session_connector_limit": self.session.connector.limit if self.session.connector else None
                }
        except Exception as e:
            raise RuntimeError(f"API test failed: {e}")
    
    async def execute_operation(self, operation: str, data: Dict[str, Any]) -> Any:
        """Execute API operations"""
        if not self.connected:
            raise RuntimeError("API session not connected")
        
        if operation == "get":
            return await self._execute_get(data)
        elif operation == "post":
            return await self._execute_post(data)
        elif operation == "put":
            return await self._execute_put(data)
        elif operation == "delete":
            return await self._execute_delete(data)
        else:
            raise ValueError(f"Unsupported API operation: {operation}")
    
    async def _execute_get(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Execute GET request"""
        endpoint = data.get("endpoint", "/get")
        params = data.get("params", {})
        
        url = f"{self.base_url}{endpoint}"
        async with self.session.get(url, params=params) as response:
            result = await response.json()
            return {
                "status_code": response.status,
                "url": str(response.url),
                "response": result
            }
    
    async def _execute_post(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Execute POST request"""
        endpoint = data.get("endpoint", "/post")
        payload = data.get("payload", {})
        
        url = f"{self.base_url}{endpoint}"
        async with self.session.post(url, json=payload) as response:
            result = await response.json()
            return {
                "status_code": response.status,
                "url": str(response.url),
                "response": result
            }
    
    async def _execute_put(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Execute PUT request"""
        endpoint = data.get("endpoint", "/put")
        payload = data.get("payload", {})
        
        url = f"{self.base_url}{endpoint}"
        async with self.session.put(url, json=payload) as response:
            result = await response.json()
            return {
                "status_code": response.status,
                "url": str(response.url),
                "response": result
            }
    
    async def _execute_delete(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Execute DELETE request"""
        endpoint = data.get("endpoint", "/delete")
        
        url = f"{self.base_url}{endpoint}"
        async with self.session.delete(url) as response:
            result = await response.json()
            return {
                "status_code": response.status,
                "url": str(response.url),
                "response": result
            }

class CacheConnection:
    """Manages in-memory cache connections with performance tracking"""
    
    def __init__(self, max_size: int = 1000):
        self.max_size = max_size
        self.cache = {}
        self.access_times = {}
        self.connected = False
        self.connection_time = None
        self.metrics = PerformanceMetrics()
        self.logger = logging.getLogger('resource_manager.cache')
        self.hit_count = 0
        self.miss_count = 0
        self.eviction_count = 0
    
    async def connect(self) -> None:
        """Initialize cache with performance tracking"""
        connect_start = time.time()
        self.logger.info(f"Initializing cache with max_size={self.max_size}")
        
        try:
            self.cache = {}
            self.access_times = {}
            self.hit_count = 0
            self.miss_count = 0
            self.eviction_count = 0
            
            connect_end = time.time()
            self.connection_time = datetime.datetime.now(datetime.timezone.utc)
            self.metrics.connection_time = connect_end - connect_start
            self.connected = True
            
            self.logger.info(f"Cache initialized successfully in {self.metrics.connection_time:.3f}s")
            print(f"✓ Cache initialized: max_size={self.max_size} ({self.metrics.connection_time:.3f}s)")
            
        except Exception as e:
            self.metrics.end_operation(success=False)
            self.logger.error(f"Cache initialization failed: {e}", exc_info=True)
            print(f"✗ Cache initialization failed: {e}")
            raise
    
    async def disconnect(self) -> None:
        """Clear cache with cleanup tracking"""
        disconnect_start = time.time()
        self.logger.info("Clearing cache")
        
        if self.connected:
            try:
                cache_size = len(self.cache)
                self.cache.clear()
                self.access_times.clear()
                
                disconnect_time = time.time() - disconnect_start
                self.connected = False
                
                self.logger.info(f"Cache cleared successfully in {disconnect_time:.3f}s (cleared {cache_size} items)")
                print(f"✓ Cache cleared ({cache_size} items, {disconnect_time:.3f}s)")
                
                # Clear references to help with garbage collection
                self.metrics = None
                self.logger = None
                
            except Exception as e:
                self.logger.error(f"Error during cache cleanup: {e}", exc_info=True)
                raise
    
    async def test_connection(self) -> Dict[str, Any]:
        """Test cache connection with performance metrics"""
        if not self.connected:
            raise RuntimeError("Cache not connected")
        
        test_start = time.time()
        self.logger.debug("Testing cache connection")
        
        try:
            # Add a test entry
            test_key = f"test_{int(time.time())}"
            self.cache[test_key] = "test_value"
            self.access_times[test_key] = time.time()
            
            test_time = time.time() - test_start
            
            result = {
                "max_size": self.max_size,
                "current_size": len(self.cache),
                "test_key": test_key,
                "connection_time": self.connection_time.isoformat() if self.connection_time else None,
                "test_execution_time": test_time,
                "performance_metrics": self.metrics.to_dict(),
                "cache_stats": {
                    "hit_count": self.hit_count,
                    "miss_count": self.miss_count,
                    "eviction_count": self.eviction_count,
                    "hit_ratio": self.hit_count / (self.hit_count + self.miss_count) if (self.hit_count + self.miss_count) > 0 else 0
                }
            }
            
            self.logger.info(f"Cache test completed successfully in {test_time:.3f}s")
            return result
            
        except Exception as e:
            self.metrics.end_operation(success=False)
            self.logger.error(f"Cache test failed: {e}", exc_info=True)
            raise
    
    async def execute_operation(self, operation: str, data: Dict[str, Any]) -> Any:
        """Execute cache operations with performance tracking"""
        if not self.connected:
            raise RuntimeError("Cache not connected")
        
        op_start = time.time()
        self.logger.debug(f"Executing cache operation: {operation}")
        
        try:
            if operation == "get":
                result = await self._execute_get(data)
            elif operation == "set":
                result = await self._execute_set(data)
            elif operation == "delete":
                result = await self._execute_delete(data)
            elif operation == "clear":
                result = await self._execute_clear(data)
            elif operation == "stats":
                result = await self._execute_stats(data)
            else:
                raise ValueError(f"Unsupported cache operation: {operation}")
            
            op_time = time.time() - op_start
            self.metrics.end_operation(success=True)
            
            result["execution_time"] = op_time
            self.logger.debug(f"Cache operation '{operation}' completed successfully in {op_time:.3f}s")
            
            return result
            
        except Exception as e:
            op_time = time.time() - op_start
            self.metrics.end_operation(success=False)
            self.logger.error(f"Cache operation '{operation}' failed after {op_time:.3f}s: {e}", exc_info=True)
            raise
    
    async def _execute_get(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Get value from cache with hit/miss tracking"""
        key = data.get("key")
        if not key:
            raise ValueError("Key is required for get operation")
        
        value = self.cache.get(key)
        if value is not None:
            self.access_times[key] = time.time()  # Update access time
            self.hit_count += 1
            self.logger.debug(f"Cache hit for key: {key}")
        else:
            self.miss_count += 1
            self.logger.debug(f"Cache miss for key: {key}")
        
        return {
            "key": key,
            "value": value,
            "found": value is not None,
            "access_time": time.time(),
            "cache_stats": {
                "hit_count": self.hit_count,
                "miss_count": self.miss_count,
                "current_hit_ratio": self.hit_count / (self.hit_count + self.miss_count) if (self.hit_count + self.miss_count) > 0 else 0
            }
        }
    
    async def _execute_set(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Set value in cache with LRU eviction tracking"""
        key = data.get("key")
        value = data.get("value")
        
        if not key:
            raise ValueError("Key is required for set operation")
        
        evicted_key = None
        # Implement LRU eviction if cache is full
        if len(self.cache) >= self.max_size and key not in self.cache:
            # Remove least recently used item
            oldest_key = min(self.access_times.keys(), key=lambda k: self.access_times[k])
            del self.cache[oldest_key]
            del self.access_times[oldest_key]
            evicted_key = oldest_key
            self.eviction_count += 1
            self.logger.debug(f"Cache eviction: removed key {oldest_key}")
        
        self.cache[key] = value
        self.access_times[key] = time.time()
        self.logger.debug(f"Cache set: key={key}, value_size={len(str(value))}")
        
        return {
            "key": key,
            "value": value,
            "cache_size": len(self.cache),
            "set_time": time.time(),
            "evicted_key": evicted_key,
            "eviction_count": self.eviction_count
        }
    
    async def _execute_delete(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Delete value from cache"""
        key = data.get("key")
        if not key:
            raise ValueError("Key is required for delete operation")
        
        deleted = key in self.cache
        if deleted:
            del self.cache[key]
            del self.access_times[key]
        
        return {
            "key": key,
            "deleted": deleted,
            "cache_size": len(self.cache)
        }
    
    async def _execute_clear(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Clear all cache entries"""
        size_before = len(self.cache)
        self.cache.clear()
        self.access_times.clear()
        
        return {
            "cleared_entries": size_before,
            "cache_size": len(self.cache)
        }
    
    async def _execute_stats(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Get cache statistics"""
        return {
            "max_size": self.max_size,
            "current_size": len(self.cache),
            "keys": list(self.cache.keys()),
            "oldest_access": min(self.access_times.values()) if self.access_times else None,
            "newest_access": max(self.access_times.values()) if self.access_times else None
        }

# ---------- Custom Context Manager ----------

class ResourceManager(dict):
    """
    Robust context manager for managing multiple external resource connections.
    Automatically handles connection setup, cleanup, error recovery, and performance tracking.
    
    Features:
    - Supports nested context managers
    - Clear API for resource acquisition and release
    - Parallel connection setup and cleanup
    - Comprehensive error handling and logging
    - Performance metrics tracking
    """
    
    def __init__(self, resource_types: List[str]):
        self.resource_types = resource_types
        self.connections: Dict[str, Any] = {}
        self.connection_errors: Dict[str, str] = {}
        self.logger = logging.getLogger('resource_manager.context')
        self.start_time = None
        self.end_time = None
        self.setup_metrics = {}
        self._is_entered = False
        self._context_id = None
    
    async def __aenter__(self) -> "ResourceManager":
        """Enter the context - establish all connections with detailed tracking"""
        import uuid
        self._context_id = str(uuid.uuid4())[:8]
        self._is_entered = True
        self.start_time = time.time()
        self.logger.info(f"Starting resource manager context [{self._context_id}] for: {', '.join(self.resource_types)}")
        print(f"🔗 Establishing connections to: {', '.join(self.resource_types)}")
        
        connection_tasks = []
        # Create connection objects and track setup time for each
        for resource_type in self.resource_types:
            connection_tasks.append(self._establish_connection(resource_type))
        
        # Execute all connections in parallel and gather results
        if connection_tasks:
            results = await asyncio.gather(*connection_tasks, return_exceptions=True)
            
            # Process results and handle any exceptions
            for i, result in enumerate(results):
                resource_type = self.resource_types[i]
                if isinstance(result, Exception):
                    error_msg = f"Failed to connect to {resource_type}: {result}"
                    self.connection_errors[resource_type] = error_msg
                    self.logger.error(error_msg, exc_info=True)
                    print(f"✗ {error_msg}")
        
        if not self.connections:
            setup_time = time.time() - self.start_time
            error_msg = f"No connections could be established after {setup_time:.3f}s"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg)
        
        setup_time = time.time() - self.start_time
        success_count = len(self.connections)
        error_count = len(self.connection_errors)
        
        self.logger.info(f"Resource manager setup completed: {success_count} successful, {error_count} failed in {setup_time:.3f}s")
        print(f"✅ Successfully connected to {success_count} resources in {setup_time:.3f}s")
        
        if self.connection_errors:
            self.logger.warning(f"Some connections failed: {list(self.connection_errors.keys())}")
        
        return self
    
    async def _establish_connection(self, resource_type: str):
        """Establish a single connection with timing"""
        connect_start = time.time()
        
        try:
            self.logger.debug(f"Creating {resource_type} connection")
            
            if resource_type == "database":
                connection = DatabaseConnection()
            elif resource_type == "api":
                connection = APIConnection()
            elif resource_type == "cache":
                connection = CacheConnection()
            else:
                raise ValueError(f"Unknown resource type: {resource_type}")
            
            await connection.connect()
            
            connect_time = time.time() - connect_start
            self.setup_metrics[resource_type] = connect_time
            self.connections[resource_type] = connection
            
            self.logger.info(f"Successfully connected to {resource_type} in {connect_time:.3f}s")
            
        except Exception as e:
            connect_time = time.time() - connect_start
            self.setup_metrics[resource_type] = connect_time
            self.logger.error(f"Failed to connect to {resource_type} after {connect_time:.3f}s: {e}")
            raise
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Exit the context - cleanup all connections with detailed tracking"""
        cleanup_start = time.time()
        self.logger.info(f"Starting cleanup of {len(self.connections)} connections")
        print(f"🔌 Cleaning up {len(self.connections)} connections")
        
        # Track cleanup performance for each resource
        cleanup_metrics = {}
        
        # Disconnect all resources in parallel
        disconnect_tasks = []
        for resource_type, connection in self.connections.items():
            disconnect_tasks.append(self._safe_disconnect(resource_type, connection, cleanup_metrics))
        
        if disconnect_tasks:
            cleanup_results = await asyncio.gather(*disconnect_tasks, return_exceptions=True)
            
            # Log any cleanup errors
            for i, result in enumerate(cleanup_results):
                if isinstance(result, Exception):
                    resource_type = list(self.connections.keys())[i]
                    self.logger.error(f"Cleanup error for {resource_type}: {result}", exc_info=True)
        
        cleanup_time = time.time() - cleanup_start
        total_time = time.time() - self.start_time if self.start_time else 0
        
        # Log comprehensive performance summary
        self.logger.info(f"Resource manager session summary:")
        self.logger.info(f"  - Total session time: {total_time:.3f}s")
        self.logger.info(f"  - Setup time: {self.setup_metrics}")
        self.logger.info(f"  - Cleanup time: {cleanup_time:.3f}s")
        self.logger.info(f"  - Successful connections: {len(self.connections)}")
        self.logger.info(f"  - Failed connections: {len(self.connection_errors)}")
        
        print(f"✅ All connections cleaned up in {cleanup_time:.3f}s (total session: {total_time:.3f}s)")
        
        # Clear connections to allow garbage collection
        self.connections.clear()
        self._is_entered = False
        
        # Handle exceptions that occurred in the with block
        if exc_type is not None:
            self.logger.error(f"Context manager exiting due to {exc_type.__name__}: {exc_val}")
            self.logger.debug("Exception traceback:", exc_info=(exc_type, exc_val, exc_tb))
            print(f"⚠️  Context manager exiting due to {exc_type.__name__}: {exc_val}")
            return False  # Propagate the exception
    
    async def _safe_disconnect(self, resource_type: str, connection: Any, cleanup_metrics: Dict[str, float]):
        """Safely disconnect a resource with performance tracking"""
        disconnect_start = time.time()
        
        try:
            await connection.disconnect()
            disconnect_time = time.time() - disconnect_start
            cleanup_metrics[resource_type] = disconnect_time
            self.logger.debug(f"Successfully disconnected {resource_type} in {disconnect_time:.3f}s")
            
        except Exception as e:
            disconnect_time = time.time() - disconnect_start
            cleanup_metrics[resource_type] = disconnect_time
            self.logger.error(f"Error disconnecting {resource_type} after {disconnect_time:.3f}s: {e}", exc_info=True)
            print(f"⚠️  Error disconnecting {resource_type}: {e}")
            # Don't re-raise, continue with other cleanups
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get comprehensive performance summary"""
        return {
            "setup_metrics": self.setup_metrics,
            "connection_errors": self.connection_errors,
            "successful_connections": list(self.connections.keys()),
            "total_setup_time": sum(self.setup_metrics.values()),
            "connection_success_rate": len(self.connections) / len(self.resource_types) if self.resource_types else 0
        }
    
    # ---------- Enhanced API for Resource Acquisition and Release ----------
    
    async def acquire_resource(self, resource_type: str) -> Any:
        """
        Explicitly acquire a single resource outside of context manager.
        Useful for dynamic resource acquisition.
        """
        if not self._is_entered:
            raise RuntimeError("Cannot acquire resource outside of context manager")
            
        if resource_type in self.connections:
            self.logger.info(f"Resource '{resource_type}' already acquired")
            return self.connections[resource_type]
            
        if resource_type in self.connection_errors:
            raise RuntimeError(f"Resource '{resource_type}' previously failed to connect: {self.connection_errors[resource_type]}")
        
        self.logger.info(f"Dynamically acquiring resource: {resource_type}")
        connection = await self._establish_connection(resource_type)
        
        if resource_type in self.connections:
            return self.connections[resource_type]
        else:
            raise RuntimeError(f"Failed to acquire resource: {resource_type}")
    
    async def release_resource(self, resource_type: str) -> bool:
        """
        Explicitly release a single resource.
        Returns True if successfully released, False otherwise.
        """
        if resource_type not in self.connections:
            self.logger.warning(f"Cannot release resource '{resource_type}': not connected")
            return False
            
        connection = self.connections[resource_type]
        cleanup_metrics = {}
        
        try:
            await self._safe_disconnect(resource_type, connection, cleanup_metrics)
            del self.connections[resource_type]
            self.logger.info(f"Successfully released resource: {resource_type}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to release resource '{resource_type}': {e}")
            return False
    
    def get_acquired_resources(self) -> List[str]:
        """Get list of currently acquired resources"""
        return list(self.connections.keys())
    
    def get_failed_resources(self) -> Dict[str, str]:
        """Get dictionary of failed resources and their error messages"""
        return self.connection_errors.copy()
    
    def is_resource_acquired(self, resource_type: str) -> bool:
        """Check if a specific resource is currently acquired"""
        return resource_type in self.connections
    
    def get_resource(self, resource_type: str) -> Any:
        """
        Get a specific acquired resource.
        Raises KeyError if resource is not acquired.
        """
        if resource_type not in self.connections:
            available = list(self.connections.keys())
            raise KeyError(f"Resource '{resource_type}' not acquired. Available: {available}")
        return self.connections[resource_type]
    
    async def test_all_resources(self) -> Dict[str, Any]:
        """Test all acquired resources and return their status"""
        if not self._is_entered:
            raise RuntimeError("Cannot test resources outside of context manager")
            
        results = {}
        for resource_type, connection in self.connections.items():
            try:
                test_result = await connection.test_connection()
                results[resource_type] = {
                    "status": "success",
                    "result": test_result,
                    "timestamp": datetime.datetime.utcnow().isoformat() + "Z"
                }
            except Exception as e:
                results[resource_type] = {
                    "status": "error",
                    "error": str(e),
                    "timestamp": datetime.datetime.utcnow().isoformat() + "Z"
                }
                
        return results
    
    # ---------- Dictionary-like Interface ----------
    
    def __getitem__(self, key: str) -> Any:
        """Allow dictionary-style access to resources"""
        return self.connections[key]
    
    def __setitem__(self, key: str, value: Any) -> None:
        """Allow dictionary-style assignment (internal use only)"""
        self.connections[key] = value
    
    def __delitem__(self, key: str) -> None:
        """Allow dictionary-style deletion"""
        if key in self.connections:
            del self.connections[key]
        else:
            raise KeyError(f"Resource '{key}' not found")
    
    def __contains__(self, key: str) -> bool:
        """Allow 'in' operator to check if resource exists"""
        return key in self.connections
    
    def __len__(self) -> int:
        """Return the number of acquired resources"""
        return len(self.connections)
    
    def __iter__(self):
        """Allow iteration over resource names"""
        return iter(self.connections)
    
    def keys(self):
        """Return resource names (dictionary-like interface)"""
        return self.connections.keys()
    
    def values(self):
        """Return resource connections (dictionary-like interface)"""
        return self.connections.values()
    
    def items(self):
        """Return resource name-connection pairs (dictionary-like interface)"""
        return self.connections.items()

# ---------- Enhanced Logging Functions ----------

async def save_connection_log(logs: List[Dict[str, Any]]):
    """Save connection logs to database with performance tracking"""
    save_start = time.time()
    logger.debug(f"Saving {len(logs)} connection logs")
    
    try:
        async with ResourceManager(["database"]) as resources:
            db_connection = resources.connections["database"]
            
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, _save_logs_sync, db_connection.connection, logs)
            
            save_time = time.time() - save_start
            logger.info(f"Successfully saved {len(logs)} connection logs in {save_time:.3f}s")
            
    except Exception as e:
        save_time = time.time() - save_start
        logger.error(f"Failed to save connection logs after {save_time:.3f}s: {e}", exc_info=True)
        print(f"✗ Failed to save connection logs: {e}")

def _save_logs_sync(connection, logs: List[Dict[str, Any]]):
    """Synchronous log saving with enhanced schema"""
    cursor = connection.cursor()
    
    for log in logs:
        cursor.execute("""
            INSERT INTO resource_logs (resource, action, status, error, execution_time, memory_usage, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            log.get("resource"),
            log.get("action"),
            log.get("status"),
            log.get("error"),
            log.get("execution_time"),
            log.get("memory_usage"),
            log.get("timestamp")
        ))
    
    connection.commit()

async def get_connection_logs(limit: int = 20) -> List[Dict[str, Any]]:
    """Get recent connection logs from database"""
    query_start = time.time()
    logger.debug(f"Retrieving {limit} connection logs")
    
    try:
        async with ResourceManager(["database"]) as resources:
            db_connection = resources["database"]
            
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, _get_logs_sync, db_connection.connection, limit)
            
            query_time = time.time() - query_start
            logger.info(f"Retrieved {len(result)} connection logs in {query_time:.3f}s")
            return result
            
    except Exception as e:
        query_time = time.time() - query_start
        logger.error(f"Failed to retrieve connection logs after {query_time:.3f}s: {e}", exc_info=True)
        print(f"✗ Failed to retrieve connection logs: {e}")
        return []

def _get_logs_sync(connection, limit: int) -> List[Dict[str, Any]]:
    """Synchronous log retrieval with enhanced fields"""
    cursor = connection.cursor()
    cursor.execute("""
        SELECT resource, action, status, error, execution_time, memory_usage, timestamp, created_at
        FROM resource_logs 
        ORDER BY created_at DESC 
        LIMIT ?
    """, (limit,))
    
    results = []
    for row in cursor.fetchall():
        results.append({
            "resource": row[0],
            "action": row[1],
            "status": row[2],
            "error": row[3],
            "execution_time": row[4],
            "memory_usage": row[5],
            "timestamp": row[6],
            "created_at": row[7]
        })
    
    return results

# ---------- Performance Analytics Functions ----------

async def get_performance_analytics(resource_type: Optional[str] = None, hours: int = 24) -> Dict[str, Any]:
    """Get comprehensive performance analytics"""
    analytics_start = time.time()
    logger.info(f"Generating performance analytics for {resource_type or 'all resources'} over {hours} hours")
    
    try:
        async with ResourceManager(["database"]) as resources:
            db_connection = resources.connections["database"]
            
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, _get_analytics_sync, db_connection.connection, resource_type, hours)
            
            analytics_time = time.time() - analytics_start
            logger.info(f"Performance analytics generated in {analytics_time:.3f}s")
            result["analytics_generation_time"] = analytics_time
            return result
            
    except Exception as e:
        analytics_time = time.time() - analytics_start
        logger.error(f"Failed to generate performance analytics after {analytics_time:.3f}s: {e}", exc_info=True)
        return {"error": str(e), "analytics_generation_time": analytics_time}

def _get_analytics_sync(connection, resource_type: Optional[str], hours: int) -> Dict[str, Any]:
    """Synchronous performance analytics generation"""
    cursor = connection.cursor()
    
    # Base query with time filter
    time_filter = f"datetime('now', '-{hours} hours')"
    where_clause = f"WHERE created_at >= {time_filter}"
    if resource_type:
        where_clause += f" AND resource_type = '{resource_type}'"
    
    analytics = {}
    
    # Get operation counts and timing
    cursor.execute(f"""
        SELECT 
            resource_type,
            operation_type,
            COUNT(*) as operation_count,
            AVG(execution_time) as avg_execution_time,
            MIN(execution_time) as min_execution_time,
            MAX(execution_time) as max_execution_time,
            SUM(success_count) as total_successes,
            SUM(error_count) as total_errors
        FROM performance_metrics 
        {where_clause}
        GROUP BY resource_type, operation_type
    """)
    
    operations = []
    for row in cursor.fetchall():
        operations.append({
            "resource_type": row[0],
            "operation_type": row[1],
            "operation_count": row[2],
            "avg_execution_time": row[3],
            "min_execution_time": row[4],
            "max_execution_time": row[5],
            "total_successes": row[6],
            "total_errors": row[7],
            "success_rate": row[6] / (row[6] + row[7]) if (row[6] + row[7]) > 0 else 0
        })
    
    analytics["operations"] = operations
    
    # Get error summary
    cursor.execute(f"""
        SELECT 
            resource,
            COUNT(*) as error_count,
            error
        FROM resource_logs 
        {where_clause.replace('resource_type', 'resource')} AND status = 'error'
        GROUP BY resource, error
        ORDER BY error_count DESC
        LIMIT 10
    """)
    
    errors = []
    for row in cursor.fetchall():
        errors.append({
            "resource": row[0],
            "error_count": row[1],
            "error_message": row[2]
        })
    
    analytics["top_errors"] = errors
    
    # Get overall summary
    total_operations = sum(op["operation_count"] for op in operations)
    total_successes = sum(op["total_successes"] for op in operations)
    total_errors = sum(op["total_errors"] for op in operations)
    
    analytics["summary"] = {
        "total_operations": total_operations,
        "total_successes": total_successes,
        "total_errors": total_errors,
        "overall_success_rate": total_successes / (total_successes + total_errors) if (total_successes + total_errors) > 0 else 0,
        "time_period_hours": hours,
        "resource_filter": resource_type
    }
    
    return analytics
