"""
Advanced Redis Configuration for High-Concurrency Applications
Supports Redis clustering, connection pooling, and distributed caching
"""

import redis
from redis.cluster import RedisCluster
from django.conf import settings
import json
import logging

logger = logging.getLogger(__name__)

class RedisClusterManager:
    """Manages Redis cluster connections and operations"""
    
    def __init__(self):
        self.cluster_nodes = [
            {'host': 'localhost', 'port': 7000},
            {'host': 'localhost', 'port': 7001},
            {'host': 'localhost', 'port': 7002},
            {'host': 'localhost', 'port': 7003},
            {'host': 'localhost', 'port': 7004},
            {'host': 'localhost', 'port': 7005},
        ]
        self.cluster = None
        self.single_redis = None
        self._initialize_connections()
    
    def _initialize_connections(self):
        """Initialize Redis cluster with fallback to single instance"""
        try:
            # Try cluster first
            self.cluster = RedisCluster(
                startup_nodes=self.cluster_nodes,
                decode_responses=True,
                skip_full_coverage_check=True,
                max_connections_per_node=100,
                retry_on_timeout=True,
                socket_timeout=5,
                socket_connect_timeout=5,
                max_connections=1000
            )
            # Test connection
            self.cluster.ping()
            logger.info("Redis cluster connected successfully")
        except Exception as e:
            logger.warning(f"Redis cluster failed, falling back to single instance: {e}")
            try:
                # Fallback to single Redis instance
                self.single_redis = redis.Redis(
                    host='localhost',
                    port=6379,
                    decode_responses=True,
                    max_connections=1000,
                    retry_on_timeout=True,
                    socket_timeout=5,
                    socket_connect_timeout=5,
                    connection_pool_max_connections=1000
                )
                self.single_redis.ping()
                logger.info("Single Redis instance connected successfully")
            except Exception as e:
                logger.error(f"Redis connection failed: {e}")
                self.single_redis = None
    
    def get_redis_client(self):
        """Get appropriate Redis client (cluster or single)"""
        return self.cluster if self.cluster else self.single_redis
    
    def get(self, key):
        """Get value from Redis with error handling"""
        client = self.get_redis_client()
        if not client:
            return None
        try:
            return client.get(key)
        except Exception as e:
            logger.error(f"Redis get error for key {key}: {e}")
            return None
    
    def set(self, key, value, timeout=300):
        """Set value in Redis with error handling"""
        client = self.get_redis_client()
        if not client:
            return False
        try:
            return client.setex(key, timeout, value)
        except Exception as e:
            logger.error(f"Redis set error for key {key}: {e}")
            return False
    
    def delete(self, key):
        """Delete key from Redis"""
        client = self.get_redis_client()
        if not client:
            return False
        try:
            return client.delete(key)
        except Exception as e:
            logger.error(f"Redis delete error for key {key}: {e}")
            return False
    
    def get_many(self, keys):
        """Get multiple keys efficiently"""
        client = self.get_redis_client()
        if not client:
            return {}
        try:
            return client.mget(keys)
        except Exception as e:
            logger.error(f"Redis mget error: {e}")
            return {}
    
    def set_many(self, mapping, timeout=300):
        """Set multiple keys efficiently"""
        client = self.get_redis_client()
        if not client:
            return False
        try:
            pipe = client.pipeline()
            for key, value in mapping.items():
                pipe.setex(key, timeout, value)
            return pipe.execute()
        except Exception as e:
            logger.error(f"Redis mset error: {e}")
            return False
    
    def increment(self, key, amount=1):
        """Increment counter"""
        client = self.get_redis_client()
        if not client:
            return 0
        try:
            return client.incrby(key, amount)
        except Exception as e:
            logger.error(f"Redis increment error for key {key}: {e}")
            return 0
    
    def exists(self, key):
        """Check if key exists"""
        client = self.get_redis_client()
        if not client:
            return False
        try:
            return client.exists(key)
        except Exception as e:
            logger.error(f"Redis exists error for key {key}: {e}")
            return False

# Global Redis manager instance
redis_manager = RedisClusterManager()

# Cache configuration
CACHE_CONFIG = {
    'default_timeout': 300,  # 5 minutes
    'user_list_timeout': 600,  # 10 minutes for user lists
    'statistics_timeout': 1800,  # 30 minutes for statistics
    'session_timeout': 3600,  # 1 hour for sessions
    'rate_limit_timeout': 60,  # 1 minute for rate limiting
}

class CacheKeyGenerator:
    """Generate optimized cache keys"""
    
    @staticmethod
    def user_list(page, page_size, search, user_type, status, sort_by, sort_order, progressive, offset, chunk_size):
        """Generate cache key for user list"""
        return f"users_list:{page}:{page_size}:{search}:{user_type}:{status}:{sort_by}:{sort_order}:{progressive}:{offset}:{chunk_size}"
    
    @staticmethod
    def user_statistics():
        """Generate cache key for user statistics"""
        return "users:statistics"
    
    @staticmethod
    def rate_limit(user_id, endpoint):
        """Generate cache key for rate limiting"""
        return f"rate_limit:{user_id}:{endpoint}"
    
    @staticmethod
    def session(user_id):
        """Generate cache key for user session"""
        return f"session:{user_id}"
    
    @staticmethod
    def search_results(query, page, page_size):
        """Generate cache key for search results"""
        return f"search:{query}:{page}:{page_size}"

# Advanced cache decorator
def cache_result(timeout=None, key_generator=None):
    """Decorator to cache function results"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            # Generate cache key
            if key_generator:
                cache_key = key_generator(*args, **kwargs)
            else:
                cache_key = f"cache:{func.__name__}:{hash(str(args) + str(kwargs))}"
            
            # Try to get from cache
            cached_result = redis_manager.get(cache_key)
            if cached_result:
                return json.loads(cached_result)
            
            # Execute function and cache result
            result = func(*args, **kwargs)
            cache_timeout = timeout or CACHE_CONFIG.get('default_timeout', 300)
            
            try:
                result_json = json.dumps(result, default=str)
                redis_manager.set(cache_key, result_json, cache_timeout)
            except Exception as e:
                logger.error(f"Cache serialization error: {e}")
            
            return result
        return wrapper
    return decorator
