"""
Advanced Database Configuration for High-Concurrency Applications
Implements connection pooling, query optimization, and performance monitoring
"""

import time
import logging
from django.conf import settings
from django.db import connections, DatabaseError
from django.db.models import Q, Count, Sum, Avg, Max, Min
from django.core.cache import cache
from django.db import transaction
from contextlib import contextmanager
import threading
from collections import defaultdict
import json

logger = logging.getLogger(__name__)

class DatabaseOptimizer:
    """Advanced database optimization and monitoring"""
    
    def __init__(self):
        self.query_stats = defaultdict(list)
        self.slow_queries = []
        self.connection_stats = defaultdict(dict)
        self.lock = threading.Lock()
    
    @contextmanager
    def monitor_query(self, query_name):
        """Monitor query performance"""
        start_time = time.time()
        connection_name = 'default'
        
        try:
            # Get connection stats before query
            connection = connections[connection_name]
            initial_queries = len(connection.queries) if settings.DEBUG else 0
            
            yield
            
            # Calculate query time
            query_time = time.time() - start_time
            
            # Record query stats
            with self.lock:
                self.query_stats[query_name].append(query_time)
                
                if query_time > 1.0:  # Slow query threshold
                    self.slow_queries.append({
                        'query': query_name,
                        'time': query_time,
                        'timestamp': time.time()
                    })
            
            # Log slow queries
            if query_time > 1.0:
                logger.warning(f"Slow query detected: {query_name} took {query_time:.2f}s")
                
        except Exception as e:
            logger.error(f"Query monitoring error for {query_name}: {e}")
            raise
    
    def get_query_stats(self, query_name=None):
        """Get query statistics"""
        with self.lock:
            if query_name:
                stats = self.query_stats.get(query_name, [])
                return {
                    'count': len(stats),
                    'avg_time': sum(stats) / len(stats) if stats else 0,
                    'max_time': max(stats) if stats else 0,
                    'min_time': min(stats) if stats else 0
                }
            else:
                return {
                    name: self.get_query_stats(name) 
                    for name in self.query_stats.keys()
                }
    
    def get_slow_queries(self, limit=10):
        """Get recent slow queries"""
        with self.lock:
            return sorted(self.slow_queries, key=lambda x: x['time'], reverse=True)[:limit]

class ConnectionPoolManager:
    """Manages database connection pools"""
    
    def __init__(self):
        self.pool_stats = defaultdict(dict)
        self.lock = threading.Lock()
    
    def get_connection_stats(self):
        """Get connection pool statistics"""
        stats = {}
        for alias in connections:
            connection = connections[alias]
            stats[alias] = {
                'is_usable': connection.is_usable(),
                'queries_logged': len(connection.queries) if settings.DEBUG else 0,
                'vendor': connection.vendor,
            }
        return stats
    
    def close_all_connections(self):
        """Close all database connections"""
        for alias in connections:
            connections[alias].close()
        logger.info("All database connections closed")

class QueryOptimizer:
    """Optimize database queries for better performance"""
    
    @staticmethod
    def optimize_user_queryset(filters=None, ordering=None, pagination=None):
        """Optimize user queryset with proper indexing and prefetching"""
        from .models import User
        
        queryset = User.objects.filter(role='1')  # Only customers
        
        # Apply filters efficiently
        if filters:
            if filters.get('search'):
                search_query = filters['search']
                queryset = queryset.filter(
                    Q(username__icontains=search_query) |
                    Q(email__icontains=search_query) |
                    Q(mobile_no__icontains=search_query)
                )
            
            if filters.get('user_type'):
                queryset = queryset.filter(user_type=filters['user_type'])
            
            if filters.get('status'):
                queryset = queryset.filter(status=filters['status'])
        
        # Use select_related and prefetch_related for optimization
        queryset = queryset.select_related(
            'user_profile'  # Add related models as needed
        ).prefetch_related(
            'user_sub_plan',
            'user_properties',
            'log_history'
        )
        
        # Apply ordering
        if ordering:
            queryset = queryset.order_by(ordering)
        
        # Apply pagination efficiently
        if pagination:
            page = pagination.get('page', 1)
            page_size = pagination.get('page_size', 20)
            offset = (page - 1) * page_size
            
            queryset = queryset[offset:offset + page_size]
        
        return queryset
    
    @staticmethod
    def bulk_update_users(users, fields):
        """Bulk update users for better performance"""
        from .models import User
        
        try:
            User.objects.bulk_update(users, fields, batch_size=100)
            return True
        except Exception as e:
            logger.error(f"Bulk update error: {e}")
            return False
    
    @staticmethod
    def bulk_create_users(users):
        """Bulk create users for better performance"""
        from .models import User
        
        try:
            User.objects.bulk_create(users, batch_size=100)
            return True
        except Exception as e:
            logger.error(f"Bulk create error: {e}")
            return False

class DatabaseCacheManager:
    """Intelligent database caching"""
    
    def __init__(self):
        self.cache_versions = {}
        self.lock = threading.Lock()
    
    def get_cache_version(self, table_name):
        """Get cache version for table"""
        with self.lock:
            return self.cache_versions.get(table_name, 0)
    
    def increment_cache_version(self, table_name):
        """Increment cache version when table changes"""
        with self.lock:
            self.cache_versions[table_name] = self.cache_versions.get(table_name, 0) + 1
            return self.cache_versions[table_name]
    
    def get_cached_queryset(self, cache_key, queryset_func, timeout=300):
        """Get cached queryset or execute and cache"""
        from .cache_config import redis_manager
        
        # Try to get from cache
        cached_data = redis_manager.get(cache_key)
        if cached_data:
            try:
                return json.loads(cached_data)
            except:
                pass
        
        # Execute queryset
        queryset = queryset_func()
        
        # Convert to list and cache
        try:
            data = list(queryset.values())
            redis_manager.set(cache_key, json.dumps(data, default=str), timeout)
            return data
        except Exception as e:
            logger.error(f"Queryset caching error: {e}")
            return list(queryset.values())

# Global instances
db_optimizer = DatabaseOptimizer()
connection_pool_manager = ConnectionPoolManager()
query_optimizer = QueryOptimizer()
db_cache_manager = DatabaseCacheManager()

# Query monitoring decorator
def monitor_query_performance(query_name):
    """Decorator to monitor query performance"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            with db_optimizer.monitor_query(query_name):
                return func(*args, **kwargs)
        return wrapper
    return decorator

# Database health check
def check_database_health():
    """Check database health and performance"""
    health_status = {
        'status': 'healthy',
        'issues': [],
        'stats': {}
    }
    
    try:
        # Check connection
        connection = connections['default']
        if not connection.is_usable():
            health_status['status'] = 'unhealthy'
            health_status['issues'].append('Database connection not usable')
        
        # Check slow queries
        slow_queries = db_optimizer.get_slow_queries(5)
        if slow_queries:
            health_status['issues'].append(f'Found {len(slow_queries)} slow queries')
        
        # Check connection stats
        connection_stats = connection_pool_manager.get_connection_stats()
        health_status['stats']['connections'] = connection_stats
        
        # Check query stats
        query_stats = db_optimizer.get_query_stats()
        health_status['stats']['queries'] = query_stats
        
    except Exception as e:
        health_status['status'] = 'unhealthy'
        health_status['issues'].append(f'Database health check failed: {e}')
    
    return health_status

# Automatic connection cleanup
class ConnectionCleanupMiddleware:
    """Middleware to clean up database connections"""
    
    def __init__(self, get_response):
        self.get_response = get_response
    
    def __call__(self, request):
        response = self.get_response(request)
        
        # Close connections after each request
        try:
            for alias in connections:
                connections[alias].close()
        except Exception as e:
            logger.error(f"Connection cleanup error: {e}")
        
        return response

# Database optimization utilities
class DatabaseUtils:
    """Utility functions for database optimization"""
    
    @staticmethod
    @contextmanager
    def transaction_with_retry(max_retries=3):
        """Transaction with automatic retry"""
        for attempt in range(max_retries):
            try:
                with transaction.atomic():
                    yield
                    break
            except DatabaseError as e:
                if attempt == max_retries - 1:
                    raise
                logger.warning(f"Database retry {attempt + 1}/{max_retries}: {e}")
                time.sleep(0.1 * (attempt + 1))
    
    @staticmethod
    def execute_raw_query(query, params=None):
        """Execute raw SQL query safely"""
        try:
            with connection_pool_manager.monitor_query('raw_query'):
                with connections['default'].cursor() as cursor:
                    cursor.execute(query, params or [])
                    columns = [col[0] for col in cursor.description]
                    results = [dict(zip(columns, row)) for row in cursor.fetchall()]
                    return results
        except Exception as e:
            logger.error(f"Raw query error: {e}")
            return []
    
    @staticmethod
    def get_table_stats(table_name):
        """Get table statistics"""
        try:
            query = """
                SELECT 
                    COUNT(*) as total_rows,
                    COUNT(DISTINCT id) as unique_ids,
                    MAX(created_at) as latest_record,
                    MIN(created_at) as earliest_record
                FROM {}
            """.format(table_name)
            
            return DatabaseUtils.execute_raw_query(query)
        except Exception as e:
            logger.error(f"Table stats error for {table_name}: {e}")
            return {}

# Performance monitoring
class PerformanceMonitor:
    """Monitor database performance metrics"""
    
    def __init__(self):
        self.metrics = defaultdict(list)
        self.lock = threading.Lock()
    
    def record_metric(self, metric_name, value):
        """Record performance metric"""
        with self.lock:
            self.metrics[metric_name].append({
                'value': value,
                'timestamp': time.time()
            })
    
    def get_metrics_summary(self, metric_name, minutes=5):
        """Get metrics summary for last N minutes"""
        with self.lock:
            cutoff_time = time.time() - (minutes * 60)
            recent_metrics = [
                m for m in self.metrics[metric_name] 
                if m['timestamp'] > cutoff_time
            ]
            
            if not recent_metrics:
                return {}
            
            values = [m['value'] for m in recent_metrics]
            return {
                'count': len(values),
                'avg': sum(values) / len(values),
                'min': min(values),
                'max': max(values),
                'latest': values[-1]
            }

# Global performance monitor
performance_monitor = PerformanceMonitor()
