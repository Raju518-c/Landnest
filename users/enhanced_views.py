"""
Enhanced Views with High-Concurrency Optimizations
Integrates Redis clustering, Kafka message queuing, rate limiting, and database optimization
"""

from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.contrib.auth.hashers import make_password, check_password
from django.shortcuts import get_object_or_404
from django.utils import timezone
from django.db.models import Q, Count, Prefetch, Max, F
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
from drf_spectacular.utils import extend_schema
from django.conf import settings
import json
import time
import uuid
import logging

# Import our optimization modules
from ..cache_config import redis_manager, CacheKeyGenerator, cache_result
from ..kafka_config import kafka_manager, RequestQueueManager
from ..rate_limiting import rate_limiter, request_throttler, rate_limit
from ..database_config import db_optimizer, query_optimizer, performance_monitor

logger = logging.getLogger(__name__)

class EnhancedUserListCreateAPIView(APIView):
    """
    Enhanced User List API with high-concurrency optimizations
    Handles thousands of concurrent requests efficiently
    """
    
    @extend_schema(
        summary="Enhanced User List API",
        description="Get users with advanced caching, rate limiting, and performance optimization"
    )
    
    @rate_limit(limit=500, window=3600, scope='user')
    def get(self, request):
        """Get users with all optimizations applied"""
        start_time = time.time()
        request_id = str(uuid.uuid4())
        
        try:
            # Extract and validate parameters
            page = int(request.GET.get('page', 1))
            page_size = min(int(request.GET.get('page_size', 20)), 5000)  # Max 5000
            search = request.GET.get('search', '').strip()
            sort_by = request.GET.get('sort_by', 'created_at')
            sort_order = request.GET.get('sort_order', 'desc')
            progressive = request.GET.get('progressive', 'false').lower() == 'true'
            chunk_size = int(request.GET.get('chunk_size', 100)) if progressive else 20
            offset = int(request.GET.get('offset', 0))
            
            # Validate sort_by field
            allowed_sort_fields = ['username', 'email', 'mobile_no', 'created_at', 'user_type']
            if sort_by not in allowed_sort_fields:
                sort_by = 'created_at'
            
            # Generate optimized cache key
            cache_key = CacheKeyGenerator.user_list(
                page, page_size, search, '', '', sort_by, sort_order, progressive, offset, chunk_size
            )
            
            # Try to get from distributed cache
            cached_data = redis_manager.get(cache_key)
            if cached_data:
                performance_monitor.record_metric('cache_hit', 1)
                return Response(json.loads(cached_data))
            
            # Check request throttling for authenticated users
            if hasattr(request, 'user') and request.user.is_authenticated:
                if not request_throttler.can_process_request(request.user.id):
                    # Queue request if throttled
                    queue_data = {
                        'request_id': request_id,
                        'user_id': request.user.id,
                        'endpoint': 'user_list',
                        'parameters': request.GET.dict(),
                        'priority': 'normal'
                    }
                    
                    if RequestQueueManager.queue_user_request(queue_data):
                        return Response({
                            'message': 'Request queued due to high load',
                            'request_id': request_id,
                            'queue_position': request_throttler.get_queue_status(request.user.id)['queued']
                        }, status=status.HTTP_202_ACCEPTED)
                    else:
                        return Response({
                            'error': 'System overloaded',
                            'message': 'Please try again later'
                        }, status=status.HTTP_503_SERVICE_UNAVAILABLE)
            
            # Execute optimized database query
            with db_optimizer.monitor_query('user_list_query'):
                queryset = query_optimizer.optimize_user_queryset(
                    filters={'search': search} if search else None,
                    ordering=f"-{sort_by}" if sort_order == 'desc' else sort_by,
                    pagination={'page': page, 'page_size': page_size}
                )
            
            # Handle progressive loading
            if progressive and page_size > 100:
                actual_page_size = min(chunk_size, page_size - offset)
                start_index = (page - 1) * page_size + offset
                queryset = queryset[start_index:start_index + actual_page_size]
                
                # Serialize the chunk
                serializer = UserSerializer(queryset, many=True)
                
                # Progressive response
                response_data = {
                    'results': serializer.data,
                    'pagination': {
                        'current_page': page,
                        'total_pages': 1,  # Will be updated by background task
                        'total_count': 0,   # Will be updated by background task
                        'page_size': page_size,
                        'has_next': offset + chunk_size < page_size,
                        'has_previous': offset > 0,
                        'progressive': True,
                        'chunk_size': chunk_size,
                        'offset': offset,
                        'has_more_chunks': offset + chunk_size < page_size
                    }
                }
            else:
                # Regular pagination
                total_count = queryset.count()
                total_pages = (total_count + page_size - 1) // page_size
                
                serializer = UserSerializer(queryset, many=True)
                
                response_data = {
                    'results': serializer.data,
                    'pagination': {
                        'current_page': page,
                        'total_pages': total_pages,
                        'total_count': total_count,
                        'page_size': page_size,
                        'has_next': page < total_pages,
                        'has_previous': page > 1,
                        'progressive': False
                    }
                }
            
            # Cache the result
            cache_timeout = 600 if progressive else 300  # Longer for progressive loading
            redis_manager.set(cache_key, json.dumps(response_data, default=str), cache_timeout)
            
            # Queue background cache warming
            if not progressive and page_size <= 100:
                warm_cache_data = {
                    'cache_key': f"users_list_warm_{page}_{page_size}_{search}_{sort_by}_{sort_order}",
                    'parameters': request.GET.dict()
                }
                RequestQueueManager.queue_cache_update(warm_cache_data['cache_key'], warm_cache_data)
            
            # Queue analytics event
            if hasattr(request, 'user') and request.user.is_authenticated:
                analytics_data = {
                    'endpoint': 'user_list',
                    'parameters': request.GET.dict(),
                    'response_time': time.time() - start_time,
                    'cache_hit': False,
                    'user_agent': request.META.get('HTTP_USER_AGENT', ''),
                    'ip_address': self._get_client_ip(request)
                }
                RequestQueueManager.queue_analytics_event('api_request', request.user.id, analytics_data)
            
            performance_monitor.record_metric('api_response_time', time.time() - start_time)
            performance_monitor.record_metric('cache_miss', 1)
            
            return Response(response_data)
            
        except Exception as e:
            logger.error(f"Enhanced UserList API error: {e}")
            performance_monitor.record_metric('api_error', 1)
            
            return Response({
                'error': 'Internal server error',
                'message': 'Please try again later',
                'request_id': request_id
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        
        finally:
            # Release request throttling slot
            if hasattr(request, 'user') and request.user.is_authenticated:
                request_throttler.release_request(request.user.id)
    
    def _get_client_ip(self, request):
        """Get client IP address"""
        x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
        if x_forwarded_for:
            return x_forwarded_for.split(',')[0].strip()
        return request.META.get('REMOTE_ADDR')

class EnhancedUserTypeAPIView(APIView):
    """
    Enhanced User Type API with optimizations
    """
    
    @extend_schema(
        summary="Enhanced User Type API",
        description="Get users by type with advanced caching and optimization"
    )
    
    @rate_limit(limit=200, window=3600, scope='user')
    def post(self, request):
        """Get users by type with optimizations"""
        start_time = time.time()
        request_id = str(uuid.uuid4())
        
        try:
            user_type = request.data.get('user_type')
            if not user_type:
                return Response({
                    'error': 'user_type is required'
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # Extract pagination parameters
            page = int(request.data.get('page', 1))
            page_size = min(int(request.data.get('page_size', 20)), 5000)
            search = request.data.get('search', '').strip()
            sort_by = request.data.get('sort_by', 'created_at')
            sort_order = request.data.get('sort_order', 'desc')
            
            # Generate cache key
            cache_key = CacheKeyGenerator.user_list(
                page, page_size, search, user_type, '', sort_by, sort_order, False, 0, page_size
            )
            
            # Try cache first
            cached_data = redis_manager.get(cache_key)
            if cached_data:
                return Response(json.loads(cached_data))
            
            # Execute optimized query
            with db_optimizer.monitor_query('user_type_query'):
                if user_type == 'Old Users':
                    queryset = query_optimizer.optimize_user_queryset(
                        filters={'user_type': None, 'search': search} if search else {'user_type': None},
                        ordering=f"-{sort_by}" if sort_order == 'desc' else sort_by,
                        pagination={'page': page, 'page_size': page_size}
                    )
                else:
                    queryset = query_optimizer.optimize_user_queryset(
                        filters={'user_type': user_type, 'search': search} if search else {'user_type': user_type},
                        ordering=f"-{sort_by}" if sort_order == 'desc' else sort_by,
                        pagination={'page': page, 'page_size': page_size}
                    )
            
            # Serialize and respond
            serializer = UserSerializer(queryset, many=True)
            total_count = queryset.count()
            total_pages = (total_count + page_size - 1) // page_size
            
            response_data = {
                'results': serializer.data,
                'pagination': {
                    'current_page': page,
                    'total_pages': total_pages,
                    'total_count': total_count,
                    'page_size': page_size,
                    'has_next': page < total_pages,
                    'has_previous': page > 1
                }
            }
            
            # Cache the result
            redis_manager.set(cache_key, json.dumps(response_data, default=str), 300)
            
            # Queue analytics
            if hasattr(request, 'user') and request.user.is_authenticated:
                analytics_data = {
                    'endpoint': 'user_type',
                    'user_type': user_type,
                    'response_time': time.time() - start_time,
                    'cache_hit': False
                }
                RequestQueueManager.queue_analytics_event('api_request', request.user.id, analytics_data)
            
            return Response(response_data)
            
        except Exception as e:
            logger.error(f"Enhanced UserType API error: {e}")
            return Response({
                'error': 'Internal server error',
                'message': 'Please try again later',
                'request_id': request_id
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class SystemHealthAPIView(APIView):
    """
    System Health Check API
    """
    
    @extend_schema(
        summary="System Health Check",
        description="Check system health and performance metrics"
    )
    
    def get(self, request):
        """Get system health status"""
        try:
            from ..database_config import check_database_health
            
            # Check database health
            db_health = check_database_health()
            
            # Check Redis health
            redis_health = {
                'status': 'healthy' if redis_manager.get_redis_client() else 'unhealthy',
                'cluster_available': redis_manager.cluster is not None,
                'single_instance_available': redis_manager.single_redis is not None
            }
            
            # Check Kafka health
            kafka_health = {
                'status': 'healthy' if kafka_manager.producer else 'unhealthy',
                'producer_available': kafka_manager.producer is not None
            }
            
            # Get performance metrics
            performance_metrics = {
                'query_stats': db_optimizer.get_query_stats(),
                'slow_queries': db_optimizer.get_slow_queries(5),
                'api_response_time': performance_monitor.get_metrics_summary('api_response_time'),
                'cache_hit_rate': performance_monitor.get_metrics_summary('cache_hit'),
                'cache_miss_rate': performance_monitor.get_metrics_summary('cache_miss')
            }
            
            # Get connection stats
            connection_stats = {
                'database': db_optimizer.query_stats,
                'rate_limiter': rate_limiter.get_remaining_requests('system', 1000, 3600)
            }
            
            health_data = {
                'status': 'healthy' if all([
                    db_health['status'] == 'healthy',
                    redis_health['status'] == 'healthy',
                    kafka_health['status'] == 'healthy'
                ]) else 'degraded',
                'timestamp': time.time(),
                'components': {
                    'database': db_health,
                    'redis': redis_health,
                    'kafka': kafka_health
                },
                'performance': performance_metrics,
                'connections': connection_stats
            }
            
            return Response(health_data)
            
        except Exception as e:
            logger.error(f"Health check error: {e}")
            return Response({
                'status': 'unhealthy',
                'error': str(e),
                'timestamp': time.time()
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class PerformanceMetricsAPIView(APIView):
    """
    Performance Metrics API
    """
    
    @extend_schema(
        summary="Performance Metrics",
        description="Get detailed performance metrics"
    )
    
    def get(self, request):
        """Get performance metrics"""
        try:
            metrics = {
                'database': {
                    'query_performance': db_optimizer.get_query_stats(),
                    'slow_queries': db_optimizer.get_slow_queries(10),
                    'connection_stats': db_optimizer.get_query_stats()
                },
                'cache': {
                    'hit_rate': performance_monitor.get_metrics_summary('cache_hit'),
                    'miss_rate': performance_monitor.get_metrics_summary('cache_miss'),
                    'redis_cluster': redis_manager.cluster is not None
                },
                'api': {
                    'response_time': performance_monitor.get_metrics_summary('api_response_time'),
                    'error_rate': performance_monitor.get_metrics_summary('api_error'),
                    'rate_limit_status': rate_limiter.get_remaining_requests('system', 1000, 3600)
                },
                'kafka': {
                    'producer_status': kafka_manager.producer is not None,
                    'consumers_active': len(kafka_manager.consumers)
                }
            }
            
            return Response(metrics)
            
        except Exception as e:
            logger.error(f"Performance metrics error: {e}")
            return Response({
                'error': 'Failed to get metrics',
                'details': str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

# Enhanced decorators and utilities
def enhanced_cache_result(timeout=300, key_prefix=""):
    """Enhanced cache decorator with multiple strategies"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            # Generate cache key
            cache_key = f"{key_prefix}:{func.__name__}:{hash(str(args) + str(kwargs))}"
            
            # Try distributed cache first
            cached_data = redis_manager.get(cache_key)
            if cached_data:
                return json.loads(cached_data)
            
            # Execute function
            result = func(*args, **kwargs)
            
            # Cache result
            try:
                redis_manager.set(cache_key, json.dumps(result, default=str), timeout)
            except Exception as e:
                logger.error(f"Enhanced cache error: {e}")
            
            return result
        return wrapper
    return decorator

# Request context for tracking
class RequestContext:
    """Track request context for performance monitoring"""
    
    def __init__(self, request):
        self.request_id = str(uuid.uuid4())
        self.user_id = getattr(request.user, 'id', None) if hasattr(request, 'user') else None
        self.ip_address = self._get_client_ip(request)
        self.user_agent = request.META.get('HTTP_USER_AGENT', '')
        self.start_time = time.time()
        self.endpoint = request.path
        self.method = request.method
    
    def _get_client_ip(self, request):
        """Get client IP address"""
        x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
        if x_forwarded_for:
            return x_forwarded_for.split(',')[0].strip()
        return request.META.get('REMOTE_ADDR')
    
    def get_duration(self):
        """Get request duration"""
        return time.time() - self.start_time
    
    def to_dict(self):
        """Convert to dictionary"""
        return {
            'request_id': self.request_id,
            'user_id': self.user_id,
            'ip_address': self.ip_address,
            'user_agent': self.user_agent,
            'endpoint': self.endpoint,
            'method': self.method,
            'duration': self.get_duration()
        }
