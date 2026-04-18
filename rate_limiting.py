"""
Advanced Rate Limiting and Request Throttling for High-Concurrency Applications
Implements user-based, IP-based, and endpoint-based rate limiting
"""

import time
import redis
from django.http import JsonResponse
from django.core.cache import cache
from django.conf import settings
from rest_framework import status
import logging
import hashlib
from collections import defaultdict
import threading

logger = logging.getLogger(__name__)

class RateLimiter:
    """Advanced rate limiting with multiple strategies"""
    
    def __init__(self):
        self.redis_client = None
        self.local_cache = defaultdict(dict)
        self.local_cache_lock = threading.Lock()
        self._initialize_redis()
    
    def _initialize_redis(self):
        """Initialize Redis connection for distributed rate limiting"""
        try:
            from .cache_config import redis_manager
            self.redis_client = redis_manager.get_redis_client()
        except Exception as e:
            logger.warning(f"Redis rate limiter unavailable, using local cache: {e}")
    
    def _get_client_ip(self, request):
        """Get client IP address"""
        x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
        if x_forwarded_for:
            ip = x_forwarded_for.split(',')[0].strip()
        else:
            ip = request.META.get('REMOTE_ADDR')
        return ip
    
    def _generate_request_key(self, identifier, window_seconds):
        """Generate rate limit key with time window"""
        time_window = int(time.time() // window_seconds)
        return f"rate_limit:{identifier}:{time_window}"
    
    def is_allowed(self, identifier, limit, window_seconds, increment=1):
        """Check if request is allowed based on rate limit"""
        key = self._generate_request_key(identifier, window_seconds)
        
        if self.redis_client:
            return self._redis_rate_limit(key, limit, increment)
        else:
            return self._local_rate_limit(key, limit, increment)
    
    def _redis_rate_limit(self, key, limit, increment):
        """Distributed rate limiting using Redis"""
        try:
            current_count = self.redis_client.get(key) or 0
            current_count = int(current_count)
            
            if current_count >= limit:
                return False
            
            # Increment counter with expiration
            pipe = self.redis_client.pipeline()
            pipe.incrby(key, increment)
            pipe.expire(key, 60)  # 1 minute expiration
            pipe.execute()
            
            return True
            
        except Exception as e:
            logger.error(f"Redis rate limit error: {e}")
            # Fallback to local rate limiting
            return self._local_rate_limit(key, limit, increment)
    
    def _local_rate_limit(self, key, limit, increment):
        """Local rate limiting using in-memory cache"""
        with self.local_cache_lock:
            current_time = time.time()
            
            # Clean up old entries
            if key in self.local_cache:
                if current_time - self.local_cache[key]['timestamp'] > 60:
                    del self.local_cache[key]
            
            # Check current count
            if key in self.local_cache:
                self.local_cache[key]['count'] += increment
            else:
                self.local_cache[key] = {'count': increment, 'timestamp': current_time}
            
            return self.local_cache[key]['count'] <= limit
    
    def get_remaining_requests(self, identifier, limit, window_seconds):
        """Get remaining requests for rate limit"""
        key = self._generate_request_key(identifier, window_seconds)
        
        if self.redis_client:
            try:
                current_count = int(self.redis_client.get(key) or 0)
                return max(0, limit - current_count)
            except:
                return limit
        else:
            with self.local_cache_lock:
                if key in self.local_cache:
                    return max(0, limit - self.local_cache[key]['count'])
                return limit

class RateLimitMiddleware:
    """Django middleware for rate limiting"""
    
    def __init__(self, get_response):
        self.get_response = get_response
        self.rate_limiter = RateLimiter()
        
        # Rate limit configurations
        self.rate_limits = {
            'default': {'limit': 1000, 'window': 3600},  # 1000 requests per hour
            'users': {'limit': 500, 'window': 3600},     # 500 user requests per hour
            'search': {'limit': 200, 'window': 3600},    # 200 search requests per hour
            'export': {'limit': 50, 'window': 3600},     # 50 export requests per hour
            'upload': {'limit': 100, 'window': 3600},    # 100 upload requests per hour
            'login': {'limit': 20, 'window': 900},       # 20 login attempts per 15 minutes
            'register': {'limit': 10, 'window': 3600},   # 10 registrations per hour
        }
        
        # IP-based rate limits
        self.ip_limits = {
            'default': {'limit': 2000, 'window': 3600},  # 2000 requests per hour per IP
            'suspicious': {'limit': 100, 'window': 3600}, # 100 requests per hour for suspicious IPs
        }
    
    def __call__(self, request):
        """Process request through rate limiting"""
        # Get request identifiers
        user_id = getattr(request.user, 'id', None) if hasattr(request, 'user') else None
        ip_address = self.rate_limiter._get_client_ip(request)
        endpoint = self._get_endpoint_identifier(request)
        
        # Check IP-based rate limiting
        if not self._check_ip_rate_limit(ip_address, request):
            return JsonResponse({
                'error': 'IP rate limit exceeded',
                'message': 'Too many requests from this IP address',
                'retry_after': 3600
            }, status=status.HTTP_429_TOO_MANY_REQUESTS)
        
        # Check user-based rate limiting
        if user_id and not self._check_user_rate_limit(user_id, endpoint):
            return JsonResponse({
                'error': 'User rate limit exceeded',
                'message': 'Too many requests for this endpoint',
                'retry_after': self._get_retry_after(endpoint)
            }, status=status.HTTP_429_TOO_MANY_REQUESTS)
        
        # Check endpoint-based rate limiting
        if not self._check_endpoint_rate_limit(endpoint, request):
            return JsonResponse({
                'error': 'Endpoint rate limit exceeded',
                'message': 'Too many requests for this service',
                'retry_after': self._get_retry_after(endpoint)
            }, status=status.HTTP_429_TOO_MANY_REQUESTS)
        
        # Add rate limit headers
        response = self.get_response(request)
        self._add_rate_limit_headers(response, user_id, endpoint, ip_address)
        
        return response
    
    def _get_endpoint_identifier(self, request):
        """Get endpoint identifier for rate limiting"""
        path = request.path.strip('/')
        
        # Map paths to endpoint types
        endpoint_mapping = {
            'users': 'users',
            'auth/login': 'login',
            'auth/register': 'register',
            'search': 'search',
            'export': 'export',
            'upload': 'upload',
        }
        
        for path_part, endpoint in endpoint_mapping.items():
            if path_part in path:
                return endpoint
        
        return 'default'
    
    def _check_ip_rate_limit(self, ip_address, request):
        """Check IP-based rate limiting"""
        # Check if IP is suspicious (too many requests in short time)
        suspicious_key = f"suspicious_ip:{ip_address}"
        if not self.rate_limiter.is_allowed(suspicious_key, 100, 300):  # 100 requests in 5 minutes
            # Use stricter limits for suspicious IPs
            return self.rate_limiter.is_allowed(f"ip:{ip_address}", 100, 3600)
        
        return self.rate_limiter.is_allowed(f"ip:{ip_address}", 2000, 3600)
    
    def _check_user_rate_limit(self, user_id, endpoint):
        """Check user-based rate limiting"""
        limit_config = self.rate_limits.get(endpoint, self.rate_limits['default'])
        return self.rate_limiter.is_allowed(f"user:{user_id}:{endpoint}", limit_config['limit'], limit_config['window'])
    
    def _check_endpoint_rate_limit(self, endpoint, request):
        """Check endpoint-based rate limiting"""
        limit_config = self.rate_limits.get(endpoint, self.rate_limits['default'])
        return self.rate_limiter.is_allowed(f"endpoint:{endpoint}", limit_config['limit'] * 10, limit_config['window'])
    
    def _get_retry_after(self, endpoint):
        """Get retry after time in seconds"""
        limit_config = self.rate_limits.get(endpoint, self.rate_limits['default'])
        return limit_config['window']
    
    def _add_rate_limit_headers(self, response, user_id, endpoint, ip_address):
        """Add rate limit headers to response"""
        try:
            # Add rate limit headers
            limit_config = self.rate_limits.get(endpoint, self.rate_limits['default'])
            
            if user_id:
                remaining = self.rate_limiter.get_remaining_requests(
                    f"user:{user_id}:{endpoint}", 
                    limit_config['limit'], 
                    limit_config['window']
                )
                response['X-RateLimit-Limit'] = str(limit_config['limit'])
                response['X-RateLimit-Remaining'] = str(remaining)
                response['X-RateLimit-Reset'] = str(int(time.time()) + limit_config['window'])
            
            # Add IP rate limit headers
            ip_remaining = self.rate_limiter.get_remaining_requests(f"ip:{ip_address}", 2000, 3600)
            response['X-RateLimit-IP-Limit'] = '2000'
            response['X-RateLimit-IP-Remaining'] = str(ip_remaining)
            
        except Exception as e:
            logger.error(f"Rate limit headers error: {e}")

class RequestThrottler:
    """Advanced request throttling for high-concurrency scenarios"""
    
    def __init__(self):
        self.concurrent_requests = defaultdict(int)
        self.request_queue = defaultdict(list)
        self.lock = threading.Lock()
        self.max_concurrent_per_user = 10
        self.max_queue_size = 50
    
    def can_process_request(self, user_id):
        """Check if request can be processed immediately"""
        with self.lock:
            if self.concurrent_requests[user_id] < self.max_concurrent_per_user:
                self.concurrent_requests[user_id] += 1
                return True
            else:
                return False
    
    def queue_request(self, user_id, request_data):
        """Queue request if cannot process immediately"""
        with self.lock:
            if len(self.request_queue[user_id]) < self.max_queue_size:
                self.request_queue[user_id].append(request_data)
                return True
            else:
                return False
    
    def release_request(self, user_id):
        """Release request slot and process next queued request"""
        with self.lock:
            self.concurrent_requests[user_id] -= 1
            
            # Process next queued request
            if self.request_queue[user_id]:
                next_request = self.request_queue[user_id].pop(0)
                self.concurrent_requests[user_id] += 1
                return next_request
            
            return None
    
    def get_queue_status(self, user_id):
        """Get queue status for user"""
        with self.lock:
            return {
                'concurrent': self.concurrent_requests[user_id],
                'queued': len(self.request_queue[user_id])
            }

# Global throttler instance
request_throttler = RequestThrottler()

# Rate limiting decorators
def rate_limit(limit=100, window=3600, scope='user'):
    """Decorator for rate limiting function calls"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            # Get identifier based on scope
            if scope == 'user':
                from django.contrib.auth import get_user
                user = get_user(args[0]) if args else None
                identifier = str(user.id) if user and user.is_authenticated else 'anonymous'
            elif scope == 'ip':
                identifier = rate_limiter._get_client_ip(args[0])
            else:
                identifier = scope
            
            # Check rate limit
            if not rate_limiter.is_allowed(identifier, limit, window):
                from django.http import JsonResponse
                return JsonResponse({
                    'error': 'Rate limit exceeded',
                    'message': f'Too many requests. Limit: {limit} per {window} seconds'
                }, status=429)
            
            return func(*args, **kwargs)
        return wrapper
    return decorator

# Global rate limiter instance
rate_limiter = RateLimiter()
