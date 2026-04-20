"""
Enhanced Universal Search Views for 8.6 Lakh Records
Optimized for sub-second response times with full-text search and intelligent caching
"""

from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
from drf_spectacular.utils import extend_schema
from django.core.cache import cache
import json
import time
import logging
from django.db.models import Q, Count
from django.contrib.postgres.search import SearchVector, SearchQuery, SearchRank

from .models import User
from .serializers import UserSerializer
from ..search_config import universal_search_manager, search_analytics
from ..rate_limiting import rate_limiter, rate_limit
from ..cache_config import redis_manager
from ..database_config import db_optimizer, performance_monitor

logger = logging.getLogger(__name__)

class UniversalSearchAPIView(APIView):
    """
    Universal Search API for 8.6 Lakh Records
    Searches across all table fields with sub-second response times
    """
    
    @extend_schema(
        summary="Universal Search API",
        description="Search users across all fields with advanced optimization and relevance ranking"
    )
    
    @rate_limit(limit=100, window=60, scope='user')  # 100 searches per minute per user
    def post(self, request):
        """Universal search with advanced optimization"""
        start_time = time.time()
        request_id = str(int(time.time() * 1000))
        
        try:
            # Extract search parameters
            data = request.data
            search_term = data.get('search_term', '').strip()
            filters = data.get('filters', {})
            sort_by = data.get('sort_by', 'relevance')  # Default to relevance ranking
            page = int(data.get('page', 1))
            page_size = min(int(data.get('page_size', 20)), 100)  # Max 100 for search
            
            # Get user ID for analytics
            user_id = getattr(request.user, 'id', None) if hasattr(request, 'user') and request.user.is_authenticated else None
            
            # Validate search term
            if not search_term or len(search_term) < 2:
                return Response({
                    'error': 'Search term must be at least 2 characters',
                    'results': [],
                    'total_count': 0,
                    'page': page,
                    'total_pages': 0
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # Perform optimized search
            search_results = universal_search_manager.get_search_results(
                search_term=search_term,
                filters=filters,
                sort_by=sort_by,
                limit=1000,  # Limit for performance
                page=page,
                page_size=page_size
            )
            
            # Track search analytics
            search_analytics.track_search(
                search_term=search_term,
                results_count=len(search_results.get('results', [])),
                response_time=search_results.get('response_time', 0),
                user_id=user_id
            )
            
            # Add performance metadata
            search_results['request_id'] = request_id
            search_results['search_optimization'] = {
                'cache_used': 'hit' if search_results.get('response_time', 0) < 0.1 else 'miss',
                'total_time': time.time() - start_time,
                'database_time': search_results.get('response_time', 0),
                'results_count': len(search_results.get('results', []))
            }
            
            return Response(search_results, status=status.HTTP_200_OK)
            
        except Exception as e:
            logger.error(f"Universal search error: {e}")
            performance_monitor.record_metric('search_error', 1)
            
            return Response({
                'error': 'Search temporarily unavailable',
                'message': 'Please try again later',
                'request_id': request_id,
                'total_time': time.time() - start_time
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class SearchSuggestionsAPIView(APIView):
    """
    Search Suggestions API for Auto-Complete
    """
    
    @extend_schema(
        summary="Search Suggestions API",
        description="Get search suggestions for auto-complete functionality"
    )
    
    @rate_limit(limit=50, window=60, scope='user')  # 50 suggestions per minute
    def get(self, request):
        """Get search suggestions"""
        try:
            search_term = request.GET.get('term', '').strip()
            limit = int(request.GET.get('limit', 10))
            
            if not search_term or len(search_term) < 2:
                return Response({'suggestions': []})
            
            # Get suggestions
            suggestions = universal_search_manager.get_search_suggestions(
                search_term=search_term,
                limit=limit
            )
            
            return Response({'suggestions': suggestions})
            
        except Exception as e:
            logger.error(f"Search suggestions error: {e}")
            return Response({
                'error': 'Suggestions temporarily unavailable',
                'suggestions': []
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class PopularSearchesAPIView(APIView):
    """
    Popular Searches API for Dashboard
    """
    
    @extend_schema(
        summary="Popular Searches API",
        description="Get popular search terms for analytics dashboard"
    )
    
    def get(self, request):
        """Get popular search terms"""
        try:
            time_range = request.GET.get('time_range', '1h')
            limit = int(request.GET.get('limit', 20))
            
            # Get popular searches
            popular_searches = universal_search_manager.get_popular_searches(
                limit=limit
            )
            
            return Response({'popular_searches': popular_searches})
            
        except Exception as e:
            logger.error(f"Popular searches error: {e}")
            return Response({
                'error': 'Analytics temporarily unavailable',
                'popular_searches': []
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class SearchAnalyticsAPIView(APIView):
    """
    Search Analytics API for Performance Monitoring
    """
    
    @extend_schema(
        summary="Search Analytics API",
        description="Get search performance analytics and metrics"
    )
    
    def get(self, request):
        """Get search analytics"""
        try:
            time_range = request.GET.get('time_range', '1h')
            
            # Get search analytics
            analytics_data = search_analytics.get_search_analytics(
                time_range=time_range
            )
            
            return Response(analytics_data)
            
        except Exception as e:
            logger.error(f"Search analytics error: {e}")
            return Response({
                'error': 'Analytics temporarily unavailable',
                'analytics': {}
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

class AdvancedSearchAPIView(APIView):
    """
    Advanced Search API with Filters and Faceting
    """
    
    @extend_schema(
        summary="Advanced Search API",
        description="Advanced search with filters, faceting, and aggregation"
    )
    
    @rate_limit(limit=50, window=60, scope='user')
    def post(self, request):
        """Advanced search with filters and faceting"""
        start_time = time.time()
        
        try:
            data = request.data
            search_term = data.get('search_term', '').strip()
            filters = data.get('filters', {})
            facets = data.get('facets', [])  # Fields to aggregate
            sort_by = data.get('sort_by', 'relevance')
            page = int(data.get('page', 1))
            page_size = min(int(data.get('page_size', 20)), 50)
            
            # Perform search
            search_results = universal_search_manager.get_search_results(
                search_term=search_term,
                filters=filters,
                sort_by=sort_by,
                limit=500,
                page=page,
                page_size=page_size
            )
            
            # Add faceting information
            if facets and search_results.get('results'):
                facet_data = self._calculate_facets(search_results['results'], facets)
                search_results['facets'] = facet_data
            
            # Add aggregations
            search_results['aggregations'] = self._calculate_aggregations(search_results.get('results', []))
            
            return Response(search_results)
            
        except Exception as e:
            logger.error(f"Advanced search error: {e}")
            return Response({
                'error': 'Advanced search temporarily unavailable',
                'results': []
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    def _calculate_facets(self, results, facet_fields):
        """Calculate facet counts for search results"""
        facets = {}
        
        for field in facet_fields:
            facet_counts = {}
            
            for result in results:
                value = self._get_field_value(result, field)
                if value:
                    facet_counts[value] = facet_counts.get(value, 0) + 1
            
            facets[field] = dict(sorted(facet_counts.items(), key=lambda x: x[1], reverse=True)[:10])
        
        return facets
    
    def _get_field_value(self, result, field_path):
        """Get field value from nested result"""
        keys = field_path.split('__')
        value = result
        
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            else:
                return None
        
        return value
    
    def _calculate_aggregations(self, results):
        """Calculate aggregations for search results"""
        if not results:
            return {}
        
        aggregations = {
            'total_count': len(results),
            'user_types': {},
            'status_distribution': {},
            'recent_activity': 0
        }
        
        # Count user types
        for result in results:
            user_type = result.get('user_type', 'Unknown')
            aggregations['user_types'][user_type] = aggregations['user_types'].get(user_type, 0) + 1
            
            status = result.get('status', 'Unknown')
            aggregations['status_distribution'][status] = aggregations['status_distribution'].get(status, 0) + 1
        
        return aggregations

# Search utility views
class SearchHealthAPIView(APIView):
    """
    Search Health Check API
    """
    
    @extend_schema(
        summary="Search Health Check",
        description="Check search system health and performance"
    )
    
    def get(self, request):
        """Get search system health"""
        try:
            # Test search performance
            test_search = universal_search_manager.get_search_results(
                search_term='test',
                filters={},
                sort_by='relevance',
                limit=10,
                page=1,
                page_size=10
            )
            
            # Get search analytics
            analytics_data = search_analytics.get_search_analytics('1h')
            
            health_data = {
                'status': 'healthy',
                'search_performance': {
                    'test_search_time': test_search.get('response_time', 0),
                    'cache_hit_rate': analytics_data.get('cache_hit_rate', 0),
                    'error_rate': analytics_data.get('error_rate', 0)
                },
                'system_metrics': {
                    'total_searches': analytics_data.get('total_searches', 0),
                    'average_response_time': analytics_data.get('average_response_time', 0),
                    'popular_terms': analytics_data.get('popular_terms', [])
                },
                'cache_status': {
                    'redis_available': redis_manager.get_redis_client() is not None,
                    'cache_hit_rate': analytics_data.get('cache_hit_rate', 0)
                }
            }
            
            return Response(health_data)
            
        except Exception as e:
            logger.error(f"Search health check error: {e}")
            return Response({
                'status': 'unhealthy',
                'error': str(e),
                'timestamp': time.time()
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

# Search optimization decorators
def cache_search_results(timeout=300):
    """Decorator to cache search results"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            # Generate cache key
            search_term = kwargs.get('search_term', '')
            filters = kwargs.get('filters', {})
            sort_by = kwargs.get('sort_by', 'relevance')
            page = kwargs.get('page', 1)
            page_size = kwargs.get('page_size', 20)
            
            cache_key = universal_search_manager.generate_search_cache_key(
                search_term, filters, sort_by, page_size
            )
            
            # Try to get from cache
            cached_results = redis_manager.get(cache_key)
            if cached_results:
                performance_monitor.record_metric('search_cache_hit', 1)
                return json.loads(cached_results)
            
            # Execute function
            result = func(*args, **kwargs)
            
            # Cache result
            try:
                redis_manager.set(cache_key, json.dumps(result, default=str), timeout)
                performance_monitor.record_metric('search_cache_miss', 1)
            except Exception as e:
                logger.error(f"Search cache error: {e}")
            
            return result
        return wrapper
    return decorator
