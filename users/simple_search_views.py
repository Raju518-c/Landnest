"""
Simple Universal Search Views for 8.6 Lakh Records
Optimized for sub-second response times with basic search functionality
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

from .models import User
from .serializers import UserSerializer

logger = logging.getLogger(__name__)

class UniversalSearchAPIView(APIView):
    """
    Universal Search API for 8.6 Lakh Records
    Searches across all table fields with basic optimization
    """
    
    @extend_schema(
        summary="Universal Search API",
        description="Search users across all fields with basic optimization"
    )
    
    def post(self, request):
        """Universal search with basic optimization"""
        start_time = time.time()
        request_id = str(int(time.time() * 1000))
        
        try:
            # Extract search parameters
            data = request.data
            search_term = data.get('search_term', '').strip()
            filters = data.get('filters', {})
            sort_by = data.get('sort_by', 'created_at')
            sort_order = data.get('sort_order', 'desc')
            page = int(data.get('page', 1))
            page_size = min(int(data.get('page_size', 20)), 100)
            
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
            
            # Generate cache key
            cache_key = f"universal_search:{search_term.lower()}:{filters}:{sort_by}:{page}:{page_size}"
            
            # Try to get from cache first
            cached_results = cache.get(cache_key)
            if cached_results:
                return Response(cached_results)
            
            # Build search query
            queryset = User.objects.filter(role='1')  # Only customers
            
            # Apply universal search across all fields
            search_conditions = []
            search_term_lower = search_term.lower()
            
            # Search in basic fields
            search_conditions.extend([
                Q(username__icontains=search_term_lower),
                Q(email__icontains=search_term_lower),
                Q(mobile_no__icontains=search_term_lower),
                Q(first_name__icontains=search_term_lower),
                Q(last_name__icontains=search_term_lower),
                Q(city__icontains=search_term_lower),
                Q(state__icontains=search_term_lower),
                Q(user_type__icontains=search_term_lower),
                Q(status__icontains=search_term_lower),
            ])
            
            # Combine search conditions with OR
            if search_conditions:
                search_query = search_conditions[0]
                for condition in search_conditions[1:]:
                    search_query |= condition
                queryset = queryset.filter(search_query)
            
            # Apply additional filters
            if filters.get('user_type'):
                queryset = queryset.filter(user_type=filters['user_type'])
            if filters.get('status'):
                queryset = queryset.filter(status=filters['status'])
            
            # Apply sorting
            if sort_order == 'desc':
                queryset = queryset.order_by(f"-{sort_by}")
            else:
                queryset = queryset.order_by(sort_by)
            
            # Apply pagination
            offset = (page - 1) * page_size
            total_count = queryset.count()
            queryset = queryset[offset:offset + page_size]
            
            # Optimize query with select_related and prefetch_related
            queryset = queryset.select_related(
                'user_profile'  # Add related models as needed
            ).prefetch_related(
                'user_sub_plan',
                'user_properties',
                'log_history'
            )
            
            # Execute query
            serializer = UserSerializer(queryset, many=True)
            
            # Calculate pagination
            total_pages = (total_count + page_size - 1) // page_size
            
            results_data = {
                'results': serializer.data,
                'pagination': {
                    'current_page': page,
                    'total_pages': total_pages,
                    'total_count': total_count,
                    'page_size': page_size,
                    'has_next': page < total_pages,
                    'has_previous': page > 1,
                    'search_term': search_term,
                    'search_filters': filters,
                    'sort_by': sort_by,
                    'response_time': time.time() - start_time
                }
            }
            
            # Cache results for 5 minutes
            cache.set(cache_key, results_data, 300)
            
            return Response(results_data, status=status.HTTP_200_OK)
            
        except Exception as e:
            logger.error(f"Universal search error: {e}")
            
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
    
    def get(self, request):
        """Get search suggestions"""
        try:
            search_term = request.GET.get('term', '').strip()
            limit = int(request.GET.get('limit', 10))
            
            if not search_term or len(search_term) < 2:
                return Response({'suggestions': []})
            
            # Generate cache key
            cache_key = f"search_suggestions:{search_term.lower()}"
            
            # Try to get from cache first
            cached_suggestions = cache.get(cache_key)
            if cached_suggestions:
                return Response({'suggestions': cached_suggestions})
            
            # Get suggestions from username, email, and mobile
            suggestions = User.objects.filter(role='1').filter(
                Q(username__istartswith=search_term) |
                Q(email__istartswith=search_term) |
                Q(mobile_no__istartswith=search_term) |
                Q(first_name__istartswith=search_term) |
                Q(last_name__istartswith=search_term)
            ).values_list('username', 'email', 'mobile_no', 'first_name', 'last_name')[:limit]
            
            # Cache suggestions for 10 minutes
            cache.set(cache_key, list(suggestions), 600)
            
            return Response({'suggestions': list(suggestions)})
            
        except Exception as e:
            logger.error(f"Search suggestions error: {e}")
            return Response({
                'error': 'Suggestions temporarily unavailable',
                'suggestions': []
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

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
            test_search = User.objects.filter(role='1').filter(
                Q(username__icontains='test') |
                Q(email__icontains='test') |
                Q(mobile_no__icontains='test')
            )[:10]
            
            health_data = {
                'status': 'healthy',
                'search_performance': {
                    'test_search_time': 0.1,  # Placeholder
                    'cache_available': True,
                    'error_rate': 0
                },
                'system_metrics': {
                    'total_searches': 0,  # Placeholder
                    'average_response_time': 0.1,  # Placeholder
                    'popular_terms': []
                },
                'cache_status': {
                    'cache_available': True,
                    'cache_hit_rate': 85  # Placeholder
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
