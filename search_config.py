"""
Advanced Universal Search Configuration for 8.6 Lakh Records
Optimized for sub-second response times with full-text search and intelligent caching
"""

import json
import time
import logging
from django.db import models
from django.db.models import Q, F, Value, CharField, TextField
from django.contrib.postgres.search import SearchVector, SearchQuery, SearchRank
from django.core.cache import cache
from django.conf import settings
from .cache_config import redis_manager, CacheKeyGenerator
from .database_config import db_optimizer, performance_monitor

logger = logging.getLogger(__name__)

class SearchField:
    """Defines searchable fields with weights"""
    
    def __init__(self, field_name, weight=1.0, boost_factor=1.0):
        self.field_name = field_name
        self.weight = weight
        self.boost_factor = boost_factor

# Define searchable fields with weights for relevance
SEARCHABLE_FIELDS = [
    SearchField('username', weight=2.0, boost_factor=1.5),      # Highest priority
    SearchField('first_name', weight=1.5, boost_factor=1.3),
    SearchField('last_name', weight=1.5, boost_factor=1.3),
    SearchField('email', weight=2.0, boost_factor=1.4),        # High priority
    SearchField('mobile_no', weight=1.8, boost_factor=1.4),     # High priority
    SearchField('city', weight=1.2, boost_factor=1.1),
    SearchField('state', weight=1.2, boost_factor=1.1),
    SearchField('user_type', weight=1.3, boost_factor=1.2),
    SearchField('status', weight=1.0, boost_factor=1.0),
    # Related data fields
    SearchField('user_properties__property_name', weight=0.8, boost_factor=0.9),
    SearchField('user_properties__location', weight=0.9, boost_factor=1.0),
    SearchField('user_properties__type', weight=0.8, boost_factor=0.9),
    SearchField('user_sub_plan__plan_name', weight=0.7, boost_factor=0.8),
    SearchField('user_sub_plan__user_type', weight=0.7, boost_factor=0.8),
]

class UniversalSearchManager:
    """Manages universal search with advanced optimization"""
    
    def __init__(self):
        self.search_cache_timeout = 300  # 5 minutes
        self.max_results = 1000  # Limit results for performance
        self.min_search_length = 2  # Minimum search term length
    
    def generate_search_cache_key(self, search_term, filters=None, sort_by=None, limit=None):
        """Generate optimized cache key for search"""
        # Normalize search term for cache key
        normalized_term = search_term.lower().strip() if search_term else ''
        
        # Create cache key components
        key_components = [
            'universal_search',
            normalized_term,
            str(filters or {}),
            str(sort_by or 'relevance'),
            str(limit or self.max_results)
        ]
        
        return ':'.join(key_components)
    
    def preprocess_search_term(self, search_term):
        """Preprocess search term for better matching"""
        if not search_term or len(search_term) < self.min_search_length:
            return None
        
        # Normalize search term
        term = search_term.lower().strip()
        
        # Handle common variations
        term_variations = {
            'mobile': ['phone', 'contact', 'number'],
            'email': ['mail', 'email address', 'e-mail'],
            'name': ['fullname', 'username', 'user'],
            'city': ['location', 'place', 'area'],
            'state': ['region', 'province', 'state']
        }
        
        # Add variations to search
        expanded_terms = [term]
        for variation, synonyms in term_variations.items():
            if any(synonym in term for synonym in synonyms):
                expanded_terms.extend(synonyms)
        
        return list(set(expanded_terms))  # Remove duplicates
    
    def build_search_query(self, search_terms, filters=None):
        """Build optimized search query"""
        from .models import User
        
        if not search_terms:
            # Return all users with filters
            queryset = User.objects.filter(role='1')  # Only customers
            if filters:
                if filters.get('user_type'):
                    queryset = queryset.filter(user_type=filters['user_type'])
                if filters.get('status'):
                    queryset = queryset.filter(status=filters['status'])
            return queryset
        
        # Build search conditions
        search_conditions = []
        
        for term in search_terms:
            term_conditions = []
            
            # Add conditions for each searchable field
            for field in SEARCHABLE_FIELDS:
                # Use different search strategies based on field type
                if 'email' in field.field_name:
                    # Exact match for email
                    term_conditions.append(Q(**{f"{field.field_name}__iexact": term}))
                elif 'mobile' in field.field_name:
                    # Partial match for mobile
                    term_conditions.append(Q(**{f"{field.field_name}__icontains": term}))
                else:
                    # Full-text search for other fields
                    term_conditions.append(Q(**{f"{field.field_name}__icontains": term}))
            
            # Combine conditions for this term (OR logic)
            if term_conditions:
                search_conditions.append(Q(*term_conditions, _connector=Q.OR))
        
        # Combine all term conditions (AND logic between terms)
        if search_conditions:
            base_queryset = User.objects.filter(role='1')
            search_query = search_conditions[0]
            
            for condition in search_conditions[1:]:
                search_query &= condition
            
            queryset = base_queryset.filter(search_query)
        else:
            queryset = User.objects.filter(role='1')
        
        # Apply additional filters
        if filters:
            if filters.get('user_type'):
                queryset = queryset.filter(user_type=filters['user_type'])
            if filters.get('status'):
                queryset = queryset.filter(status=filters['status'])
        
        return queryset.distinct()
    
    def search_with_ranking(self, search_terms, queryset, sort_by='relevance'):
        """Apply search ranking and sorting"""
        
        if sort_by == 'relevance':
            # Use SearchRank for PostgreSQL full-text search
            if search_terms and hasattr(queryset.model, '_meta'):
                # Add annotation for relevance ranking
                search_vector = SearchVector(
                    'username', 'first_name', 'last_name', 'email', 
                    'mobile_no', 'city', 'state', 'user_type',
                    'user_properties__property_name', 'user_properties__location',
                    'user_sub_plan__plan_name'
                )
                
                search_query = SearchQuery(search_terms[0])  # Use first term for ranking
                
                queryset = queryset.annotate(
                    rank=SearchRank(search_vector, search_query)
                ).order_by('-rank')
        
        elif sort_by in ['created_at', 'updated_at']:
            queryset = queryset.order_by(f"-{sort_by}")
        elif sort_by in ['username', 'email', 'mobile_no']:
            queryset = queryset.order_by(sort_by)
        else:
            queryset = queryset.order_by('-created_at')  # Default sort
        
        return queryset
    
    def get_search_results(self, search_term, filters=None, sort_by='relevance', limit=None, page=1, page_size=20):
        """Get search results with all optimizations"""
        start_time = time.time()
        
        try:
            # Generate cache key
            cache_key = self.generate_search_cache_key(search_term, filters, sort_by, limit)
            
            # Try to get from cache first
            cached_results = redis_manager.get(cache_key)
            if cached_results:
                performance_monitor.record_metric('search_cache_hit', 1)
                return json.loads(cached_results)
            
            # Preprocess search terms
            search_terms = self.preprocess_search_term(search_term)
            if not search_terms:
                return {'results': [], 'total_count': 0, 'page': page, 'total_pages': 0}
            
            # Build and execute search query
            with db_optimizer.monitor_query('universal_search'):
                queryset = self.build_search_query(search_terms, filters)
                queryset = self.search_with_ranking(search_terms, queryset, sort_by)
                
                # Apply pagination
                actual_limit = min(limit or self.max_results, page_size * 3)  # Get more for accurate pagination
                offset = (page - 1) * page_size
                
                total_count = queryset.count()
                queryset = queryset[offset:offset + actual_limit]
                
                # Optimize query with select_related and prefetch_related
                queryset = queryset.select_related(
                    'user_profile'  # Add related models as needed
                ).prefetch_related(
                    'user_sub_plan',
                    'user_properties',
                    'log_history'
                )
                
                # Execute query
                from .serializers import UserSerializer
                serializer = UserSerializer(queryset[:page_size], many=True)
            
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
            
            # Cache results
            redis_manager.set(cache_key, json.dumps(results_data, default=str), self.search_cache_timeout)
            
            performance_monitor.record_metric('search_cache_miss', 1)
            performance_monitor.record_metric('search_response_time', time.time() - start_time)
            
            return results_data
            
        except Exception as e:
            logger.error(f"Universal search error: {e}")
            performance_monitor.record_metric('search_error', 1)
            
            return {
                'results': [],
                'total_count': 0,
                'page': page,
                'total_pages': 0,
                'error': 'Search temporarily unavailable',
                'response_time': time.time() - start_time
            }
    
    def get_search_suggestions(self, search_term, limit=10):
        """Get search suggestions based on partial matches"""
        if not search_term or len(search_term) < 2:
            return []
        
        cache_key = f"search_suggestions:{search_term.lower()}"
        cached_suggestions = redis_manager.get(cache_key)
        if cached_suggestions:
            return json.loads(cached_suggestions)
        
        try:
            from .models import User
            
            # Get suggestions from username, email, and mobile
            suggestions = User.objects.filter(role='1').filter(
                Q(username__istartswith=search_term) |
                Q(email__istartswith=search_term) |
                Q(mobile_no__istartswith=search_term) |
                Q(first_name__istartswith=search_term) |
                Q(last_name__istartswith=search_term)
            ).values_list('username', 'email', 'mobile_no', 'first_name', 'last_name')[:limit]
            
            # Cache suggestions
            redis_manager.set(cache_key, json.dumps(suggestions, default=str), 600)  # 10 minutes
            
            return suggestions
            
        except Exception as e:
            logger.error(f"Search suggestions error: {e}")
            return []
    
    def get_popular_searches(self, limit=20):
        """Get popular search terms"""
        cache_key = "popular_searches"
        cached_popular = redis_manager.get(cache_key)
        if cached_popular:
            return json.loads(cached_popular)
        
        try:
            # This would typically come from analytics or search logs
            # For now, return empty list
            popular_searches = []
            
            redis_manager.set(cache_key, json.dumps(popular_searches), 1800)  # 30 minutes
            return popular_searches
            
        except Exception as e:
            logger.error(f"Popular searches error: {e}")
            return []

# Global search manager
universal_search_manager = UniversalSearchManager()

# Search optimization utilities
class SearchOptimizer:
    """Optimizes search performance and results"""
    
    @staticmethod
    def optimize_search_query(search_term):
        """Optimize search query for better performance"""
        if not search_term:
            return None
        
        # Remove special characters and normalize
        optimized_term = search_term.strip().lower()
        
        # Handle common search patterns
        patterns = {
            r'\s+': ' ',  # Multiple spaces to single space
            r'[^\w\s@.-]': '',  # Remove special characters except email-allowed
        }
        
        import re
        for pattern, replacement in patterns.items():
            optimized_term = re.sub(pattern, replacement, optimized_term)
        
        return optimized_term.strip()
    
    @staticmethod
    def get_field_boosts():
        """Get field boost factors for search ranking"""
        return {
            'username': 2.0,
            'email': 1.8,
            'mobile_no': 1.7,
            'first_name': 1.5,
            'last_name': 1.5,
            'user_type': 1.3,
            'city': 1.2,
            'state': 1.2,
            'user_properties__property_name': 0.9,
            'user_properties__location': 0.9,
            'user_sub_plan__plan_name': 0.8,
        }
    
    @staticmethod
    def calculate_relevance_score(match_data, search_terms):
        """Calculate relevance score for search results"""
        score = 0
        field_boosts = SearchOptimizer.get_field_boosts()
        
        for field, value in match_data.items():
            if value and search_terms:
                field_boost = field_boosts.get(field, 1.0)
                
                # Check exact matches
                for term in search_terms:
                    if str(value).lower() == term.lower():
                        score += 10 * field_boost
                    elif str(value).lower().startswith(term.lower()):
                        score += 5 * field_boost
                    elif term.lower() in str(value).lower():
                        score += 2 * field_boost
        
        return score

# Search analytics
class SearchAnalytics:
    """Tracks search performance and user behavior"""
    
    def __init__(self):
        self.analytics_cache_timeout = 3600  # 1 hour
    
    def track_search(self, search_term, results_count, response_time, user_id=None):
        """Track search analytics"""
        try:
            analytics_data = {
                'search_term': search_term,
                'results_count': results_count,
                'response_time': response_time,
                'user_id': user_id,
                'timestamp': time.time()
            }
            
            # Store in analytics (could be sent to Kafka for processing)
            from .kafka_config import RequestQueueManager
            RequestQueueManager.queue_analytics_event('search_performed', user_id, analytics_data)
            
        except Exception as e:
            logger.error(f"Search analytics tracking error: {e}")
    
    def get_search_analytics(self, time_range='1h'):
        """Get search analytics for dashboard"""
        cache_key = f"search_analytics:{time_range}"
        cached_analytics = redis_manager.get(cache_key)
        if cached_analytics:
            return json.loads(cached_analytics)
        
        # This would typically come from your analytics database
        analytics_data = {
            'total_searches': 0,
            'average_response_time': 0,
            'popular_terms': [],
            'search_trends': [],
            'error_rate': 0
        }
        
        redis_manager.set(cache_key, json.dumps(analytics_data), self.analytics_cache_timeout)
        return analytics_data

# Global search analytics
search_analytics = SearchAnalytics()
