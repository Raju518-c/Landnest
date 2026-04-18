import threading
import time
from .models import *
from datetime import datetime, timedelta
from django.utils import timezone
from .serializers import *
from property.models import Property 
from django.db.models import Q
# from celery import shared_task  # Uncomment after installing celery properly
from django.core.cache import cache
from django.conf import settings

def print_every_5_seconds():

    while True:
        
        all_users = sub_user.objects.filter(status = True)                   

        for user in all_users:
            try:
                today = timezone.now() + timedelta(hours=5, minutes=30)

                if user.expired_date and today >= user.expired_date:
                    # 1. Set status=False
                    print('user.user_type', user.user_type)
                    print('user.expired_date', user.expired_date)
                    print('today', today)
                    serializer = sub_userSerializer(user, data={'status': False}, partial=True)
                    serializer.is_valid(raise_exception=True)
                    serializer.save()
                    
                    try:
                        features = UserFeatures.objects.filter(user_id=user.user_id, user_type=user.user_type)
                    except UserFeatures.DoesNotExist:
                        continue

                    features.delete()                    

                    try: 
                        if user.user_type == 'Individual Owner/Builder' or user.user_type == 'Landlord' or user.user_type == 'Agent':
                            if not user.no_of_properties_unlimited:                                                   
                                print('1')
                                try:
                                    if user.user_type == 'Individual Owner/Builder':
                                        print('2')
                                        all_props = Property.objects.filter(user_id=user.user_id,status=True).filter((Q(posted_by='Owner') | Q(posted_by='Builder')) &(Q(type='sell') | Q(type='best-deal')))
                                    elif user.user_type == 'Landlord':
                                        print('3')
                                        all_props = Property.objects.filter(user_id=user.user_id,status=True).filter(Q(type='rent') | Q(type='lease'))
                                    elif user.user_type == 'Agent':
                                        print('4')
                                        all_props = Property.objects.filter(user_id=user.user_id,status=True, posted_by = 'Agent').filter(Q(type='sell') | Q(type='best-deal'))
                                except:
                                    all_props = None

                                if all_props:
                                    for prop in all_props:
                                        prop.status = False
                                        prop.save()
                        else:
                            if not user.buyer_no_unlimited:   
                                try:                                 
                                    if user.user_type == 'Buyer':
                                        all_cart = user_cart.objects.filter(user_id=user.user_id, status=True, activity_as = 'Buyer')
                                    elif user.user_type == 'Tenant':
                                        all_cart = user_cart.objects.filter(user_id=user.user_id, status=True, activity_as = 'Tenant')
                                except:
                                    all_cart = None

                                if all_cart:
                                    for i in all_cart:
                                        i.status = False
                                        i.save()

                                try:                                 
                                    if user.user_type == 'Buyer':
                                        all_act = activity_tbl.objects.filter(user_id=user.user_id, status=True, activity_as = 'Buyer')
                                    elif user.user_type == 'Tenant':
                                        all_act = activity_tbl.objects.filter(user_id=user.user_id, status=True, activity_as = 'Tenant')
                                except:
                                    all_act = None

                                if all_act:
                                    for i in all_act:
                                        i.status = False
                                        i.save()

                    except Exception as e:
                        print(f"Error updating property visibility for user {user.user_id}: {e}")
            except Exception as e:
                print(f"Error processing user {user.sub_id}: {e}") 
              

        time.sleep(1)  


# @shared_task  # Uncomment after installing celery properly
def warm_user_cache():
    """Background task to warm up cache with frequently accessed user data"""
    try:
        # Clear existing user cache
        cache_pattern = "users_list_*"
        # Note: In production, you might want to use redis-py to delete patterns
        # For now, we'll just warm new cache entries
        
        # Warm cache for first few pages of common queries
        common_queries = [
            {'page': 1, 'page_size': 50, 'search': '', 'user_type': '', 'sort_by': 'created_at', 'sort_order': 'desc'},
            {'page': 1, 'page_size': 50, 'search': '', 'user_type': 'Individual Owner/Builder', 'sort_by': 'created_at', 'sort_order': 'desc'},
            {'page': 1, 'page_size': 50, 'search': '', 'user_type': 'Agent', 'sort_by': 'created_at', 'sort_order': 'desc'},
            {'page': 1, 'page_size': 50, 'search': '', 'user_type': 'Buyer', 'sort_by': 'created_at', 'sort_order': 'desc'},
        ]
        
        from .views import UserListCreateAPIView
        view_instance = UserListCreateAPIView()
        
        for query_params in common_queries:
            # Create mock request object
            class MockRequest:
                def __init__(self, params):
                    self.GET = params
                    self.META = {}
            
            mock_request = MockRequest(query_params)
            try:
                # This will cache the result
                view_instance.get(mock_request)
            except Exception as e:
                print(f"Error warming cache for query {query_params}: {e}")
                
        return "User cache warmed successfully"
    except Exception as e:
        return f"Error warming user cache: {str(e)}"

# @shared_task  # Uncomment after installing celery properly
def cleanup_expired_cache():
    """Background task to clean up expired cache entries"""
    try:
        # This would be enhanced with Redis pattern matching in production
        # For now, we rely on Django's built-in cache expiration
        return "Cache cleanup completed"
    except Exception as e:
        return f"Error during cache cleanup: {str(e)}"

# @shared_task  # Uncomment after installing celery properly
def generate_user_statistics():
    """Background task to generate user statistics for dashboard"""
    try:
        stats = {
            'total_users': User.objects.filter(role='1').count(),
            'active_users': User.objects.filter(role='1', user_sub_plan__status=True).distinct().count(),
            'users_by_type': {},
            'recent_registrations': User.objects.filter(
                role='1', 
                created_at__gte=timezone.now() - timedelta(days=7)
            ).count()
        }
        
        # Count users by type
        user_types = ['Individual Owner/Builder', 'Landlord', 'Agent', 'Bank Auction', 'Buyer', 'Tenant']
        for user_type in user_types:
            stats['users_by_type'][user_type] = User.objects.filter(
                role='1', 
                user_type=user_type
            ).count()
        
        # Cache statistics for 10 minutes
        cache.set('user_statistics', stats, timeout=600)
        
        return f"User statistics generated: {stats}"
    except Exception as e:
        return f"Error generating user statistics: {str(e)}"

def start_thread():
    printer_thread = threading.Thread(target=print_every_5_seconds)
    printer_thread.daemon = True
    printer_thread.start()
