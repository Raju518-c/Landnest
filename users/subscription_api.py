from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.db import transaction
from decimal import Decimal
from faker import Faker
import random
from django.utils import timezone
from datetime import timedelta
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt

from .models import sub_user, UserAddOn, UserFeatures, User

@method_decorator(csrf_exempt, name='dispatch')
class GenerateSubscriptionDataAPI(APIView):
    """
    API View to generate 30,000 records in sub_user, UserAddOn, and UserFeatures tables
    for user_ids 2003-20000
    """
    
    def post(self, request):
        try:
            fake = Faker('en_IN')
            
            # Configuration
            total_records = int(request.data.get('count', 30000))
            batch_size = 1000
            user_id_start = 2003
            user_id_end = 20000
            
            # Plan and User Type choices
            plan_names = ['Free', '1 Months', '3 Months', '6 Months', '12 Months', 'Lifetime']
            user_types = ['Buyer', 'Tenant', 'Individual Owner/Builder', 'Landlord', 'Builder', 'Agent', 'Bank Auction']
            sub_types = ['New', 'Upgraded', 'Referral']
            
            print(f"Starting generation of {total_records} subscription records...")
            
            created_data = {
                'sub_users': 0,
                'user_addons': 0,
                'user_features': 0
            }
            
            with transaction.atomic():
                for batch_start in range(0, total_records, batch_size):
                    batch_end = min(batch_start + batch_size, total_records)
                    
                    # Generate sub_users
                    sub_user_data = []
                    for i in range(batch_start, batch_end):
                        user_id_id = random.randint(user_id_start, user_id_end)
                        plan_name = random.choice(plan_names)
                        user_type = random.choice(user_types)
                        
                        # Calculate expired_date based on plan
                        expired_date = None
                        if plan_name == '1 Months':
                            expired_date = timezone.now() + timedelta(days=30)
                        elif plan_name == '3 Months':
                            expired_date = timezone.now() + timedelta(days=90)
                        elif plan_name == '6 Months':
                            expired_date = timezone.now() + timedelta(days=180)
                        elif plan_name == '12 Months':
                            expired_date = timezone.now() + timedelta(days=365)
                        elif plan_name == 'Free':
                            expired_date = timezone.now() + timedelta(days=random.randint(7, 30))
                        
                        sub_user_record = sub_user(
                            user_id_id=user_id_id,
                            plan_name=plan_name,
                            razor_plan_id=f'razor_plan_{random.randint(100000, 999999)}',
                            razor_user_id=f'razor_user_{random.randint(100000, 999999)}',
                            razor_subscription_id=f'razor_sub_{random.randint(100000, 999999)}',
                            user_type=user_type,
                            charges=Decimal(str(random.uniform(0, 50000))),
                            reward_amount=Decimal(str(random.uniform(0, 1000))),
                            pay_amount=Decimal(str(random.uniform(0, 50000))),
                            buyer_no_unlimited=random.choice([True, False]),
                            buyer_no=random.randint(0, 1000) if not random.choice([True, False]) else None,
                            no_of_properties_unlimited=random.choice([True, False]),
                            no_of_liked_data_unlimited=random.choice([True, False]),
                            matching_enquiry_unlimited=random.choice([True, False]),
                            no_of_properties=random.randint(0, 500) if not random.choice([True, False]) else None,
                            no_of_liked_data=random.randint(0, 1000) if not random.choice([True, False]) else None,
                            matching_enquiry=random.randint(0, 1000) if not random.choice([True, False]) else None,
                            expired_date=expired_date,
                            status=random.choice([True, True, True, False]),  # 75% active
                            upgraded_from=random.choice(['Free', '1 Months', '3 Months', None]) if random.random() > 0.7 else None,
                            upgraded_to=random.choice(['1 Months', '3 Months', '6 Months', '12 Months', None]) if random.random() > 0.7 else None,
                            sub_type=random.choice(sub_types)
                        )
                        sub_user_data.append(sub_user_record)
                    
                    # Bulk create sub_users
                    if sub_user_data:
                        created_sub_users = sub_user.objects.bulk_create(sub_user_data)
                        created_data['sub_users'] += len(created_sub_users)
                        
                        # Fetch saved sub_users from database to ensure proper primary keys
                        saved_sub_users = sub_user.objects.filter(
                            sub_id__in=[sub.sub_id for sub in created_sub_users]
                        )
                        
                        # Generate UserAddOn records (0-2 per sub_user)
                        addon_data = []
                        for sub in saved_sub_users:
                            num_addons = random.randint(0, 2)
                            for _ in range(num_addons):
                                addon_record = UserAddOn(
                                    user_id_id=sub.user_id_id,
                                    extend_to=sub,
                                    user_type=sub.user_type,
                                    charges=Decimal(str(random.uniform(100, 10000))),
                                    reward_amount=Decimal(str(random.uniform(0, 500))),
                                    pay_amount=Decimal(str(random.uniform(100, 10000))),
                                    buyer_no_unlimited=random.choice([True, False]),
                                    buyer_no=random.randint(0, 500) if not random.choice([True, False]) else None,
                                    no_of_properties_unlimited=random.choice([True, False]),
                                    no_of_liked_data_unlimited=random.choice([True, False]),
                                    matching_enquiry_unlimited=random.choice([True, False]),
                                    no_of_properties=random.randint(0, 200) if not random.choice([True, False]) else None,
                                    no_of_liked_data=random.randint(0, 500) if not random.choice([True, False]) else None,
                                    matching_enquiry=random.randint(0, 500) if not random.choice([True, False]) else None
                                )
                                addon_data.append(addon_record)
                        
                        if addon_data:
                            UserAddOn.objects.bulk_create(addon_data)
                            created_data['user_addons'] += len(addon_data)
                        
                        # Generate UserFeatures records (1 per user_id)
                        feature_data = []
                        user_ids_processed = set()
                        for sub in saved_sub_users:
                            if sub.user_id_id not in user_ids_processed:
                                user_ids_processed.add(sub.user_id_id)
                                feature_record = UserFeatures(
                                    user_id_id=sub.user_id_id,
                                    user_type=sub.user_type,
                                    buyer_no_unlimited=sub.buyer_no_unlimited,
                                    buyer_no=sub.buyer_no,
                                    no_of_properties_unlimited=sub.no_of_properties_unlimited,
                                    no_of_liked_data_unlimited=sub.no_of_liked_data_unlimited,
                                    matching_enquiry_unlimited=sub.matching_enquiry_unlimited,
                                    no_of_properties=sub.no_of_properties,
                                    no_of_liked_data=sub.no_of_liked_data,
                                    matching_enquiry=sub.matching_enquiry
                                )
                                feature_data.append(feature_record)
                        
                        if feature_data:
                            UserFeatures.objects.bulk_create(feature_data)
                            created_data['user_features'] += len(feature_data)
                    
                    print(f"Processed batch {batch_start//batch_size + 1}: {batch_end - batch_start} records")
            
            print(f"Successfully created subscription data!")
            print(f"sub_users: {created_data['sub_users']}")
            print(f"user_addons: {created_data['user_addons']}")
            print(f"user_features: {created_data['user_features']}")
            
            return Response({
                "status": True,
                "message": f"Successfully generated subscription data",
                "data": created_data
            }, status=status.HTTP_201_CREATED)
            
        except Exception as e:
            print(f"Error in GenerateSubscriptionDataAPI: {str(e)}")
            return Response({
                "status": False,
                "message": f"Error: {str(e)}",
                "data": None
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
