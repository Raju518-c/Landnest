from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.db import transaction
from decimal import Decimal
from faker import Faker
import random
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from users.models import *
from .models import subAdminplans, AddOnPlans

@method_decorator(csrf_exempt, name='dispatch')
class GenerateAdminPlansAPI(APIView):
    """
    API View to generate subAdminplans and AddOnPlans records for all combinations
    """
    
    def post(self, request):
        try:
            fake = Faker('en_IN')
            
            # Plan and User Type choices
            Plan_Name = [
                'Free', '1 Months', '3 Months', '6 Months', '12 Months', 'Lifetime'
            ]
            User_Type = [
                'Buyer', 'Tenant', 'Individual Owner/Builder', 'Landlord', 
                'Builder', 'Agent', 'Bank Auction'
            ]
            
            print("Starting generation of Admin Plans...")
            admin_user = User.objects.get(user_id = 2001)
            
            with transaction.atomic():
                # Generate subAdminplans records
                sub_admin_plans = []
                for plan_name in Plan_Name:
                    for user_type in User_Type:
                        plan_data = {
                            'razor_plan_id': f'plan_{plan_name.lower().replace(" ", "_")}_{user_type.lower().replace(" ", "_")}_{random.randint(100, 999)}',
                            'plan_name': plan_name,
                            'user_type': user_type,
                            'actual_price': Decimal(str(random.uniform(0, 50000))),
                            'charges': Decimal(str(random.uniform(0, 50000))),
                            'status': True,
                            'trial_days': random.randint(0, 365) if plan_name != 'Lifetime' else None,
                            'buyer_no_unlimited': random.choice([True, False]),
                            'buyer_no': random.randint(0, 1000) if not random.choice([True, False]) else None,
                            'no_of_properties_unlimited': random.choice([True, False]),
                            'no_of_liked_data_unlimited': random.choice([True, False]),
                            'matching_enquiry_unlimited': random.choice([True, False]),
                            'no_of_properties': random.randint(0, 500) if not random.choice([True, False]) else None,
                            'no_of_liked_data': random.randint(0, 1000) if not random.choice([True, False]) else None,
                            'matching_enquiry': random.randint(0, 1000) if not random.choice([True, False]) else None,
                        }
                        sub_admin_plans.append(subAdminplans(**plan_data))
                
                if sub_admin_plans:
                    subAdminplans.objects.bulk_create(sub_admin_plans)
                    print(f"Created {len(sub_admin_plans)} subAdminplans records")
                
                # Generate AddOnPlans records
                addon_plans = []
                for user_type in User_Type:
                    addon_data = {
                        'user_id': admin_user,  # Will be set to null since no specific user
                        'user_type': user_type,
                        'actual_price': Decimal(str(random.uniform(100, 10000))),
                        'charges': Decimal(str(random.uniform(50, 5000))),
                        'status': True,
                        'buyer_no_unlimited': random.choice([True, False]),
                        'buyer_no': random.randint(0, 500) if not random.choice([True, False]) else None,
                        'no_of_properties_unlimited': random.choice([True, False]),
                        'no_of_liked_data_unlimited': random.choice([True, False]),
                        'matching_enquiry_unlimited': random.choice([True, False]),
                        'no_of_properties': random.randint(0, 200) if not random.choice([True, False]) else None,
                        'no_of_liked_data': random.randint(0, 500) if not random.choice([True, False]) else None,
                        'matching_enquiry': random.randint(0, 500) if not random.choice([True, False]) else None,
                    }
                    addon_plans.append(AddOnPlans(**addon_data))
                
                if addon_plans:
                    AddOnPlans.objects.bulk_create(addon_plans)
                    print(f"Created {len(addon_plans)} AddOnPlans records")
            
            print("Successfully generated Admin Plans!")
            
            return Response({
                "status": True,
                "message": f"Successfully created {len(sub_admin_plans)} subAdminplans and {len(addon_plans)} AddOnPlans records.",
                "data": {
                    "sub_admin_plans_created": len(sub_admin_plans),
                    "addon_plans_created": len(addon_plans)
                }
            }, status=status.HTTP_201_CREATED)
            
        except Exception as e:
            print(f"Error in GenerateAdminPlansAPI: {str(e)}")
            return Response({
                "status": False,
                "message": f"Error: {str(e)}",
                "data": None
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
