from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.contrib.auth.hashers import make_password
from django.contrib.auth.hashers import check_password
from django.shortcuts import get_object_or_404
from django.db import models
from .models import *
from .serializers import *
import os
from datetime import datetime, timedelta
from django.utils import timezone
from django.db import transaction
from decimal import Decimal
from faker import Faker
import random

from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from drf_spectacular.utils import extend_schema


@method_decorator(csrf_exempt, name='dispatch')
class ConstructionCatView(APIView):

    def get(self, request, pk=None):
        try:
            if pk:
                category = construction_cat.objects.get(pk=pk)
                serializer = construction_catSerializer(category)
                return Response(serializer.data)
            else:
                categories = construction_cat.objects.all()
                serializer = construction_catSerializer(categories, many=True)
                return Response(serializer.data)
        except construction_cat.DoesNotExist:
            return Response({'error': 'Category not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    @extend_schema(request=construction_catSerializer)
    def post(self, request):        
        category = request.data.get('category')
        sub_cat = request.data.get('sub_cat')
        
        if construction_cat.objects.filter(category__iexact=category, sub_cat__iexact=sub_cat).exists():
            return Response({'error': 'This category and sub-category combination already exists.'}, status=status.HTTP_400_BAD_REQUEST)

        
        try:
            serializer = construction_catSerializer(data=request.data)
            if serializer.is_valid():
                serializer.save()
                return Response({'message': 'Category created successfully', 'data': serializer.data}, status=status.HTTP_201_CREATED)
            return Response({'error': serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    @extend_schema(request=construction_catSerializer)
    def put(self, request, pk):
        try:
            category = construction_cat.objects.get(pk=pk)
            serializer = construction_catSerializer(category, data=request.data, partial=True)
            if serializer.is_valid():
                serializer.save()
                return Response({'message': 'Category updated successfully', 'data': serializer.data})
            return Response({'error': serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except construction_cat.DoesNotExist:
            return Response({'error': 'Category not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def delete(self, request, pk):
        try:
            category = construction_cat.objects.get(pk=pk)
            category.delete()
            return Response({'message': 'Category deleted successfully'})
        except construction_cat.DoesNotExist:
            return Response({'error': 'Category not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@method_decorator(csrf_exempt, name='dispatch')
class ConstructionContentView(APIView):

    def get(self, request, pk=None):
        try:
            if pk:
                content = construction_content.objects.get(pk=pk)
                serializer = construction_contentSerializer(content)
                return Response(serializer.data)
            else:
                contents = construction_content.objects.all()
                serializer = construction_contentSerializer(contents, many=True)
                return Response(serializer.data)
        except construction_content.DoesNotExist:
            return Response({'error': 'Content not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    @extend_schema(request=construction_contentSerializer)
    def post(self, request):       
        try:
            serializer = construction_contentSerializer(data=request.data)
            if serializer.is_valid():
                serializer.save()
                return Response({'message': 'Content created successfully', 'data': serializer.data}, status=status.HTTP_201_CREATED)
            return Response({'error': serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    @extend_schema(request=construction_contentSerializer)
    def put(self, request, pk):
        try:
            content = construction_content.objects.get(pk=pk)
            serializer = construction_contentSerializer(content, data=request.data, partial=True)
            if serializer.is_valid():
                serializer.save()
                return Response({'message': 'Content updated successfully', 'data': serializer.data})
            return Response({'error': serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except construction_content.DoesNotExist:
            return Response({'error': 'Content not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def delete(self, request, pk):
        try:
            content = construction_content.objects.get(pk=pk)
            content.delete()
            return Response({'message': 'Content deleted successfully'})
        except construction_content.DoesNotExist:
            return Response({'error': 'Content not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        

@method_decorator(csrf_exempt, name='dispatch')
class PackagesView(APIView):

    def get(self, request, pk=None):
        try:
            if pk:
                package = Packages.objects.get(pk=pk)
                serializer = PackagesSerializer(package)
                return Response(serializer.data)
            else:
                packages = Packages.objects.all()
                serializer = PackagesSerializer(packages, many=True)
                return Response(serializer.data)
        except Packages.DoesNotExist:
            return Response({'error': 'Package not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    @extend_schema(request=PackagesSerializer)
    def post(self, request):        
        category = request.data.get('category')
        
        if Packages.objects.filter(category__iexact=category).exists():            
            return Response({'error': 'Package with this category already exists.'}, status=status.HTTP_400_BAD_REQUEST)
        
        try:
            serializer = PackagesSerializer(data=request.data)
            if serializer.is_valid():
                serializer.save()
                return Response({'message': 'Package created successfully', 'data': serializer.data}, status=status.HTTP_201_CREATED)
            return Response({'error': serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    @extend_schema(request=PackagesSerializer)
    def put(self, request, pk):
        try:
            package = Packages.objects.get(pk=pk)
            serializer = PackagesSerializer(package, data=request.data, partial=True)
            if serializer.is_valid():
                serializer.save()
                return Response({'message': 'Package updated successfully', 'data': serializer.data})
            return Response({'error': serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except Packages.DoesNotExist:
            return Response({'error': 'Package not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def delete(self, request, pk):
        try:
            package = Packages.objects.get(pk=pk)
            package.delete()
            return Response({'message': 'Package deleted successfully'})
        except Packages.DoesNotExist:
            return Response({'error': 'Package not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)




@method_decorator(csrf_exempt, name='dispatch')
class MaterialCatView(APIView):

    def get(self, request, pk=None):
        try:
            if pk:
                category = material_cat.objects.get(pk=pk)
                serializer = material_catSerializer(category)
                return Response(serializer.data)
            else:
                categories = material_cat.objects.all()
                serializer = material_catSerializer(categories, many=True)
                return Response(serializer.data)
        except material_cat.DoesNotExist:
            return Response({'error': 'Category not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    @extend_schema(request=material_catSerializer)
    def post(self, request):        
        category = request.data.get('category')
        
        if material_cat.objects.filter(category__iexact=category).exists():            
            return Response({'error': 'category already exists.'}, status=status.HTTP_400_BAD_REQUEST)
        
        try:
            serializer = material_catSerializer(data=request.data)
            if serializer.is_valid():
                serializer.save()
                return Response({'message': 'Category created successfully', 'data': serializer.data}, status=status.HTTP_201_CREATED)
            return Response({'error': serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    @extend_schema(request=material_catSerializer)
    def put(self, request, pk):
        try:
            category = material_cat.objects.get(pk=pk)
            serializer = material_catSerializer(category, data=request.data, partial=True)
            if serializer.is_valid():
                serializer.save()
                return Response({'message': 'Category updated successfully', 'data': serializer.data})
            return Response({'error': serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except material_cat.DoesNotExist:
            return Response({'error': 'Category not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def delete(self, request, pk):
        try:
            category = material_cat.objects.get(pk=pk)
            category.delete()
            return Response({'message': 'Category deleted successfully'})
        except material_cat.DoesNotExist:
            return Response({'error': 'Category not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@method_decorator(csrf_exempt, name='dispatch')
class MaterialContentView(APIView):

    def get(self, request, pk=None):
        try:
            if pk:
                content = material_content.objects.get(pk=pk)
                serializer = material_contentSerializer(content)
                return Response(serializer.data)
            else:
                contents = material_content.objects.all()
                serializer = material_contentSerializer(contents, many=True)
                return Response(serializer.data)
        except material_content.DoesNotExist:
            return Response({'error': 'Content not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    @extend_schema(request=material_contentSerializer)
    def post(self, request):       
        try:
            serializer = material_contentSerializer(data=request.data)
            if serializer.is_valid():
                serializer.save()
                return Response({'message': 'Content created successfully', 'data': serializer.data}, status=status.HTTP_201_CREATED)
            return Response({'error': serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    @extend_schema(request=material_contentSerializer)
    def put(self, request, pk):
        try:
            content = material_content.objects.get(pk=pk)
            serializer = material_contentSerializer(content, data=request.data, partial=True)
            if serializer.is_valid():
                serializer.save()
                return Response({'message': 'Content updated successfully', 'data': serializer.data})
            return Response({'error': serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except material_content.DoesNotExist:
            return Response({'error': 'Content not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def delete(self, request, pk):
        try:
            content = material_content.objects.get(pk=pk)
            content.delete()
            return Response({'message': 'Content deleted successfully'})
        except material_content.DoesNotExist:
            return Response({'error': 'Content not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        

@method_decorator(csrf_exempt, name='dispatch')
class ApproveMultipleDealsView(APIView):    
    def post(self, request):
        try:
            deal_ids = request.data.get('deal_ids', [])
            if not deal_ids:
                return Response({'error': 'No deal IDs provided'}, status=status.HTTP_400_BAD_REQUEST)

            updated_count = best_deals.objects.filter(deal_id__in=deal_ids).update(Admin_status="Approved")
            return Response({'message': f'{updated_count} deal(s) approved successfully'}, status=status.HTTP_200_OK)

        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)



@method_decorator(csrf_exempt, name='dispatch')
class SubscriptionAPI(APIView):
    
    def get(self, request, pk=None):
        try:
            if pk:
                plan = subAdminplans.objects.get(pk=pk)
                serializer = subAdminplansSerializer(plan)
            else:
                plans = subAdminplans.objects.all()
                serializer = subAdminplansSerializer(plans, many=True)
            return Response(serializer.data)
        except subAdminplans.DoesNotExist:
            return Response({'error': 'Sub Plan not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    @extend_schema(request=subAdminplansSerializer)
    def post(self, request):
        try:

            data = request.data.copy()

            plan_name = request.data.get('plan_name')
            user_type = request.data.get('user_type')
            
          
            if subAdminplans.objects.filter(plan_name=plan_name, user_type=user_type).exists():
                return Response({'error': 'This plan already exists.'}, status=status.HTTP_400_BAD_REQUEST)            

            serializer = subAdminplansSerializer(data=data)
            if serializer.is_valid():
                serializer.save()
                return Response({'message': 'Sub Plan created', 'data': serializer.data}, status=status.HTTP_201_CREATED)
            return Response({'error': serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    @extend_schema(request=subAdminplansSerializer)
    def put(self, request, pk):
        try:
            plan = subAdminplans.objects.get(pk=pk)
            serializer = subAdminplansSerializer(plan, data=request.data, partial=True)
            if serializer.is_valid():
                serializer.save()
                return Response({'message': 'Sub Plan updated', 'data': serializer.data})
            return Response({'error': serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except subAdminplans.DoesNotExist:
            return Response({'error': 'Sub Plan not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def delete(self, request, pk):
        try:
            plan = subAdminplans.objects.get(pk=pk)
            plan.delete()
            return Response({'message': 'Sub Plan deleted'})
        except subAdminplans.DoesNotExist:
            return Response({'error': 'Sub Plan not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
      

@method_decorator(csrf_exempt, name='dispatch')
class AddOnAPI(APIView):
    
    def get(self, request, pk=None):
        try:
            if pk:
                plan = AddOnPlans.objects.get(pk=pk)
                serializer = AddOnPlansSerializer(plan)
            else:
                plans = AddOnPlans.objects.all()
                serializer = AddOnPlansSerializer(plans, many=True)
            return Response(serializer.data)
        except AddOnPlans.DoesNotExist:
            return Response({'error': 'Add on Plan not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    @extend_schema(request=AddOnPlansSerializer)
    def post(self, request):
        try:

            data = request.data.copy()
 
            user_type = request.data.get('user_type')
            charges = request.data.get('charges')
          
            if AddOnPlans.objects.filter(charges=charges, user_type=user_type).exists():
                return Response({'error': 'This plan already exists.'}, status=status.HTTP_400_BAD_REQUEST)            

            serializer = AddOnPlansSerializer(data=data)
            if serializer.is_valid():
                serializer.save()
                return Response({'message': 'Add on Plan created', 'data': serializer.data}, status=status.HTTP_201_CREATED)
            return Response({'error': serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    @extend_schema(request=AddOnPlansSerializer)
    def put(self, request, pk):
        try:
            plan = AddOnPlans.objects.get(pk=pk)
            serializer = AddOnPlansSerializer(plan, data=request.data, partial=True)
            if serializer.is_valid():
                serializer.save()
                return Response({'message': 'Add On Plan updated', 'data': serializer.data})
            return Response({'error': serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except AddOnPlans.DoesNotExist:
            return Response({'error': 'Add On Plan not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def delete(self, request, pk):
        try:
            plan = AddOnPlans.objects.get(pk=pk)
            plan.delete()
            return Response({'message': 'Add On Plan deleted'})
        except AddOnPlans.DoesNotExist:
            return Response({'error': 'Add On Plan not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)




@method_decorator(csrf_exempt, name='dispatch')
class ReferralRewardView(APIView):
    
    def get(self, request, pk=None):
        try:
            if pk:
                reward = referral_reward.objects.get(pk=pk)
                serializer = referral_rewardSerializer(reward)
                return Response(serializer.data)
            else:
                rewards = referral_reward.objects.all()
                serializer = referral_rewardSerializer(rewards, many=True)
                return Response(serializer.data)
        except referral_reward.DoesNotExist:
            return Response({'error': 'Reward not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    @extend_schema(request=referral_rewardSerializer)
    def post(self, request):
        try:
            serializer = referral_rewardSerializer(data=request.data)
            if serializer.is_valid():
                new_reward = serializer.save()

                current_time = timezone.now() + timedelta(hours=5, minutes=30)

                # Update all previous records for same user where disabled_at is NULL
                referral_reward.objects.filter(                    
                    disabled_at__isnull=True
                ).exclude(rew_id=new_reward.rew_id).update(disabled_at=current_time)


                return Response({'message': 'Reward created successfully', 'data': serializer.data}, status=status.HTTP_201_CREATED)
            return Response({'error': serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    @extend_schema(request=referral_rewardSerializer)
    def put(self, request, pk):
        try:
            reward = referral_reward.objects.get(pk=pk)
            serializer = referral_rewardSerializer(reward, data=request.data, partial=True)
            if serializer.is_valid():
                serializer.save()
                return Response({'message': 'Reward updated successfully', 'data': serializer.data})
            return Response({'error': serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except referral_reward.DoesNotExist:
            return Response({'error': 'Reward not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def delete(self, request, pk):
        try:
            reward = referral_reward.objects.get(pk=pk)
            reward.delete()
            return Response({'message': 'Reward deleted successfully'})
        except referral_reward.DoesNotExist:
            return Response({'error': 'Reward not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)






@method_decorator(csrf_exempt, name='dispatch')
class OfferView(APIView):
    def get(self, request, pk=None):
        try:
            now = timezone.now()
            if pk:
                offer = offers.objects.get(pk=pk)
                serializer = offersSerializer(offer)
            else:
                active_only = request.query_params.get('active_only', 'false').lower() == 'true'
                qs = offers.objects.all() if not active_only else offers.objects.filter(status=True).filter(
                    Q(valid_life_time=True) |
                    Q(valid_from__lte=now, valid_to__gte=now)
                )
                serializer = offersSerializer(qs, many=True)
            return Response(serializer.data)
        except offers.DoesNotExist:
            return Response({'error': 'Offer not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    @extend_schema(request=offersSerializer)
    def post(self, request):
        try:
            serializer = offersSerializer(data=request.data)
            if serializer.is_valid():
                offer = serializer.save()

                # Create user offer records for every user (unclaimed state)
                for user in User.objects.all():
                    UserOfferClaim.objects.get_or_create(
                        user_id=user,
                        offer_id=offer,
                        defaults={
                            'claimed_status': False,
                            'no_of_times': offer.no_of_times or 0,
                            'no_of_claimed': 0,
                            'claimed_at': None,
                        }
                    )

                return Response(serializer.data, status=status.HTTP_201_CREATED)
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    @extend_schema(request=offersSerializer)
    def put(self, request, pk):
        try:
            offer = offers.objects.get(pk=pk)
            serializer = offersSerializer(offer, data=request.data, partial=True)
            if serializer.is_valid():
                serializer.save()
                return Response(serializer.data)
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        except offers.DoesNotExist:
            return Response({'error': 'Offer not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def delete(self, request, pk):
        try:
            offer = offers.objects.get(pk=pk)
            offer.delete()
            return Response({'message': 'Offer deleted successfully'})
        except offers.DoesNotExist:
            return Response({'error': 'Offer not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


def _claim_offer_to_user(user, offer):
    now = timezone.now()
    if not offer.valid_life_time:
        if offer.valid_from and now < offer.valid_from:
            return False, 'Offer is not currently valid', status.HTTP_400_BAD_REQUEST, None
        if offer.valid_to and now > offer.valid_to:
            return False, 'Offer is not currently valid', status.HTTP_400_BAD_REQUEST, None

    user_offer_claim = UserOfferClaim.objects.filter(user_id=user, offer_id=offer).first()
    if not user_offer_claim:
        return False, 'No claim record exists for this user and offer', status.HTTP_400_BAD_REQUEST, None

    if user_offer_claim.no_of_times and user_offer_claim.no_of_claimed >= user_offer_claim.no_of_times:
        user_offer_claim.claimed_status = True
        user_offer_claim.save(update_fields=['claimed_status'])
        return False, 'Offer claim limit reached', status.HTTP_400_BAD_REQUEST, None

    user_offer_claim.no_of_claimed += 1
    user_offer_claim.claimed_at = now
    if user_offer_claim.no_of_times and user_offer_claim.no_of_claimed >= user_offer_claim.no_of_times:
        user_offer_claim.claimed_status = True
    user_offer_claim.save()

    return True, 'Offer claimed', status.HTTP_201_CREATED, {
        'claim_id': user_offer_claim.claim_id,
        'no_of_claimed': user_offer_claim.no_of_claimed
    }


@method_decorator(csrf_exempt, name='dispatch')
class ClaimOfferView(APIView):
    @extend_schema(request=UserOfferClaimSerializer)
    def post(self, request):
        user_id = request.data.get('user_id')
        offer_id = request.data.get('offer_id')

        if not user_id or not offer_id:
            return Response({'error': 'user_id and offer_id are required'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            user = User.objects.get(user_id=user_id)
        except User.DoesNotExist:
            return Response({'error': 'User not found'}, status=status.HTTP_404_NOT_FOUND)

        try:
            offer = offers.objects.get(offer_id=offer_id, status=True)
        except offers.DoesNotExist:
            return Response({'error': 'Offer not found or inactive'}, status=status.HTTP_404_NOT_FOUND)

        success, message, st, data = _claim_offer_to_user(user, offer)
        if not success:
            return Response({'error': message}, status=st)
        payload = {'message': message}
        payload.update(data)
        return Response(payload, status=st)


@method_decorator(csrf_exempt, name='dispatch')
class ClaimOfferByUrlView(APIView):
    @extend_schema(request=UserOfferClaimSerializer)
    def post(self, request):
        user_id = request.data.get('user_id')
        offerurl_id = request.data.get('offerurl_id')

        if not user_id or not offerurl_id:
            return Response({'error': 'user_id and offerurl_id are required'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            user = User.objects.get(user_id=user_id)
        except User.DoesNotExist:
            return Response({'error': 'User not found'}, status=status.HTTP_404_NOT_FOUND)

        try:
            offer_url_obj = offerurls.objects.get(url_id=offerurl_id)
        except offerurls.DoesNotExist:
            return Response({'error': 'Offer URL not found'}, status=status.HTTP_404_NOT_FOUND)

        try:
            offer = offers.objects.get(offerurl=offer_url_obj, status=True)
        except offers.DoesNotExist:
            return Response({'error': 'Offer not found for this URL or inactive'}, status=status.HTTP_404_NOT_FOUND)

        success, message, st, data = _claim_offer_to_user(user, offer)
        if not success:
            return Response({'error': message}, status=st)
        payload = {'message': message}
        payload.update(data)
        return Response(payload, status=st)


@method_decorator(csrf_exempt, name='dispatch')
class AvailableOffersForUserView(APIView):
    def get(self, request, user_id):
        try:
            user = User.objects.get(user_id=user_id)
        except User.DoesNotExist:
            return Response({'error': 'User not found'}, status=status.HTTP_404_NOT_FOUND)

        now = timezone.now()
        claimed_offer_ids = UserOfferClaim.objects.filter(
            user_id=user
        ).filter(
            Q(claimed_status=True) | Q(no_of_times__isnull=False, no_of_claimed__gte=F('no_of_times'))
        ).values_list('offer_id', flat=True)
        available = offers.objects.filter(status=True).filter(
            Q(valid_life_time=True) |
            Q(valid_from__isnull=True, valid_to__isnull=True) |
            Q(valid_from__isnull=True, valid_to__gte=now) |
            Q(valid_from__lte=now, valid_to__isnull=True) |
            Q(valid_from__lte=now, valid_to__gte=now)
        ).exclude(offer_id__in=claimed_offer_ids)

        serializer = offersSerializer(available, many=True)
        return Response(serializer.data)


@method_decorator(csrf_exempt, name='dispatch')
class AssignFreePlanView(APIView):    
    @extend_schema(request=AssignFreePlanViewSerializer)
    def post(self, request):
        plan_id = request.data.get('plan_id')
        if not plan_id:
            return Response({"error": "plan_id is required"}, status=status.HTTP_400_BAD_REQUEST)
        
        try:
            plan = subAdminplans.objects.get(plan_id=plan_id, plan_name='Free')
        except subAdminplans.DoesNotExist:
            return Response({"error": "Free plan not found"}, status=status.HTTP_400_BAD_REQUEST)
        
        user_type = plan.user_type
        now = timezone.now()
        
        # Find users who have active subscriptions for this user_type
        users_with_active_plan = sub_user.objects.filter(
            user_type=user_type,
            status=True
        ).filter(
            Q(expired_date__isnull=True) | Q(expired_date__gt=now)
        ).values_list('user_id', flat=True)
        
        # Users without active plan for this user_type
        users_without_plan = User.objects.exclude(user_id__in=users_with_active_plan)
        
        assigned_count = 0
        for user in users_without_plan:
            # Calculate expired_date
            if plan.plan_name == 'Lifetime':
                expired_date = None
            else:
                expired_date = now + timedelta(days=plan.trial_days or 0) if plan.trial_days else None
            
            # Create sub_user entry
            sub_user_obj = sub_user(
                user_id=user,
                plan_name=plan.plan_name,
                razor_plan_id=plan.razor_plan_id,
                user_type=user_type,
                charges=plan.charges,
                buyer_no_unlimited=plan.buyer_no_unlimited,
                buyer_no=plan.buyer_no,
                no_of_properties_unlimited=plan.no_of_properties_unlimited,
                no_of_liked_data_unlimited=plan.no_of_liked_data_unlimited,
                matching_enquiry_unlimited=plan.matching_enquiry_unlimited,
                no_of_properties=plan.no_of_properties,
                no_of_liked_data=plan.no_of_liked_data,
                matching_enquiry=plan.matching_enquiry,
                expired_date=expired_date,
                status=True,
                sub_type='New'
            )
            sub_user_obj.save()
            
            # Update or create UserFeatures
            features, created = UserFeatures.objects.get_or_create(
                user_id=user,
                user_type=user_type,
                defaults={
                    'buyer_no_unlimited': plan.buyer_no_unlimited,
                    'buyer_no': plan.buyer_no,
                    'no_of_properties_unlimited': plan.no_of_properties_unlimited,
                    'no_of_liked_data_unlimited': plan.no_of_liked_data_unlimited,
                    'matching_enquiry_unlimited': plan.matching_enquiry_unlimited,
                    'no_of_properties': plan.no_of_properties,
                    'no_of_liked_data': plan.no_of_liked_data,
                    'matching_enquiry': plan.matching_enquiry,
                }
            )
            if not created:
                features.buyer_no_unlimited = plan.buyer_no_unlimited
                features.buyer_no = features.buyer_no + plan.buyer_no
                features.no_of_properties_unlimited = plan.no_of_properties_unlimited
                features.no_of_liked_data_unlimited = plan.no_of_liked_data_unlimited
                features.matching_enquiry_unlimited = plan.matching_enquiry_unlimited
                features.no_of_properties = features.no_of_properties + plan.no_of_properties
                features.no_of_liked_data = features.no_of_liked_data + plan.no_of_liked_data
                features.matching_enquiry = features.matching_enquiry + plan.matching_enquiry
                features.save()
            
            assigned_count += 1
        
        return Response({"message": f"Free plan assigned to {assigned_count} users"}, status=status.HTTP_200_OK)


@method_decorator(csrf_exempt, name='dispatch')
class ClaimAllFreePlansView(APIView):
    @extend_schema(request=ClaimAllFreePlansSerializer)
    def post(self, request):
        user_id = request.data.get('user_id')
        if not user_id:
            return Response({"error": "user_id is required"}, status=status.HTTP_400_BAD_REQUEST)

        try:
            user = User.objects.get(user_id=user_id)
        except User.DoesNotExist:
            return Response({"error": "User not found"}, status=status.HTTP_404_NOT_FOUND)

        free_plans = subAdminplans.objects.filter(plan_name='Free', status=True)
        if not free_plans.exists():
            return Response({"message": "No active Free plans available"}, status=status.HTTP_200_OK)

        now = timezone.now()
        assigned = []
        skipped = []

        for plan in free_plans:
            user_type = plan.user_type
            if not user_type:
                continue

            active_exists = sub_user.objects.filter(
                user_id=user,
                user_type=user_type,
                status=True
            ).filter(
                Q(expired_date__isnull=True) | Q(expired_date__gt=now)
            ).exists()

            if active_exists:
                skipped.append({'user_type': user_type, 'reason': 'active plan exists'})
                continue

            expired_date = None
            if plan.plan_name != 'Lifetime':
                expired_date = now + timedelta(days=plan.trial_days or 0) if plan.trial_days else None

            sub_user.objects.create(
                user_id=user,
                plan_name=plan.plan_name,
                razor_plan_id=plan.razor_plan_id,
                user_type=user_type,
                charges=plan.charges,
                buyer_no_unlimited=plan.buyer_no_unlimited,
                buyer_no=plan.buyer_no,
                no_of_properties_unlimited=plan.no_of_properties_unlimited,
                no_of_liked_data_unlimited=plan.no_of_liked_data_unlimited,
                matching_enquiry_unlimited=plan.matching_enquiry_unlimited,
                no_of_properties=plan.no_of_properties,
                no_of_liked_data=plan.no_of_liked_data,
                matching_enquiry=plan.matching_enquiry,
                expired_date=expired_date,
                status=True,
                sub_type='New'
            )

            features, created = UserFeatures.objects.get_or_create(
                user_id=user,
                user_type=user_type,
                defaults={
                    'buyer_no_unlimited': plan.buyer_no_unlimited,
                    'buyer_no': plan.buyer_no,
                    'no_of_properties_unlimited': plan.no_of_properties_unlimited,
                    'no_of_liked_data_unlimited': plan.no_of_liked_data_unlimited,
                    'matching_enquiry_unlimited': plan.matching_enquiry_unlimited,
                    'no_of_properties': plan.no_of_properties,
                    'no_of_liked_data': plan.no_of_liked_data,
                    'matching_enquiry': plan.matching_enquiry,
                }
            )
            if not created:
                features.buyer_no_unlimited = plan.buyer_no_unlimited
                features.buyer_no = plan.buyer_no
                features.no_of_properties_unlimited = plan.no_of_properties_unlimited
                features.no_of_liked_data_unlimited = plan.no_of_liked_data_unlimited
                features.matching_enquiry_unlimited = plan.matching_enquiry_unlimited
                features.no_of_properties = plan.no_of_properties
                features.no_of_liked_data = plan.no_of_liked_data
                features.matching_enquiry = plan.matching_enquiry
                features.save()

            assigned.append(user_type)

        return Response({
            'message': 'Free plans claimed',
            'assigned_user_types': assigned,
            'skipped_user_types': skipped
        }, status=status.HTTP_200_OK)


@method_decorator(csrf_exempt, name='dispatch')
class getofferurls(APIView):   
    def get(self, request, pk=None):
        try:
            if pk:
                urls = offerurls.objects.get(pk=pk)
                serializer = offerurlsSerializer(urls)
                return Response(serializer.data)
            else:
                urls = offerurls.objects.all()
                serializer = offerurlsSerializer(urls, many=True)
                return Response(serializer.data)
        except offerurls.DoesNotExist:
            return Response({'error': 'Offer URL not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)




@method_decorator(csrf_exempt, name='dispatch')
class AllUsersClaimAllFreePlansView(APIView):    
    def post(self, request):
        try:
            users_lst = User.objects.all()
        except:
            users_lst = None
        
        if users_lst is None:
            return Response({"error": "No users found"}, status=status.HTTP_404_NOT_FOUND)
        
        try:
            free_plans = subAdminplans.objects.filter(plan_name='Free', status=True)
        except:
            free_plans = None

        if not free_plans.exists():
            return Response({"message": "No active Free plans available"}, status=status.HTTP_200_OK)

        now = timezone.now()
        assigned = []
        skipped = []

        for user in users_lst:
            for plan in free_plans:
                user_type = plan.user_type
                if not user_type:
                    continue

                active_exists = sub_user.objects.filter(
                    user_id=user,
                    user_type=user_type,
                    status=True
                ).filter(
                    Q(expired_date__isnull=True) | Q(expired_date__gt=now)
                ).exists()

                if active_exists:
                    skipped.append({'user_type': user_type, 'reason': 'active plan exists'})
                    continue

                expired_date = None
                if plan.plan_name != 'Lifetime':
                    expired_date = now + timedelta(days=plan.trial_days or 0) if plan.trial_days else None

                sub_user.objects.create(
                    user_id=user,
                    plan_name=plan.plan_name,
                    razor_plan_id=plan.razor_plan_id,
                    user_type=user_type,
                    charges=plan.charges,
                    buyer_no_unlimited=plan.buyer_no_unlimited,
                    buyer_no=plan.buyer_no,
                    no_of_properties_unlimited=plan.no_of_properties_unlimited,
                    no_of_liked_data_unlimited=plan.no_of_liked_data_unlimited,
                    matching_enquiry_unlimited=plan.matching_enquiry_unlimited,
                    no_of_properties=plan.no_of_properties,
                    no_of_liked_data=plan.no_of_liked_data,
                    matching_enquiry=plan.matching_enquiry,
                    expired_date=expired_date,
                    status=True,
                    sub_type='New'
                )

                features, created = UserFeatures.objects.get_or_create(
                    user_id=user,
                    user_type=user_type,
                    defaults={
                        'buyer_no_unlimited': plan.buyer_no_unlimited,
                        'buyer_no': plan.buyer_no,
                        'no_of_properties_unlimited': plan.no_of_properties_unlimited,
                        'no_of_liked_data_unlimited': plan.no_of_liked_data_unlimited,
                        'matching_enquiry_unlimited': plan.matching_enquiry_unlimited,
                        'no_of_properties': plan.no_of_properties,
                        'no_of_liked_data': plan.no_of_liked_data,
                        'matching_enquiry': plan.matching_enquiry,
                    }
                )
                if not created:
                    features.buyer_no_unlimited = plan.buyer_no_unlimited
                    features.buyer_no = plan.buyer_no
                    features.no_of_properties_unlimited = plan.no_of_properties_unlimited
                    features.no_of_liked_data_unlimited = plan.no_of_liked_data_unlimited
                    features.matching_enquiry_unlimited = plan.matching_enquiry_unlimited
                    features.no_of_properties = plan.no_of_properties
                    features.no_of_liked_data = plan.no_of_liked_data
                    features.matching_enquiry = plan.matching_enquiry
                    features.save()

                assigned.append(user_type)

        return Response({
            'message': 'Free plans claimed',
            'assigned_user_types': assigned,
            'skipped_user_types': skipped
        }, status=status.HTTP_200_OK)
    

