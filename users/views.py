
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.contrib.auth.hashers import make_password
from django.contrib.auth.hashers import check_password
from django.shortcuts import get_object_or_404
from .models import *
from .serializers import *
from landnest_admin.models import *
from dateutil.relativedelta import relativedelta
from django.utils import timezone
import os
import json
from datetime import datetime, timedelta
from django.conf import settings
from uuid import uuid4
import random
import string
from django.db.models import F, Q
from django.conf import settings

import importlib
import uuid
import random
from django.core.mail import send_mail

from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from drf_spectacular.utils import extend_schema
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
from django.core.cache import cache
from django.db.models import Q, Count, Prefetch, Max
from django.conf import settings
import json
from datetime import timedelta
# import redis  # Uncomment after installing redis package


# import razorpay  # Uncomment after installing razorpay package
from django.http import JsonResponse
from django.shortcuts import render


# from rest_framework_simplejwt.tokens import RefreshToken, TokenError
# from rest_framework.permissions import IsAuthenticated
# from rest_framework_simplejwt.authentication import JWTAuthentication


# def get_tokens_for_custom_user(custom_user):
#     refresh = RefreshToken()
#     refresh['user_id'] = custom_user.user_id
#     refresh['username'] = custom_user.username
#     refresh['email'] = custom_user.email
#     refresh['role'] = custom_user.role.role_name  # or role_id

#     return {
#         'refresh': str(refresh),
#         'access': str(refresh.access_token),
#     }

@method_decorator(csrf_exempt, name='dispatch')
class ValidateSessionAPIView(APIView):
    def get(self, request):
        return Response({"message": "Session valid"})



@method_decorator(csrf_exempt, name='dispatch')
class RoleListCreateAPIView(APIView):
    def get(self, request):
        try:
            roles = Role.objects.all()
            serializer = RoleSerializer(roles, many=True)
            return Response(serializer.data)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        
    # @extend_schema(request=RoleSerializer)
    # def post(self, request):
        
    #     role_name = request.data.get('role_name')
     
    #     if Role.objects.filter(role_name=role_name).exists():            
    #         return Response({'error': 'This role already exists.'}, status=status.HTTP_400_BAD_REQUEST)        

    #     try:
    #         serializer = RoleSerializer(data=request.data, partial=True)
    #         if serializer.is_valid():
    #             role = serializer.save()

    #             rolePermission.objects.create(role_name=role)

    #             return Response(serializer.data, status=status.HTTP_201_CREATED)
    #         return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
    #     except Exception as e:
    #         return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    @extend_schema(request=RoleSerializer)
    def post(self, request):
        role_name = request.data.get('role_name')

        if Role.objects.filter(role_name=role_name).exists():
            return Response({'error': 'This role already exists.'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            serializer = RoleSerializer(data=request.data)
            if serializer.is_valid():
                role = serializer.save()

                # 👉 Automatically create rolePermission with defaults
                rolePermission.objects.create(role_name=role)

                return Response(serializer.data, status=status.HTTP_201_CREATED)
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@method_decorator(csrf_exempt, name='dispatch')
class RoleDetailAPIView(APIView):
    def get(self, request, pk):
        try:
            role = get_object_or_404(Role, pk=pk)
            serializer = RoleSerializer(role)
            return Response(serializer.data)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    @extend_schema(request=RoleSerializer)
    def put(self, request, pk):
        try:
            role = get_object_or_404(Role, pk=pk)
            serializer = RoleSerializer(role, data=request.data, partial=True)
            if serializer.is_valid():
                serializer.save()
                return Response(serializer.data)
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def delete(self, request, pk):
        try:
            role = get_object_or_404(Role, pk=pk)
            role.delete()
            return Response({'message': 'role deleted successfully'})            
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@method_decorator(csrf_exempt, name='dispatch')
class RolePermissionDetailAPIView(APIView):
    @extend_schema(request=rolePermissionSerializer)
    def put(self, request, pk):
        try:
            permission = get_object_or_404(rolePermission, pk=pk)
            serializer = rolePermissionSerializer(permission, data=request.data, partial=True)
            if serializer.is_valid():
                serializer.save()
                return Response(serializer.data, status=status.HTTP_200_OK)
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        


@method_decorator(csrf_exempt, name='dispatch')
class RegSendOTPAPIView(APIView):
    @extend_schema(request=sendotpserializer)
    def post(self, request):
        try:            
            email = request.data.get('email')
            mobile_no = request.data.get('mobile_no')               
            
            if not email:
                return Response({"status": False, "message": "Email is required."}, status=status.HTTP_400_BAD_REQUEST)
                                        
            if User.objects.filter(email = email).exists():
                return Response({'error': 'This email already exists.'}, status=status.HTTP_400_BAD_REQUEST)
            
            if User.objects.filter(mobile_no = mobile_no).exists():
                return Response({'error': 'This mobile number already exists.'}, status=status.HTTP_400_BAD_REQUEST)       

            otp = str(random.randint(1000, 9999))            

            # Save OTP
            UserOTP.objects.create(email=email, otp=otp)

            # Send OTP via email            
            send_mail(
                subject='Your OTP for Password Reset',
                message=f'Hello, \n\nYour OTP is: {otp}',
                from_email=settings.EMAIL_HOST_USER,   
                recipient_list=[email],
                fail_silently=False,
            )

            return Response({"status": True, "message": "OTP sent successfully."}, status=status.HTTP_200_OK)

        except Exception as e:
            return Response({"status": False, "message": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@method_decorator(csrf_exempt, name='dispatch')
class RegVerifyOTPAPIView(APIView):
    @extend_schema(request=verifyotpserializer)
    def post(self, request):
        try:
            email = request.data.get('email')
            otp = request.data.get('otp')

            if not email or not otp:
                return Response({"status": False, "message": "Email and OTP are required."}, status=status.HTTP_400_BAD_REQUEST)

            try:
                otp_recs = UserOTP.objects.filter(email=email)
                otp_rec = otp_recs.last() 
                print('otp_recs', otp_recs)
                print('otp_rec', otp_rec)
                if otp_rec and otp_rec.otp == otp:
                    return Response({"status": True, "message": "OTP verified. You can now reset your password."}, status=status.HTTP_200_OK)
                else:
                    return Response({"status": False, "message": "Invalid OTP or email."}, status=status.HTTP_400_BAD_REQUEST)
                
            except UserOTP.DoesNotExist:
                return Response({"status": False, "message": "Invalid OTP or email."}, status=status.HTTP_400_BAD_REQUEST)

        except Exception as e:
            return Response({"status": False, "message": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)



@method_decorator(csrf_exempt, name='dispatch')
class UserListCreateAPIView(APIView):
    
    def get(self, request):
        try:
            # Get pagination parameters - default to 20 records
            page = int(request.GET.get('page', 1))
            page_size = int(request.GET.get('page_size', 20))
            search = request.GET.get('search', '')
            user_type_filter = request.GET.get('user_type', '')
            status_filter = request.GET.get('status', '')
            sort_by = request.GET.get('sort_by', 'created_at')
            sort_order = request.GET.get('sort_order', 'desc')
            
            # Progressive loading parameters
            progressive = request.GET.get('progressive', 'false').lower() == 'true'
            chunk_size = int(request.GET.get('chunk_size', 100))
            offset = int(request.GET.get('offset', 0))
            
            # Validate page_size
            max_page_size = 5000
            page_size = min(page_size, max_page_size)
            
            # Generate cache key
            cache_key = f"users_list_{page}_{page_size}_{search}_{user_type_filter}_{status_filter}_{sort_by}_{sort_order}_{progressive}_{offset}_{chunk_size}"
            
            # Try to get from cache first (only if cache is available)
            try:
                cached_data = cache.get(cache_key)
                if cached_data:
                    return Response(cached_data)
            except:
                pass  # Cache not available, continue with database query
            
            # Start with optimized queryset to avoid N+1 queries
            queryset = User.objects.filter(role='1').select_related(
                'referred_by'  # Only if referred_by is a ForeignKey
            ).prefetch_related(
                # Add prefetch_related only if you actually need these relationships
                # For now, keeping it minimal to test performance
            )
            
            # Apply filters
            if search:
                queryset = queryset.filter(
                    Q(username__icontains=search) |
                    Q(email__icontains=search) |
                    Q(mobile_no__icontains=search)
                )
            
            if user_type_filter:
                queryset = queryset.filter(user_type=user_type_filter)
            
            # Apply simple sorting
            valid_sort_fields = ['created_at', 'username', 'email', 'mobile_no']
            if sort_by in valid_sort_fields:
                order_prefix = '-' if sort_order == 'desc' else ''
                queryset = queryset.order_by(f"{order_prefix}{sort_by}")
            else:
                queryset = queryset.order_by('-created_at')
            
            # Skip total count for maximum performance - use has_next detection instead
            
            # Handle progressive loading
            if progressive and page_size > 100:
                # For progressive loading, return chunk_size records at a time
                actual_page_size = min(chunk_size, page_size - offset)
                start_index = (page - 1) * page_size + offset
                
                # Apply slicing for progressive loading
                users_queryset = queryset[start_index:start_index + actual_page_size]
                
                # Serialize the data with lightweight serializer
                serializer = UserListSerializer(users_queryset, many=True)
                
                # Check if there are more records without expensive count()
                has_next = len(users_queryset) == actual_page_size
                has_previous = page > 1 or offset > 0
                
                # Progressive loading response
                response_data = {
                    'results': serializer.data,
                    'pagination': {
                        'current_page': page,
                        'total_pages': None,  # Skipped for performance
                        'total_count': None,  # Skipped for performance
                        'page_size': page_size,
                        'has_next': has_next,
                        'has_previous': has_previous,
                        'next_page': page + 1 if has_next else None,
                        'previous_page': page - 1 if has_previous else None,
                        'progressive': True,
                        'offset': offset,
                        'chunk_size': actual_page_size,
                        'has_more_chunks': offset + actual_page_size < page_size,
                    },
                    'filters_applied': {
                        'search': search,
                        'user_type': user_type_filter,
                        'status': status_filter,
                        'sort_by': sort_by,
                        'sort_order': sort_order,
                    }
                }
            else:
                # Use efficient offset-based pagination instead of Django Paginator
                start_index = (page - 1) * page_size
                end_index = start_index + page_size
                
                # Get one extra record to check if there are more pages
                users_queryset = queryset[start_index:end_index + 1]
                
                # Serialize the data with lightweight serializer (excluding the extra record)
                serializer = UserListSerializer(users_queryset[:page_size], many=True)
                
                # Check if there are more records
                has_next = len(users_queryset) > page_size
                has_previous = page > 1
                
                # Normal pagination response
                response_data = {
                    'results': serializer.data,
                    'pagination': {
                        'current_page': page,
                        'total_pages': None,  # Skipped for performance
                        'total_count': None,  # Skipped for performance
                        'page_size': page_size,
                        'has_next': has_next,
                        'has_previous': has_previous,
                        'next_page': page + 1 if has_next else None,
                        'previous_page': page - 1 if has_previous else None,
                        'progressive': False,
                    },
                    'filters_applied': {
                        'search': search,
                        'user_type': user_type_filter,
                        'status': status_filter,
                        'sort_by': sort_by,
                        'sort_order': sort_order,
                    }
                }
            
            # Try to cache the result (only if cache is available)
            try:
                cache.set(cache_key, response_data, timeout=300)
            except:
                pass  # Cache not available, continue
            
            return Response(response_data)
            
        except Exception as e:
            # Log the error for debugging
            print(f"Error in UserListCreateAPIView: {str(e)}")
            return Response({
                'error': str(e),
                'message': 'Failed to fetch users. Please try again.'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    @extend_schema(request=UserSerializer)
    def post(self, request):
        
        username = request.data.get('username')
        email = request.data.get('email')
        mobile_no = request.data.get('mobile_no')   
        referred_by = request.data.get('referred_by', None)             
        
        if User.objects.filter(email = email).exists():
            return Response({'error': 'This email already exists.'}, status=status.HTTP_400_BAD_REQUEST)
        
        if User.objects.filter(mobile_no = mobile_no).exists():
            return Response({'error': 'This mobile number already exists.'}, status=status.HTTP_400_BAD_REQUEST)       

        try:            
            user_data = request.data.copy()            

            if referred_by:
                try:
                    referred_user = User.objects.get(mobile_no=referred_by)
                    user_data['referred_by'] = referred_user.user_id                                
                except User.DoesNotExist:
                    return Response({'error': 'Referred by code is invalid.'}, status=status.HTTP_400_BAD_REQUEST)
            else:
                user_data['referred_by'] = None


            serializer = UserSerializer(data=user_data)
            if serializer.is_valid():
                user = serializer.save()

                # Auto assign Free plans to new user for all available user types
                free_user_types = subAdminplans.objects.filter(
                    plan_name='Free',
                    status=True
                ).exclude(user_type__isnull=True).exclude(user_type='').values_list('user_type', flat=True).distinct()

                now = timezone.now()
                assigned_types = []

                for user_type in free_user_types:
                    active_exists = sub_user.objects.filter(
                        user_id=user,
                        user_type=user_type,
                        status=True
                    ).filter(
                        Q(expired_date__isnull=True) | Q(expired_date__gt=now)
                    ).exists()

                    if active_exists:
                        continue

                    plan = subAdminplans.objects.filter(plan_name='Free', user_type=user_type, status=True).first()
                    if not plan:
                        continue
                    
                    if plan.trial_days:
                        expired_date = now + timedelta(days=plan.trial_days or 0) if plan.trial_days else None
                    else:
                        expired_date = None

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

                    assigned_types.append(user_type)

                # Create UserOfferClaim for valid offers
                now = timezone.now()
                try:
                    valid_offers = offers.objects.filter(status=True).filter(
                        Q(valid_life_time=True) |
                        Q(valid_from__lte=now, valid_to__gte=now)
                    )
                except Exception:
                    valid_offers = None

                if valid_offers:
                    for offer in valid_offers:
                        UserOfferClaim.objects.get_or_create(
                            user_id=user,
                            offer_id=offer,
                            defaults={
                                'claimed_status': False,
                                'no_of_times': offer.no_of_times,
                                'no_of_claimed': 0,
                                'claimed_at': None,
                            }
                        )

                return Response(serializer.data, status=status.HTTP_201_CREATED)
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)




@method_decorator(csrf_exempt, name='dispatch')
class UserGetUserTypeAPIView(APIView):
    @extend_schema(request=UserGetUserTypeAPIViewSerializer)
    def post(self, request):
        try:
            user_type = request.data.get('user_type')
            page = int(request.data.get('page', 1))
            page_size = min(int(request.data.get('page_size', 20)), 100)
            search = request.data.get('search', '')
            sort_by = request.data.get('sort_by', 'created_at')
            sort_order = request.data.get('sort_order', 'desc')

            if not user_type:
                return Response(
                    {"error": "user_type is required"},
                    status=status.HTTP_400_BAD_REQUEST
                )

            # Generate cache key
            cache_key = f"users_type_{user_type}_{page}_{page_size}_{search}_{sort_by}_{sort_order}"
            
            # Try to get from cache first
            cached_data = cache.get(cache_key)
            if cached_data:
                return Response(cached_data)

            # Start with base queryset
            if user_type == 'Old Users':
                queryset = User.objects.filter(user_type=None, role='1')
            else:
                queryset = User.objects.filter(user_type=user_type, role='1')

            # Apply search filter
            if search:
                search_terms = search.split()
                search_query = Q()
                for term in search_terms:
                    search_query |= (
                        Q(username__icontains=term) |
                        Q(email__icontains=term) |
                        Q(mobile_no__icontains=term) |
                        Q(first_name__icontains=term) |
                        Q(last_name__icontains=term)
                    )
                queryset = queryset.filter(search_query)

            # Apply sorting
            valid_sort_fields = ['created_at', 'username', 'email', 'mobile_no', 'first_login']
            if sort_by in valid_sort_fields:
                if sort_by == 'first_login':
                    queryset = queryset.annotate(
                        first_login=Max('log_history__login_time')
                    ).order_by(f"{'-' if sort_order == 'desc' else ''}first_login")
                else:
                    order_prefix = '-' if sort_order == 'desc' else ''
                    queryset = queryset.order_by(f"{order_prefix}{sort_by}")

            # Optimize with select_related and prefetch_related
            queryset = queryset.select_related('role').prefetch_related(
                Prefetch('user_sub_plan', queryset=sub_user.objects.filter(status=True).order_by('-expired_date')),
                Prefetch('user_properties', queryset=Property.objects.all().order_by('-created_at')[:5]),
                Prefetch('log_history', queryset=loghistory.objects.all().order_by('-login_time')[:3])
            )

            # Get total count
            total_count = queryset.count()

            # Apply pagination
            paginator = Paginator(queryset, page_size)
            try:
                users_page = paginator.page(page)
            except PageNotAnInteger:
                users_page = paginator.page(1)
            except EmptyPage:
                users_page = paginator.page(paginator.num_pages)

            # Serialize the data
            serializer = UserSerializer(users_page.object_list, many=True)

            # Prepare response with pagination metadata
            response_data = {
                'results': serializer.data,
                'pagination': {
                    'current_page': page,
                    'total_pages': paginator.num_pages,
                    'total_count': total_count,
                    'page_size': page_size,
                    'has_next': users_page.has_next(),
                    'has_previous': users_page.has_previous(),
                    'next_page': page + 1 if users_page.has_next() else None,
                    'previous_page': page - 1 if users_page.has_previous() else None,
                },
                'filters_applied': {
                    'user_type': user_type,
                    'search': search,
                    'sort_by': sort_by,
                    'sort_order': sort_order,
                }
            }

            # Cache the result for 5 minutes
            cache.set(cache_key, response_data, timeout=300)

            return Response(response_data, status=status.HTTP_200_OK)

        except Exception as e:
            return Response(
                {'error': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

@method_decorator(csrf_exempt, name='dispatch')
class UserGetByIdAPIView(APIView):
    """
    Get user by user_id (for login functionality)
    """
    def get(self, request, user_id):
        try:
            user = User.objects.get(user_id=user_id)
            serializer = UserSerializer(user)
            return Response(serializer.data)
        except User.DoesNotExist:
            return Response({"error": "User not found"}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@method_decorator(csrf_exempt, name='dispatch')
class UserRetrieveUpdateDeleteAPIView(APIView):

    def get_object(self, pk): 
        try: 
            return User.objects.get(pk=pk) 
        except User.DoesNotExist: 
            return None
    
    
    def get(self, request, pk):
        try:
            user = self.get_object(pk)
            if not user:
                return Response({"error": "User not found"}, status=status.HTTP_404_NOT_FOUND)
            serializer = UserSerializer(user)
            return Response(serializer.data)  
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        
        
    @extend_schema(request=UserSerializer)
    def put(self, request, pk):
        try:
            user = self.get_object(pk)
            if not user:
                return Response({"error": "User not found"}, status=status.HTTP_404_NOT_FOUND)
            serializer = UserSerializer(user, data=request.data, partial=True)
            if serializer.is_valid():
                serializer.save()
                return Response(serializer.data)
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def delete(self, request, pk):
        try:
            user = self.get_object(pk)
            if not user:
                return Response({"error": "User not found"}, status=status.HTTP_404_NOT_FOUND)
            user.delete()
            return Response({'message': 'user deleted successfully'})            
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@method_decorator(csrf_exempt, name='dispatch')
class UserGetByIdentifierAPIView(APIView):
        
    def get_object(self, identifier): 
        try: 
            return User.objects.get(mobile_no=identifier) 
        except User.DoesNotExist: 
            try:
                return User.objects.get(email=identifier)
            except User.DoesNotExist:
                return None
    
    def get(self, request, identifier):
        try:
            user = self.get_object(identifier)
            if not user:
                return Response({"error": "User not found"}, status=status.HTTP_404_NOT_FOUND)
            serializer = UserSerializer(user)
            return Response(serializer.data)  
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        
        


@method_decorator(csrf_exempt, name='dispatch')
class UserChangePasswordAPIView(APIView):
    @extend_schema(request=PasswordChangeSerializer)    
    def put(self, request, pk):
        try:
            user = User.objects.get(pk=pk)
        except User.DoesNotExist:
            return Response({"error": "User not found"}, status=status.HTTP_404_NOT_FOUND)

        serializer = PasswordChangeSerializer(data=request.data)
        if serializer.is_valid():
            new_password = serializer.validated_data['password']
            user.password = new_password  # Triggers save(), which hashes it if needed
            user.save()
            return Response({"message": "Password updated successfully."})
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


@method_decorator(csrf_exempt, name='dispatch')
class UserLoginAPIView(APIView):
    @extend_schema(request=loginserializer)
    def post(self, request):
        identifier = request.data.get('identifier')
        password = request.data.get('password')
        fcm_token = request.data.get('fcm_token')

        print('identifier', identifier)
        print('password', password)
        if not identifier or not password:
            return Response({"error": "Please provide email/mobile and password."}, status=status.HTTP_400_BAD_REQUEST)

        try:
            user = User.objects.get(email=identifier)
        except User.DoesNotExist:
            try:
                user = User.objects.get(mobile_no=identifier)
            except User.DoesNotExist:
                return Response({"error": "Invalid credentials1."}, status=status.HTTP_401_UNAUTHORIZED)

        if not check_password(password, user.password):
            return Response({"error": "Invalid credentials."}, status=status.HTTP_401_UNAUTHORIZED)
        
        # if user.current_session_token:
        #         # Already logged in elsewhere                                   
        #         return Response({
        #             "status": "already_logged_in",
        #             "message": "You are already logged in on another device. Do you want to logout from that device and continue?",
        #             "fcm_token": fcm_token if fcm_token else None
        #         }, status=status.HTTP_409_CONFLICT)

        # Generate new session token (invalidate old device)
        session_token = str(uuid4())
        user.current_session_token = session_token

        if user.first_login is None:
            user.first_login = timezone.now()

        if fcm_token:
            user.fcm_token = fcm_token

        user.save(update_fields=["current_session_token", "fcm_token"])

        # Create loghistory entry
        loghistory.objects.create(user_id=user, login_time=timezone.now(), logout_time=None)

        return Response({
            "message": "Login successful",
            "user_id": user.user_id,
            "username": user.username,
            "session_token": session_token
        }, status=status.HTTP_200_OK)


@method_decorator(csrf_exempt, name='dispatch')
class ForceLoginAPIView(APIView):
    @extend_schema(request=forceloginserializer)
    def post(self, request):
        identifier = request.data.get("identifier")
        fcm_token = request.data.get("fcm_token")
        try:            
            try:
                user = User.objects.get(email=identifier)
            except User.DoesNotExist:
                try:
                    user = User.objects.get(mobile_no=identifier)
                except User.DoesNotExist:
                    return Response({"error": "Invalid Identifier."}, status=status.HTTP_401_UNAUTHORIZED)
            new_token = str(uuid4())
            user.current_session_token = new_token
            if fcm_token:
                user.fcm_token = fcm_token
            user.save()
            
            last_login = loghistory.objects.filter(user_id=user, logout_time__isnull=True).last()
            if last_login:
                last_login.logout_time = timezone.now()
                last_login.save()
            
            # Create loghistory entry
            loghistory.objects.create(user_id=user, login_time=timezone.now(), logout_time=None)
            
            return Response({
                "message": "Login successful",
                "user_id": user.user_id,
                "username": user.username,
                "session_token": new_token
            }, status=status.HTTP_200_OK)
                    
        except User.DoesNotExist:
            return Response({"error": "User not found"}, status=status.HTTP_404_NOT_FOUND)


@method_decorator(csrf_exempt, name='dispatch')
class UserLogoutAPIView(APIView):
    @extend_schema(request=logoutserializer)
    def post(self, request):
        user_id = request.data.get("user_id")        
        if user_id:         
            try:
                user = User.objects.get(user_id=user_id)
                
                # Update logout time for the last login record
                last_login = loghistory.objects.filter(user_id=user, logout_time__isnull=True).last()
                if last_login:
                    last_login.logout_time = timezone.now()
                    last_login.save()
                
                user.current_session_token = None
                user.save(update_fields=["current_session_token"])
            except User.DoesNotExist:
                pass

            request.session.flush()
            return Response({"message": "Logout successful."}, status=status.HTTP_200_OK)

        return Response({"error": "No user is currently logged in."}, status=status.HTTP_400_BAD_REQUEST)




@method_decorator(csrf_exempt, name='dispatch')
class VendorAPIView(APIView):

    def get(self, request, pk=None):
        try:
            if pk:
                vendor = Vendors.objects.get(pk=pk)
                serializer = VendorSerializer(vendor)
                return Response(serializer.data)
            else:
                vendors = Vendors.objects.all()
                serializer = VendorSerializer(vendors, many=True)
                return Response(serializer.data)
        except Exception as e:
            return Response({'error': f'An error occurred: {str(e)}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    @extend_schema(request=VendorSerializer)
    def post(self, request):
        try:           
            name = request.data.get('name')
            profession = request.data.get('profession')
            mobile = request.data.get('mobile')

            if Vendors.objects.filter(name=name, profession=profession, mobile=mobile).exists():
                return Response({'error': 'This vendor already exists.'}, status=status.HTTP_400_BAD_REQUEST)

            serializer = VendorSerializer(data=request.data)
            if serializer.is_valid():
                vendor = serializer.save()
                
                return Response({'message': 'Vendor created successfully', 'data': serializer.data}, status=status.HTTP_201_CREATED)
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({'error': f'An error occurred: {str(e)}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    @extend_schema(request=VendorSerializer)
    def put(self, request, pk):
        try:
            vendor_obj = Vendors.objects.get(pk=pk)
        except Vendors.DoesNotExist:
            return Response({'error': 'Vendors not found'}, status=status.HTTP_404_NOT_FOUND)


        deleted_ids = request.data.get('deleted_image_ids', '')
        deleted_ids = [int(i.strip()) for i in deleted_ids.split(',') if i.strip().isdigit()]
        

        for img_id in deleted_ids:
            try:
                image = VendorWorkImage.objects.get(id=img_id, vendor=vendor_obj)
                image_path = image.image.path

                if os.path.exists(image_path):
                    os.remove(image_path)
                image.delete()
            except VendorWorkImage.DoesNotExist:
                return Response(
                    {'error': f'Image with ID {img_id} not found for this Vendor.'},
                    status=status.HTTP_404_NOT_FOUND
                )
            except Exception as e:
                return Response(
                    {'error': f'Failed to delete image {img_id}: {str(e)}'},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )
                
        serializer = VendorSerializer(vendor_obj, data=request.data, partial=True)

        if serializer.is_valid():
            serializer.save()

            # Handle new images
            new_images = request.FILES.getlist('new_work_images')
            for image in new_images:
                VendorWorkImage.objects.create(vendor=vendor_obj, image=image)

            return Response({'message': 'Vendor updated successfully', 'data': serializer.data}, status=200)
        else:            
            return Response(serializer.errors, status=400)





    def delete(self, request, pk):
        try:
            vendor = Vendors.objects.get(pk=pk)
            vendor.delete()
            return Response({'message': 'Vendor deleted successfully'})
        except Vendors.DoesNotExist:
            return Response({'error': 'Vendor not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': f'An error occurred: {str(e)}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)





@method_decorator(csrf_exempt, name='dispatch')
class UserCartView(APIView):

    def get(self, request, pk=None):
        try:
            if pk:
                cart = user_cart.objects.get(pk=pk)
                serializer = user_cartSerializer(cart)
                return Response(serializer.data)
            else:
                carts = user_cart.objects.all()
                serializer = user_cartSerializer(carts, many=True)
                return Response(serializer.data)
        except user_cart.DoesNotExist:
            return Response({'error': 'Cart not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    @extend_schema(request=user_cartSerializer)
    def post(self, request):        
        try:
            serializer = user_cartSerializer(data=request.data)
            if serializer.is_valid():
                serializer.save()
                return Response({'message': 'Cart created successfully', 'data': serializer.data}, status=status.HTTP_201_CREATED)
            return Response({'error': serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    @extend_schema(request=user_cartSerializer)
    def put(self, request, pk):
        try:
            cart = user_cart.objects.get(pk=pk)
            serializer = user_cartSerializer(cart, data=request.data, partial=True)
            if serializer.is_valid():
                serializer.save()
                return Response({'message': 'Cart updated successfully', 'data': serializer.data})
            return Response({'error': serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except user_cart.DoesNotExist:
            return Response({'error': 'Cart not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def delete(self, request, pk):
        try:
            cart = user_cart.objects.get(pk=pk)
            cart.delete()
            return Response({'message': 'Cart deleted successfully'})
        except user_cart.DoesNotExist:
            return Response({'error': 'Cart not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@method_decorator(csrf_exempt, name='dispatch')
class BestDealsView(APIView):

    def get(self, request, pk=None):
        try:
            if pk:
                deal = best_deals.objects.get(pk=pk)
                serializer = best_dealsSerializer(deal)
                return Response(serializer.data)
            else:
                deals = best_deals.objects.all()
                serializer = best_dealsSerializer(deals, many=True)
                return Response(serializer.data)
        except best_deals.DoesNotExist:
            return Response({'error': 'Deal not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    @extend_schema(request=best_dealsSerializer)
    def post(self, request):        
        try:
            serializer = best_dealsSerializer(data=request.data)
            if serializer.is_valid():
                serializer.save()
                return Response({'message': 'Deal created successfully', 'data': serializer.data}, status=status.HTTP_201_CREATED)
            return Response({'error': serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    @extend_schema(request=best_dealsSerializer)
    def put(self, request, pk):
        try:
            deal = best_deals.objects.get(pk=pk)
            serializer = best_dealsSerializer(deal, data=request.data, partial=True)
            if serializer.is_valid():
                serializer.save()
                return Response({'message': 'Deal updated successfully', 'data': serializer.data})
            return Response({'error': serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except best_deals.DoesNotExist:
            return Response({'error': 'Deal not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def delete(self, request, pk):
        try:
            deal = best_deals.objects.get(pk=pk)
            deal.delete()
            return Response({'message': 'Deal deleted successfully'})
        except best_deals.DoesNotExist:
            return Response({'error': 'Deal not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@method_decorator(csrf_exempt, name='dispatch')
class ConsultantReqView(APIView):

    def get(self, request, pk=None):
        try:
            if pk:
                request = consultant_req.objects.get(pk=pk)
                serializer = consultant_reqSerializer(request)
                return Response(serializer.data)
            else:
                requests = consultant_req.objects.all()
                serializer = consultant_reqSerializer(requests, many=True)
                return Response(serializer.data)
        except consultant_req.DoesNotExist:
            return Response({'error': 'Request not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    @extend_schema(request=consultant_reqSerializer)
    def post(self, request):       
        try:
            serializer = consultant_reqSerializer(data=request.data)
            if serializer.is_valid():
                serializer.save()
                return Response({'message': 'Request created successfully', 'data': serializer.data}, status=status.HTTP_201_CREATED)
            return Response({'error': serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    @extend_schema(request=consultant_reqSerializer)
    def put(self, request, pk):
        try:
            req = consultant_req.objects.get(pk=pk)
            serializer = consultant_reqSerializer(req, data=request.data, partial=True)
            if serializer.is_valid():
                serializer.save()
                return Response({'message': 'Request updated successfully', 'data': serializer.data})
            return Response({'error': serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except consultant_req.DoesNotExist:
            return Response({'error': 'Request not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def delete(self, request, pk):
        try:
            req = consultant_req.objects.get(pk=pk)
            req.delete()
            return Response({'message': 'Request deleted successfully'})
        except consultant_req.DoesNotExist:
            return Response({'error': 'Request not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@method_decorator(csrf_exempt, name='dispatch')
class GetUserVendor(APIView):
    def get(self, request, user_id):
        try:
            vendor = Vendors.objects.filter(user_id=user_id).first()
            if vendor:
                serializer = VendorSerializer(vendor)
                return Response(serializer.data)
            else:
                return Response({}, status=status.HTTP_200_OK)
        except Exception as e:
            return Response({'error': f'An error occurred: {str(e)}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@method_decorator(csrf_exempt, name='dispatch')
class GetUserCart(APIView):
    def get(self, request, user_id):
        try:
            cart = user_cart.objects.filter(user_id=user_id)
            serializer = user_cartSerializer(cart, many=True)
            return Response(serializer.data, status=status.HTTP_200_OK)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@method_decorator(csrf_exempt, name='dispatch')
class GetUserDeals(APIView):
    def get(self, request, user_id):
        try:
            deal = best_deals.objects.filter(user_id=user_id)
            serializer = best_dealsSerializer(deal, many=True)
            return Response(serializer.data, status=status.HTTP_200_OK)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@method_decorator(csrf_exempt, name='dispatch')
class GetConsultantReq(APIView):
    def get(self, request, user_id):
        try:
            reqs = consultant_req.objects.filter(user_id=user_id)
            serializer = consultant_reqSerializer(reqs, many=True)
            return Response(serializer.data, status=status.HTTP_200_OK)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@method_decorator(csrf_exempt, name='dispatch')
class ChatMessageView(APIView):

    def get(self, request, pk=None):
        try:
            if pk:
                message = ChatMessage.objects.get(pk=pk)
                serializer = ChatMessageSerializer(message)
                return Response(serializer.data)
            else:
                messages = ChatMessage.objects.all()
                serializer = ChatMessageSerializer(messages, many=True)
                return Response(serializer.data)
        except ChatMessage.DoesNotExist:
            return Response({'error': 'Message not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    @extend_schema(request=ChatMessageSerializer)
    def post(self, request):
        try:
            serializer = ChatMessageSerializer(data=request.data)
            if serializer.is_valid():
                chat_rec = serializer.save()
                property_rec = Property.objects.get(property_id=chat_rec.property_id)
                # create linked notification
                notifications.objects.create(
                    message_sender=chat_rec.user_id,
                    message_receiver=chat_rec.receiver,
                    property_id=chat_rec.property_id,
                    property_type=property_rec.type,
                    notification_type="Chat",
                    message=f"New message came from {chat_rec.user_id.username} on property "
                            f"{chat_rec.property_id.property_name if chat_rec.property_id.property_name else chat_rec.property_id.property_id}",
                    action_from_table="ChatMessage", 
                    action_tbl_id=str(chat_rec.pk)
                )
                return Response({'message': 'Message created successfully', 'data': serializer.data}, status=status.HTTP_201_CREATED)
            return Response({'error': serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    @extend_schema(request=ChatMessageSerializer)
    def put(self, request, pk):
        try:
            message = ChatMessage.objects.get(pk=pk)
            serializer = ChatMessageSerializer(message, data=request.data, partial=True)
            if serializer.is_valid():
                serializer.save()
                return Response({'message': 'Message updated successfully', 'data': serializer.data})
            return Response({'error': serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except ChatMessage.DoesNotExist:
            return Response({'error': 'Message not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def delete(self, request, pk):
        try:
            message = ChatMessage.objects.get(pk=pk)
            message.delete()
            return Response({'message': 'Message deleted successfully'})
        except ChatMessage.DoesNotExist:
            return Response({'error': 'Message not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)




@method_decorator(csrf_exempt, name='dispatch')
class Enquiry_FormView(APIView):

    def get(self, request, pk=None):
        try:
            if pk:
                message = Enquiry_Form.objects.get(pk=pk)
                serializer = Enquiry_FormSerializer(message)
                return Response(serializer.data)
            else:
                messages = Enquiry_Form.objects.all()
                serializer = Enquiry_FormSerializer(messages, many=True)
                return Response(serializer.data)
        except Enquiry_Form.DoesNotExist:
            return Response({'error': 'Enquiry not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    @extend_schema(request=Enquiry_FormSerializer)
    def post(self, request):
        try:
            serializer = Enquiry_FormSerializer(data=request.data)
            if serializer.is_valid():
                serializer.save()
                return Response({'message': 'Enquiry created successfully', 'data': serializer.data}, status=status.HTTP_201_CREATED)
            return Response({'error': serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    @extend_schema(request=Enquiry_FormSerializer)
    def put(self, request, pk):
        try:
            message = Enquiry_Form.objects.get(pk=pk)
            serializer = Enquiry_FormSerializer(message, data=request.data, partial=True)
            if serializer.is_valid():
                serializer.save()
                return Response({'message': 'Enquiry updated successfully', 'data': serializer.data})
            return Response({'error': serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except Enquiry_Form.DoesNotExist:
            return Response({'error': 'Enquiry not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def delete(self, request, pk):
        try:
            message = Enquiry_Form.objects.get(pk=pk)
            message.delete()
            return Response({'message': 'Enquiry deleted successfully'})
        except Enquiry_Form.DoesNotExist:
            return Response({'error': 'Enquiry not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        




@method_decorator(csrf_exempt, name='dispatch')
class Activity_TblView(APIView):
    def get(self, request, pk=None):
        try:
            if pk:
                act = activity_tbl.objects.get(pk=pk)
                serializer = activity_tblSerializer(act)
            else:
                act = activity_tbl.objects.all()
                serializer = activity_tblSerializer(act, many=True)
            return Response(serializer.data)
        except activity_tbl.DoesNotExist:
            return Response({'error': 'Activity not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        
    @extend_schema(request=activity_tblSerializer)
    def post(self, request):
        try:
            user_id = request.data.get('user_id')
            property_id = request.data.get('property_id')
            activity_type = request.data.get('activity_type')

            # ✅ Fetch username
            try:
                user = User.objects.get(pk=user_id)
                username = user.username
            except User.DoesNotExist:
                return Response({'error': 'User not found'}, status=status.HTTP_404_NOT_FOUND)

            # ✅ Fetch property
            try:
                prop = Property.objects.get(pk=property_id)
                property_name = prop.property_name or f"Property #{prop.property_id}"
            except Property.DoesNotExist:
                return Response({'error': 'Property not found'}, status=status.HTTP_404_NOT_FOUND)

            # ✅ Prepare notification message
            if activity_type == 'Call':
                message = f"{username} has called you regarding your property {property_name}"
            elif activity_type == 'Liked':
                message = f"{username} liked your property {property_name}"
            elif activity_type == 'Location':
                message = f"{username} checked location for property {property_name}"
            elif activity_type == 'Enquiry':
                message = f"{username} enquired about your property {property_name}"
            elif activity_type == 'Description':
                message = f"{username} viewed description of your property {property_name}"
            else:
                message = f"{username} interacted with your property {property_name}"

            # ✅ Check if activity already exists
            try:
                act = activity_tbl.objects.get(
                    user_id=user_id, property_id=property_id, activity_type=activity_type
                )
            except activity_tbl.DoesNotExist:
                act = None

            if act:
                act.updated_at = timezone.now()
                act.save()

                act_data = activity_tblSerializer(act).data
                return Response({'message': 'Activity updated', 'data': act_data}, status=status.HTTP_200_OK)

            # ✅ Create new activity
            serializer = activity_tblSerializer(data=request.data)
            if serializer.is_valid():
                activity = serializer.save()
                
                # create linked notification
                notifications.objects.create(
                    message_sender=user,
                    message_receiver=prop.user_id,
                    property_id=prop,
                    property_type=prop.type,
                    notification_type=activity_type,
                    message=message,
                    action_from_table="activity_tbl", 
                    action_tbl_id=str(activity.pk)
                )

                return Response({'message': 'Activity created', 'data': serializer.data}, status=status.HTTP_201_CREATED)

            return Response({'error': serializer.errors}, status=status.HTTP_400_BAD_REQUEST)

        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    @extend_schema(request=activity_tblSerializer)
    def put(self, request, pk):
        try:
            act = activity_tbl.objects.get(pk=pk)
            serializer = activity_tblSerializer(act, data=request.data, partial=True)
            if serializer.is_valid():
                serializer.save()
                return Response({'message': 'Activity updated', 'data': serializer.data})
            return Response({'error': serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except activity_tbl.DoesNotExist:
            return Response({'error': 'Activity not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def delete(self, request, pk):
        try:
            act = activity_tbl.objects.get(pk=pk)
            act.delete()
            return Response({'message': 'Activity deleted'})
        except activity_tbl.DoesNotExist:
            return Response({'error': 'Activity not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@method_decorator(csrf_exempt, name='dispatch')     
class UserUnreadNotificationsAPI(APIView):
    def get(self, request, user_id):
        try:            
            unread_notifications_liked = notifications.objects.filter(message_receiver_id=user_id, notification_type="Liked", is_read=False).order_by('-created_at')        
            unread_count_liked = unread_notifications_liked.count()            
            liked_serializer = notificationsSerializer(unread_notifications_liked, many=True)

            unread_notifications_chat = notifications.objects.filter(message_receiver_id=user_id, notification_type="Chat", is_read=False).order_by('-created_at')        
            unread_count_chat= unread_notifications_chat.count()            
            chat_serializer = notificationsSerializer(unread_notifications_chat, many=True)

            unread_notifications_newprop = notifications.objects.filter(message_receiver_id=user_id, notification_type="general", is_read=False).order_by('-created_at')        
            unread_count_newprop = unread_notifications_newprop.count()            
            newprop_serializer = notificationsSerializer(unread_notifications_newprop, many=True)

            return Response({
                "unread_count_liked": unread_count_liked,
                "notifications_liked": liked_serializer.data,

                "unread_count_chat": unread_count_chat,
                "notifications_chat": chat_serializer.data,

                "unread_count_newprop": unread_count_newprop,
                "notifications_newprop": newprop_serializer.data
            }, status=status.HTTP_200_OK)

        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@method_decorator(csrf_exempt, name='dispatch')
class MarkAsReadAPI(APIView):
    @extend_schema(request=markAsReadSerializer)
    def post(self, request):
        try:
            notification_id = request.data.get("notification_id")
            user_id = request.data.get("message_receiver")
            notification_type = request.data.get("notification_type")

            if notification_id:  
                # mark single notification
                unread_notifications = notifications.objects.filter(
                    pk=notification_id
                )
                if not unread_notifications.exists():
                    return Response({"error": "Notification not found"}, status=status.HTTP_404_NOT_FOUND)

                unread_notifications.update(is_read=True)
                serializer = notificationsSerializer(unread_notifications.first())
                return Response({
                    "message": "Notification marked as read",
                    "notification": serializer.data
                }, status=status.HTTP_200_OK)

            elif user_id:  
                # mark all for user
                unread_notifications = notifications.objects.filter(message_receiver_id=user_id, notification_type=notification_type, is_read=False)
                unread_notifications.update(is_read=True)
                serializer = notificationsSerializer(unread_notifications, many=True)
                return Response({
                    "message": "All notifications marked as read",
                    "notifications": serializer.data
                }, status=status.HTTP_200_OK)

            else:
                return Response({"error": "Provide notification_id or user_id"}, status=status.HTTP_400_BAD_REQUEST)

        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)



@method_decorator(csrf_exempt, name='dispatch')
class NotificationActionAPI(APIView):
    def get(self, request, pk):
        try:
            notification_det = notifications.objects.get(pk=pk)

            table_name = notification_det.action_from_table   
            action_id = notification_det.action_tbl_id        
      
            if not table_name or not action_id:
                return Response({"error": "Notification missing action details"}, status=status.HTTP_400_BAD_REQUEST)

            # Dynamically load model class (assumes models are imported in current namespace)
            model_class = globals().get(table_name)
            serializer_class = globals().get(f"{table_name}Serializer")

            if not model_class or not serializer_class:
                return Response({"error": f"Model/Serializer not found for {table_name}"}, status=status.HTTP_400_BAD_REQUEST)            

            obj = model_class.objects.get(pk=action_id)
            serializer = serializer_class(obj)

            return Response(serializer.data, status=status.HTTP_200_OK)

        except notifications.DoesNotExist:
            return Response({"error": "Notification not found"}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@method_decorator(csrf_exempt, name='dispatch')
class ActivitiesByPropertyOwner(APIView):
    def get(self, request, user_id):
        activities = activity_tbl.objects.filter(property_by_id=user_id)
        serializer = activity_tblSerializer(activities, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)
    


    
    
def update_user_features(user_id, data, user_type):
    boolean_fields = [
        'buyer_no_unlimited',
        'no_of_properties_unlimited',
        'no_of_liked_data_unlimited',
        'matching_enquiry_unlimited'
    ]

    bigint_fields = [
        'buyer_no',
        'no_of_properties',
        'no_of_liked_data',
        'matching_enquiry'
    ]

    try:
        user_instance = User.objects.get(user_id=user_id)
    except User.DoesNotExist:
        return  # Or handle it accordingly
        

    features, _ = UserFeatures.objects.get_or_create(user_id=user_instance, user_type = user_type)

    for field in boolean_fields:
        new_value = data.get(field)
        if new_value is not None and str(new_value).lower() == 'true' and not getattr(features, field):
            setattr(features, field, True)

    for field in bigint_fields:
        new_value = data.get(field)
        if new_value:
            current_value = getattr(features, field) or 0
            setattr(features, field, current_value + int(new_value))

    features.save()


@method_decorator(csrf_exempt, name='dispatch')
class Sub_UserFreePlansView(APIView):
    @extend_schema(request=sub_userSerializer)
    def post(self, request):
        try:
            data = request.data.copy()
            user_id = data.get('user_id')
            plan_name = data.get('plan_name')  # Should be 'Free'            

            if plan_name != 'Free':
                return Response({'error': 'Only Free plan subscription is allowed in this view.'}, status=400)

            try:
                user_data = User.objects.get(user_id=user_id)
            except User.DoesNotExist:
                return Response({'error': 'User not found'}, status=404)

            # Get all Free plans
            free_plans = subAdminplans.objects.filter(plan_name='Free')
            created_plans = []

            now = timezone.now()
            

            for plan in free_plans:
                user_type = plan.user_type
                
                if sub_user.objects.filter(user_id=user_id, user_type=user_type, plan_name='Free', status=True).exists():
                    continue
            
                expired_date = now + relativedelta(days=int(plan.trial_days)) + timedelta(hours=5, minutes=30)

                sub_user_data = {
                    'user_id': user_id,
                    'plan_name': 'Free',
                    'user_type': user_type,
                    'charges': plan.charges,
                    'reward_amount': 0,
                    'pay_amount': 0,
                    'buyer_no_unlimited': plan.buyer_no_unlimited,
                    'buyer_no': plan.buyer_no,
                    'no_of_properties_unlimited': plan.no_of_properties_unlimited,
                    'no_of_liked_data_unlimited': plan.no_of_liked_data_unlimited,
                    'matching_enquiry_unlimited': plan.matching_enquiry_unlimited,
                    'no_of_properties': plan.no_of_properties,
                    'no_of_liked_data': plan.no_of_liked_data,
                    'matching_enquiry': plan.matching_enquiry,
                    'expired_date': expired_date,
                    'sub_type': 'New',
                    'status': True
                }

                serializer = sub_userSerializer(data=sub_user_data)
                if serializer.is_valid():
                    serializer.save()
                    update_user_features(user_id, sub_user_data, user_type)
                    created_plans.append(user_type)
                else:
                    print(f"Validation error for user_type '{user_type}': {serializer.errors}")

            if created_plans:
                return Response({'message': f"Free plans created for: {created_plans}"}, status=201)
            else:
                return Response({'message': 'No new free plans created. Possibly already subscribed.'}, status=200)

        except Exception as e:
            return Response({'error': str(e)}, status=500)
        

@method_decorator(csrf_exempt, name='dispatch')
class Sub_UserPlanByIdView(APIView):
    @extend_schema(request=sub_userSerializer)
    def post(self, request):
        try:
            data = request.data.copy()
            user_id = data.get('user_id')
            plan_id = data.get('plan_id')

            if not user_id or not plan_id:
                return Response({'error': 'user_id and plan_id are required'}, status=status.HTTP_400_BAD_REQUEST)

            try:
                user_data = User.objects.get(user_id=user_id)
            except User.DoesNotExist:
                return Response({'error': 'User not found'}, status=status.HTTP_404_NOT_FOUND)

            # Get the specific plan by plan_id
            try:
                plan = subAdminplans.objects.get(plan_id=plan_id)
            except subAdminplans.DoesNotExist:
                return Response({'error': 'Plan not found'}, status=status.HTTP_404_NOT_FOUND)

            user_type = plan.user_type
            trial_days = plan.trial_days

            # Check if user already has this plan active
            if sub_user.objects.filter(user_id=user_id, user_type=user_type, plan_name=plan.plan_name, status=True).exists():
                return Response({'message': 'User already has this plan'}, status=status.HTTP_400_BAD_REQUEST)

            now = timezone.now()
            expired_date = now + relativedelta(days=int(trial_days)) + timedelta(hours=5, minutes=30)

            sub_user_data = {
                'user_id': user_id,
                'plan_name': plan.plan_name,
                'user_type': user_type,
                'charges': plan.charges,
                'reward_amount': 0,
                'pay_amount': 0,
                'buyer_no_unlimited': plan.buyer_no_unlimited,
                'buyer_no': plan.buyer_no,
                'no_of_properties_unlimited': plan.no_of_properties_unlimited,
                'no_of_liked_data_unlimited': plan.no_of_liked_data_unlimited,
                'matching_enquiry_unlimited': plan.matching_enquiry_unlimited,
                'no_of_properties': plan.no_of_properties,
                'no_of_liked_data': plan.no_of_liked_data,
                'matching_enquiry': plan.matching_enquiry,
                'expired_date': expired_date,
                'sub_type': 'New',
                'status': True
            }

            serializer = sub_userSerializer(data=sub_user_data)
            if serializer.is_valid():
                serializer.save()
                update_user_features(user_id, sub_user_data, user_type)
                return Response({'message': f"Plan subscribed successfully for user_type: {user_type}"}, status=status.HTTP_201_CREATED)
            else:
                return Response({'error': serializer.errors}, status=status.HTTP_400_BAD_REQUEST)

        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@method_decorator(csrf_exempt, name='dispatch')
class Sub_UserView(APIView):    
    def get(self, request, pk=None):
        try:
            if pk:
                sub = sub_user.objects.get(pk=pk)
                serializer = sub_userSerializer(sub)
            else:
                sub = sub_user.objects.all()
                serializer = sub_userSerializer(sub, many=True)
            return Response(serializer.data)
        except sub_user.DoesNotExist:
            return Response({'error': 'Sub User not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        
    @extend_schema(request=sub_userSerializer)
    def post(self, request):
        try:

            data = request.data.copy() 
            
            user_id = data.get('user_id')          
            plan_name = data.get('plan_name')            
            user_type = data.get('user_type')       
            reward_amount = data.get('reward_amount')   
            trial_days = data.get('trial_days')

            try: 
                user_data = User.objects.filter(user_id=user_id).first()  # Use filter().first() to avoid exceptions
            except:
                user_data = None
            
            if user_data is None:
                return Response({'error': 'User not found'}, status=status.HTTP_404_NOT_FOUND)       
            
            try:
                current_sub = sub_user.objects.get(user_id=user_id, user_type=user_type, status=True).exclude(plan_name='Free')
            except:
                current_sub = None
            
            if current_sub:         
                return Response({'error': 'You have already subscribed plan.'}, status=status.HTTP_400_BAD_REQUEST)


            try:
                current_free_sub = sub_user.objects.get(user_id=user_id, user_type=user_type, status=True, plan_name='Free')
            except:
                current_free_sub = None
            
            if current_free_sub:      
                current_free_sub.status = False                
                current_free_sub.save()
                features = UserFeatures.objects.filter(user_id=user_id, user_type=current_free_sub.user_type)                
                features.delete()


            now = timezone.now()

            if plan_name not in ['Free', 'Lifetime']:
                no_of_mnts = int(plan_name.split()[0])
                expired_date = now + relativedelta(months=no_of_mnts) + timedelta(hours=5, minutes=30)            
                data['expired_date'] = expired_date
            elif plan_name == 'Free':
                if trial_days:
                    expired_date = now + relativedelta(days=trial_days) + timedelta(hours=5, minutes=30)
                    data['expired_date'] = expired_date
                else:
                    expired_date= None
            elif plan_name == 'Lifetime':
                expired_date= None



            data['sub_type'] = 'New'                             
            try:
                referred_user = User.objects.get(user_id=user_data.referred_by)            
                ref_sub_user = sub_user.objects.filter(user_id=referred_user.user_id, status=True)
            except:
                referred_user = None
                ref_sub_user = None            

            try:
                prev_plan = sub_user.objects.filter(user_id=user_id).exclude(plan_name='Free')
            except:
                prev_plan = None
            
            if plan_name != 'Free':
                if ref_sub_user and not prev_plan:                                      
                    for i in ref_sub_user:
                        if i.user_type == user_type: 
                            if user_type == "Buyer":
                                referred_user.no_of_referred_for_buyer = referred_user.no_of_referred_for_buyer + 1
                                referred_user.save()
                            elif user_type == "Tenant":
                                referred_user.no_of_referred_for_tenant = referred_user.no_of_referred_for_tenant + 1
                                referred_user.save()
                            elif user_type == "Individual Owner/Builder":
                                referred_user.no_of_referred_for_owner = referred_user.no_of_referred_for_owner + 1
                                referred_user.save()
                            elif user_type == "Landlord":
                                referred_user.no_of_referred_for_landlord = referred_user.no_of_referred_for_landlord + 1
                                referred_user.save()
                            elif user_type == "Agent":
                                referred_user.no_of_referred_for_agent = referred_user.no_of_referred_for_agent + 1
                                referred_user.save()   
                        
            
            if reward_amount:
                user_data.total_points_earned = int(user_data.total_points_earned) + int(reward_amount)
                remaining_credits = int(user_data.credit_points)- int(reward_amount)
                if remaining_credits > 0:
                    user_data.credit_points = remaining_credits
                else:
                    user_data.credit_points = 0
                        
            serializer = sub_userSerializer(data=data)
            if serializer.is_valid():

                serializer.save()
                
                update_user_features(user_id, data, user_type)
                                
                return Response({'message': 'Sub User created', 'data': serializer.data}, status=status.HTTP_201_CREATED)

            return Response({'error': serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    @extend_schema(request=sub_userSerializer)
    def put(self, request, pk):
        try:
            sub = sub_user.objects.get(pk=pk)
            serializer = sub_userSerializer(sub, data=request.data, partial=True)
            if serializer.is_valid():
                serializer.save()
                return Response({'message': 'Sub User updated', 'data': serializer.data})
            return Response({'error': serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except sub_user.DoesNotExist:
            return Response({'error': 'Sub User not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def delete(self, request, pk):
        try:
            sub = sub_user.objects.get(pk=pk)
            sub.delete()
            return Response({'message': 'Sub User deleted'})
        except sub_user.DoesNotExist:
            return Response({'error': 'Sub User not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@method_decorator(csrf_exempt, name='dispatch')
class AddOnUserAPI(APIView):    
    @extend_schema(request=UserAddOnSerializer)
    def post(self, request):
        try:     

            data = request.data.copy()             
            user_id = data.get('user_id')
            user_type = data.get('user_type')      

            try:
                current_sub = sub_user.objects.get(user_id=user_id, user_type=user_type, status=True)
            except:
                current_sub = None
            
            if current_sub is None:         
                return Response({'error': 'You have No subscribed plan.'}, status=status.HTTP_400_BAD_REQUEST)

            data['extend_to'] = current_sub.sub_id
      
            serializer = UserAddOnSerializer(data=data)
            if serializer.is_valid():
                serializer.save()
                
                update_user_features(user_id, data, user_type)

                return Response({'message': 'Add on plan created', 'data': serializer.data}, status=status.HTTP_201_CREATED)
            return Response({'error': serializer.errors}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@method_decorator(csrf_exempt, name='dispatch')
class DeactivateSubUser(APIView):   
    @extend_schema(request=sub_userSerializer)
    def put(self, request, sub_id):
        sub = get_object_or_404(sub_user, pk=sub_id)

        # Force-set status to False
        serializer = sub_userSerializer(sub, data={'status': False}, partial=True)
        serializer.is_valid(raise_exception=True)
        serializer.save()
        
        return Response({'message': f'sub_user {sub_id} deactivated', 'data': serializer.data})





@method_decorator(csrf_exempt, name='dispatch')
class GetUserSubPlan(APIView):
    def get(self, request, user_id):
        try:
            user_plan = sub_user.objects.filter(user_id=user_id, status=True)
            serializer = sub_userSerializer(user_plan, many=True)
            return Response(serializer.data, status=status.HTTP_200_OK)
        except Exception as e:
            return Response({"status": False, "message": str(e)}, status=status.HTTP_400_BAD_REQUEST)

@method_decorator(csrf_exempt, name='dispatch')
class GetUserAddon(APIView):
    def get(self, request, user_id):
        try:
            addOn_plan = UserAddOn.objects.filter(user_id=user_id)
            serializer = UserAddOnSerializer(addOn_plan, many=True)
            return Response(serializer.data, status=status.HTTP_200_OK)
        except Exception as e:
            return Response({"status": False, "message": str(e)}, status=status.HTTP_400_BAD_REQUEST)

@method_decorator(csrf_exempt, name='dispatch')
class GetUserPlan(APIView):
    def get(self, request, user_id):
        try:                                    
            plan_feature = UserFeatures.objects.filter(user_id=user_id)
            serializer = UserFeaturesSerializer(plan_feature, many=True)            
            return Response(serializer.data, status=status.HTTP_200_OK)        
        except Exception as e:
            return Response({"status": False, "message": str(e)}, status=status.HTTP_400_BAD_REQUEST)


@method_decorator(csrf_exempt, name='dispatch')
class DeactivateFreePlansAPIView(APIView):
    @extend_schema(request=DeactivateFreePlansSerializer)
    def post(self, request):
        try:
            serializer = DeactivateFreePlansSerializer(data=request.data)
            if not serializer.is_valid():
                return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
            
            user_ids = serializer.validated_data['user_ids']
            if not user_ids:
                return Response({'error': 'user_ids must be a non-empty list'}, status=status.HTTP_400_BAD_REQUEST)

            deactivated_plans = []
            for user_id in user_ids:
                # Get all active free plans for this user
                free_plans = sub_user.objects.filter(user_id=user_id, plan_name='Free', status=True)
                if free_plans.exists():
                    user_types = list(free_plans.values_list('user_type', flat=True))
                    # Deactivate the plans
                    free_plans.update(status=False)
                    # Delete related UserFeatures
                    UserFeatures.objects.filter(user_id=user_id, user_type__in=user_types).delete()
                    deactivated_plans.append({'user_id': user_id, 'user_types': user_types})
                else:
                    deactivated_plans.append({'user_id': user_id, 'user_types': []})

            return Response({
                'message': 'Free plans deactivated and features removed for specified users',
                'data': deactivated_plans
            }, status=status.HTTP_200_OK)

        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@method_decorator(csrf_exempt, name='dispatch')
class UpdateFreePlanTrialDaysAPIView(APIView):
    @extend_schema(request=UpdateFreePlanTrialDaysSerializer)
    def put(self, request):
        try:
            serializer = UpdateFreePlanTrialDaysSerializer(data=request.data)
            if not serializer.is_valid():
                return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

            trial_days = serializer.validated_data['trial_days']
            plan_ids = serializer.validated_data['plan_ids']
            now = timezone.now()
            expired_date = now + relativedelta(days=trial_days) + timedelta(hours=5, minutes=30)

            # Filter plans by IDs and ensure they are Free plans
            plans_to_update = subAdminplans.objects.filter(plan_id__in=plan_ids, plan_name='Free')
            if not plans_to_update.exists():
                return Response({'error': 'No valid Free plans found for the provided IDs'}, status=status.HTTP_400_BAD_REQUEST)

            updated_plan_count = plans_to_update.update(trial_days=trial_days)

            # For each updated plan, update corresponding sub_user records
            updated_sub_user_ids = []
            for plan in plans_to_update:
                free_sub_users = sub_user.objects.filter(plan_name='Free', user_type=plan.user_type, status=True)
                for subscription in free_sub_users:
                    subscription.expired_date = expired_date
                    subscription.save(update_fields=['expired_date', 'updated_at'])
                    updated_sub_user_ids.append(subscription.sub_id)

            return Response({
                'message': 'Specified Free plans and their active free subscriptions updated',
                'updated_free_plans': updated_plan_count,
                'updated_sub_users': updated_sub_user_ids,
                'new_expired_date': expired_date
            }, status=status.HTTP_200_OK)

        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@method_decorator(csrf_exempt, name='dispatch')
class CheckUserHasFreePlanView(APIView):
    def get(self, request, user_id):
        try:
            user = User.objects.get(user_id=user_id)
        except User.DoesNotExist:
            return Response({'has_free_plan': False, 'message': 'User not found'}, status=status.HTTP_404_NOT_FOUND)

        free_plans = subAdminplans.objects.filter(plan_name='Free', status=True)
        if not free_plans.exists():
            return Response({'has_free_plan': False, 'message': 'No Free plans are defined in subAdminplans'}, status=status.HTTP_200_OK)

        # If each Free plan (by user_type) exists in sub_user for this user, then true.
        missing_user_types = []
        for plan in free_plans:
            if not sub_user.objects.filter(user_id=user, user_type=plan.user_type, plan_name='Free', status=True).exists():
                missing_user_types.append(plan.user_type)

        has_free_plan = len(missing_user_types) == 0
        return Response(
            {
                'has_free_plan': has_free_plan,
                'missing_free_user_types': missing_user_types,
                'required_free_count': free_plans.count(),
            },
            status=status.HTTP_200_OK,
        )

    





@method_decorator(csrf_exempt, name='dispatch')
class UserReward(APIView):
    @extend_schema(request=sub_userSerializer)
    def post(self, request):
        try:
            
            data = request.data.copy()  
            user_id = data.get('user_id')    
            reward_type = data.get('reward_type')   
            sub_id = data.get('sub_id', None) 
            user_type = data.get('user_type')   
              

            user_data = get_object_or_404(User, pk=user_id)            


            if reward_type == 'Credits':
                    credit_points = data.get('credit_points')  
                    user_data.credit_points = int(user_data.credit_points) + int(credit_points)

                    if user_type == "Buyer":                        
                        user_data.total_referred = user_data.total_referred + user_data.no_of_referred_for_buyer
                        user_data.no_of_referred_for_buyer = 0
                        user_data.save()
                    elif user_type == "Tenant":                        
                        user_data.total_referred = user_data.total_referred + user_data.no_of_referred_for_tenant
                        user_data.no_of_referred_for_tenant = 0
                        user_data.save()
                    elif user_type == "Individual Owner/Builder":                    
                        user_data.total_referred = user_data.total_referred + user_data.no_of_referred_for_owner
                        user_data.no_of_referred_for_owner = 0
                        user_data.save()
                    elif user_type == "Landlord":                        
                        user_data.total_referred = user_data.total_referred + user_data.no_of_referred_for_landlord
                        user_data.no_of_referred_for_landlord = 0
                        user_data.save()
                    elif user_type == "Agent":                        
                        user_data.total_referred = user_data.total_referred + user_data.no_of_referred_for_agent
                        user_data.no_of_referred_for_agent = 0
                        user_data.save()                      

                    return Response({'message': 'You got credits points, use them to get plan.'}, status=status.HTTP_200_OK)                
            else:
                
                try:
                    previous_sub = sub_user.objects.get(user_id=user_id, sub_id=sub_id, user_type=user_type, status=True)
                except:
                    previous_sub = None
                
                if previous_sub is None:
                        return Response({'error': 'No active plan found for the user to upgrade.'}, status=status.HTTP_404_NOT_FOUND)   
                                                                
                try:
                    no_of_mnts = int(previous_sub.plan_name.split()[0])
                except (ValueError, AttributeError, IndexError):
                    return Response({'error': 'Invalid plan_name format for month extraction.'}, status=status.HTTP_400_BAD_REQUEST)
                
                new_expired_date = previous_sub.expired_date + relativedelta(months=no_of_mnts) if previous_sub.expired_date else None

                # Step 4: Create new subscription record
                new_sub = sub_user.objects.create(
                    user_id=previous_sub.user_id,
                    plan_name=previous_sub.plan_name,
                    user_type=previous_sub.user_type,
                    charges=previous_sub.charges,
                    reward_amount=0,
                    pay_amount=0,
                    buyer_no_unlimited =previous_sub.buyer_no_unlimited,
                    buyer_no=previous_sub.buyer_no,
                    no_of_properties_unlimited=previous_sub.no_of_properties_unlimited,
                    no_of_liked_data_unlimited=previous_sub.no_of_liked_data_unlimited,
                    matching_enquiry_unlimited=previous_sub.matching_enquiry_unlimited,
                    no_of_properties=previous_sub.no_of_properties,
                    no_of_liked_data=previous_sub.no_of_liked_data,
                    matching_enquiry=previous_sub.matching_enquiry,                    
                    expired_date=new_expired_date,
                    status=True,
                    upgraded_from=previous_sub.sub_id,
                    sub_type='Referral',
                )

                # Step 5: Update old subscription
                previous_sub.status = False
                previous_sub.upgraded_to = new_sub.sub_id
                previous_sub.save()

                user_data.upgrade_plan = user_data.upgrade_plan + 1

                if user_type == "Buyer":                        
                    user_data.total_referred = user_data.total_referred + user_data.no_of_referred_for_buyer
                    user_data.no_of_referred_for_buyer = 0
                    user_data.save()
                elif user_type == "Tenant":                        
                    user_data.total_referred = user_data.total_referred + user_data.no_of_referred_for_tenant
                    user_data.no_of_referred_for_tenant = 0
                    user_data.save()
                elif user_type == "Individual Owner/Builder":                    
                    user_data.total_referred = user_data.total_referred + user_data.no_of_referred_for_owner
                    user_data.no_of_referred_for_owner = 0
                    user_data.save()
                elif user_type == "Landlord":                        
                    user_data.total_referred = user_data.total_referred + user_data.no_of_referred_for_landlord
                    user_data.no_of_referred_for_landlord = 0
                    user_data.save()
                elif user_type == "Agent":                        
                    user_data.total_referred = user_data.total_referred + user_data.no_of_referred_for_agent
                    user_data.no_of_referred_for_agent = 0
                    user_data.save() 

                user_data.save()


                serializer = sub_userSerializer(new_sub)
                return Response({'message': 'Referral upgrade successful.', 'data': serializer.data}, status=status.HTTP_201_CREATED)
                                       
        except Exception as e:
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)





@method_decorator(csrf_exempt, name='dispatch')
class SendOTPAPIView(APIView):
    @extend_schema(request=sendotpserializer)
    def post(self, request):
        try:
            email = request.data.get('email')
            if not email:
                return Response({"status": False, "message": "Email is required."}, status=status.HTTP_400_BAD_REQUEST)

            try:
                user = User.objects.get(email=email)
            except User.DoesNotExist:
                return Response({"status": False, "message": "User with this email does not exist."}, status=status.HTTP_404_NOT_FOUND)

            otp = str(random.randint(1000, 9999))            

            # Save OTP
            UserOTP.objects.create(email=email, otp=otp)

            # Send OTP via email            
            send_mail(
                subject='Your OTP for Password Reset',
                message=f'Hello {user.first_name},\n\nYour OTP is: {otp}',
                from_email=settings.EMAIL_HOST_USER,   
                recipient_list=[email],
                fail_silently=False,
            )


            return Response({"status": True, "message": "OTP sent successfully."}, status=status.HTTP_200_OK)

        except Exception as e:
            return Response({"status": False, "message": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@method_decorator(csrf_exempt, name='dispatch')
class VerifyOTPAPIView(APIView):
    @extend_schema(request=verifyotpserializer)
    def post(self, request):
        try:
            email = request.data.get('email')
            otp = request.data.get('otp')

            if not email or not otp:
                return Response({"status": False, "message": "Email and OTP are required."}, status=status.HTTP_400_BAD_REQUEST)

            try:
                otp_rec = UserOTP.objects.filter(email=email).last()
                if otp_rec and otp_rec.otp == otp:
                    return Response({"status": True, "message": "OTP verified. You can now reset your password."}, status=status.HTTP_200_OK)
                else:
                    return Response({"status": False, "message": "Invalid OTP or email."}, status=status.HTTP_400_BAD_REQUEST)                
            except UserOTP.DoesNotExist:
                return Response({"status": False, "message": "Invalid OTP or email."}, status=status.HTTP_400_BAD_REQUEST)

        except Exception as e:
            return Response({"status": False, "message": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@method_decorator(csrf_exempt, name='dispatch')
class ResetPasswordAPIView(APIView):

    @extend_schema(request=resetpasswordserializer)
    def post(self, request):
        try:
            email = request.data.get('email')
            otp = request.data.get('otp')
            password = request.data.get('password')

            if not email or not otp or not password:
                return Response({"status": False, "message": "Email, OTP, and new password are required."}, status=status.HTTP_400_BAD_REQUEST)

            try:
                otp_record = UserOTP.objects.get(email=email, otp=otp)
            except UserOTP.DoesNotExist:
                return Response({"status": False, "message": "Invalid OTP."}, status=status.HTTP_400_BAD_REQUEST)

            try:
                user = User.objects.get(email=email)
                user.password = make_password(password)
                user.save()

                # Delete OTP after successful reset
                otp_record.delete()

                return Response({"status": True, "message": "Password reset successful."}, status=status.HTTP_200_OK)

            except User.DoesNotExist:
                return Response({"status": False, "message": "User not found."}, status=status.HTTP_404_NOT_FOUND)

        except Exception as e:
            return Response({"status": False, "message": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)







@method_decorator(csrf_exempt, name='dispatch')
class RazorpayWebhookView(APIView):
    """
    Handle Razorpay webhook events
    """
    def verify_webhook_signature(self, request):
        webhook_secret = settings.RAZORPAY_WEBHOOK_SECRET
        received_signature = request.headers.get('X-Razorpay-Signature', '')
        
        # Get the raw request body as bytes
        request_body = request.body
        
        # Verify the signature
        import hmac
        import hashlib
        
        key = bytes(webhook_secret, 'utf-8')
        signature = hmac.new(key, request_body, hashlib.sha256).hexdigest()
        
        return hmac.compare_digest(signature, received_signature)

    
    

    @extend_schema(request=TransactionSerializer)
    def post(self, request):
        try:
            # Verify webhook signature
            if not self.verify_webhook_signature(request):
                return Response({'error': 'Invalid signature'}, status=status.HTTP_401_UNAUTHORIZED)
            
            event = request.data
            event_type = event.get('event')
            
            # Extract relevant data from the webhook payload
            payment_entity = event.get('payload', {}).get('payment', {}).get('entity', {})
            subscription_entity = event.get('payload', {}).get('subscription', {}).get('entity', {})
            
            # Common fields
            payment_id = payment_entity.get('id')
            order_id = payment_entity.get('order_id')
            amount = payment_entity.get('amount')
            if amount:
                amount = float(amount) / 100  # Convert to currency unit
            
            # Get subscription details if available
            subscription_id = subscription_entity.get('id') or payment_entity.get('subscription_id')
            plan_id = subscription_entity.get('plan_id')
            
            # Get customer details
            customer_id = payment_entity.get('customer_id') or subscription_entity.get('customer_id')
            
            # Get user instance by razor_user_id
            try:
                user = User.objects.get(razor_user_id=customer_id)                
            except User.DoesNotExist:
                # Handle case where user is not found
                print(f"User with razor_user_id {customer_id} not found")             

            # Create or update transaction
            transaction_data = {
                'user_id': user,  # Assuming customer_id maps to user_id in your system
                'customer_id': customer_id,
                'plan_name': '',  # Will be updated if plan details are available
                'order_id': order_id,
                'payment_id': payment_id,
                'plan_id': plan_id,
                'subscription_id': subscription_id,
                'amount': amount,
                'transaction_status': payment_entity.get('status', '').upper(),
                'invoice_id': payment_entity.get('invoice_id')
            }
            
            # If this is a subscription-related event, try to get plan details
            if subscription_id or plan_id:
                try:
                    client = razorpay.Client(auth=(settings.RAZORPAY_KEY_ID, settings.RAZORPAY_KEY_SECRET))
                    
                    # Get plan details
                    if plan_id:
                        plan = client.plan.fetch(plan_id)
                        transaction_data['plan_name'] = plan.get('item', {}).get('name', '')
                    
                    # If we have subscription_id but not plan_id, get it from subscription
                    elif subscription_id and not plan_id:
                        subscription = client.subscription.fetch(subscription_id)
                        plan_id = subscription.get('plan_id')
                        if plan_id:
                            plan = client.plan.fetch(plan_id)
                            transaction_data['plan_name'] = plan.get('item', {}).get('name', '')
                            transaction_data['plan_id'] = plan_id
                except Exception as e:
                    print(f"Error fetching plan/subscription details: {str(e)}")
            
            # Create or update the transaction
            if payment_id:
                transaction, created = Transaction.objects.update_or_create(
                    payment_id=payment_id,
                    defaults=transaction_data
                )
            
            # Handle specific event types
            if event_type == 'payment.captured':                                
                transaction.transaction_status = 'PAYMENT CAPTURED'
                transaction.save()

            elif event_type == 'payment.authorized':
                transaction.transaction_status = 'PAYMENT AUTHORIZED'
                transaction.save()
                
            elif event_type == 'payment.failed':
                transaction.transaction_status = 'PAYMENT FAILED'
                transaction.save()
                
            elif event_type == 'subscription.activated':
                transaction.transaction_status = 'SUBSCRIPTION ACTIVATED'
                transaction.save()
                
            elif event_type == 'subscription.charged':
                transaction.transaction_status = 'SUBSCRIPTION CHARGED'
                transaction.save()
                
            elif event_type == 'subscription.halted':                
                transaction.transaction_status = 'SUBSCRIPTION HALTED'
                transaction.save()
                
            elif event_type == 'subscription.cancelled':
                transaction.transaction_status = 'SUBSCRIPTION CANCELLED'
                transaction.save()
                
            elif event_type == 'subscription.completed':
                transaction.transaction_status = 'SUBSCRIPTION COMPLETED'
                transaction.save()

            return Response({'status': 'success'}, status=status.HTTP_200_OK)
            
        except Exception as e:
            print(f"Error processing webhook: {str(e)}")
            return Response(
                {'error': 'Error processing webhook', 'details': str(e)}, 
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    

@method_decorator(csrf_exempt, name='dispatch')
class ConversationAPIView(APIView):

    @extend_schema(request=markAsReadChartSerializer)
    def post(self, request):

        user_id = request.data.get("user_id")
        receiver_id = request.data.get("receiver_id")
        property_id = request.data.get("property_id")

        if not user_id or not receiver_id or not property_id:
            return Response(
                {"error": "user_id, receiver_id, and property_id required"},
                status=status.HTTP_400_BAD_REQUEST
            )

        # Update unread messages
        ChatMessage.objects.filter(
            user_id=user_id,
            receiver=receiver_id,
            property_id=property_id,
            is_read=False
        ).update(is_read=True)

        return Response({
            "status": True            
        })

from faker import Faker
import math

# Local imports
from .models import *
from .serializers import *

import random
fake = Faker()
from faker import Faker
from django.db import transaction
from django.utils import timezone
import random


@method_decorator(csrf_exempt, name='dispatch')
class GenerateDummyUsersAPI(APIView):    

    def post(self, request):
        try:
            total = int(request.data.get('count', 1000000))
            batch_size = 1000
            user_data = []      

            user_types = ['Buyer', 'Tenant', 'Individual Owner/Builder', 'Landlord', 'Builder', 'Agent', 'Bank Auction']
            states = ['Maharashtra', 'Karnataka', 'Tamil Nadu', 'Gujarat', 'Rajasthan', 'Delhi', 'Uttar Pradesh', 'West Bengal']
            cities = ['Mumbai', 'Bangalore', 'Chennai', 'Ahmedabad', 'Jaipur', 'Delhi', 'Lucknow', 'Kolkata']
              

            for i in range(total):                                
                user_dt = User(
                    username=fake.user_name() + str(random.randint(1000, 9999)),
                    first_name=fake.first_name(),
                    last_name=fake.last_name(),
                    email=fake.email(),
                    mobile_no=fake.phone_number()[:15],                    
                    state=random.choice(states),
                    city=random.choice(cities),
                    lat=str(fake.latitude()),
                    long=str(fake.longitude()),
                    role='User',
                    user_type=random.choice(user_types),
                    tersm_and_condition=True,
                    upgrade_plan=random.randint(0, 5),
                    credit_points=random.randint(0, 10000),
                    total_points_earned=random.randint(0, 50000),
                    no_of_referred_for_buyer=random.randint(0, 50),
                    no_of_referred_for_tenant=random.randint(0, 50),
                    no_of_referred_for_owner=random.randint(0, 50),
                    no_of_referred_for_landlord=random.randint(0, 50),
                    no_of_referred_for_agent=random.randint(0, 50),
                    total_referred=random.randint(0, 200),
                    first_login=timezone.make_aware(fake.date_time_between(start_date='-2y', end_date='now')) if random.random() > 0.3 else None,
                    user_remark=fake.sentence() if random.random() > 0.7 else None
                )

                user_data.append(user_dt)

                # 🚀 Bulk insert
                if len(user_data) >= batch_size:
                    User.objects.bulk_create(user_data)
                    user_data = []

            # Insert remaining
            if user_data:
                User.objects.bulk_create(user_data)

            return Response({
                "status": True,
                "message": f"{total} dummy records inserted successfully.",
                "data": None
            }, status=status.HTTP_201_CREATED)

        except Exception as e:
            return Response({
                "status": False,
                "message": f"Error: {str(e)}",
                "data": None
            }, status=status.HTTP_400_BAD_REQUEST)



@method_decorator(csrf_exempt, name='dispatch')
class GenerateFakeDataAPIView(APIView):
    """
    API View to generate fake users in User table only
    """
    
    def post(self, request):
        try:
            from faker import Faker
            from django.db import transaction
            from django.utils import timezone
            import random
            
            fake = Faker('en_IN')
            
            # Get the number of users to create (default 100000)
            num_users = request.data.get('num_users', 100000)
            
            if num_users > 100000:
                return Response({
                    'error': 'Maximum limit is 100,000 users per request'
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # User types and other choices
            user_types = ['Buyer', 'Tenant', 'Individual Owner/Builder', 'Landlord', 'Builder', 'Agent', 'Bank Auction']
            states = ['Maharashtra', 'Karnataka', 'Tamil Nadu', 'Gujarat', 'Rajasthan', 'Delhi', 'Uttar Pradesh', 'West Bengal']
            cities = ['Mumbai', 'Bangalore', 'Chennai', 'Ahmedabad', 'Jaipur', 'Delhi', 'Lucknow', 'Kolkata']
            
            print(f"Starting generation of {num_users} fake users...")
            
            # Generate users in batches for better performance
            batch_size = 1000
            total_created = 0
            
            with transaction.atomic():
                for batch_start in range(0, num_users, batch_size):
                    batch_end = min(batch_start + batch_size, num_users)
                    batch_users = []
                    
                    for i in range(batch_start, batch_end):
                        # Create fake user
                        user_data = {
                            'username': fake.user_name() + str(random.randint(1000, 9999)),
                            'first_name': fake.first_name(),
                            'last_name': fake.last_name(),
                            'email': fake.email(),
                            'mobile_no': fake.phone_number()[:15],
                            'password': make_password('password123'),
                            'state': random.choice(states),
                            'city': random.choice(cities),
                            'lat': str(fake.latitude()),
                            'long': str(fake.longitude()),
                            'role': 'User',
                            'user_type': random.choice(user_types),
                            'tersm_and_condition': True,
                            'upgrade_plan': random.randint(0, 5),
                            'credit_points': random.randint(0, 10000),
                            'total_points_earned': random.randint(0, 50000),
                            'no_of_referred_for_buyer': random.randint(0, 50),
                            'no_of_referred_for_tenant': random.randint(0, 50),
                            'no_of_referred_for_owner': random.randint(0, 50),
                            'no_of_referred_for_landlord': random.randint(0, 50),
                            'no_of_referred_for_agent': random.randint(0, 50),
                            'total_referred': random.randint(0, 200),
                            'first_login': timezone.make_aware(fake.date_time_between(start_date='-2y', end_date='now')) if random.random() > 0.3 else None,
                            'user_remark': fake.sentence() if random.random() > 0.7 else None
                        }
                        
                        user = User(**user_data)
                        batch_users.append(user)
                    
                    # Bulk create users
                    users = User.objects.bulk_create(batch_users)
                    batch_count = len(users)
                    total_created += batch_count
                    
                    print(f"Created {batch_count} users. Total: {total_created}")
            
            print("User generation completed!")
            
            return Response({
                'message': f'Successfully generated {total_created} fake users',
                'users_created': total_created
            }, status=status.HTTP_201_CREATED)
            
        except ImportError:
            return Response({
                'error': 'Faker library not installed. Please install it using: pip install faker'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            
        except Exception as e:
            return Response({
                'error': f'Error generating fake users: {str(e)}'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)