from django.urls import path
from .views import *
from .simple_search_views import *
from .subscription_api import GenerateSubscriptionDataAPI

urlpatterns = [
    
    path('validate-session/', ValidateSessionAPIView.as_view()),

    path('roles/', RoleListCreateAPIView.as_view()),
    path('roles/<int:pk>/', RoleDetailAPIView.as_view()),
    path('permissions/<int:pk>/', RolePermissionDetailAPIView.as_view()),
    path('reg-send-otp/', RegSendOTPAPIView.as_view()),
    path('reg-verify-otp/', RegVerifyOTPAPIView.as_view()),
    path('users/', UserListCreateAPIView.as_view()),
    path('users/<int:pk>/', UserRetrieveUpdateDeleteAPIView.as_view()),
    path('users/by-id/<str:user_id>/', UserGetByIdAPIView.as_view()),
    path('users/type/', UserGetUserTypeAPIView.as_view()),
    path('get-user-by-identifier/<str:identifier>/', UserGetByIdentifierAPIView.as_view()),
    path('change-password/<int:pk>/', UserChangePasswordAPIView.as_view()),
    path('login/', UserLoginAPIView.as_view()),
    path('force-login/', ForceLoginAPIView.as_view()),
    path('logout/', UserLogoutAPIView.as_view()),
    path('vendors/', VendorAPIView.as_view()),
    path('vendors/<int:pk>/', VendorAPIView.as_view()),    
    path('user-cart/', UserCartView.as_view()),
    path('user-cart/<int:pk>/', UserCartView.as_view()),
    path('best-deals/', BestDealsView.as_view()),
    path('best-deals/<int:pk>/', BestDealsView.as_view()),
    path('consultant-req/', ConsultantReqView.as_view()),
    path('consultant-req/<int:pk>/', ConsultantReqView.as_view()),
    
    path('user-vendors/<str:user_id>/', GetUserVendor.as_view()), 
    path('get-user-cart/<str:user_id>/', GetUserCart.as_view()),
    path('get-best-deals/<str:user_id>/', GetUserDeals.as_view()),
    path('get-consultant-req/<str:user_id>/', GetConsultantReq.as_view()),
    path('chat-messages/', ChatMessageView.as_view()),
    path('chat-messages/<int:pk>/', ChatMessageView.as_view()),
    
    path('Enquiry/', Enquiry_FormView.as_view()),
    path('Enquiry/<int:pk>/', Enquiry_FormView.as_view()),

    path('activities/', Activity_TblView.as_view()),
    path('activities/<int:pk>/', Activity_TblView.as_view()),


    path('notifications/unread/<int:user_id>/', UserUnreadNotificationsAPI.as_view()),  
    path('notifications/read/', MarkAsReadAPI.as_view()),
    path("get-notifications-data/<int:pk>/", NotificationActionAPI.as_view()),


    path('subscribe/free/', Sub_UserFreePlansView.as_view()),    
    path('subscribe/free_plan_id/', Sub_UserPlanByIdView.as_view()),    
    
    path('subusers/', Sub_UserView.as_view()),
    path('subusers/<int:pk>/', Sub_UserView.as_view()),

    path('deactivate-free-plans/', DeactivateFreePlansAPIView.as_view()),
    path('update-free-plan-trial-days/', UpdateFreePlanTrialDaysAPIView.as_view()),

    path('user-addon/', AddOnUserAPI.as_view()),
    path('user-addon/<int:pk>/', AddOnUserAPI.as_view()),    
    
    path('activities-property-by/<int:user_id>/', ActivitiesByPropertyOwner.as_view()),
    path('sub-deactivate/<int:sub_id>/', DeactivateSubUser.as_view()),

    path('Get-user-plan/<int:user_id>/', GetUserPlan.as_view()),
    path('Get-addon/<int:user_id>/', GetUserAddon.as_view()),
    path('Get-all-plan/<int:user_id>/', GetUserSubPlan.as_view()),
    path('check-free-plan/<int:user_id>/', CheckUserHasFreePlanView.as_view()),
    
    path('user-reward/', UserReward.as_view()),    

    path('send-otp/', SendOTPAPIView.as_view()),
    path('verify-otp/', VerifyOTPAPIView.as_view()),
    path('reset-password/', ResetPasswordAPIView.as_view()),

    path('api/webhook/razorpay/', RazorpayWebhookView.as_view(), name='razorpay-webhook'),
    
    path("read-conversation/", ConversationAPIView.as_view()),


    path("fake-records/", GenerateFakeDataAPIView.as_view()),

    path("generate-dummy-users/", GenerateDummyUsersAPI.as_view()),
    
    path("generate-subscription-data/", GenerateSubscriptionDataAPI.as_view()),

    # Search API endpoints
    path('api/search/universal/', UniversalSearchAPIView.as_view()),
    path('api/search/suggestions/', SearchSuggestionsAPIView.as_view()),
    path('api/search/health/', SearchHealthAPIView.as_view()),

    
]   


