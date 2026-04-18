from django.urls import path
from .views import *
from .admin_plans_api import GenerateAdminPlansAPI

urlpatterns = [   
    path('construction-categories/', ConstructionCatView.as_view()),
    path('construction-categories/<int:pk>/', ConstructionCatView.as_view()), 
    path('construction-content/', ConstructionContentView.as_view()),
    path('construction-content/<int:pk>/', ConstructionContentView.as_view()),        
    path('packages/', PackagesView.as_view()),
    path('packages/<int:pk>/', PackagesView.as_view()),

    path('material-categories/', MaterialCatView.as_view()),
    path('material-categories/<int:pk>/', MaterialCatView.as_view()), 
    path('material-content/', MaterialContentView.as_view()),
    path('material-content/<int:pk>/', MaterialContentView.as_view()),  

    path('approve-multiple-deals/', ApproveMultipleDealsView.as_view()),

    path('subplans/', SubscriptionAPI.as_view()),
    path('subplans/<int:pk>/', SubscriptionAPI.as_view()),

    path('add-on/', AddOnAPI.as_view()),
    path('add-on/<int:pk>/', AddOnAPI.as_view()),

    path('rewards/', ReferralRewardView.as_view()),         
    path('rewards/<int:pk>/', ReferralRewardView.as_view()),

    path('offers/', OfferView.as_view()),
    path('offers/<int:pk>/', OfferView.as_view()),
    path('offers/claim/', ClaimOfferView.as_view()),
    path('offers/claim-by-url/', ClaimOfferByUrlView.as_view()),
    path('offers/available/<int:user_id>/', AvailableOffersForUserView.as_view()),
    path('assign-free-plan/', AssignFreePlanView.as_view()),
    path('claim-all-free-plans/', ClaimAllFreePlansView.as_view()),
    path('all-users-claim-all-free-plans/', AllUsersClaimAllFreePlansView.as_view()),

    path('get-offer-urls/', getofferurls.as_view()),
    path('get-offer-urls/<int:pk>/', getofferurls.as_view()),
    
    path('generate-admin-plans/', GenerateAdminPlansAPI.as_view()),
]   
