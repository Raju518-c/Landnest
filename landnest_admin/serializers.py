
from rest_framework import serializers
from .models import *
from users.models import *
from django.contrib.auth import get_user_model
from django.utils import timezone
from django.contrib.auth.hashers import make_password



class construction_catSerializer(serializers.ModelSerializer):

    class Meta:
        model = construction_cat
        fields = '__all__'           

class construction_contentSerializer(serializers.ModelSerializer):    
    class Meta:
        model = construction_content
        fields = '__all__'
        

class PackagesSerializer(serializers.ModelSerializer):   

    class Meta:
        model = Packages
        fields = '__all__'


class material_catSerializer(serializers.ModelSerializer):
    class Meta:
        model = material_cat
        fields = '__all__'           

class material_contentSerializer(serializers.ModelSerializer):    
    class Meta:
        model = material_content
        fields = '__all__'
        


class subAdminplansSerializer(serializers.ModelSerializer):
    class Meta:
        model = subAdminplans
        fields = '__all__'



class AddOnPlansSerializer(serializers.ModelSerializer):
    class Meta:
        model = AddOnPlans
        fields = '__all__'


class referral_rewardSerializer(serializers.ModelSerializer):
    class Meta:
        model = referral_reward
        fields = '__all__'


class offersSerializer(serializers.ModelSerializer):
    class Meta:
        model = offers
        fields = '__all__'


class offerurlsSerializer(serializers.ModelSerializer):
    class Meta:
        model = offerurls
        fields = '__all__'

class UserOfferClaimSerializer(serializers.ModelSerializer):
    class Meta:
        model = UserOfferClaim
        fields = '__all__'


class AssignFreePlanViewSerializer(serializers.Serializer):    
    plan_id = serializers.CharField(required=False)


class ClaimAllFreePlansSerializer(serializers.Serializer):
    user_id = serializers.IntegerField(required=True)
