from rest_framework import serializers
from .models import *
from users.models import *
from django.contrib.auth import get_user_model
from django.utils import timezone
from django.contrib.auth.hashers import make_password


class Property_CatSerializer(serializers.ModelSerializer):
    class Meta:
        model = Property_Cat
        fields = '__all__'


class Property_imagesSerializer(serializers.ModelSerializer):
    class Meta:
        model = Property_images
        fields = '__all__'


class PropertySerializer(serializers.ModelSerializer):
    property_images = Property_imagesSerializer(many=True, read_only=True)
    new_property_images = serializers.ListField(child=serializers.ImageField(), write_only=True, required=False)

    category_name = serializers.CharField(
        source='category_id.category',
        read_only=True
    )

    class Meta:
        model = Property
        fields = '__all__'

    

    def create(self, validated_data):
        new_images = validated_data.pop('new_property_images', [])
        property_instance = Property.objects.create(**validated_data)

        for image in new_images:
            Property_images.objects.create(property=property_instance, image=image)

        return property_instance




class boostpropertyserializer(serializers.Serializer):    
    user_id = serializers.CharField(write_only=True) 



class ResponsePropertyRequestSerializer(serializers.ModelSerializer):    
    class Meta:
        model = ResponsePropertyRequest
        fields = '__all__'


class PropertyRequestLocationsSerializer(serializers.ModelSerializer):
    class Meta:
        model = PropertyRequestLocations
        fields = '__all__'


class NewLocationSerializer(serializers.Serializer):
    # This defines the exact fields Swagger should show for new_locations
    location = serializers.CharField(required=False, allow_blank=True, allow_null=True)
    lat = serializers.CharField(required=False, allow_blank=True, allow_null=True)
    long = serializers.CharField(required=False, allow_blank=True, allow_null=True)


class PropertyRequestSerializer(serializers.ModelSerializer):
    pro_loc = PropertyRequestLocationsSerializer(many=True, read_only=True)

    # Use the typed nested serializer so swagger shows exact keys instead of additionalPropX
    new_locations = NewLocationSerializer(many=True, write_only=True, required=False)

    class Meta:
        model = PropertyRequest
        fields = '__all__'

    def create(self, validated_data):
        new_locations = validated_data.pop('new_locations', [])
        req_obj = PropertyRequest.objects.create(**validated_data)

        # create location rows
        for loc in new_locations:
            # loc is a dict with keys: location, lat, long
            PropertyRequestLocations.objects.create(req_id=req_obj, **loc)

        return req_obj

    def update(self, instance, validated_data):
        new_locations = validated_data.pop('new_locations', [])

        # update main request fields
        instance = super().update(instance, validated_data)

        # add new locations
        for loc in new_locations:
            PropertyRequestLocations.objects.create(req_id=instance, **loc)

        return instance


class BankAuctionPropertyDocsSerializer(serializers.ModelSerializer):    
    class Meta:
        model = BankAuctionPropertyDocs
        fields = '__all__'


class BankAuctionPropertySerializer(serializers.ModelSerializer):

    bank_pro_doc = BankAuctionPropertyDocsSerializer(many=True, read_only=True)

    # For uploading new documents
    new_documents = serializers.ListField(child=serializers.FileField(), write_only=True, required=False)

    class Meta:
        model = BankAuctionProperty
        fields = '__all__'

    def create(self, validated_data):
        new_docs = validated_data.pop('new_documents', [])

        obj = BankAuctionProperty.objects.create(**validated_data)

        for doc in new_docs:
            BankAuctionPropertyDocs.objects.create(
                bankpro_id=obj,
                document=doc
            )

        return obj

    def update(self, instance, validated_data):
        new_docs = validated_data.pop('new_documents', [])

        instance = super().update(instance, validated_data)

        # Add new documents
        for doc in new_docs:
            BankAuctionPropertyDocs.objects.create(
                bankpro_id=instance,
                document=doc
            )

        return instance


