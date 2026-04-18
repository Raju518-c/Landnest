from django.db import models
from django.core.files.storage import default_storage
from django.core.files.base import ContentFile
from django.contrib.auth.hashers import make_password



class CleanFloatField(models.FloatField):
    def from_db_value(self, value, expression, connection):
        return self._clean(value)

    def to_python(self, value):
        value = super().to_python(value)
        return self._clean(value)

    def _clean(self, value):
        if value is None:
            return value
        if isinstance(value, float) and value.is_integer():
            return int(value)
        return value

class construction_cat(models.Model):
    category_choices = [
        ('2D', '2D'),
        ('3D', '3D'),
        ('Elevation', 'Elevation'),
        ('2D-Interiors', '2D-Interiors'),
        ('3D-Interiors', '3D-Interiors')
    ]

    category_id = models.AutoField(primary_key=True)
    user_id = models.ForeignKey('users.User', on_delete=models.CASCADE) #PROTECT
    category = models.CharField(max_length=100, choices=category_choices)
    sub_cat = models.CharField(max_length=100,  null=True, blank=True)  
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now = True)

    def __str__(self):
        return f"{self.category}"
    

class construction_content(models.Model):
    content_id = models.AutoField(primary_key=True)
    category_id = models.ForeignKey('construction_cat', on_delete=models.CASCADE) #PROTECT
    user_id = models.ForeignKey('users.User', on_delete=models.CASCADE) #PROTECT
    content = models.TextField()
    image = models.ImageField(upload_to='media/construction_content/')    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now = True)

    def __str__(self):
        return f"{self.content_id}"

class Packages(models.Model):
    package_id = models.AutoField(primary_key=True)
    user_id = models.ForeignKey('users.User', on_delete=models.CASCADE) #PROTECT
    category = models.CharField(max_length=100)    
    package_cost = CleanFloatField(null=True, blank=True)     #float
    tile_general = CleanFloatField(null=True, blank=True)     #float
    tile_stair = CleanFloatField(null=True, blank=True)     #float
    tile_balcony = CleanFloatField(null=True, blank=True)     #float
    title_bathroom = CleanFloatField(null=True, blank=True)     #float
    tile_parking = CleanFloatField(null=True, blank=True)     #float
    tile_kitchen_countertop = CleanFloatField(null=True, blank=True)     #float
    tile_kitchen_backsplash = CleanFloatField(null=True, blank=True)     #float
    window_ms_grill =  CleanFloatField(null=True, blank=True)     #float
    window_standered = CleanFloatField(null=True, blank=True)     #float
    doors_main = CleanFloatField(null=True, blank=True)     #float
    doors_pooja = CleanFloatField(null=True, blank=True)     #float
    doors_internal = CleanFloatField(null=True, blank=True)     #float
    fabrication_stair_rail = CleanFloatField(null=True, blank=True)     #float
    fabrication_gate = CleanFloatField(null=True, blank=True)     #float
    sanitary_overheadtank = CleanFloatField(null=True, blank=True)     #float
    sanitary_commode = CleanFloatField(null=True, blank=True)     #float
    sanitary_wallmixer = CleanFloatField(null=True, blank=True)     #float
    sanitary_sewage_chamber = CleanFloatField(null=True, blank=True)     #float
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now = True)


    def __str__(self):
        return f"{self.category}"
    

class material_cat(models.Model):
    category_id = models.AutoField(primary_key=True)
    user_id = models.ForeignKey('users.User', on_delete=models.CASCADE) #PROTECT
    category = models.CharField(max_length=100)    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now = True)

    def __str__(self):
        return f"{self.category_id}"

class material_content(models.Model):
    content_id = models.AutoField(primary_key=True)
    category_id = models.ForeignKey('material_cat', on_delete=models.CASCADE) #PROTECT
    user_id = models.ForeignKey('users.User', on_delete=models.CASCADE) #PROTECT
    content = models.TextField()
    image = models.ImageField(upload_to='media/material_content/')    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now = True)

    def __str__(self):
        return f"{self.content_id}"


class subAdminplans(models.Model):   
    Plan_Name = [
        ('Free', 'Free'),
        ('1 Months', '1 Months'),
        ('3 Months', '3 Months'),
        ('6 Months', '6 Months'),
        ('12 Months', '12 Months'),
        ('Lifetime', 'Lifetime'),
    ]
    User_Type = [
        ('Buyer', 'Buyer'),
        ('Tenant', 'Tenant'),
        ('Individual Owner/Builder', 'Individual Owner/Builder'),
        ('Landlord', 'Landlord'),
        ('Builder', 'Builder'),
        ('Agent', 'Agent'),
        ('Bank Auction', 'Bank Auction'),
    ]
   
    plan_id = models.AutoField(primary_key=True)
    razor_plan_id = models.CharField(max_length=100, null=True, blank=True)
    user_id = models.ForeignKey('users.User', on_delete=models.CASCADE, null=True, blank=True) #PROTECT
    plan_name = models.CharField(null=True, blank=True, max_length=100, choices=Plan_Name)    
    user_type = models.CharField(null=True, blank=True, max_length=100, choices=User_Type)
    actual_price = CleanFloatField(null=True, blank=True)     #float
    charges = CleanFloatField(null=True, blank=True)        #float
    status = models.BooleanField(default=True)
    trial_days = models.BigIntegerField(null=True, blank=True)

    buyer_no_unlimited = models.BooleanField(default=False)
    buyer_no = models.BigIntegerField(null=True, blank=True)

    no_of_properties_unlimited = models.BooleanField(default=False)
    no_of_liked_data_unlimited = models.BooleanField(default=False)
    matching_enquiry_unlimited = models.BooleanField(default=False)

    no_of_properties = models.BigIntegerField(null=True, blank=True)
    no_of_liked_data = models.BigIntegerField(null=True, blank=True)    
    matching_enquiry = models.BigIntegerField(null=True, blank=True)
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    def __str__(self):
        return f"{self.plan_id}"


class AddOnPlans(models.Model):
    User_Type = [
        ('Buyer', 'Buyer'),
        ('Tenant', 'Tenant'),
        ('Individual Owner/Builder', 'Individual Owner/Builder'),
        ('Landlord', 'Landlord'),
        ('Agent', 'Agent'),
        ('Bank Auction', 'Bank Auction'),
    ]

    addOn_id = models.AutoField(primary_key=True)
    user_id = models.ForeignKey('users.User', on_delete=models.CASCADE) #PROTECT    
    user_type = models.CharField(max_length=100, choices=User_Type)
    actual_price = CleanFloatField(null=True, blank=True)     #float
    charges = CleanFloatField(null=True, blank=True)        #float
    status = models.BooleanField(default=True)

    buyer_no_unlimited  = models.BooleanField(default=False)
    buyer_no = models.BigIntegerField(null=True, blank=True)

    no_of_properties_unlimited = models.BooleanField(default=False)
    no_of_liked_data_unlimited = models.BooleanField(default=False)
    matching_enquiry_unlimited = models.BooleanField(default=False)

    no_of_properties = models.BigIntegerField(null=True, blank=True)
    no_of_liked_data = models.BigIntegerField(null=True, blank=True)    
    matching_enquiry = models.BigIntegerField(null=True, blank=True)
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    def __str__(self):
        return f"{self.addOn_id}"


class referral_reward(models.Model):
    #Reward_Type = [
    #   ('Credits', 'Credits'),
    #    ('Extends_Time', 'Extends_Time')
    #]
    rew_id = models.AutoField(primary_key=True)
    user_id = models.ForeignKey('users.User', on_delete=models.CASCADE) #PROTECT
    no_of_users = models.BigIntegerField(null=True, blank=True)
    #reward_type = models.CharField(null=True, blank=True, max_length=50, choices=Reward_Type)
    credit_points = CleanFloatField(null=True, blank=True)     #float
    disabled_at = models.DateTimeField(null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.rew_id}"


class offerurls(models.Model):
    url_id = models.AutoField(primary_key=True)  
    offername = models.CharField(max_length=255, null=True, blank=True)  
    offerurl = models.TextField(null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.url_id}"

class offers(models.Model):
    offer_id = models.AutoField(primary_key=True)
    user_id = models.ForeignKey('users.User', on_delete=models.CASCADE) #PROTECT
    offer_code = models.CharField(max_length=100, unique=True)
    description = models.TextField(null=True, blank=True)
    discount_percentage = CleanFloatField(null=True, blank=True)     #float
    max_discount_amount = CleanFloatField(null=True, blank=True)     #float
    offerurl = models.ForeignKey('offerurls', on_delete=models.CASCADE, null=True, blank=True)
    no_of_times = models.IntegerField(null=True, blank=True, default=1)
    valid_life_time = models.BooleanField(default=False)
    valid_from = models.DateTimeField(null=True, blank=True)
    valid_to = models.DateTimeField(null=True, blank=True)
    status = models.BooleanField(default=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.offer_code}"


class UserOfferClaim(models.Model):
    claim_id = models.AutoField(primary_key=True)
    user_id = models.ForeignKey('users.User', on_delete=models.CASCADE, related_name='offer_claims')
    offer_id = models.ForeignKey('offers', on_delete=models.CASCADE, related_name='user_claims')
    claimed_status = models.BooleanField(default=False)
    no_of_times = models.IntegerField(null=True, blank=True, default=1)
    no_of_claimed = models.IntegerField(default=0)
    claimed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        unique_together = ('user_id', 'offer_id')

    def __str__(self):
        return f"{self.user_id.user_id}-{self.offer_id.offer_code}"

