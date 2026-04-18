from django.db import models
from django.core.files.storage import default_storage
from django.core.files.base import ContentFile
from django.contrib.auth.hashers import make_password
from django.utils import timezone
from property.models import *



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

class Role(models.Model):
    role_id = models.AutoField(primary_key=True)
    role_name = models.CharField(max_length=20)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.role_id}"


class rolePermission(models.Model):
    per_id = models.AutoField(primary_key=True)
    role_name = models.ForeignKey('Role', on_delete=models.CASCADE, related_name='role_permission') 

    user_v = models.BooleanField(default=False)
    user_a = models.BooleanField(default=False)
    user_e = models.BooleanField(default=False)
    user_d = models.BooleanField(default=False)
    user_p = models.BooleanField(default=False)

    roles_v = models.BooleanField(default=False)
    roles_a = models.BooleanField(default=False)
    roles_e = models.BooleanField(default=False)
    roles_d = models.BooleanField(default=False)
    roles_p = models.BooleanField(default=False)

    permissions_v = models.BooleanField(default=False)
    permissions_a = models.BooleanField(default=False)
    permissions_e = models.BooleanField(default=False)
    permissions_d = models.BooleanField(default=False)
    permissions_p = models.BooleanField(default=False)

    construction_cat_v = models.BooleanField(default=False)
    construction_cat_a = models.BooleanField(default=False)
    construction_cat_e = models.BooleanField(default=False)
    construction_cat_d = models.BooleanField(default=False)
    construction_cat_p = models.BooleanField(default=False)

    construction_content_v = models.BooleanField(default=False)
    construction_content_a = models.BooleanField(default=False)
    construction_content_e = models.BooleanField(default=False)
    construction_content_d = models.BooleanField(default=False)
    construction_content_p = models.BooleanField(default=False)

    packages_v = models.BooleanField(default=False)
    packages_a = models.BooleanField(default=False)
    packages_e = models.BooleanField(default=False)
    packages_d = models.BooleanField(default=False)
    packages_p = models.BooleanField(default=False)

    material_cat_v = models.BooleanField(default=False)
    material_cat_a = models.BooleanField(default=False)
    material_cat_e = models.BooleanField(default=False)
    material_cat_d = models.BooleanField(default=False)
    material_cat_p = models.BooleanField(default=False)

    material_content_v = models.BooleanField(default=False)
    material_content_a = models.BooleanField(default=False)
    material_content_e = models.BooleanField(default=False)
    material_content_d = models.BooleanField(default=False)
    material_content_p = models.BooleanField(default=False)

    plans_v = models.BooleanField(default=False)
    plans_a = models.BooleanField(default=False)
    plans_e = models.BooleanField(default=False)
    plans_d = models.BooleanField(default=False)
    plans_p = models.BooleanField(default=False)

    addon_v = models.BooleanField(default=False)
    addon_a = models.BooleanField(default=False)
    addon_e = models.BooleanField(default=False)
    addon_d = models.BooleanField(default=False)
    addon_p = models.BooleanField(default=False)

    referral_reward_v = models.BooleanField(default=False)
    referral_reward_a = models.BooleanField(default=False)
    referral_reward_e = models.BooleanField(default=False)
    referral_reward_d = models.BooleanField(default=False)
    referral_reward_p = models.BooleanField(default=False)

    property_v = models.BooleanField(default=False)
    property_a = models.BooleanField(default=False)
    property_e = models.BooleanField(default=False)
    property_d = models.BooleanField(default=False)
    property_p = models.BooleanField(default=False)

    best_deals_v = models.BooleanField(default=False)
    best_deals_a = models.BooleanField(default=False)
    best_deals_e = models.BooleanField(default=False)
    best_deals_d = models.BooleanField(default=False)
    best_deals_p = models.BooleanField(default=False)    

    def __str__(self):
        return f"{self.per_id}"



class User(models.Model):

    User_Type = [
        ('Buyer', 'Buyer'),
        ('Tenant', 'Tenant'),
        ('Individual Owner/Builder', 'Individual Owner/Builder'),
        ('Landlord', 'Landlord'),
        ('Builder', 'Builder'),
        ('Agent', 'Agent'),
        ('Bank Auction', 'Bank Auction'),
    ]


    user_id = models.AutoField(primary_key=True)
    razor_user_id = models.CharField(max_length=100, null=True, blank=True)
    username = models.CharField(max_length=100)
    first_name = models.CharField(max_length=50)
    last_name = models.CharField(max_length=50)
    email = models.EmailField()
    mobile_no = models.CharField(max_length=15)
    profile = models.ImageField(null=True, blank=True, default=None, upload_to='media/user_profiles/')
    password = models.CharField(max_length=255, blank=True)
    state = models.CharField(max_length=255, null=True, blank=True)
    city = models.CharField(max_length=255, null=True, blank=True)
    lat = models.CharField(max_length=200, null=True, blank=True, default=None)
    long = models.CharField(max_length=200, null=True, blank=True, default=None)
    role = models.CharField(max_length=255, default='User')
    fcm_token = models.TextField(null=True, blank=True)
    
    referred_by = models.CharField(max_length=20, null=True, blank=True) #user_id
    no_of_referred_for_buyer = models.BigIntegerField(null=True, blank=True, default=0)
    no_of_referred_for_tenant = models.BigIntegerField(null=True, blank=True, default=0)
    no_of_referred_for_owner = models.BigIntegerField(null=True, blank=True, default=0)
    no_of_referred_for_landlord = models.BigIntegerField(null=True, blank=True, default=0)
    no_of_referred_for_agent = models.BigIntegerField(null=True, blank=True, default=0) 
    total_referred = models.BigIntegerField(null=True, blank=True, default=0)  
    upgrade_plan = models.BigIntegerField(null=True, blank=True, default=0)
    credit_points = models.BigIntegerField(null=True, blank=True, default=0)
    total_points_earned = models.BigIntegerField(null=True, blank=True, default=0)

    current_session_token = models.CharField(max_length=255, null=True, blank=True)
    first_login = models.DateTimeField(null=True, blank=True)
    user_remark = models.TextField(null=True, blank=True)    
    tersm_and_condition = models.BooleanField(default=False, null=True, blank=True)

    user_type = models.CharField(null=True, blank=True, max_length=100, choices=User_Type)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now = True)
   
    def __str__(self):
        return f"{self.user_id}"

    def save(self, *args, **kwargs):
        if self.password and not self.password.startswith('pbkdf2_'):
            self.password = make_password(self.password)
        super(User, self).save(*args, **kwargs)






class UserOTP(models.Model):    
    email = models.EmailField()    
    otp = models.CharField(max_length=4)    
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.email} - OTP: {self.otp}"  






class Vendors(models.Model):
    vendor_id = models.AutoField(primary_key=True)
    user_id = models.ForeignKey('User', on_delete=models.CASCADE, related_name='user_vendor') #PROTECT
    name = models.CharField(max_length=100)
    profession = models.CharField(max_length=100)
    mobile = models.CharField(max_length=15)
    email = models.EmailField()
    address = models.TextField()
    lat = models.CharField(max_length=200, null=True, blank=True, default=None)
    long = models.CharField(max_length=200, null=True, blank=True, default=None)
    experience = models.BigIntegerField()
    profile = models.ImageField(upload_to='media/vendor_profiles/')
    description = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now = True)

    def __str__(self):
        return f"{self.vendor_id}"
    

class VendorWorkImage(models.Model):
    vendor = models.ForeignKey('Vendors', on_delete=models.CASCADE, related_name='work_images')
    image = models.ImageField(upload_to='media/vendor_work_images/')
    uploaded_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"Image for Vendor ID {self.vendor.vendor_id}"



class best_deals(models.Model):     
    deal_id = models.AutoField(primary_key=True)
    user_id = models.ForeignKey('User', on_delete=models.CASCADE, related_name='user_best_deals') #PROTECT    
    property_type = models.CharField(max_length=200)
    budget = CleanFloatField(null=True, blank=True)     #float
    location = models.CharField(max_length=200)
    lat = models.CharField(max_length=200, null=True, blank=True, default=None)
    long = models.CharField(max_length=200, null=True, blank=True, default=None)
    description = models.TextField()
    Admin_status = models.CharField(default=None, null=True, blank=True, max_length=255)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now = True)

    def __str__(self):
        return f"{self.deal_id}"
    

class consultant_req(models.Model):
    request_id = models.AutoField(primary_key=True)
    user_id = models.ForeignKey('User', on_delete=models.CASCADE, related_name='user_consultant_req') #PROTECT        
    interested_on = models.CharField(max_length=100, null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now = True)

    def __str__(self):
        return f"{self.request_id}"


class ChatMessage(models.Model):
    chatid = models.AutoField(primary_key=True)
    user_id = models.ForeignKey('User', on_delete=models.CASCADE, related_name='sent_messages') 
    receiver = models.ForeignKey('User', on_delete=models.CASCADE, related_name='received_messages') 
    property_id = models.ForeignKey('property.Property', on_delete=models.CASCADE, null=True, blank=True) 
    message = models.TextField()
    is_read = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def _str_(self):
        return f"{self.chatid}"



class Enquiry_Form(models.Model):
    enqid = models.AutoField(primary_key=True)
    user_id = models.ForeignKey('User', on_delete=models.CASCADE, related_name='user_enquiry_form')  
    property_type = models.CharField(max_length=200)
    min_budget = CleanFloatField(null=True, blank=True)     #float
    max_budget = CleanFloatField(null=True, blank=True)     #float
    area = models.CharField(max_length=200) 
    lat = models.CharField(max_length=200, null=True, blank=True)
    long = models.CharField(max_length=200, null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    def __str__(self):
        return f"{self.enqid}"


class user_cart(models.Model):
    cart_id = models.AutoField(primary_key=True)
    user_id = models.ForeignKey('User', on_delete=models.CASCADE, related_name='user_cart_items') #PROTECT
    property_id = models.ForeignKey('property.Property', on_delete=models.CASCADE) #PROTECT
    activity_as = models.CharField(max_length=100, null=True, blank=True)
    status  = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now = True)

    def __str__(self):
        return f"{self.cart_id}"

class activity_tbl(models.Model):
    Activity_Type = [
        ('Call', 'Call'),
        ('Liked', 'Liked'),
        ('Location', 'Location'),
        ('Enquiry', 'Enquiry'), 
        ('Description', 'Description'), 
    ]
    log_id = models.AutoField(primary_key=True)
    user_id = models.ForeignKey('User', on_delete=models.CASCADE, related_name='activities_sent')  
    property_id = models.ForeignKey('property.Property', on_delete=models.CASCADE)
    activity_type = models.CharField(max_length=50, choices=Activity_Type) 
    property_by = models.ForeignKey('User', on_delete=models.CASCADE, related_name='activities_received', )  
    activity_as = models.CharField(max_length=100, null=True, blank=True)             
    status  = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
 
    def __str__(self):
        return f"{self.log_id}"
    


class notifications(models.Model):
    notification_id = models.AutoField(primary_key=True)
    message_sender = models.ForeignKey('User', on_delete=models.CASCADE, null=True, blank=True, related_name='notification_sent')
    message_receiver = models.ForeignKey('User', on_delete=models.CASCADE, related_name='notification_received', null=True, blank=True)
    property_id = models.ForeignKey('property.Property', on_delete=models.CASCADE, null=True, blank=True)
    property_type = models.CharField(max_length=50, null=True, blank=True)  #sell or rent or lease
    notification_type = models.CharField(max_length=100, null=True, blank=True)
    message = models.TextField()
    is_read = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)

    action_from_table = models.CharField(max_length=100, null=True, blank=True)
    action_tbl_id = models.CharField(max_length=100, null=True, blank=True)

    def __str__(self):
        return f"{self.notification_id}"




class sub_user(models.Model):
    Sub_Type = [
        ('New', 'New'),
        ('Upgraded', 'Upgraded'),
        ('Referral', 'Referral'),
    ]
    sub_id = models.AutoField(primary_key=True)
    user_id = models.ForeignKey('User', on_delete=models.CASCADE, related_name='user_sub_plan')  
    
    plan_name = models.CharField(max_length=100, null=True, blank=True)

    razor_plan_id = models.CharField(max_length=100, null=True, blank=True)
    razor_user_id = models.CharField(max_length=100, null=True, blank=True)
    razor_subscription_id = models.CharField(max_length=100, null=True, blank=True)
    
    user_type = models.CharField(max_length=100, null=True, blank=True)
    charges = CleanFloatField(null=True, blank=True)      #float

    reward_amount = CleanFloatField(null=True, blank=True, default=0)     #float
    pay_amount = CleanFloatField(null=True, blank=True, default=0)       #float     

    buyer_no_unlimited = models.BooleanField(default=False)
    buyer_no = models.BigIntegerField(null=True, blank=True)

    no_of_properties_unlimited = models.BooleanField(default=False)
    no_of_liked_data_unlimited = models.BooleanField(default=False)
    matching_enquiry_unlimited = models.BooleanField(default=False)

    no_of_properties = models.BigIntegerField(null=True, blank=True)
    no_of_liked_data = models.BigIntegerField(null=True, blank=True)
    matching_enquiry = models.BigIntegerField(null=True, blank=True)
    
    expired_date = models.DateTimeField(null=True, blank=True)
    status = models.BooleanField(default=True)

    upgraded_from = models.CharField(max_length=50, null=True, blank=True)
    upgraded_to = models.CharField(max_length=20, null=True, blank=True)
    sub_type = models.CharField(max_length=50, choices=Sub_Type, null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.sub_id}"


class UserAddOn(models.Model):
    
    add_on_id = models.AutoField(primary_key=True)
    user_id = models.ForeignKey('User', on_delete=models.CASCADE, related_name='user_addon_plan')  
    extend_to = models.ForeignKey('sub_user', on_delete=models.CASCADE)
        
    user_type = models.CharField(max_length=100)
    charges = CleanFloatField(null=True, blank=True)      #float

    reward_amount = CleanFloatField(null=True, blank=True, default=0)      #float
    pay_amount = CleanFloatField(null=True, blank=True, default=0)        #float  
    
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
        return f"{self.add_on_id}"
    
    
class UserFeatures(models.Model):
    
    feature_id = models.AutoField(primary_key=True)
    user_id = models.ForeignKey('User', on_delete=models.CASCADE, related_name='user_features')      
    
    user_type = models.CharField(max_length=100, null=True, blank=True)   

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
        return f"{self.feature_id}"




class Transaction(models.Model):
    user_id = models.ForeignKey('User', on_delete=models.CASCADE, related_name='user_transaction')
    customer_id = models.CharField(max_length=255, null=True, blank=True)
    plan_name = models.CharField(max_length=255, null=True, blank=True)
    order_id = models.CharField(max_length=255, null=True, blank=True)
    payment_id = models.CharField(max_length=255, null=True, blank=True)
    plan_id = models.CharField(max_length=255, null=True, blank=True)
    subscription_id = models.CharField(max_length=255, null=True, blank=True)
    amount = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    transaction_status = models.CharField(max_length=50, null=True, blank=True)
    invoice_id = models.CharField(max_length=255, null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"Transaction {self.id} - {self.order_id}"




class loghistory(models.Model):
    log_id = models.AutoField(primary_key=True)
    user_id = models.ForeignKey('User', on_delete=models.CASCADE, related_name='log_history')  
    login_time = models.DateTimeField(null=True, blank=True)
    logout_time = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.user_id}-{self.login_time}-{self.logout_time}"


