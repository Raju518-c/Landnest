from django.db import models
from django.core.files.storage import default_storage
from django.core.files.base import ContentFile
from django.contrib.auth.hashers import make_password
from users.models import *



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

class Property_Cat(models.Model):
    category_types = [
        ('sell', 'sell'),
        ('rent/lease', 'Rent/Lease'),
        ('rent/lease', 'Rent/Lease'),
        ('rent/lease', 'Rent/Lease'),        
    ]
    category_id = models.AutoField(primary_key=True)
    user_id = models.ForeignKey('users.User', on_delete=models.CASCADE) #PROTECT
    category = models.CharField(max_length=100)
    category_type = models.CharField(max_length=50, choices=category_types)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now = True)

    def __str__(self):
        return f"{self.category_id}"
    

class Property(models.Model):    
    property_id = models.AutoField(primary_key=True)
    user_id = models.ForeignKey('users.User', on_delete=models.CASCADE, related_name='user_properties') #PROTECT
    mobile_no = models.CharField(max_length=15, null=True, blank=True, default=None)
    category_id = models.ForeignKey('Property_Cat', on_delete=models.CASCADE, null=True, blank=True,) #PROTECT  
    type = models.CharField(max_length=50, null=True, blank=True)  #sell or rent or lease or jd/jv
    admin_mobile = models.CharField(max_length=50, null=True, blank=True) 
    Admin_status = models.CharField(max_length=200, null=True, blank=True, default=None)
    property_name = models.CharField(max_length=50, null=True, blank=True, default=None)

    property_type = models.CharField(max_length=50, null=True, blank=True)       

    min_budget = CleanFloatField(null=True, blank=True)     #float   
    max_budget = CleanFloatField(null=True, blank=True)     #float   

    min_acres = CleanFloatField(null=True, blank=True)     #float   
    max_acres = CleanFloatField(null=True, blank=True)     #float   
    ratio = models.CharField(max_length=50, null=True, blank=True)
    floor = models.IntegerField(null=True, blank=True)
    
    comment = models.TextField(null=True, blank=True)

    facing =  models.CharField(max_length=50, null=True, blank=True)
    roadwidth = CleanFloatField(null=True, blank=True)     #float   
    site_area = CleanFloatField(null=True, blank=True)     #float   
    length = CleanFloatField(null=True, blank=True)     #float   
    width = CleanFloatField(null=True, blank=True)     #float   
    units = models.CharField(max_length=100, null=True, blank=True)
    buildup_area = CleanFloatField(null=True, blank=True)     #float   
    posted_by = models.CharField(max_length=50, null=True, blank=True)
    price = CleanFloatField(null=True, blank=True)     #float   
    location = models.CharField(max_length=200, null=True, blank=True)
    lat = models.CharField(max_length=200, null=True, blank=True, default=None)
    long = models.CharField(max_length=200, null=True, blank=True, default=None)
    nearby = models.CharField(max_length=100, null=True, blank=True)    
    no_of_flores = models.IntegerField(null=True, blank=True)
    _1bhk_count = models.IntegerField(null=True, blank=True)
    _2bhk_count = models.IntegerField(null=True, blank=True)
    _3bhk_count = models.IntegerField(null=True, blank=True)
    _4bhk_count = models.IntegerField(null=True, blank=True)
    rooms_count = models.IntegerField(null=True, blank=True)
    duplex_bedrooms = models.IntegerField(null=True, blank=True)
    bedrooms_count = models.IntegerField(null=True, blank=True)
    bathrooms_count = models.IntegerField(null=True, blank=True)
    shop_count = models.IntegerField(null=True, blank=True)
    house_count = models.IntegerField(null=True, blank=True)
    balcony = models.CharField(max_length=200, null=True, blank=True)
    power_backup = models.CharField(max_length=200, null=True, blank=True)
    gated_security =  models.CharField(max_length=200, null=True, blank=True)
    borewell = models.CharField(max_length=200, null=True, blank=True)
    parking = models.CharField(max_length=200, null=True, blank=True)
    lift = models.CharField(max_length=200, null=True, blank=True)    
    advance_payment = CleanFloatField(null=True, blank=True)     #float   
    boost_date = models.DateTimeField(null=True, blank=True)
    description = models.TextField(null=True, blank=True)
    status  = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now = True)


    def __str__(self):
        return f"{self.property_id}"
    

class Property_images(models.Model):    
    property = models.ForeignKey('Property', on_delete=models.CASCADE, related_name='property_images') #PROTECT
    image = models.ImageField(upload_to='media/property_images/')    
    uploaded_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"Image for Property ID {self.property.property_id}"



class PropertyRequest(models.Model):
    req_id = models.AutoField(primary_key=True)
    user_id = models.ForeignKey('users.User', on_delete=models.CASCADE, related_name='user_prop_req') #PROTECT

    Looking_For_Choices = [
        ('purchase', 'purchase'),
        ('rent', 'rent'),
        ('lease', 'lease'),
        ('jv/jd', 'jv/jd'),
        ('build to suit', 'build to suit')
    ]
    looking_for = models.CharField(max_length=50, null=True, blank=True, choices=Looking_For_Choices)
    
    
    property_type = models.CharField(max_length=50, null=True, blank=True)
    
    length = CleanFloatField(null=True, blank=True)     #float   
    width = CleanFloatField(null=True, blank=True)     #float   
    units = models.CharField(max_length=50, null=True, blank=True)
    area = CleanFloatField(null=True, blank=True)     #float   

    min_budget = CleanFloatField(null=True, blank=True)          #float      
    max_budget = CleanFloatField(null=True, blank=True)     #float   

    no_of_bedrooms = models.IntegerField(null=True, blank=True)
    min_monthly_rent = CleanFloatField(null=True, blank=True)      #float   
    max_monthly_rent = CleanFloatField(null=True, blank=True)       #float     
    
    min_year_lease = CleanFloatField(null=True, blank=True)     #float   
    max_year_lease = CleanFloatField(null=True, blank=True)     #float   

    min_acres = CleanFloatField(null=True, blank=True)     #float   
    max_acres = CleanFloatField(null=True, blank=True)     #float   

    min_budget_per_acre = CleanFloatField(null=True, blank=True)     #float   
    max_budget_per_acre = CleanFloatField(null=True, blank=True)     #float   
    min_expected_rental_income = CleanFloatField(null=True, blank=True)     #float   
    max_expected_rental_income = CleanFloatField(null=True, blank=True)     #float   
    
    ratio = models.CharField(max_length=50, null=True, blank=True)
    floor = models.IntegerField(null=True, blank=True)
    
    comment = models.TextField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now = True)
    
    def __str__(self):
        return f"{self.req_id}"


class PropertyRequestLocations(models.Model):
    loc_id = models.AutoField(primary_key=True)
    req_id = models.ForeignKey('PropertyRequest', on_delete=models.CASCADE, related_name='pro_loc') #PROTECT 
    location = models.CharField(max_length=200, null=True, blank=True)
    lat = models.CharField(max_length=200, null=True, blank=True, default=None)
    long = models.CharField(max_length=200, null=True, blank=True, default=None)

    def __str__(self):
        return f"{self.loc_id}"
    
    
class ResponsePropertyRequest(models.Model):
    resp_id = models.AutoField(primary_key=True)
    req_id = models.ForeignKey('PropertyRequest', on_delete=models.CASCADE, related_name='user_req') #PROTECT 
    user_id = models.ForeignKey('users.User', on_delete=models.CASCADE, related_name='user_prop_res') #PROTECT

    comment = models.TextField(null=True, blank=True)
    proerty_link = models.TextField(null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now = True)
    
    def __str__(self):
        return f"{self.resp_id}"
    



class BankAuctionProperty(models.Model):

    Status = [
        ('Approved', 'Approved'),
        ('Rejected', 'Rejected'),
    ]
    bankprop_id = models.AutoField(primary_key=True)
    user_id = models.ForeignKey('users.User', on_delete=models.CASCADE, related_name='user_bank_prop', blank=True, null=True) #PROTECT
    
    auction_id = models.CharField(max_length=50, unique=True, blank=True, null=True)    
    bank_name = models.CharField(max_length=200, blank=True, null=True)
    property_type = models.CharField(max_length=200, blank=True, null=True)
    action_type = models.CharField(max_length=100, blank=True, null=True)

    location = models.CharField(max_length=255, blank=True, null=True)
    city_town = models.CharField(max_length=100, blank=True, null=True)
    area_town = models.CharField(max_length=100, blank=True, null=True)
    lat = models.CharField(max_length=200, null=True, blank=True, default=None)
    long = models.CharField(max_length=200, null=True, blank=True, default=None)

    area = CleanFloatField(blank=True, null=True)  # 900 sft     #float   
    units = models.CharField(max_length=100, blank=True, null=True)
    possession = models.CharField(max_length=100, blank=True, null=True)
    reserve_price = models.CharField(max_length=100, blank=True, null=True)
    possession_status = models.CharField(max_length=100, blank=True, null=True)
    emd_amount = CleanFloatField(blank=True, null=True)     #float   

    bid_increment = CleanFloatField(blank=True, null=True)     #float   

    emd_submission = models.DateTimeField(blank=True, null=True)
    auction_start_datetime = models.DateTimeField(blank=True, null=True)
    auction_end_datetime = models.DateTimeField(blank=True, null=True)

    bank_contact_details = models.CharField(max_length=200, blank=True, null=True)    

    description = models.TextField(blank=True, null=True)
    status = models.CharField(choices=Status, max_length=100, default=None, null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)   # auto refresh on update

    def __str__(self):
        return f"{self.bank_name} - {self.auction_id}"


class BankAuctionPropertyDocs(models.Model):
    doc_id = models.AutoField(primary_key=True)
    bankpro_id = models.ForeignKey('BankAuctionProperty', on_delete=models.CASCADE, related_name='bank_pro_doc') #PROTECT 
    document = models.FileField(upload_to='media/bank_property_docs/')    
    uploaded_at = models.DateTimeField(auto_now_add=True)   

    def __str__(self):
        return f"{self.doc_id}"
    


