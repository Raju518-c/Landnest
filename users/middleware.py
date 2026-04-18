from django.http import JsonResponse
from users.models import User

class SingleDeviceLoginMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):        
        user_id = request.headers.get("X-User-Id")
        session_token = request.headers.get("X-Session-Token")

        if user_id and session_token:            
            try:
                user = User.objects.get(user_id=user_id)                
                if user.current_session_token != session_token:                            
                    request.session.flush()
                    return JsonResponse({"error": "You have been logged out as you logged in from another device."}, status=401)                
            except User.DoesNotExist:
                request.session.flush()

        return self.get_response(request)
