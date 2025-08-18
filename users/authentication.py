from rest_framework import authentication
from rest_framework.exceptions import AuthenticationFailed
from django.utils.translation import gettext_lazy as _
from .jwt_utils import jwt_handler


class JWTAuthentication(authentication.BaseAuthentication):
    """
    Custom JWT authentication backend for Django REST Framework
    """
    
    def authenticate(self, request):
        """
        Authenticate the request and return a two-tuple of (user, token).
        """
        # Get the token from the Authorization header
        auth_header = request.META.get('HTTP_AUTHORIZATION', '')
        
        if not auth_header:
            return None
        
        # Check if the header starts with 'Bearer '
        if not auth_header.startswith('Bearer '):
            return None
        
        # Extract the token
        token = auth_header.split(' ')[1]
        
        if not token:
            return None
        
        # Validate the token
        user, error = jwt_handler.validate_access_token(token)
        
        if error:
            raise AuthenticationFailed(error)
        
        if not user or not user.is_active:
            raise AuthenticationFailed(_('User inactive or deleted.'))
        
        return (user, token)
    
    def authenticate_header(self, request):
        """
        Return a string to be used as a value of the `WWW-Authenticate`
        header in a `401 Unauthenticated` response.
        """
        return 'Bearer realm="api"'
