import jwt
import uuid
from datetime import datetime, timedelta
from django.conf import settings
from django.utils import timezone
from .models import User, UserSession


class JWTHandler:
    """
    JWT token handler for user authentication
    """
    
    def __init__(self):
        self.secret_key = getattr(settings, 'JWT_SECRET_KEY', settings.SECRET_KEY)
        self.algorithm = getattr(settings, 'JWT_ALGORITHM', 'HS256')
        self.access_token_lifetime = getattr(settings, 'JWT_ACCESS_TOKEN_LIFETIME', timedelta(minutes=60))
        self.refresh_token_lifetime = getattr(settings, 'JWT_REFRESH_TOKEN_LIFETIME', timedelta(days=7))
    
    def generate_tokens(self, user, request=None):
        """
        Generate access and refresh tokens for a user
        """
        # Generate unique token IDs
        access_token_id = str(uuid.uuid4())
        refresh_token_id = str(uuid.uuid4())
        
        # Calculate expiry times
        access_token_expires = timezone.now() + self.access_token_lifetime
        refresh_token_expires = timezone.now() + self.refresh_token_lifetime
        
        # Create access token payload
        access_token_payload = {
            'token_id': access_token_id,
            'user_id': str(user.id),
            'username': user.username,
            'email': user.email,
            'exp': access_token_expires.timestamp(),
            'iat': timezone.now().timestamp(),
            'type': 'access'
        }
        
        # Create refresh token payload
        refresh_token_payload = {
            'token_id': refresh_token_id,
            'user_id': str(user.id),
            'exp': refresh_token_expires.timestamp(),
            'iat': timezone.now().timestamp(),
            'type': 'refresh'
        }
        
        # Generate JWT tokens
        access_token = jwt.encode(access_token_payload, self.secret_key, algorithm=self.algorithm)
        refresh_token = jwt.encode(refresh_token_payload, self.secret_key, algorithm=self.algorithm)
        
        # Get device information
        device_info = self._get_device_info(request) if request else {}
        ip_address = self._get_client_ip(request) if request else None
        user_agent = request.META.get('HTTP_USER_AGENT', '') if request else ''
        
        # Create user session
        session = UserSession.objects.create(
            user=user,
            session_token=access_token_id,
            refresh_token=refresh_token_id,
            device_info=device_info,
            ip_address=ip_address,
            user_agent=user_agent,
            access_token_expires=access_token_expires,
            refresh_token_expires=refresh_token_expires
        )
        
        return {
            'access_token': access_token,
            'refresh_token': refresh_token,
            'access_token_expires': access_token_expires,
            'refresh_token_expires': refresh_token_expires,
            'session_id': str(session.id)
        }
    
    def validate_access_token(self, token):
        """
        Validate access token and return user
        """
        try:
            # Decode token
            payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
            
            # Check token type
            if payload.get('type') != 'access':
                return None, "Invalid token type"
            
            # Check if token is expired
            if datetime.fromtimestamp(payload['exp']) < datetime.now():
                return None, "Token expired"
            
            # Get user
            try:
                user = User.objects.get(id=payload['user_id'], is_active=True)
            except User.DoesNotExist:
                return None, "User not found"
            
            # Check if session exists and is active
            try:
                session = UserSession.objects.get(
                    session_token=payload['token_id'],
                    is_active=True,
                    is_revoked=False
                )
                
                # Check if session is expired
                if session.is_expired:
                    session.is_active = False
                    session.save()
                    return None, "Session expired"
                
                # Update last used timestamp
                session.save()
                
            except UserSession.DoesNotExist:
                return None, "Invalid session"
            
            return user, None
            
        except jwt.InvalidTokenError:
            return None, "Invalid token"
        except Exception as e:
            return None, str(e)
    
    def validate_refresh_token(self, token):
        """
        Validate refresh token and return user
        """
        try:
            # Decode token
            payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
            
            # Check token type
            if payload.get('type') != 'refresh':
                return None, "Invalid token type"
            
            # Check if token is expired
            if datetime.fromtimestamp(payload['exp']) < datetime.now():
                return None, "Token expired"
            
            # Get user
            try:
                user = User.objects.get(id=payload['user_id'], is_active=True)
            except User.DoesNotExist:
                return None, "User not found"
            
            # Check if session exists and is active
            try:
                session = UserSession.objects.get(
                    refresh_token=payload['token_id'],
                    is_active=True,
                    is_revoked=False
                )
                
                # Check if refresh token is expired
                if session.is_refresh_expired:
                    session.is_active = False
                    session.save()
                    return None, "Refresh token expired"
                
            except UserSession.DoesNotExist:
                return None, "Invalid session"
            
            return user, session, None
            
        except jwt.InvalidTokenError:
            return None, None, "Invalid token"
        except Exception as e:
            return None, None, str(e)
    
    def refresh_tokens(self, refresh_token, request=None):
        """
        Generate new access token using refresh token
        """
        user, session, error = self.validate_refresh_token(refresh_token)
        if error:
            return None, error
        
        # Revoke old session
        session.revoke()
        
        # Generate new tokens
        return self.generate_tokens(user, request), None
    
    def revoke_tokens(self, user, session_id=None):
        """
        Revoke user tokens/sessions
        """
        if session_id:
            # Revoke specific session
            try:
                session = UserSession.objects.get(id=session_id, user=user)
                session.revoke()
                return True
            except UserSession.DoesNotExist:
                return False
        else:
            # Revoke all user sessions
            UserSession.objects.filter(user=user, is_active=True).update(
                is_active=False,
                is_revoked=True,
                revoked_at=timezone.now()
            )
            return True
    
    def _get_device_info(self, request):
        """
        Extract device information from request
        """
        if not request:
            return {}
        
        user_agent = request.META.get('HTTP_USER_AGENT', '')
        
        # Simple device detection
        device_info = {
            'user_agent': user_agent,
            'ip_address': self._get_client_ip(request),
        }
        
        # Detect browser and OS (simplified)
        if 'Chrome' in user_agent:
            device_info['browser'] = 'Chrome'
        elif 'Firefox' in user_agent:
            device_info['browser'] = 'Firefox'
        elif 'Safari' in user_agent:
            device_info['browser'] = 'Safari'
        elif 'Edge' in user_agent:
            device_info['browser'] = 'Edge'
        else:
            device_info['browser'] = 'Unknown'
        
        if 'Windows' in user_agent:
            device_info['os'] = 'Windows'
        elif 'Mac' in user_agent:
            device_info['os'] = 'macOS'
        elif 'Linux' in user_agent:
            device_info['os'] = 'Linux'
        elif 'Android' in user_agent:
            device_info['os'] = 'Android'
        elif 'iOS' in user_agent:
            device_info['os'] = 'iOS'
        else:
            device_info['os'] = 'Unknown'
        
        return device_info
    
    def _get_client_ip(self, request):
        """
        Get client IP address from request
        """
        if not request:
            return None
        
        x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
        if x_forwarded_for:
            ip = x_forwarded_for.split(',')[0]
        else:
            ip = request.META.get('REMOTE_ADDR')
        
        return ip


# Global JWT handler instance
jwt_handler = JWTHandler()
