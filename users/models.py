from django.db import models
from django.contrib.auth.models import AbstractUser
from django.utils import timezone
from django.db.models.signals import post_save
from django.dispatch import receiver
import uuid


class User(AbstractUser):
    """
    Custom User model with additional fields for JWT authentication
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    email = models.EmailField(unique=True, help_text='User email address')
    
    # Use email as the username field
    USERNAME_FIELD = 'email'
    REQUIRED_FIELDS = ['username']  # username is still required but not used for login
    phone_number = models.CharField(max_length=15, blank=True, null=True, help_text='User phone number')
    date_of_birth = models.DateField(blank=True, null=True, help_text='User date of birth')
    profile_picture = models.ImageField(upload_to='profile_pictures/', blank=True, null=True, help_text='User profile picture')
    
    # Additional user information
    address = models.TextField(blank=True, null=True, help_text='User address')
    city = models.CharField(max_length=100, blank=True, null=True, help_text='User city')
    country = models.CharField(max_length=100, blank=True, null=True, help_text='User country')
    
    # Account status
    is_email_verified = models.BooleanField(default=False, help_text='Whether email is verified')
    is_phone_verified = models.BooleanField(default=False, help_text='Whether phone is verified')
    is_active = models.BooleanField(default=True, help_text='Whether user account is active')
    
    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True, help_text='Account creation timestamp')
    updated_at = models.DateTimeField(auto_now=True, help_text='Last update timestamp')
    last_login = models.DateTimeField(blank=True, null=True, help_text='Last login timestamp')
    
    # Password reset fields
    password_reset_token = models.CharField(max_length=255, blank=True, null=True, help_text='Password reset token')
    password_reset_expires = models.DateTimeField(blank=True, null=True, help_text='Password reset token expiry')
    
    # Email verification fields
    email_verification_token = models.CharField(max_length=255, blank=True, null=True, help_text='Email verification token')
    email_verification_expires = models.DateTimeField(blank=True, null=True, help_text='Email verification token expiry')
    
    class Meta:
        db_table = 'users'
        verbose_name = 'User'
        verbose_name_plural = 'Users'
        ordering = ['-created_at']
    
    def __str__(self):
        return f"{self.username} ({self.email})"
    
    @property
    def full_name(self):
        """Get user's full name"""
        if self.first_name and self.last_name:
            return f"{self.first_name} {self.last_name}"
        return self.username
    
    @property
    def is_verified(self):
        """Check if user is verified (email or phone)"""
        return self.is_email_verified or self.is_phone_verified
    
    def update_last_login(self):
        """Update last login timestamp"""
        self.last_login = timezone.now()
        self.save(update_fields=['last_login'])


class UserProfile(models.Model):
    """
    Extended user profile information
    """
    user = models.OneToOneField(User, on_delete=models.CASCADE, related_name='profile', help_text='Associated user')
    
    # Professional information
    job_title = models.CharField(max_length=100, blank=True, null=True, help_text='User job title')
    company = models.CharField(max_length=100, blank=True, null=True, help_text='User company')
    department = models.CharField(max_length=100, blank=True, null=True, help_text='User department')
    
    # Preferences
    timezone = models.CharField(max_length=50, default='UTC', help_text='User timezone')
    language = models.CharField(max_length=10, default='en', help_text='User preferred language')
    notification_preferences = models.JSONField(default=dict, help_text='User notification preferences')
    
    # Social media links
    linkedin_url = models.URLField(blank=True, null=True, help_text='LinkedIn profile URL')
    twitter_url = models.URLField(blank=True, null=True, help_text='Twitter profile URL')
    github_url = models.URLField(blank=True, null=True, help_text='GitHub profile URL')
    
    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True, help_text='Profile creation timestamp')
    updated_at = models.DateTimeField(auto_now=True, help_text='Last profile update timestamp')
    
    class Meta:
        db_table = 'user_profiles'
        verbose_name = 'User Profile'
        verbose_name_plural = 'User Profiles'
    
    def __str__(self):
        return f"Profile for {self.user.username}"


class UserSession(models.Model):
    """
    Track user sessions for JWT token management
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='sessions', help_text='Associated user')
    
    # Session information
    session_token = models.CharField(max_length=255, unique=True, help_text='JWT session token')
    refresh_token = models.CharField(max_length=255, unique=True, help_text='JWT refresh token')
    device_info = models.JSONField(default=dict, help_text='Device information (browser, OS, etc.)')
    ip_address = models.GenericIPAddressField(blank=True, null=True, help_text='IP address of the session')
    user_agent = models.TextField(blank=True, null=True, help_text='User agent string')
    
    # Token expiry
    access_token_expires = models.DateTimeField(help_text='Access token expiry time')
    refresh_token_expires = models.DateTimeField(help_text='Refresh token expiry time')
    
    # Session status
    is_active = models.BooleanField(default=True, help_text='Whether session is active')
    is_revoked = models.BooleanField(default=False, help_text='Whether session is revoked')
    
    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True, help_text='Session creation timestamp')
    last_used_at = models.DateTimeField(auto_now=True, help_text='Last session usage timestamp')
    revoked_at = models.DateTimeField(blank=True, null=True, help_text='Session revocation timestamp')
    
    class Meta:
        db_table = 'user_sessions'
        verbose_name = 'User Session'
        verbose_name_plural = 'User Sessions'
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['user', 'is_active']),
            models.Index(fields=['session_token']),
            models.Index(fields=['refresh_token']),
            models.Index(fields=['access_token_expires']),
        ]
    
    def __str__(self):
        return f"Session for {self.user.username} ({self.created_at.strftime('%Y-%m-%d %H:%M')})"
    
    @property
    def is_expired(self):
        """Check if session is expired"""
        return timezone.now() > self.access_token_expires
    
    @property
    def is_refresh_expired(self):
        """Check if refresh token is expired"""
        return timezone.now() > self.refresh_token_expires
    
    def revoke(self):
        """Revoke the session"""
        self.is_active = False
        self.is_revoked = True
        self.revoked_at = timezone.now()
        self.save()


class PasswordResetRequest(models.Model):
    """
    Track password reset requests
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='password_reset_requests', help_text='User requesting password reset')
    
    # Reset information
    token = models.CharField(max_length=255, unique=True, help_text='Password reset token')
    ip_address = models.GenericIPAddressField(blank=True, null=True, help_text='IP address of the request')
    user_agent = models.TextField(blank=True, null=True, help_text='User agent string')
    
    # Status
    is_used = models.BooleanField(default=False, help_text='Whether reset token has been used')
    is_expired = models.BooleanField(default=False, help_text='Whether reset token has expired')
    
    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True, help_text='Request creation timestamp')
    expires_at = models.DateTimeField(help_text='Token expiry timestamp')
    used_at = models.DateTimeField(blank=True, null=True, help_text='When token was used')
    
    class Meta:
        db_table = 'password_reset_requests'
        verbose_name = 'Password Reset Request'
        verbose_name_plural = 'Password Reset Requests'
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['user', 'is_used']),
            models.Index(fields=['token']),
            models.Index(fields=['expires_at']),
        ]
    
    def __str__(self):
        return f"Password reset for {self.user.username} ({self.created_at.strftime('%Y-%m-%d %H:%M')})"
    
    @property
    def is_valid(self):
        """Check if reset request is valid"""
        return not self.is_used and not self.is_expired and timezone.now() < self.expires_at


# Signal to automatically create UserProfile when User is created
@receiver(post_save, sender=User)
def create_user_profile(sender, instance, created, **kwargs):
    """Create UserProfile when User is created"""
    if created:
        UserProfile.objects.create(user=instance)


@receiver(post_save, sender=User)
def save_user_profile(sender, instance, **kwargs):
    """Save UserProfile when User is saved"""
    if hasattr(instance, 'profile'):
        instance.profile.save()
