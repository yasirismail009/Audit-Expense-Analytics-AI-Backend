from django.contrib import admin
from django.contrib.auth.admin import UserAdmin as BaseUserAdmin
from .models import User, UserProfile, UserSession, PasswordResetRequest


@admin.register(User)
class UserAdmin(BaseUserAdmin):
    """
    Admin interface for User model
    """
    list_display = ['username', 'email', 'first_name', 'last_name', 'is_active', 'is_email_verified', 'created_at']
    list_filter = ['is_active', 'is_email_verified', 'is_phone_verified', 'created_at', 'last_login']
    search_fields = ['username', 'email', 'first_name', 'last_name']
    ordering = ['-created_at']
    
    fieldsets = BaseUserAdmin.fieldsets + (
        ('Additional Information', {
            'fields': ('phone_number', 'date_of_birth', 'profile_picture', 'address', 'city', 'country')
        }),
        ('Verification Status', {
            'fields': ('is_email_verified', 'is_phone_verified')
        }),
        ('Password Reset', {
            'fields': ('password_reset_token', 'password_reset_expires'),
            'classes': ('collapse',)
        }),
        ('Email Verification', {
            'fields': ('email_verification_token', 'email_verification_expires'),
            'classes': ('collapse',)
        }),
    )
    
    readonly_fields = ['created_at', 'updated_at', 'last_login']


@admin.register(UserProfile)
class UserProfileAdmin(admin.ModelAdmin):
    """
    Admin interface for UserProfile model
    """
    list_display = ['user', 'job_title', 'company', 'department', 'timezone', 'created_at']
    list_filter = ['timezone', 'language', 'created_at']
    search_fields = ['user__username', 'user__email', 'job_title', 'company']
    ordering = ['-created_at']
    
    fieldsets = (
        ('User Information', {
            'fields': ('user',)
        }),
        ('Professional Information', {
            'fields': ('job_title', 'company', 'department')
        }),
        ('Preferences', {
            'fields': ('timezone', 'language', 'notification_preferences')
        }),
        ('Social Media', {
            'fields': ('linkedin_url', 'twitter_url', 'github_url'),
            'classes': ('collapse',)
        }),
    )
    
    readonly_fields = ['created_at', 'updated_at']


@admin.register(UserSession)
class UserSessionAdmin(admin.ModelAdmin):
    """
    Admin interface for UserSession model
    """
    list_display = ['user', 'ip_address', 'is_active', 'is_revoked', 'created_at', 'last_used_at']
    list_filter = ['is_active', 'is_revoked', 'created_at', 'last_used_at']
    search_fields = ['user__username', 'user__email', 'ip_address', 'user_agent']
    ordering = ['-created_at']
    
    fieldsets = (
        ('Session Information', {
            'fields': ('user', 'session_token', 'refresh_token')
        }),
        ('Device Information', {
            'fields': ('device_info', 'ip_address', 'user_agent')
        }),
        ('Token Expiry', {
            'fields': ('access_token_expires', 'refresh_token_expires')
        }),
        ('Status', {
            'fields': ('is_active', 'is_revoked', 'revoked_at')
        }),
    )
    
    readonly_fields = ['created_at', 'last_used_at', 'session_token', 'refresh_token']
    
    def has_add_permission(self, request):
        """Disable manual creation of sessions"""
        return False


@admin.register(PasswordResetRequest)
class PasswordResetRequestAdmin(admin.ModelAdmin):
    """
    Admin interface for PasswordResetRequest model
    """
    list_display = ['user', 'ip_address', 'is_used', 'is_expired', 'created_at', 'expires_at']
    list_filter = ['is_used', 'is_expired', 'created_at', 'expires_at']
    search_fields = ['user__username', 'user__email', 'ip_address', 'token']
    ordering = ['-created_at']
    
    fieldsets = (
        ('Request Information', {
            'fields': ('user', 'token')
        }),
        ('Request Details', {
            'fields': ('ip_address', 'user_agent')
        }),
        ('Status', {
            'fields': ('is_used', 'is_expired', 'used_at')
        }),
        ('Timestamps', {
            'fields': ('created_at', 'expires_at'),
            'classes': ('collapse',)
        }),
    )
    
    readonly_fields = ['created_at', 'token']
    
    def has_add_permission(self, request):
        """Disable manual creation of password reset requests"""
        return False
