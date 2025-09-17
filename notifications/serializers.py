from rest_framework import serializers
from .models import Notification, NotificationChannel, NotificationTemplate

class NotificationSerializer(serializers.ModelSerializer):
    """
    Serializer for Notification model
    """
    age_in_seconds = serializers.ReadOnlyField()
    is_expired = serializers.ReadOnlyField()
    
    class Meta:
        model = Notification
        fields = [
            'id', 'title', 'message', 'notification_type', 'priority',
            'data', 'related_object_type', 'related_object_id',
            'is_read', 'is_sent', 'sent_at', 'read_at',
            'created_at', 'updated_at', 'expires_at',
            'age_in_seconds', 'is_expired'
        ]
        read_only_fields = [
            'id', 'is_sent', 'sent_at', 'created_at', 'updated_at',
            'age_in_seconds', 'is_expired'
        ]

class NotificationChannelSerializer(serializers.ModelSerializer):
    """
    Serializer for NotificationChannel model
    """
    user_email = serializers.EmailField(source='user.email', read_only=True)
    
    class Meta:
        model = NotificationChannel
        fields = [
            'id', 'user_email', 'connection_id', 'user_agent', 'ip_address',
            'is_active', 'connected_at', 'last_seen', 'disconnected_at',
            'notification_types', 'min_priority'
        ]
        read_only_fields = [
            'id', 'user_email', 'connection_id', 'user_agent', 'ip_address',
            'is_active', 'connected_at', 'last_seen', 'disconnected_at'
        ]

class NotificationTemplateSerializer(serializers.ModelSerializer):
    """
    Serializer for NotificationTemplate model
    """
    
    class Meta:
        model = NotificationTemplate
        fields = [
            'id', 'name', 'notification_type', 'title_template',
            'message_template', 'default_priority', 'is_active',
            'created_at', 'updated_at'
        ]
        read_only_fields = ['id', 'created_at', 'updated_at']

class CreateNotificationSerializer(serializers.Serializer):
    """
    Serializer for creating notifications via API
    """
    title = serializers.CharField(max_length=255)
    message = serializers.CharField()
    notification_type = serializers.ChoiceField(
        choices=Notification._meta.get_field('notification_type').choices,
        default='system_alert'
    )
    priority = serializers.ChoiceField(
        choices=Notification._meta.get_field('priority').choices,
        default='normal'
    )
    data = serializers.JSONField(required=False, allow_null=True)
    related_object_type = serializers.CharField(
        max_length=100, 
        required=False, 
        allow_blank=True
    )
    related_object_id = serializers.CharField(
        max_length=255, 
        required=False, 
        allow_blank=True
    )
    expires_at = serializers.DateTimeField(required=False, allow_null=True)
    target_user_email = serializers.EmailField(
        required=False, 
        allow_null=True,
        help_text="Email of target user (leave empty for broadcast)"
    )

class NotificationStatsSerializer(serializers.Serializer):
    """
    Serializer for notification statistics
    """
    total = serializers.IntegerField()
    unread = serializers.IntegerField()
    read = serializers.IntegerField()
    by_type = serializers.DictField()
    by_priority = serializers.DictField()
