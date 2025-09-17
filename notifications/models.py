from django.db import models
from django.contrib.auth import get_user_model
from django.utils import timezone
import uuid

User = get_user_model()

class NotificationType(models.TextChoices):
    """Types of notifications in the analytics system"""
    FILE_PROCESSING = 'file_processing', 'File Processing'
    ANALYSIS_COMPLETE = 'analysis_complete', 'Analysis Complete'
    ANALYSIS_ERROR = 'analysis_error', 'Analysis Error'
    SYSTEM_ALERT = 'system_alert', 'System Alert'
    USER_ACTION = 'user_action', 'User Action'
    TASK_PROGRESS = 'task_progress', 'Task Progress'
    RISK_ALERT = 'risk_alert', 'Risk Alert'

class NotificationPriority(models.TextChoices):
    """Priority levels for notifications"""
    LOW = 'low', 'Low'
    NORMAL = 'normal', 'Normal'
    HIGH = 'high', 'High'
    URGENT = 'urgent', 'Urgent'

class Notification(models.Model):
    """
    Model for storing and managing notifications
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    
    # Recipient information
    user = models.ForeignKey(
        User, 
        on_delete=models.CASCADE, 
        related_name='notifications',
        null=True, 
        blank=True,
        help_text="Specific user (null for broadcast notifications)"
    )
    
    # Notification content
    title = models.CharField(max_length=255, help_text="Notification title")
    message = models.TextField(help_text="Notification message content")
    notification_type = models.CharField(
        max_length=50,
        choices=NotificationType.choices,
        default=NotificationType.SYSTEM_ALERT,
        help_text="Type of notification"
    )
    priority = models.CharField(
        max_length=20,
        choices=NotificationPriority.choices,
        default=NotificationPriority.NORMAL,
        help_text="Notification priority level"
    )
    
    # Metadata
    data = models.JSONField(
        null=True, 
        blank=True, 
        help_text="Additional data payload for the notification"
    )
    
    # Related object information (optional)
    related_object_type = models.CharField(
        max_length=100, 
        null=True, 
        blank=True,
        help_text="Type of related object (e.g., 'FileProcessingJob', 'AnalysisResult')"
    )
    related_object_id = models.CharField(
        max_length=255, 
        null=True, 
        blank=True,
        help_text="ID of the related object"
    )
    
    # Status tracking
    is_read = models.BooleanField(default=False, help_text="Whether notification has been read")
    is_sent = models.BooleanField(default=False, help_text="Whether notification has been sent via WebSocket")
    sent_at = models.DateTimeField(null=True, blank=True, help_text="When notification was sent")
    read_at = models.DateTimeField(null=True, blank=True, help_text="When notification was read")
    
    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    # Auto-deletion (optional)
    expires_at = models.DateTimeField(
        null=True, 
        blank=True, 
        help_text="When this notification should be automatically deleted"
    )
    
    class Meta:
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['user', '-created_at']),
            models.Index(fields=['notification_type', '-created_at']),
            models.Index(fields=['is_read', '-created_at']),
            models.Index(fields=['priority', '-created_at']),
        ]
    
    def __str__(self):
        recipient = f"to {self.user.email}" if self.user else "broadcast"
        return f"{self.title} ({recipient})"
    
    def mark_as_read(self):
        """Mark notification as read"""
        if not self.is_read:
            self.is_read = True
            self.read_at = timezone.now()
            self.save(update_fields=['is_read', 'read_at', 'updated_at'])
    
    def mark_as_sent(self):
        """Mark notification as sent via WebSocket"""
        if not self.is_sent:
            self.is_sent = True
            self.sent_at = timezone.now()
            self.save(update_fields=['is_sent', 'sent_at', 'updated_at'])
    
    @property
    def age_in_seconds(self):
        """Get age of notification in seconds"""
        return (timezone.now() - self.created_at).total_seconds()
    
    @property
    def is_expired(self):
        """Check if notification has expired"""
        if self.expires_at:
            return timezone.now() > self.expires_at
        return False

class NotificationChannel(models.Model):
    """
    Model for tracking active WebSocket connections and user preferences
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    
    user = models.ForeignKey(
        User, 
        on_delete=models.CASCADE, 
        related_name='notification_channels'
    )
    
    # Connection information
    channel_name = models.CharField(
        max_length=255, 
        unique=True, 
        help_text="Channels layer channel name"
    )
    connection_id = models.CharField(
        max_length=255, 
        help_text="Unique identifier for this connection"
    )
    
    # Connection metadata
    user_agent = models.TextField(null=True, blank=True)
    ip_address = models.GenericIPAddressField(null=True, blank=True)
    
    # Status
    is_active = models.BooleanField(default=True)
    connected_at = models.DateTimeField(auto_now_add=True)
    last_seen = models.DateTimeField(auto_now=True)
    disconnected_at = models.DateTimeField(null=True, blank=True)
    
    # Notification preferences for this channel
    notification_types = models.JSONField(
        default=list,
        help_text="List of notification types this channel wants to receive"
    )
    min_priority = models.CharField(
        max_length=20,
        choices=NotificationPriority.choices,
        default=NotificationPriority.NORMAL,
        help_text="Minimum priority level for notifications"
    )
    
    class Meta:
        indexes = [
            models.Index(fields=['user', 'is_active']),
            models.Index(fields=['channel_name']),
            models.Index(fields=['is_active', '-last_seen']),
        ]
    
    def __str__(self):
        status = "active" if self.is_active else "inactive"
        return f"{self.user.email} - {self.connection_id} ({status})"
    
    def mark_disconnected(self):
        """Mark channel as disconnected"""
        self.is_active = False
        self.disconnected_at = timezone.now()
        self.save(update_fields=['is_active', 'disconnected_at'])
    
    def update_last_seen(self):
        """Update last seen timestamp"""
        self.last_seen = timezone.now()
        self.save(update_fields=['last_seen'])

class NotificationTemplate(models.Model):
    """
    Model for storing notification templates
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    
    name = models.CharField(max_length=100, unique=True)
    notification_type = models.CharField(
        max_length=50,
        choices=NotificationType.choices,
        help_text="Type of notification this template is for"
    )
    
    # Template content
    title_template = models.CharField(
        max_length=255,
        help_text="Title template with placeholders like {file_name}"
    )
    message_template = models.TextField(
        help_text="Message template with placeholders"
    )
    
    # Default settings
    default_priority = models.CharField(
        max_length=20,
        choices=NotificationPriority.choices,
        default=NotificationPriority.NORMAL
    )
    
    # Status
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        ordering = ['name']
    
    def __str__(self):
        return f"{self.name} ({self.notification_type})"
    
    def render(self, context=None):
        """
        Render template with given context
        
        Args:
            context: Dictionary of variables to substitute in template
            
        Returns:
            Dictionary with rendered title and message
        """
        if context is None:
            context = {}
            
        try:
            title = self.title_template.format(**context)
            message = self.message_template.format(**context)
            return {
                'title': title,
                'message': message,
                'priority': self.default_priority,
                'notification_type': self.notification_type
            }
        except KeyError as e:
            # Handle missing context variables gracefully
            return {
                'title': f"Notification Template Error: Missing variable {e}",
                'message': f"Template rendering failed: {e}",
                'priority': NotificationPriority.HIGH,
                'notification_type': self.notification_type
            }
