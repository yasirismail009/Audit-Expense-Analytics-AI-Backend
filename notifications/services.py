"""
Notification service utilities for creating and sending notifications
"""

import logging
from typing import Optional, Dict, Any, List
from django.contrib.auth import get_user_model
from django.utils import timezone
from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync
from .models import (
    Notification, NotificationChannel, NotificationTemplate, 
    NotificationType, NotificationPriority
)

logger = logging.getLogger(__name__)
User = get_user_model()
channel_layer = get_channel_layer()

class NotificationService:
    """
    Service class for creating and sending notifications
    """
    
    @staticmethod
    def create_notification(
        title: str,
        message: str,
        user: Optional[User] = None,
        notification_type: str = NotificationType.SYSTEM_ALERT,
        priority: str = NotificationPriority.NORMAL,
        data: Optional[Dict[str, Any]] = None,
        related_object_type: Optional[str] = None,
        related_object_id: Optional[str] = None,
        expires_at: Optional[timezone.datetime] = None,
        send_immediately: bool = True
    ) -> Notification:
        """
        Create a new notification
        
        Args:
            title: Notification title
            message: Notification message
            user: Target user (None for broadcast)
            notification_type: Type of notification
            priority: Priority level
            data: Additional data payload
            related_object_type: Type of related object
            related_object_id: ID of related object
            expires_at: Expiration time
            send_immediately: Whether to send via WebSocket immediately
            
        Returns:
            Created Notification instance
        """
        try:
            notification = Notification.objects.create(
                title=title,
                message=message,
                user=user,
                notification_type=notification_type,
                priority=priority,
                data=data or {},
                related_object_type=related_object_type,
                related_object_id=related_object_id,
                expires_at=expires_at
            )
            
            if send_immediately:
                NotificationService.send_notification(notification)
            
            logger.info(f"Created notification: {notification.id}")
            return notification
            
        except Exception as e:
            logger.error(f"Error creating notification: {e}")
            raise
    
    @staticmethod
    def send_notification(notification: Notification) -> bool:
        """
        Send notification via WebSocket
        
        Args:
            notification: Notification to send
            
        Returns:
            True if sent successfully, False otherwise
        """
        try:
            if not channel_layer:
                logger.warning("Channel layer not available")
                return False
            
            # Serialize notification
            notification_data = {
                'id': str(notification.id),
                'title': notification.title,
                'message': notification.message,
                'type': notification.notification_type,
                'priority': notification.priority,
                'data': notification.data,
                'is_read': notification.is_read,
                'created_at': notification.created_at.isoformat(),
                'related_object_type': notification.related_object_type,
                'related_object_id': notification.related_object_id,
            }
            
            if notification.user:
                # Send to specific user
                group_name = f"user_{notification.user.id}"
                async_to_sync(channel_layer.group_send)(
                    group_name,
                    {
                        "type": "notification_message",
                        "notification": notification_data
                    }
                )
            else:
                # Send broadcast notification
                async_to_sync(channel_layer.group_send)(
                    "broadcast",
                    {
                        "type": "notification_message",
                        "notification": notification_data
                    }
                )
            
            # Mark as sent
            notification.mark_as_sent()
            logger.info(f"Sent notification: {notification.id}")
            return True
            
        except Exception as e:
            logger.error(f"Error sending notification {notification.id}: {e}")
            return False
    
    @staticmethod
    def create_from_template(
        template_name: str,
        context: Dict[str, Any],
        user: Optional[User] = None,
        **kwargs
    ) -> Optional[Notification]:
        """
        Create notification from template
        
        Args:
            template_name: Name of the template to use
            context: Context variables for template rendering
            user: Target user
            **kwargs: Additional arguments for create_notification
            
        Returns:
            Created Notification instance or None if template not found
        """
        try:
            template = NotificationTemplate.objects.get(
                name=template_name,
                is_active=True
            )
            
            rendered = template.render(context)
            
            return NotificationService.create_notification(
                title=rendered['title'],
                message=rendered['message'],
                user=user,
                notification_type=rendered['notification_type'],
                priority=rendered['priority'],
                **kwargs
            )
            
        except NotificationTemplate.DoesNotExist:
            logger.error(f"Notification template not found: {template_name}")
            return None
        except Exception as e:
            logger.error(f"Error creating notification from template: {e}")
            return None
    
    @staticmethod
    def send_system_message(message: str) -> bool:
        """
        Send system message to all connected users
        
        Args:
            message: System message to send
            
        Returns:
            True if sent successfully
        """
        try:
            if not channel_layer:
                return False
                
            async_to_sync(channel_layer.group_send)(
                "broadcast",
                {
                    "type": "system_message",
                    "message": message
                }
            )
            
            logger.info("Sent system message")
            return True
            
        except Exception as e:
            logger.error(f"Error sending system message: {e}")
            return False
    
    @staticmethod
    def notify_file_processing_complete(
        user: User,
        file_name: str,
        job_id: str,
        success: bool = True,
        error_message: Optional[str] = None
    ) -> Notification:
        """
        Convenience method for file processing completion notifications
        """
        if success:
            title = f"File processing complete: {file_name}"
            message = f"Your file '{file_name}' has been processed successfully."
            priority = NotificationPriority.NORMAL
            notification_type = NotificationType.FILE_PROCESSING
        else:
            title = f"File processing failed: {file_name}"
            message = f"Processing failed for '{file_name}'. Error: {error_message or 'Unknown error'}"
            priority = NotificationPriority.HIGH
            notification_type = NotificationType.ANALYSIS_ERROR
        
        return NotificationService.create_notification(
            title=title,
            message=message,
            user=user,
            notification_type=notification_type,
            priority=priority,
            data={
                'file_name': file_name,
                'success': success,
                'error_message': error_message
            },
            related_object_type='FileProcessingJob',
            related_object_id=job_id
        )
    
    @staticmethod
    def notify_analysis_complete(
        user: User,
        analysis_type: str,
        job_id: str,
        results_summary: Optional[Dict[str, Any]] = None
    ) -> Notification:
        """
        Convenience method for analysis completion notifications
        """
        title = f"Analysis complete: {analysis_type}"
        message = f"Your {analysis_type} analysis has been completed."
        
        if results_summary:
            message += f" Found {results_summary.get('total_items', 0)} items to review."
        
        return NotificationService.create_notification(
            title=title,
            message=message,
            user=user,
            notification_type=NotificationType.ANALYSIS_COMPLETE,
            priority=NotificationPriority.NORMAL,
            data={
                'analysis_type': analysis_type,
                'results_summary': results_summary or {}
            },
            related_object_type='AnalysisJob',
            related_object_id=job_id
        )
    
    @staticmethod
    def notify_risk_alert(
        user: User,
        risk_level: str,
        risk_description: str,
        related_object_type: Optional[str] = None,
        related_object_id: Optional[str] = None
    ) -> Notification:
        """
        Convenience method for risk alerts
        """
        priority_map = {
            'LOW': NotificationPriority.LOW,
            'MEDIUM': NotificationPriority.NORMAL,
            'HIGH': NotificationPriority.HIGH,
            'CRITICAL': NotificationPriority.URGENT
        }
        
        title = f"Risk Alert: {risk_level} Risk Detected"
        message = f"A {risk_level.lower()} risk has been identified: {risk_description}"
        
        return NotificationService.create_notification(
            title=title,
            message=message,
            user=user,
            notification_type=NotificationType.RISK_ALERT,
            priority=priority_map.get(risk_level, NotificationPriority.NORMAL),
            data={
                'risk_level': risk_level,
                'risk_description': risk_description
            },
            related_object_type=related_object_type,
            related_object_id=related_object_id
        )
    
    @staticmethod
    def notify_task_progress(
        user: User,
        task_name: str,
        progress_percentage: int,
        status_message: str,
        task_id: Optional[str] = None
    ) -> Notification:
        """
        Convenience method for task progress notifications
        """
        title = f"Task Progress: {task_name}"
        message = f"{status_message} ({progress_percentage}% complete)"
        
        return NotificationService.create_notification(
            title=title,
            message=message,
            user=user,
            notification_type=NotificationType.TASK_PROGRESS,
            priority=NotificationPriority.LOW,
            data={
                'task_name': task_name,
                'progress_percentage': progress_percentage,
                'status_message': status_message
            },
            related_object_type='Task',
            related_object_id=task_id
        )
    
    @staticmethod
    def cleanup_expired_notifications() -> int:
        """
        Clean up expired notifications
        
        Returns:
            Number of notifications deleted
        """
        try:
            expired_count = Notification.objects.filter(
                expires_at__lt=timezone.now()
            ).count()
            
            Notification.objects.filter(
                expires_at__lt=timezone.now()
            ).delete()
            
            logger.info(f"Cleaned up {expired_count} expired notifications")
            return expired_count
            
        except Exception as e:
            logger.error(f"Error cleaning up notifications: {e}")
            return 0
    
    @staticmethod
    def cleanup_old_notifications(days: int = 30) -> int:
        """
        Clean up old read notifications
        
        Args:
            days: Number of days to keep read notifications
            
        Returns:
            Number of notifications deleted
        """
        try:
            cutoff_date = timezone.now() - timezone.timedelta(days=days)
            
            old_count = Notification.objects.filter(
                is_read=True,
                read_at__lt=cutoff_date
            ).count()
            
            Notification.objects.filter(
                is_read=True,
                read_at__lt=cutoff_date
            ).delete()
            
            logger.info(f"Cleaned up {old_count} old notifications")
            return old_count
            
        except Exception as e:
            logger.error(f"Error cleaning up old notifications: {e}")
            return 0
    
    @staticmethod
    def get_user_notification_stats(user: User) -> Dict[str, Any]:
        """
        Get notification statistics for a user
        
        Args:
            user: User to get stats for
            
        Returns:
            Dictionary with notification statistics
        """
        try:
            total = Notification.objects.filter(user=user).count()
            unread = Notification.objects.filter(user=user, is_read=False).count()
            
            # Count by type
            type_counts = {}
            for choice in NotificationType.choices:
                count = Notification.objects.filter(
                    user=user,
                    notification_type=choice[0]
                ).count()
                if count > 0:
                    type_counts[choice[1]] = count
            
            # Count by priority
            priority_counts = {}
            for choice in NotificationPriority.choices:
                count = Notification.objects.filter(
                    user=user,
                    priority=choice[0]
                ).count()
                if count > 0:
                    priority_counts[choice[1]] = count
            
            return {
                'total': total,
                'unread': unread,
                'read': total - unread,
                'by_type': type_counts,
                'by_priority': priority_counts
            }
            
        except Exception as e:
            logger.error(f"Error getting notification stats: {e}")
            return {}
