"""
Celery tasks for notification management
"""

from celery import shared_task
from django.utils import timezone
from django.contrib.auth import get_user_model
from .services import NotificationService
from .models import Notification, NotificationChannel
import logging

logger = logging.getLogger(__name__)
User = get_user_model()

@shared_task(bind=True)
def send_notification_async(self, notification_id):
    """
    Send notification asynchronously via Celery
    
    Args:
        notification_id: ID of the notification to send
    """
    try:
        notification = Notification.objects.get(id=notification_id)
        success = NotificationService.send_notification(notification)
        
        if success:
            logger.info(f"Successfully sent notification {notification_id}")
        else:
            logger.error(f"Failed to send notification {notification_id}")
            
        return success
        
    except Notification.DoesNotExist:
        logger.error(f"Notification {notification_id} not found")
        return False
    except Exception as e:
        logger.error(f"Error sending notification {notification_id}: {e}")
        return False

@shared_task
def cleanup_expired_notifications():
    """
    Periodic task to clean up expired notifications
    """
    try:
        deleted_count = NotificationService.cleanup_expired_notifications()
        logger.info(f"Cleaned up {deleted_count} expired notifications")
        return deleted_count
    except Exception as e:
        logger.error(f"Error in cleanup_expired_notifications: {e}")
        return 0

@shared_task
def cleanup_old_notifications(days=30):
    """
    Periodic task to clean up old read notifications
    
    Args:
        days: Number of days to keep read notifications
    """
    try:
        deleted_count = NotificationService.cleanup_old_notifications(days)
        logger.info(f"Cleaned up {deleted_count} old notifications (older than {days} days)")
        return deleted_count
    except Exception as e:
        logger.error(f"Error in cleanup_old_notifications: {e}")
        return 0

@shared_task
def cleanup_inactive_channels():
    """
    Clean up inactive WebSocket channels (older than 1 hour)
    """
    try:
        cutoff_time = timezone.now() - timezone.timedelta(hours=1)
        
        inactive_channels = NotificationChannel.objects.filter(
            is_active=True,
            last_seen__lt=cutoff_time
        )
        
        count = inactive_channels.count()
        inactive_channels.update(
            is_active=False,
            disconnected_at=timezone.now()
        )
        
        logger.info(f"Marked {count} inactive channels as disconnected")
        return count
        
    except Exception as e:
        logger.error(f"Error cleaning up inactive channels: {e}")
        return 0

@shared_task
def send_bulk_notifications(notification_data_list):
    """
    Send multiple notifications in bulk
    
    Args:
        notification_data_list: List of notification data dictionaries
    """
    success_count = 0
    error_count = 0
    
    for notification_data in notification_data_list:
        try:
            user_id = notification_data.get('user_id')
            user = User.objects.get(id=user_id) if user_id else None
            
            notification = NotificationService.create_notification(
                title=notification_data['title'],
                message=notification_data['message'],
                user=user,
                notification_type=notification_data.get('notification_type', 'system_alert'),
                priority=notification_data.get('priority', 'normal'),
                data=notification_data.get('data'),
                related_object_type=notification_data.get('related_object_type'),
                related_object_id=notification_data.get('related_object_id'),
                send_immediately=True
            )
            
            success_count += 1
            logger.debug(f"Created bulk notification: {notification.id}")
            
        except Exception as e:
            error_count += 1
            logger.error(f"Error creating bulk notification: {e}")
    
    logger.info(f"Bulk notification task completed: {success_count} success, {error_count} errors")
    return {'success': success_count, 'errors': error_count}

@shared_task
def notify_file_processing_status(user_id, file_name, job_id, status, progress=None, error_message=None):
    """
    Send file processing status notification
    
    Args:
        user_id: ID of the user to notify
        file_name: Name of the file being processed
        job_id: ID of the processing job
        status: Status of the processing ('started', 'progress', 'completed', 'failed')
        progress: Progress percentage (for progress updates)
        error_message: Error message (for failed status)
    """
    try:
        user = User.objects.get(id=user_id)
        
        if status == 'started':
            NotificationService.create_notification(
                title=f"File processing started: {file_name}",
                message=f"Processing has begun for '{file_name}'.",
                user=user,
                notification_type='file_processing',
                priority='low',
                data={'file_name': file_name, 'status': status},
                related_object_type='FileProcessingJob',
                related_object_id=job_id
            )
            
        elif status == 'progress' and progress is not None:
            NotificationService.notify_task_progress(
                user=user,
                task_name=f"Processing {file_name}",
                progress_percentage=progress,
                status_message=f"Processing file '{file_name}'",
                task_id=job_id
            )
            
        elif status == 'completed':
            NotificationService.notify_file_processing_complete(
                user=user,
                file_name=file_name,
                job_id=job_id,
                success=True
            )
            
        elif status == 'failed':
            NotificationService.notify_file_processing_complete(
                user=user,
                file_name=file_name,
                job_id=job_id,
                success=False,
                error_message=error_message
            )
        
        logger.info(f"Sent file processing notification: {status} for {file_name}")
        
    except User.DoesNotExist:
        logger.error(f"User {user_id} not found for file processing notification")
    except Exception as e:
        logger.error(f"Error sending file processing notification: {e}")

@shared_task
def notify_analysis_status(user_id, analysis_type, job_id, status, results_summary=None, error_message=None):
    """
    Send analysis status notification
    
    Args:
        user_id: ID of the user to notify
        analysis_type: Type of analysis being performed
        job_id: ID of the analysis job
        status: Status of the analysis ('started', 'completed', 'failed')
        results_summary: Summary of analysis results
        error_message: Error message (for failed status)
    """
    try:
        user = User.objects.get(id=user_id)
        
        if status == 'started':
            NotificationService.create_notification(
                title=f"Analysis started: {analysis_type}",
                message=f"Your {analysis_type} analysis has begun.",
                user=user,
                notification_type='analysis_complete',
                priority='low',
                data={'analysis_type': analysis_type, 'status': status},
                related_object_type='AnalysisJob',
                related_object_id=job_id
            )
            
        elif status == 'completed':
            NotificationService.notify_analysis_complete(
                user=user,
                analysis_type=analysis_type,
                job_id=job_id,
                results_summary=results_summary
            )
            
        elif status == 'failed':
            NotificationService.create_notification(
                title=f"Analysis failed: {analysis_type}",
                message=f"Your {analysis_type} analysis has failed. Error: {error_message or 'Unknown error'}",
                user=user,
                notification_type='analysis_error',
                priority='high',
                data={
                    'analysis_type': analysis_type,
                    'status': status,
                    'error_message': error_message
                },
                related_object_type='AnalysisJob',
                related_object_id=job_id
            )
        
        logger.info(f"Sent analysis notification: {status} for {analysis_type}")
        
    except User.DoesNotExist:
        logger.error(f"User {user_id} not found for analysis notification")
    except Exception as e:
        logger.error(f"Error sending analysis notification: {e}")

@shared_task
def notify_system_status(message, priority='normal', notification_type='system_alert'):
    """
    Send system-wide notification to all users
    
    Args:
        message: System message to broadcast
        priority: Priority level of the notification
        notification_type: Type of notification
    """
    try:
        # Create broadcast notification (user=None)
        notification = NotificationService.create_notification(
            title="System Notification",
            message=message,
            user=None,  # Broadcast to all users
            notification_type=notification_type,
            priority=priority,
            send_immediately=True
        )
        
        logger.info(f"Sent system notification: {notification.id}")
        return str(notification.id)
        
    except Exception as e:
        logger.error(f"Error sending system notification: {e}")
        return None
