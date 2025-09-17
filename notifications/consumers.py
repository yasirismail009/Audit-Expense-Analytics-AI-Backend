import json
import logging
from channels.generic.websocket import AsyncWebsocketConsumer
from channels.db import database_sync_to_async
from django.contrib.auth import get_user_model
from django.utils import timezone
from .models import NotificationChannel, Notification, NotificationPriority
import uuid

logger = logging.getLogger(__name__)
User = get_user_model()

class NotificationConsumer(AsyncWebsocketConsumer):
    """
    WebSocket consumer for handling real-time notifications
    """
    
    async def connect(self):
        """Handle WebSocket connection"""
        try:
            # Get user from scope (set by AuthMiddleware)
            self.user = self.scope.get("user")
            
            if not self.user or not self.user.is_authenticated:
                logger.warning("Unauthenticated WebSocket connection attempt")
                await self.close(code=4001)  # Custom code for authentication error
                return
            
            # Generate unique connection ID
            self.connection_id = str(uuid.uuid4())
            
            # Create user-specific group name
            self.user_group_name = f"user_{self.user.id}"
            
            # Join user-specific group
            await self.channel_layer.group_add(
                self.user_group_name,
                self.channel_name
            )
            
            # Also join broadcast group for system-wide notifications
            await self.channel_layer.group_add(
                "broadcast",
                self.channel_name
            )
            
            # Accept the connection
            await self.accept()
            
            # Store connection in database
            await self.store_connection()
            
            # Send connection confirmation
            await self.send(text_data=json.dumps({
                'type': 'connection_established',
                'connection_id': self.connection_id,
                'message': 'Connected to notification service',
                'timestamp': timezone.now().isoformat()
            }))
            
            # Send any pending notifications
            await self.send_pending_notifications()
            
            logger.info(f"WebSocket connected: user={self.user.email}, connection={self.connection_id}")
            
        except Exception as e:
            logger.error(f"Error in WebSocket connect: {e}")
            await self.close(code=4000)
    
    async def disconnect(self, close_code):
        """Handle WebSocket disconnection"""
        try:
            if hasattr(self, 'user_group_name'):
                # Leave user group
                await self.channel_layer.group_discard(
                    self.user_group_name,
                    self.channel_name
                )
                
                # Leave broadcast group
                await self.channel_layer.group_discard(
                    "broadcast",
                    self.channel_name
                )
            
            # Mark connection as disconnected in database
            if hasattr(self, 'connection_id'):
                await self.mark_connection_disconnected()
            
            if hasattr(self, 'user'):
                logger.info(f"WebSocket disconnected: user={self.user.email}, code={close_code}")
            
        except Exception as e:
            logger.error(f"Error in WebSocket disconnect: {e}")
    
    async def receive(self, text_data):
        """Handle incoming WebSocket messages"""
        try:
            data = json.loads(text_data)
            message_type = data.get('type')
            
            if message_type == 'ping':
                # Handle ping for connection keep-alive
                await self.send(text_data=json.dumps({
                    'type': 'pong',
                    'timestamp': timezone.now().isoformat()
                }))
                await self.update_last_seen()
                
            elif message_type == 'mark_read':
                # Mark notification as read
                notification_id = data.get('notification_id')
                await self.mark_notification_read(notification_id)
                
            elif message_type == 'get_notifications':
                # Send recent notifications
                limit = data.get('limit', 50)
                await self.send_recent_notifications(limit)
                
            elif message_type == 'update_preferences':
                # Update notification preferences
                preferences = data.get('preferences', {})
                await self.update_notification_preferences(preferences)
                
            else:
                logger.warning(f"Unknown message type: {message_type}")
                await self.send_error("Unknown message type")
                
        except json.JSONDecodeError:
            logger.error("Invalid JSON received")
            await self.send_error("Invalid JSON format")
        except Exception as e:
            logger.error(f"Error handling WebSocket message: {e}")
            await self.send_error("Internal server error")
    
    async def notification_message(self, event):
        """Handle notification messages from group"""
        try:
            # Send notification to WebSocket
            await self.send(text_data=json.dumps({
                'type': 'notification',
                'notification': event['notification'],
                'timestamp': timezone.now().isoformat()
            }))
            
            # Mark notification as sent
            notification_id = event['notification'].get('id')
            if notification_id:
                await self.mark_notification_sent(notification_id)
                
        except Exception as e:
            logger.error(f"Error sending notification: {e}")
    
    async def system_message(self, event):
        """Handle system messages from group"""
        try:
            await self.send(text_data=json.dumps({
                'type': 'system',
                'message': event['message'],
                'timestamp': timezone.now().isoformat()
            }))
        except Exception as e:
            logger.error(f"Error sending system message: {e}")
    
    # Database operations (using database_sync_to_async)
    
    @database_sync_to_async
    def store_connection(self):
        """Store connection information in database"""
        # Get connection metadata
        headers = dict(self.scope.get('headers', []))
        user_agent = headers.get(b'user-agent', b'').decode('utf-8')
        
        # Get IP address
        client_ip = None
        if 'client' in self.scope:
            client_ip = self.scope['client'][0]
        
        # Create or update connection record
        NotificationChannel.objects.update_or_create(
            user=self.user,
            channel_name=self.channel_name,
            defaults={
                'connection_id': self.connection_id,
                'user_agent': user_agent,
                'ip_address': client_ip,
                'is_active': True,
                'notification_types': [],  # Default to all types
                'min_priority': NotificationPriority.NORMAL
            }
        )
    
    @database_sync_to_async
    def mark_connection_disconnected(self):
        """Mark connection as disconnected"""
        try:
            channel = NotificationChannel.objects.get(
                channel_name=self.channel_name
            )
            channel.mark_disconnected()
        except NotificationChannel.DoesNotExist:
            logger.warning(f"Connection record not found: {self.channel_name}")
    
    @database_sync_to_async
    def update_last_seen(self):
        """Update last seen timestamp"""
        try:
            channel = NotificationChannel.objects.get(
                channel_name=self.channel_name
            )
            channel.update_last_seen()
        except NotificationChannel.DoesNotExist:
            logger.warning(f"Connection record not found: {self.channel_name}")
    
    @database_sync_to_async
    def mark_notification_read(self, notification_id):
        """Mark notification as read"""
        try:
            notification = Notification.objects.get(
                id=notification_id,
                user=self.user
            )
            notification.mark_as_read()
            return True
        except Notification.DoesNotExist:
            logger.warning(f"Notification not found: {notification_id}")
            return False
    
    @database_sync_to_async
    def mark_notification_sent(self, notification_id):
        """Mark notification as sent"""
        try:
            notification = Notification.objects.get(id=notification_id)
            notification.mark_as_sent()
        except Notification.DoesNotExist:
            logger.warning(f"Notification not found: {notification_id}")
    
    @database_sync_to_async
    def get_pending_notifications(self):
        """Get pending notifications for user"""
        notifications = Notification.objects.filter(
            user=self.user,
            is_sent=False
        ).order_by('-created_at')[:50]
        
        return [self.serialize_notification(n) for n in notifications]
    
    @database_sync_to_async
    def get_recent_notifications(self, limit=50):
        """Get recent notifications for user"""
        notifications = Notification.objects.filter(
            user=self.user
        ).order_by('-created_at')[:limit]
        
        return [self.serialize_notification(n) for n in notifications]
    
    @database_sync_to_async
    def update_notification_preferences(self, preferences):
        """Update notification preferences for this connection"""
        try:
            channel = NotificationChannel.objects.get(
                channel_name=self.channel_name
            )
            
            if 'notification_types' in preferences:
                channel.notification_types = preferences['notification_types']
            
            if 'min_priority' in preferences:
                channel.min_priority = preferences['min_priority']
            
            channel.save()
            return True
        except NotificationChannel.DoesNotExist:
            logger.warning(f"Connection record not found: {self.channel_name}")
            return False
    
    def serialize_notification(self, notification):
        """Serialize notification for JSON transmission"""
        return {
            'id': str(notification.id),
            'title': notification.title,
            'message': notification.message,
            'type': notification.notification_type,
            'priority': notification.priority,
            'data': notification.data,
            'is_read': notification.is_read,
            'created_at': notification.created_at.isoformat(),
            'read_at': notification.read_at.isoformat() if notification.read_at else None,
            'related_object_type': notification.related_object_type,
            'related_object_id': notification.related_object_id,
        }
    
    async def send_pending_notifications(self):
        """Send all pending notifications to the client"""
        try:
            notifications = await self.get_pending_notifications()
            
            for notification in notifications:
                await self.send(text_data=json.dumps({
                    'type': 'notification',
                    'notification': notification,
                    'timestamp': timezone.now().isoformat()
                }))
            
            # Mark notifications as sent
            for notification in notifications:
                await self.mark_notification_sent(notification['id'])
                
        except Exception as e:
            logger.error(f"Error sending pending notifications: {e}")
    
    async def send_recent_notifications(self, limit=50):
        """Send recent notifications to client"""
        try:
            notifications = await self.get_recent_notifications(limit)
            
            await self.send(text_data=json.dumps({
                'type': 'notification_list',
                'notifications': notifications,
                'timestamp': timezone.now().isoformat()
            }))
            
        except Exception as e:
            logger.error(f"Error sending recent notifications: {e}")
    
    async def send_error(self, message):
        """Send error message to client"""
        await self.send(text_data=json.dumps({
            'type': 'error',
            'message': message,
            'timestamp': timezone.now().isoformat()
        }))

class SystemNotificationConsumer(AsyncWebsocketConsumer):
    """
    WebSocket consumer for system administrators to send broadcast notifications
    """
    
    async def connect(self):
        """Handle connection for system notifications"""
        self.user = self.scope.get("user")
        
        # Only allow staff/admin users
        if not self.user or not self.user.is_authenticated or not self.user.is_staff:
            await self.close(code=4003)  # Forbidden
            return
        
        # Join admin group
        await self.channel_layer.group_add("admin", self.channel_name)
        await self.accept()
        
        logger.info(f"System notification WebSocket connected: {self.user.email}")
    
    async def disconnect(self, close_code):
        """Handle disconnection"""
        if hasattr(self, 'user') and self.user:
            await self.channel_layer.group_discard("admin", self.channel_name)
            logger.info(f"System notification WebSocket disconnected: {self.user.email}")
    
    async def receive(self, text_data):
        """Handle incoming system messages"""
        try:
            data = json.loads(text_data)
            message_type = data.get('type')
            
            if message_type == 'broadcast':
                # Send broadcast notification
                message = data.get('message', '')
                await self.send_broadcast_notification(message)
                
            elif message_type == 'system_status':
                # Send system status update
                status = data.get('status', {})
                await self.send_system_status(status)
                
        except Exception as e:
            logger.error(f"Error handling system message: {e}")
    
    async def send_broadcast_notification(self, message):
        """Send notification to all connected users"""
        await self.channel_layer.group_send(
            "broadcast",
            {
                "type": "system_message",
                "message": message
            }
        )
    
    async def send_system_status(self, status):
        """Send system status to all connected users"""
        await self.channel_layer.group_send(
            "broadcast",
            {
                "type": "system_message",
                "message": {
                    "type": "system_status",
                    "status": status
                }
            }
        )
