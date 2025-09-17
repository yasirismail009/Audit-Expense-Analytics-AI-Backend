from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from django.utils import timezone
from django.db.models import Q
from .models import Notification, NotificationChannel
from .serializers import NotificationSerializer, NotificationChannelSerializer
from .services import NotificationService
import logging

logger = logging.getLogger(__name__)

class NotificationViewSet(viewsets.ReadOnlyModelViewSet):
    """
    ViewSet for managing user notifications
    """
    serializer_class = NotificationSerializer
    permission_classes = [IsAuthenticated]
    
    def get_queryset(self):
        """Get notifications for the current user"""
        return Notification.objects.filter(
            user=self.request.user
        ).order_by('-created_at')
    
    @action(detail=False, methods=['get'])
    def unread(self, request):
        """Get unread notifications"""
        unread_notifications = self.get_queryset().filter(is_read=False)
        serializer = self.get_serializer(unread_notifications, many=True)
        return Response({
            'count': unread_notifications.count(),
            'notifications': serializer.data
        })
    
    @action(detail=True, methods=['post'])
    def mark_read(self, request, pk=None):
        """Mark a notification as read"""
        try:
            notification = self.get_object()
            notification.mark_as_read()
            return Response({'status': 'marked_read'})
        except Exception as e:
            logger.error(f"Error marking notification as read: {e}")
            return Response(
                {'error': 'Failed to mark notification as read'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    @action(detail=False, methods=['post'])
    def mark_all_read(self, request):
        """Mark all notifications as read for the current user"""
        try:
            updated_count = self.get_queryset().filter(
                is_read=False
            ).update(
                is_read=True,
                read_at=timezone.now()
            )
            return Response({
                'status': 'success',
                'marked_read': updated_count
            })
        except Exception as e:
            logger.error(f"Error marking all notifications as read: {e}")
            return Response(
                {'error': 'Failed to mark notifications as read'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    @action(detail=False, methods=['get'])
    def stats(self, request):
        """Get notification statistics for the current user"""
        try:
            stats = NotificationService.get_user_notification_stats(request.user)
            return Response(stats)
        except Exception as e:
            logger.error(f"Error getting notification stats: {e}")
            return Response(
                {'error': 'Failed to get notification statistics'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    @action(detail=False, methods=['delete'])
    def clear_read(self, request):
        """Delete all read notifications for the current user"""
        try:
            deleted_count = self.get_queryset().filter(
                is_read=True
            ).count()
            
            self.get_queryset().filter(is_read=True).delete()
            
            return Response({
                'status': 'success',
                'deleted_count': deleted_count
            })
        except Exception as e:
            logger.error(f"Error clearing read notifications: {e}")
            return Response(
                {'error': 'Failed to clear read notifications'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
    
    @action(detail=False, methods=['get'])
    def by_type(self, request):
        """Get notifications filtered by type"""
        notification_type = request.query_params.get('type')
        if not notification_type:
            return Response(
                {'error': 'type parameter is required'},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        notifications = self.get_queryset().filter(
            notification_type=notification_type
        )
        
        page = self.paginate_queryset(notifications)
        if page is not None:
            serializer = self.get_serializer(page, many=True)
            return self.get_paginated_response(serializer.data)
        
        serializer = self.get_serializer(notifications, many=True)
        return Response(serializer.data)
    
    @action(detail=False, methods=['get'])
    def search(self, request):
        """Search notifications by title or message"""
        query = request.query_params.get('q', '').strip()
        if not query:
            return Response(
                {'error': 'q parameter is required'},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        notifications = self.get_queryset().filter(
            Q(title__icontains=query) | Q(message__icontains=query)
        )
        
        page = self.paginate_queryset(notifications)
        if page is not None:
            serializer = self.get_serializer(page, many=True)
            return self.get_paginated_response(serializer.data)
        
        serializer = self.get_serializer(notifications, many=True)
        return Response(serializer.data)

class NotificationChannelViewSet(viewsets.ReadOnlyModelViewSet):
    """
    ViewSet for managing notification channels (WebSocket connections)
    """
    serializer_class = NotificationChannelSerializer
    permission_classes = [IsAuthenticated]
    
    def get_queryset(self):
        """Get notification channels for the current user"""
        return NotificationChannel.objects.filter(
            user=self.request.user
        ).order_by('-connected_at')
    
    @action(detail=False, methods=['get'])
    def active(self, request):
        """Get active notification channels"""
        active_channels = self.get_queryset().filter(is_active=True)
        serializer = self.get_serializer(active_channels, many=True)
        return Response({
            'count': active_channels.count(),
            'channels': serializer.data
        })
    
    @action(detail=False, methods=['post'])
    def test_notification(self, request):
        """Send a test notification (for development/testing)"""
        try:
            notification = NotificationService.create_notification(
                title="Test Notification",
                message="This is a test notification sent from the API.",
                user=request.user,
                notification_type="system_alert",
                priority="normal"
            )
            
            serializer = NotificationSerializer(notification)
            return Response({
                'status': 'success',
                'notification': serializer.data
            })
        except Exception as e:
            logger.error(f"Error sending test notification: {e}")
            return Response(
                {'error': 'Failed to send test notification'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
