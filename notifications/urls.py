from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import NotificationViewSet, NotificationChannelViewSet

router = DefaultRouter()
router.register(r'notifications', NotificationViewSet, basename='notification')
router.register(r'channels', NotificationChannelViewSet, basename='notification-channel')

urlpatterns = [
    path('api/', include(router.urls)),
]
