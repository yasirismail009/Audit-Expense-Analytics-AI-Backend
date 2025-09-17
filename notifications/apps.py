from django.apps import AppConfig

class NotificationsConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'notifications'
    
    def ready(self):
        # Import tasks to register them with Celery
        try:
            from . import tasks
        except ImportError:
            pass
