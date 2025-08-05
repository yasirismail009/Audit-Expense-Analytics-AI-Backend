"""
Celery configuration for analytics project.
"""

import os
from celery import Celery

# Set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'analytics.settings')

app = Celery('analytics')

# Using a string here means the worker doesn't have to serialize
# the configuration object to child processes.
app.config_from_object('django.conf:settings', namespace='CELERY')

# Explicitly configure broker to use Redis
app.conf.update(
    broker_url=os.environ.get('REDIS_URL', 'redis://localhost:6379/0'),
    result_backend=os.environ.get('REDIS_URL', 'redis://localhost:6379/0'),
    broker_transport='redis',
    result_backend_transport='redis',
)

# Load task modules from all registered Django apps.
app.autodiscover_tasks()

@app.task(bind=True)
def debug_task(self):
    """Debug task to test Celery setup."""
    print(f'Request: {self.request!r}') 