import os
from celery import Celery as CeleryApp
from django.conf import settings

# Set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'landnest.settings')

app = CeleryApp('landnest')

# Using a string here means the worker doesn't have to serialize
# the configuration object to child processes.
app.config_from_object('django.conf:settings', namespace='CELERY')

# Load task modules from all registered Django apps.
app.autodiscover_tasks()

# Configure Celery
app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='Asia/Kolkata',
    enable_utc=True,
    task_track_started=True,
    task_time_limit=30 * 60,  # 30 minutes
    task_soft_time_limit=25 * 60,  # 25 minutes
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=1000,
)

# Schedule tasks
app.conf.beat_schedule = {
    'warm-user-cache': {
        'task': 'users.tasks.warm_user_cache',
        'schedule': 300.0,  # Every 5 minutes
    },
    'cleanup-expired-cache': {
        'task': 'users.tasks.cleanup_expired_cache',
        'schedule': 600.0,  # Every 10 minutes
    },
    'generate-user-statistics': {
        'task': 'users.tasks.generate_user_statistics',
        'schedule': 1800.0,  # Every 30 minutes
    },
}

@app.task(bind=True)
def debug_task(self):
    print(f'Request: {self.request!r}')
