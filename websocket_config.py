"""
WebSocket Configuration for Real-Time Updates
Handles live data updates, notifications, and real-time collaboration
"""

import json
import asyncio
import logging
from datetime import datetime
from channels.generic.websocket import AsyncWebsocketConsumer
from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync, sync_to_async
from django.contrib.auth.models import AnonymousUser
from django.core.cache import cache
from .cache_config import redis_manager
from .kafka_config import kafka_manager, RequestQueueManager

logger = logging.getLogger(__name__)

class RealTimeConsumer(AsyncWebsocketConsumer):
    """
    WebSocket consumer for real-time updates
    """
    
    async def connect(self):
        """Handle WebSocket connection"""
        self.user = self.scope["user"]
        self.user_group_name = f"user_{self.user.id}" if self.user.is_authenticated else "anonymous"
        
        # Join user group
        await self.channel_layer.group_add(
            self.user_group_name,
            self.channel_name
        )
        
        # Join global updates group
        await self.channel_layer.group_add(
            "global_updates",
            self.channel_name
        )
        
        await self.accept()
        
        # Send welcome message
        await self.send_text({
            'type': 'connection_established',
            'message': 'Connected to real-time updates',
            'timestamp': datetime.now().isoformat(),
            'user_id': self.user.id if self.user.is_authenticated else None
        })
        
        logger.info(f"WebSocket connected for user {self.user.id if self.user.is_authenticated else 'anonymous'}")
    
    async def disconnect(self, close_code):
        """Handle WebSocket disconnection"""
        # Leave user group
        await self.channel_layer.group_discard(
            self.user_group_name,
            self.channel_name
        )
        
        # Leave global updates group
        await self.channel_layer.group_discard(
            "global_updates",
            self.channel_name
        )
        
        logger.info(f"WebSocket disconnected for user {self.user.id if self.user.is_authenticated else 'anonymous'}")
    
    async def receive(self, text_data):
        """Handle incoming WebSocket messages"""
        try:
            data = json.loads(text_data)
            message_type = data.get('type')
            
            if message_type == 'subscribe':
                await self.handle_subscription(data)
            elif message_type == 'unsubscribe':
                await self.handle_unsubscription(data)
            elif message_type == 'ping':
                await self.send_text({'type': 'pong', 'timestamp': datetime.now().isoformat()})
            elif message_type == 'get_status':
                await self.send_status_update()
            else:
                await self.send_text({
                    'type': 'error',
                    'message': f'Unknown message type: {message_type}'
                })
                
        except json.JSONDecodeError:
            await self.send_text({
                'type': 'error',
                'message': 'Invalid JSON format'
            })
        except Exception as e:
            logger.error(f"WebSocket receive error: {e}")
            await self.send_text({
                'type': 'error',
                'message': 'Internal server error'
            })
    
    async def handle_subscription(self, data):
        """Handle subscription requests"""
        subscription_type = data.get('subscription_type')
        
        if subscription_type == 'user_updates':
            await self.channel_layer.group_add(
                "user_updates",
                self.channel_name
            )
            await self.send_text({
                'type': 'subscription_confirmed',
                'subscription': subscription_type,
                'message': 'Subscribed to user updates'
            })
        
        elif subscription_type == 'system_notifications':
            await self.channel_layer.group_add(
                "system_notifications",
                self.channel_name
            )
            await self.send_text({
                'type': 'subscription_confirmed',
                'subscription': subscription_type,
                'message': 'Subscribed to system notifications'
            })
        
        elif subscription_type == 'cache_updates':
            await self.channel_layer.group_add(
                "cache_updates",
                self.channel_name
            )
            await self.send_text({
                'type': 'subscription_confirmed',
                'subscription': subscription_type,
                'message': 'Subscribed to cache updates'
            })
        
        else:
            await self.send_text({
                'type': 'error',
                'message': f'Unknown subscription type: {subscription_type}'
            })
    
    async def handle_unsubscription(self, data):
        """Handle unsubscription requests"""
        subscription_type = data.get('subscription_type')
        
        if subscription_type == 'user_updates':
            await self.channel_layer.group_discard(
                "user_updates",
                self.channel_name
            )
            await self.send_text({
                'type': 'unsubscription_confirmed',
                'subscription': subscription_type,
                'message': 'Unsubscribed from user updates'
            })
        
        elif subscription_type == 'system_notifications':
            await self.channel_layer.group_discard(
                "system_notifications",
                self.channel_name
            )
            await self.send_text({
                'type': 'unsubscription_confirmed',
                'subscription': subscription_type,
                'message': 'Unsubscribed from system notifications'
            })
        
        elif subscription_type == 'cache_updates':
            await self.channel_layer.group_discard(
                "cache_updates",
                self.channel_name
            )
            await self.send_text({
                'type': 'unsubscription_confirmed',
                'subscription': subscription_type,
                'message': 'Unsubscribed from cache updates'
            })
        
        else:
            await self.send_text({
                'type': 'error',
                'message': f'Unknown subscription type: {subscription_type}'
            })
    
    async def send_status_update(self):
        """Send current system status"""
        try:
            from .database_config import check_database_health
            
            status_data = {
                'type': 'status_update',
                'timestamp': datetime.now().isoformat(),
                'database': check_database_health(),
                'redis_available': redis_manager.get_redis_client() is not None,
                'kafka_available': kafka_manager.producer is not None,
                'user_id': self.user.id if self.user.is_authenticated else None
            }
            
            await self.send_text(status_data)
            
        except Exception as e:
            logger.error(f"Status update error: {e}")
            await self.send_text({
                'type': 'error',
                'message': 'Failed to get status update'
            })
    
    async def send_text(self, data):
        """Send text message to WebSocket"""
        await self.send(text_data=json.dumps(data))
    
    # Channel layer message handlers
    async def user_update(self, event):
        """Handle user update messages"""
        await self.send_text({
            'type': 'user_update',
            'data': event['data'],
            'timestamp': datetime.now().isoformat()
        })
    
    async def system_notification(self, event):
        """Handle system notification messages"""
        await self.send_text({
            'type': 'system_notification',
            'data': event['data'],
            'timestamp': datetime.now().isoformat()
        })
    
    async def cache_update(self, event):
        """Handle cache update messages"""
        await self.send_text({
            'type': 'cache_update',
            'data': event['data'],
            'timestamp': datetime.now().isoformat()
        })
    
    async def global_update(self, event):
        """Handle global update messages"""
        await self.send_text({
            'type': 'global_update',
            'data': event['data'],
            'timestamp': datetime.now().isoformat()
        })

class WebSocketManager:
    """
    Manages WebSocket connections and broadcasts
    """
    
    def __init__(self):
        self.channel_layer = get_channel_layer()
        self.connected_users = set()
        self.user_groups = {}
    
    async def broadcast_to_user(self, user_id, message_type, data):
        """Broadcast message to specific user"""
        group_name = f"user_{user_id}"
        
        await self.channel_layer.group_send(
            group_name,
            {
                'type': message_type,
                'data': data
            }
        )
    
    async def broadcast_to_group(self, group_name, message_type, data):
        """Broadcast message to group"""
        await self.channel_layer.group_send(
            group_name,
            {
                'type': message_type,
                'data': data
            }
        )
    
    async def broadcast_global(self, message_type, data):
        """Broadcast message to all connected users"""
        await self.channel_layer.group_send(
            "global_updates",
            {
                'type': message_type,
                'data': data
            }
        )
    
    async def notify_user_update(self, user_id, update_data):
        """Notify about user update"""
        await self.broadcast_to_user(user_id, 'user_update', {
            'user_id': user_id,
            'update': update_data,
            'timestamp': datetime.now().isoformat()
        })
    
    async def notify_cache_update(self, cache_key, operation):
        """Notify about cache update"""
        await self.broadcast_to_group('cache_updates', 'cache_update', {
            'cache_key': cache_key,
            'operation': operation,
            'timestamp': datetime.now().isoformat()
        })
    
    async def notify_system_notification(self, notification_data):
        """Notify about system notification"""
        await self.broadcast_to_group('system_notifications', 'system_notification', {
            'notification': notification_data,
            'timestamp': datetime.now().isoformat()
        })
    
    async def notify_progressive_loading(self, user_id, progress_data):
        """Notify about progressive loading progress"""
        await self.broadcast_to_user(user_id, 'global_update', {
            'type': 'progressive_loading',
            'progress': progress_data,
            'timestamp': datetime.now().isoformat()
        })

# Global WebSocket manager
websocket_manager = WebSocketManager()

class RealTimeEventProcessor:
    """
    Processes real-time events and broadcasts via WebSocket
    """
    
    def __init__(self):
        self.websocket_manager = websocket_manager
    
    async def process_user_data_change(self, user_id, change_type, data):
        """Process user data changes"""
        event_data = {
            'user_id': user_id,
            'change_type': change_type,
            'data': data,
            'timestamp': datetime.now().isoformat()
        }
        
        # Broadcast to user updates group
        await self.websocket_manager.broadcast_to_group(
            'user_updates',
            'user_update',
            event_data
        )
        
        # Also broadcast to specific user if authenticated
        if user_id:
            await self.websocket_manager.broadcast_to_user(
                user_id,
                'user_update',
                event_data
            )
    
    async def process_cache_invalidation(self, cache_key):
        """Process cache invalidation"""
        event_data = {
            'cache_key': cache_key,
            'operation': 'invalidate',
            'timestamp': datetime.now().isoformat()
        }
        
        await self.websocket_manager.broadcast_to_group(
            'cache_updates',
            'cache_update',
            event_data
        )
    
    async def process_system_alert(self, alert_type, message, severity='info'):
        """Process system alerts"""
        event_data = {
            'alert_type': alert_type,
            'message': message,
            'severity': severity,
            'timestamp': datetime.now().isoformat()
        }
        
        await self.websocket_manager.broadcast_to_group(
            'system_notifications',
            'system_notification',
            event_data
        )
    
    async def process_progressive_loading_update(self, user_id, current_chunk, total_chunks, percentage):
        """Process progressive loading updates"""
        event_data = {
            'current_chunk': current_chunk,
            'total_chunks': total_chunks,
            'percentage': percentage,
            'timestamp': datetime.now().isoformat()
        }
        
        await self.websocket_manager.broadcast_to_user(
            user_id,
            'global_update',
            event_data
        )

# Global event processor
event_processor = RealTimeEventProcessor()

# Utility functions for broadcasting
async def broadcast_user_update(user_id, update_data):
    """Broadcast user update"""
    await event_processor.process_user_data_change(user_id, 'update', update_data)

async def broadcast_cache_invalidation(cache_key):
    """Broadcast cache invalidation"""
    await event_processor.process_cache_invalidation(cache_key)

async def broadcast_system_alert(alert_type, message, severity='info'):
    """Broadcast system alert"""
    await event_processor.process_system_alert(alert_type, message, severity)

async def broadcast_progress_update(user_id, current_chunk, total_chunks, percentage):
    """Broadcast progress update"""
    await event_processor.process_progressive_loading_update(user_id, current_chunk, total_chunks, percentage)

# Django sync wrapper for async functions
def sync_broadcast_user_update(user_id, update_data):
    """Sync wrapper for broadcasting user update"""
    async_to_sync(broadcast_user_update)(user_id, update_data)

def sync_broadcast_cache_invalidation(cache_key):
    """Sync wrapper for broadcasting cache invalidation"""
    async_to_sync(broadcast_cache_invalidation)(cache_key)

def sync_broadcast_system_alert(alert_type, message, severity='info'):
    """Sync wrapper for broadcasting system alert"""
    async_to_sync(broadcast_system_alert)(alert_type, message, severity)

def sync_broadcast_progress_update(user_id, current_chunk, total_chunks, percentage):
    """Sync wrapper for broadcasting progress update"""
    async_to_sync(broadcast_progress_update)(user_id, current_chunk, total_chunks, percentage)

# Integration with existing views
class WebSocketMixin:
    """
    Mixin for views to integrate WebSocket functionality
    """
    
    def broadcast_user_action(self, user_id, action, data):
        """Broadcast user action via WebSocket"""
        try:
            sync_broadcast_user_update(user_id, {
                'action': action,
                'data': data,
                'timestamp': datetime.now().isoformat()
            })
        except Exception as e:
            logger.error(f"WebSocket broadcast error: {e}")
    
    def broadcast_cache_update(self, cache_key, operation):
        """Broadcast cache update via WebSocket"""
        try:
            sync_broadcast_cache_invalidation(cache_key)
        except Exception as e:
            logger.error(f"WebSocket cache broadcast error: {e}")
    
    def broadcast_system_notification(self, message, severity='info'):
        """Broadcast system notification via WebSocket"""
        try:
            sync_broadcast_system_alert('system_notification', message, severity)
        except Exception as e:
            logger.error(f"WebSocket notification broadcast error: {e}")
