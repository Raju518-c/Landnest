"""
Kafka Configuration for High-Concurrency Message Processing
Handles request queuing, async processing, and event streaming
"""

from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic
import json
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from django.conf import settings
import time

logger = logging.getLogger(__name__)

class KafkaManager:
    """Manages Kafka producer, consumer, and topic operations"""
    
    def __init__(self):
        self.bootstrap_servers = ['localhost:9092']
        self.producer = None
        self.consumers = {}
        self.admin_client = None
        self.executor = ThreadPoolExecutor(max_workers=10)
        self._initialize_kafka()
    
    def _initialize_kafka(self):
        """Initialize Kafka components"""
        try:
            # Initialize admin client
            self.admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers,
                client_id='landnest-admin'
            )
            
            # Initialize producer
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=3,
                batch_size=16384,
                linger_ms=10,
                buffer_memory=33554432,
                compression_type='gzip',
                max_in_flight_requests_per_connection=5,
                request_timeout_ms=30000,
                retry_backoff_ms=100,
                security_protocol='PLAINTEXT'
            )
            
            # Create necessary topics
            self._create_topics()
            
            logger.info("Kafka initialized successfully")
        except Exception as e:
            logger.error(f"Kafka initialization failed: {e}")
            self.producer = None
    
    def _create_topics(self):
        """Create Kafka topics if they don't exist"""
        topics = [
            NewTopic(
                name='user_requests',
                num_partitions=10,
                replication_factor=1,
                topic_configs={
                    'retention.ms': '3600000',  # 1 hour
                    'segment.ms': '300000',     # 5 minutes
                    'cleanup.policy': 'delete'
                }
            ),
            NewTopic(
                name='user_cache_updates',
                num_partitions=6,
                replication_factor=1,
                topic_configs={
                    'retention.ms': '1800000',  # 30 minutes
                    'cleanup.policy': 'delete'
                }
            ),
            NewTopic(
                name='user_analytics',
                num_partitions=4,
                replication_factor=1,
                topic_configs={
                    'retention.ms': '7200000',  # 2 hours
                    'cleanup.policy': 'delete'
                }
            ),
            NewTopic(
                name='system_notifications',
                num_partitions=3,
                replication_factor=1,
                topic_configs={
                    'retention.ms': '1800000',  # 30 minutes
                    'cleanup.policy': 'delete'
                }
            )
        ]
        
        try:
            existing_topics = self.admin_client.list_topics()
            new_topics = [t for t in topics if t.name not in existing_topics]
            if new_topics:
                self.admin_client.create_topics(new_topics)
                logger.info(f"Created {len(new_topics)} new Kafka topics")
        except Exception as e:
            logger.error(f"Topic creation error: {e}")
    
    def publish_message(self, topic, message, key=None):
        """Publish message to Kafka topic"""
        if not self.producer:
            return False
        
        try:
            future = self.producer.send(topic, value=message, key=key)
            # Add callback for error handling
            future.add_callback(self._send_success_callback)
            future.add_errback(self._send_error_callback)
            return True
        except Exception as e:
            logger.error(f"Kafka publish error: {e}")
            return False
    
    def _send_success_callback(self, record_metadata):
        """Callback for successful message delivery"""
        logger.debug(f"Message delivered to {record_metadata.topic}:{record_metadata.partition}:{record_metadata.offset}")
    
    def _send_error_callback(self, exception):
        """Callback for failed message delivery"""
        logger.error(f"Message delivery failed: {exception}")
    
    def start_consumer(self, topic, group_id, callback_function):
        """Start consumer for a topic"""
        def consume_messages():
            try:
                consumer = KafkaConsumer(
                    topic,
                    bootstrap_servers=self.bootstrap_servers,
                    group_id=group_id,
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    key_deserializer=lambda k: k.decode('utf-8') if k else None,
                    auto_offset_reset='latest',
                    enable_auto_commit=True,
                    auto_commit_interval_ms=1000,
                    session_timeout_ms=30000,
                    heartbeat_interval_ms=3000,
                    max_poll_records=100,
                    max_poll_interval_ms=300000,
                    consumer_timeout_ms=100
                )
                
                self.consumers[group_id] = consumer
                
                for message in consumer:
                    try:
                        callback_function(message)
                    except Exception as e:
                        logger.error(f"Consumer callback error: {e}")
                        
            except Exception as e:
                logger.error(f"Consumer error: {e}")
        
        # Start consumer in separate thread
        consumer_thread = threading.Thread(target=consume_messages, daemon=True)
        consumer_thread.start()
        return consumer_thread
    
    def flush(self):
        """Flush all pending messages"""
        if self.producer:
            self.producer.flush()
    
    def close(self):
        """Close Kafka connections"""
        if self.producer:
            self.producer.close()
        for consumer in self.consumers.values():
            consumer.close()
        if self.admin_client:
            self.admin_client.close()

# Global Kafka manager
kafka_manager = KafkaManager()

class RequestQueueManager:
    """Manages request queuing and processing"""
    
    @staticmethod
    def queue_user_request(request_data):
        """Queue user request for processing"""
        message = {
            'request_id': request_data.get('request_id'),
            'user_id': request_data.get('user_id'),
            'endpoint': request_data.get('endpoint'),
            'parameters': request_data.get('parameters', {}),
            'timestamp': time.time(),
            'priority': request_data.get('priority', 'normal')
        }
        
        return kafka_manager.publish_message(
            'user_requests',
            message,
            key=str(request_data.get('user_id', 'anonymous'))
        )
    
    @staticmethod
    def queue_cache_update(cache_key, data):
        """Queue cache update"""
        message = {
            'cache_key': cache_key,
            'data': data,
            'timestamp': time.time(),
            'operation': 'update'
        }
        
        return kafka_manager.publish_message('user_cache_updates', message)
    
    @staticmethod
    def queue_analytics_event(event_type, user_id, data):
        """Queue analytics event"""
        message = {
            'event_type': event_type,
            'user_id': user_id,
            'data': data,
            'timestamp': time.time(),
            'user_agent': data.get('user_agent'),
            'ip_address': data.get('ip_address')
        }
        
        return kafka_manager.publish_message('user_analytics', message, key=str(user_id))
    
    @staticmethod
    def queue_notification(notification_type, recipients, message_data):
        """Queue system notification"""
        message = {
            'notification_type': notification_type,
            'recipients': recipients,
            'message': message_data,
            'timestamp': time.time()
        }
        
        return kafka_manager.publish_message('system_notifications', message)

# Request processing functions
def process_user_request(message):
    """Process queued user request"""
    try:
        request_data = message.value
        logger.info(f"Processing request: {request_data.get('request_id')}")
        
        # Here you would implement the actual request processing logic
        # For now, we'll just log it
        
        # Queue cache update after processing
        cache_key = f"processed_request:{request_data.get('request_id')}"
        RequestQueueManager.queue_cache_update(cache_key, {'status': 'completed'})
        
    except Exception as e:
        logger.error(f"Request processing error: {e}")

def process_cache_update(message):
    """Process cache update"""
    try:
        update_data = message.value
        cache_key = update_data.get('cache_key')
        data = update_data.get('data')
        
        # Update cache
        from .cache_config import redis_manager
        redis_manager.set(cache_key, json.dumps(data), timeout=300)
        
        logger.info(f"Cache updated: {cache_key}")
        
    except Exception as e:
        logger.error(f"Cache update error: {e}")

def process_analytics_event(message):
    """Process analytics event"""
    try:
        event_data = message.value
        logger.info(f"Analytics event: {event_data.get('event_type')} from user {event_data.get('user_id')}")
        
        # Here you would store analytics data
        # For now, we'll just log it
        
    except Exception as e:
        logger.error(f"Analytics processing error: {e}")

# Start consumers
def start_kafka_consumers():
    """Start all Kafka consumers"""
    try:
        # Start request processor
        kafka_manager.start_consumer(
            'user_requests',
            'user_request_processor',
            process_user_request
        )
        
        # Start cache update processor
        kafka_manager.start_consumer(
            'user_cache_updates',
            'cache_update_processor',
            process_cache_update
        )
        
        # Start analytics processor
        kafka_manager.start_consumer(
            'user_analytics',
            'analytics_processor',
            process_analytics_event
        )
        
        logger.info("Kafka consumers started successfully")
        
    except Exception as e:
        logger.error(f"Failed to start Kafka consumers: {e}")

# Auto-start consumers when module is imported
try:
    start_kafka_consumers()
except Exception as e:
    logger.error(f"Auto-start consumers failed: {e}")
