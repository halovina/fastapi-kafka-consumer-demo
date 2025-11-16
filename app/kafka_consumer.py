import asyncio
import json
from aiokafka import AIOKafkaConsumer
from contextlib import asynccontextmanager
from fastapi import FastAPI
from config import settings
from fcm_sender import send_fcm_notification, initialize_firebase_app

async def consume_notifications(consumer: AIOKafkaConsumer):
    """Main loop to consume messages from Kafka."""
    print("Consumer task started, waiting for messages...")
    try:
        async for msg in consumer:
            print(f"Received message: {msg.value} from topic: {msg.topic}")
            try:
                message_data = msg.value
                user_id = message_data.get('user_id')
                title = message_data.get('title')
                body = message_data.get('message')
                
                if not all([user_id, title, body]):
                    print("Message incomplete or invalid format, skipping.")
                    continue

                # Process FCM notification sending
                await send_fcm_notification(user_id, title, body)

            except Exception as e:
                # Error processing single message, don't stop consumer
                print(f"Error processing message {msg.value}: {e}")
    
    except asyncio.CancelledError:
        print("Consumer task cancelled.")
    
    finally:
        await consumer.stop()
        print("Consumer stopped.")

@asynccontextmanager
async def kafka_consumer_lifespan(app: FastAPI):
    """
    Manage startup/shutdown:
    1. Initialize Firebase
    2. Start Kafka Consumer
    3. Run 'consume_notifications' as background task 
    """
    print("Initializing Firebase and Kafka consumer...")
    initialize_firebase_app()
    
    consumer = AIOKafkaConsumer(
        settings.KAFKA_NOTIFICATIONS_TOPIC,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id=settings.KAFKA_CONSUMER_GROUP_ID,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest'  # Start reading from beginning if new consumer
    )
    
    await consumer.start()
    # Run consumer loop as background task 
    app.state.kafka_consumer_task = asyncio.create_task(consume_notifications(consumer))
    print("Kafka consumer started as background task.")
    
    yield
    
    print("Stopping Kafka consumer task...")
    app.state.kafka_consumer_task.cancel()
    try:
        await app.state.kafka_consumer_task
    except asyncio.CancelledError:
        print("Kafka consumer task successfully cancelled.")