from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_NOTIFICATIONS_TOPIC: str = "notifications"
    KAFKA_CONSUMER_GROUP_ID: str = "notification_dispatchers"
    # This path is the path INSIDE the Docker container
    FIREBASE_CREDENTIALS_PATH: str = "./app/serviceAccountKey.json" 

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

settings = Settings()