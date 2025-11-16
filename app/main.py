from fastapi import FastAPI
from kafka_consumer import kafka_consumer_lifespan

# Initialize FastAPI application with lifespan event
app = FastAPI(lifespan=kafka_consumer_lifespan)

@app.get("/health")
def health_check():
    """Endpoint to check if the consumer service is running."""
    task = app.state.kafka_consumer_task
    if task.done():
        try:
            # If task is done, check for errors
            task.result()
            return {"status": "error", "message": "Consumer task stopped without error."}
        except Exception as e:
            return {"status": "error", "message": f"Consumer task failed: {e}"}
    return {"status": "ok", "message": "Consumer service is running"}