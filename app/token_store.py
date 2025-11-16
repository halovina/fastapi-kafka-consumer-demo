import asyncio

# This is a MOCK DATABASE. Replace with your database connection (Redis, Postgres, etc.)
MOCK_TOKEN_DB = {
    "user_123": ["device_token_1_user_123", "device_token_2_user_123"],
    "user_456": ["device_token_1_user_456"],
    "user_457": ["device_token_1_user_457"],
    # Add other users and their tokens here for testing
}

async def get_tokens(user_id: str) -> list[str]:
    """Retrieve a list of FCM tokens for a given user_id."""
    print(f"Retrieving tokens for user_id: {user_id}")
    # Simulate asynchronous database call
    await asyncio.sleep(0.01) 
    return MOCK_TOKEN_DB.get(user_id, [])

async def delete_invalid_token(user_id: str, token: str):
    """Delete an invalid token from the database."""
    print(f"Deleting invalid token: {token} for user: {user_id}")
    # Simulate asynchronous database call
    await asyncio.sleep(0.01)
    if user_id in MOCK_TOKEN_DB and token in MOCK_TOKEN_DB[user_id]:
        MOCK_TOKEN_DB[user_id].remove(token)
        print(f"Token {token} deleted.")