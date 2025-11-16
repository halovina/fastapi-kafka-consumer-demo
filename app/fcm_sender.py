import asyncio
import firebase_admin
from firebase_admin import credentials, messaging, exceptions
from config import settings
import token_store

def initialize_firebase_app():
    """Initialize the Firebase Admin SDK."""
    try:
        cred = credentials.Certificate(settings.FIREBASE_CREDENTIALS_PATH)
        firebase_admin.initialize_app(cred)
        print("Firebase Admin SDK initialized successfully.")
    except Exception as e:
        print(f"ERROR: Failed to initialize Firebase: {e}")

async def send_fcm_notification(user_id: str, title: str, body: str):
    """Retrieve tokens and send an FCM notification to all user's devices."""
    try:
        device_tokens = await token_store.get_tokens(user_id)
        
        if not device_tokens:
            print(f"No device tokens found for user_id: {user_id}. Message skipped.")
            return

        # Create a multicast message to send to all tokens
        message = messaging.MulticastMessage(
            notification=messaging.Notification(title=title, body=body),
            tokens=device_tokens,
        )

        # 2. Run the blocking function (send_each_for_multicast) in a separate thread
        #    This is the modern and correct way to handle synchronous I/O
        #    inside asynchronous code.
        response = await asyncio.to_thread(messaging.send_each_for_multicast, message)
        
        print(f"FCM result: {response.success_count} succeeded, {response.failure_count} failed for user: {user_id}.")
        
        # Self-healing logic: remove failed tokens
        if response.failure_count > 0:
            invalid_tokens = []
            for idx, resp in enumerate(response.responses):
                if not resp.success:
                    token = device_tokens[idx]
                    # Check if the failure is due to an unregistered token
                    # Make sure to check 'resp.exception' exists before accessing 'code'
                    if resp.exception and resp.exception.code == 'UNREGISTERED':
                        invalid_tokens.append(token)
            
            if invalid_tokens:
                print(f"Removing {len(invalid_tokens)} invalid tokens...")
                # Ensure token_store.delete_invalid_token is also async
                tasks = [token_store.delete_invalid_token(user_id, token) for token in invalid_tokens]
                await asyncio.gather(*tasks)

    except exceptions.FirebaseError as e:
        print(f"Error sending FCM message: {e}")
    except Exception as e:
        print(f"Unexpected error while sending FCM: {e}")