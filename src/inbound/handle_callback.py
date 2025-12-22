import time
from datetime import datetime, timedelta
from redis_manager.daily_flag import DailyFlagManager
from logger_config import logger


def handle_callback(callback_func):

    flg_manager = DailyFlagManager()

    def wrapper(ch, method, properties, body):
        """Wrapper function that receives RabbitMQ callback parameters"""
        
        key_exists = flg_manager.key_exist()
        key_value = flg_manager.getkey()


        # 1. key does NOT exist

        if not key_exists:
            flg_manager.setkey(0)   # false
            return callback_func(ch, method, properties, body)


        # 2. key exists AND key is false

        if key_exists and key_value == 0:
            return callback_func(ch, method, properties, body)


        # 3. key exists AND key is true - daily limit reached
        # Sleep in small chunks to keep connection alive and check for early reset

        if key_exists and key_value == 1:
            logger.warning("Daily limit reached. Waiting for reset at 1 AM...")
            
            # Calculate total wait time until 1 AM
            now = datetime.now()
            wakeup_time = (now + timedelta(days=1)).replace(
                hour=1, minute=0, second=0, microsecond=0
            )
            total_wait_seconds = (wakeup_time - now).total_seconds()
            
            # Sleep in 10-second chunks to allow heartbeats
            chunk_size = 10  # Sleep 10 seconds at a time
            chunks_to_sleep = int(total_wait_seconds / chunk_size)
            
            logger.info(f"Will wait {int(total_wait_seconds/60)} minutes until 1 AM (checking flag every {chunk_size}s)")
            
            for i in range(chunks_to_sleep):
                time.sleep(chunk_size)
                
                # Check if flag was reset early (manual reset or testing)
                if not flg_manager.key_exist() or flg_manager.getkey() == 0:
                    logger.info("Flag was reset early! Resuming processing.")
                    break
                
                # Log progress every minute
                if (i + 1) % 6 == 0:  # Every 60 seconds
                    remaining_minutes = int((chunks_to_sleep - i - 1) * chunk_size / 60)
                    logger.info(f"Still waiting... {remaining_minutes} minutes until 1 AM")
            
            # Sleep any remaining seconds
            remaining_seconds = total_wait_seconds % chunk_size
            if remaining_seconds > 0:
                time.sleep(remaining_seconds)
            
            logger.info("Wait complete. Resetting flag and resuming processing.")
            flg_manager.setkey(0)
            return callback_func(ch, method, properties, body)
    
    return wrapper
