import time
from datetime import datetime, timedelta
from redis_manager.daily_flag import DailyFlagManager
from logger_config import logger


def handle_callback(callback_func):
    """
    Wraps the RabbitMQ callback to add daily limit checking.
    Returns a wrapper function that accepts RabbitMQ parameters.
    """
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


        # 3. key exists AND key is true → sleep till 1 AM

        if key_exists and key_value == 1:
            logger.warning("Daily limit reached. Sleeping until 1 AM.")

            now = datetime.now()
            wakeup_time = (now + timedelta(days=1)).replace(
                hour=1, minute=0, second=0, microsecond=0
            )

            sleep_seconds = (wakeup_time - now).total_seconds()
            time.sleep(sleep_seconds)

            # Reset key after sleep
            flg_manager.setkey(0)
            return callback_func(ch, method, properties, body)
    
    return wrapper
