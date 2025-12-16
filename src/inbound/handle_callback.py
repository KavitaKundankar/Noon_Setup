import time
from datetime import datetime, timedelta
from redis_manager.daily_limit import setkey, key_exist
from logger_config import logger

def handle_callback(callback_func, limit_flag):
    key_exists = key_exist()

    if (key_exist() == False or key_exists == False) :
        setkey(False)
        return callback_func
    
    if (key_exist() == True or key_exists == False) :
        return callback_func
    
    if(limit_flag == True) :

        logger.warning("Limit reached. Sleeping until reset time.")

        now = datetime.now()
        midnight = (now + timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )

        sleep_seconds = (midnight - now).total_seconds()
        time.sleep(sleep_seconds)
