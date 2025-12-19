import os
import redis
from datetime import datetime
from logger_config import logger
from dotenv import load_dotenv

load_dotenv()

class RedisClient:
    """Singleton Redis client for managing daily report counts."""
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        self._initialized = True
        self._connect()
    
    def _connect(self):
        """Establish connection to Redis server."""
        try:
            self.client = redis.Redis(
                host=os.getenv("REDIS_HOST", "localhost"),
                port=int(os.getenv("REDIS_PORT", 6379)),
                password=os.getenv("REDIS_PASSWORD", None),
                db=int(os.getenv("REDIS_DB", 0)),
                decode_responses=True
            )
            # Test connection
            self.client.ping()
            logger.info("Redis connection established successfully")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise
    
    def get_total_reports(self):
        """Get the total reports limit from Redis."""
        try:
            total = self.client.get("total_reports")
            if total is None:
                logger.warning("total_reports key not found in Redis, using default value 10")
                return 10
            return int(total)
        except Exception as e:
            logger.error(f"Error getting total_reports from Redis: {e}")
            return 10
    
    def get_report_count(self):
        """Get current report count for today."""
        try:
            count = self.client.get("report_count")
            return int(count) if count else 0
        except Exception as e:
            logger.error(f"Error getting report_count from Redis: {e}")
            return 0
    
    def increment_report_count(self):
        """Increment the report count by 1."""
        try:
            new_count = self.client.incr("report_count")
            logger.info(f"Report count incremented to {new_count}")
            return new_count
        except Exception as e:
            logger.error(f"Error incrementing report_count: {e}")
            return None
    
    def check_and_reset_daily(self):
        """Check if a new day has started and reset the report count if needed."""
        try:
            today = datetime.now().strftime("%Y-%m-%d")
            last_reset = self.client.get("last_reset_date")
            
            if last_reset != today:
                # New day detected, reset the count
                self.client.set("report_count", 0)
                self.client.set("last_reset_date", today)
                logger.info(f"Daily reset performed. Report count reset to 0 for {today}")
                return True
            return False
        except Exception as e:
            logger.error(f"Error during daily reset check: {e}")
            return False
    
    def should_continue_fetching(self):
        """Check if we should continue fetching reports from RabbitMQ."""
        try:
            current_count = self.get_report_count()
            total_reports = self.get_total_reports()
            
            should_continue = current_count < total_reports
            
            if not should_continue:
                logger.info(f"Report limit reached: {current_count}/{total_reports}. Stopping message fetch.")
            
            return should_continue
        except Exception as e:
            logger.error(f"Error checking if should continue fetching: {e}")
            return False
