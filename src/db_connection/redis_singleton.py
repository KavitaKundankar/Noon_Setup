import redis
import json
import os
from dotenv import load_dotenv
from logger_config import logger

load_dotenv()


class RedisClient:

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(RedisClient, cls).__new__(cls)
            cls._instance._connect()
        return cls._instance

    def _connect(self):
        try:
            self.client = redis.Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", 9226)),
            username = os.getenv("REDIS_USERNAME"),
            password=os.getenv("REDIS_PASSWORD"),
            db=int(os.getenv("REDIS_DB", 0)),
            decode_responses=True
        )


            # Test connection
            self.client.ping()
            logger.info("Redis connected successfully")

        except Exception as e:
            logger.error(f"Redis connection failed: {e}")
            raise


    # Basic operations

    def get(self, key):
        try:
            value = self.client.get(key)
            if value is None:
                return None

            # Try JSON decode
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return value

        except Exception as e:
            logger.error(f"Redis GET failed for {key}: {e}")
            return None

    def set(self, key, value, ttl=None):
        try:
            if isinstance(value, (dict, list)):
                value = json.dumps(value)

            if ttl:
                self.client.setex(key, ttl, value)
            else:
                self.client.set(key, value)

        except Exception as e:
            logger.error(f"Redis SET failed for {key}: {e}")

    def incr(self, key):
        try:
            return self.client.incr(key)
        except Exception as e:
            logger.error(f"Redis INCR failed for {key}: {e}")
            return None

    def exists(self, key):
        try:
            return self.client.exists(key)
        except Exception:
            return False

    def delete(self, key):
        try:
            self.client.delete(key)
        except Exception:
            pass
