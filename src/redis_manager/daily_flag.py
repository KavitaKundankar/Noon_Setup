from datetime import datetime, timedelta
from logger_config import logger
from db_connection.redis_singleton import RedisClient


class DailyFlagManager:
    def __init__(self):
        self.redis = RedisClient()

    def _seconds_until_1am(self):
        now = datetime.now()
        tomorrow_1am = (now + timedelta(days=1)).replace(
            hour=1, minute=0, second=0, microsecond=0
        )
        return int((tomorrow_1am - now).total_seconds())

    def setkey(self, value: int):
        self.redis.set(
            key="daily_limit_key",
            value=value,
            ttl=self._seconds_until_1am()
        )

    def getkey(self) -> int:
        value = self.redis.get("daily_limit_key")
        return int(value) if value else 0

    def key_exist(self) -> bool:
        return self.redis.exists("daily_limit_key")
