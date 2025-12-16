from datetime import datetime, timedelta
from logger_config import logger
from db_connection.redis_singleton import RedisClient


class DailyLimitManager:
    def __init__(self, max_daily_limit: int):
        self.max_daily_limit = max_daily_limit
        self.redis = RedisClient()

    def _seconds_until_midnight(self):
        now = datetime.utcnow()
        tomorrow = now.date() + timedelta(days=1)
        midnight = datetime.combine(tomorrow, datetime.min.time())
        return int((midnight - now).total_seconds())

    def can_parse_today(self) -> bool:

        today = datetime.utcnow().date()
        key = f"noon:parse:count:{today}"

        count = self.redis.get(key)

        if count is None:
            self.redis.set(key, 1, ttl=self._seconds_until_midnight())
            return True

        count = int(count)
        print("count : ", count)

        if count >= self.max_daily_limit:
            return False

        new_count = self.redis.incr(key)

        # Log only once when limit is reached
        if new_count == self.max_daily_limit:
            logger.warning(
                f"Daily parsing limit reached ({self.max_daily_limit}/day)."
            )
            

        return True
