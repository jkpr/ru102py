# Uncomment for Challenge #7
import time
import uuid
from redis.client import Redis

from redisolar.dao.base import RateLimiterDaoBase
from redisolar.dao.redis.base import RedisDaoBase
from redisolar.dao.redis.key_schema import KeySchema
# Uncomment for Challenge #7
from redisolar.dao.base import RateLimitExceededException


class SlidingWindowRateLimiter(RateLimiterDaoBase, RedisDaoBase):
    """A sliding-window rate-limiter."""
    def __init__(self,
                 window_size_ms: float,
                 max_hits: int,
                 redis_client: Redis,
                 key_schema: KeySchema = None,
                 **kwargs):
        self.window_size_ms = window_size_ms
        self.max_hits = max_hits
        super().__init__(redis_client, key_schema, **kwargs)

    def _get_key(self, name):
        key = self.key_schema.sliding_window_rate_limiter_key(
            name, self.window_size_ms, self.max_hits)
        return key

    @staticmethod
    def _get_current_timestamp_ms() -> int:
        return int(time.time() * 1000.0)

    def hit(self, name: str):
        """Record a hit using the rate-limiter."""
        # START Challenge #7
        key = self._get_key(name)
        pipeline = self.redis.pipeline(transaction=True)
        now = self._get_current_timestamp_ms()
        time_lower_bound = now - self.window_size_ms
        pipeline.zadd(key, {f"{now}-{uuid.uuid1()}": now})
        pipeline.zremrangebyscore(key, '-inf', f'({str(time_lower_bound)}')
        pipeline.zcard(key)
        results = pipeline.execute()
        hits = results[-1]
        if hits > self.max_hits:
            raise RateLimitExceededException()
        # END Challenge #7
