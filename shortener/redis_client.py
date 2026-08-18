import redis
from django.conf import settings

_pool = redis.ConnectionPool.from_url(settings.REDIS_URL, decode_responses=True)


def get_redis():
    return redis.Redis(connection_pool=_pool)
