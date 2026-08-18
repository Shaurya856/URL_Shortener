import hashlib
import random
import time

BASE62_ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
BASE62_LEN = len(BASE62_ALPHABET)

# Push short codes past a handful of trivially-guessable single characters.
CODE_OFFSET = 175_760  # 62^3, i.e. first code is 4 chars long


def base62_encode(number: int) -> str:
    number += CODE_OFFSET
    if number == 0:
        return BASE62_ALPHABET[0]
    digits = []
    while number:
        number, rem = divmod(number, BASE62_LEN)
        digits.append(BASE62_ALPHABET[rem])
    return "".join(reversed(digits))


def hash_ip(ip: str) -> str:
    """One-way hash so we never persist raw client IPs."""
    salt = "url-shortener-ip-salt"
    return hashlib.sha256(f"{salt}:{ip}".encode()).hexdigest()[:32]


def client_ip(request) -> str:
    forwarded_for = request.META.get("HTTP_X_FORWARDED_FOR")
    if forwarded_for:
        return forwarded_for.split(",")[0].strip()
    return request.META.get("REMOTE_ADDR", "unknown")


def sliding_window_allow(redis_conn, key: str, limit: int, window_seconds: int) -> bool:
    """Redis sorted-set sliding window rate limiter.

    Each call records `now` as both member and score in a per-key zset,
    trims anything older than the window, then checks cardinality against
    the limit. Atomic enough for a demo without needing a Lua script.
    """
    now = time.time()
    window_start = now - window_seconds
    pipe = redis_conn.pipeline()
    pipe.zremrangebyscore(key, 0, window_start)
    pipe.zadd(key, {f"{now}:{random.random()}": now})
    pipe.zcard(key)
    pipe.expire(key, window_seconds)
    _, _, count, _ = pipe.execute()
    return count <= limit
