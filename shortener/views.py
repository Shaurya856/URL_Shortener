import json

from django.conf import settings
from django.http import Http404, HttpResponseRedirect, JsonResponse
from django.shortcuts import render
from django.urls import reverse
from django.utils import timezone
from django.views.decorators.http import require_GET, require_POST

from .id_allocator import next_code_id
from .kafka_producer import publish_click_event
from .models import Click, ShortURL
from .redis_client import get_redis
from .utils import base62_encode, client_ip, hash_ip, sliding_window_allow

CACHE_KEY_PREFIX = "short:"
STATS_TOTAL_KEY = "stats:total:"
STATS_DAILY_KEY = "stats:daily:"  # + code -> hash {YYYY-MM-DD: count}
STATS_REFERRER_KEY = "stats:referrers:"  # + code -> zset {referrer: count}


def _ensure_session(request):
    if not request.session.session_key:
        request.session.create()
    return request.session.session_key


def home(request):
    return render(request, "shortener/home.html")


@require_POST
def shorten(request):
    long_url = request.POST.get("long_url", "").strip()
    session_key = _ensure_session(request)
    r = get_redis()

    ip = client_ip(request)
    rate_key = f"ratelimit:shorten:{ip}"
    allowed = sliding_window_allow(
        r, rate_key, settings.SHORTEN_RATE_LIMIT, settings.SHORTEN_RATE_WINDOW_SECONDS
    )

    if not allowed:
        return render(
            request,
            "shortener/home.html",
            {"error": "Rate limit exceeded. Please slow down and try again shortly."},
            status=429,
        )

    if not long_url:
        return render(
            request, "shortener/home.html", {"error": "Please enter a URL to shorten."}, status=400
        )

    if not (long_url.startswith("http://") or long_url.startswith("https://")):
        long_url = f"https://{long_url}"

    from django.core.validators import URLValidator
    from django.core.exceptions import ValidationError

    try:
        URLValidator()(long_url)
    except ValidationError:
        return render(
            request, "shortener/home.html", {"error": "That doesn't look like a valid URL."}, status=400
        )

    code = base62_encode(next_code_id())

    short = ShortURL.objects.create(
        code=code,
        long_url=long_url,
        session_key=session_key,
        owner=request.user if request.user.is_authenticated else None,
    )

    return render(request, "shortener/home.html", {"short": short})


@require_GET
def redirect_short_url(request, code):
    r = get_redis()
    cache_key = f"{CACHE_KEY_PREFIX}{code}"
    long_url = r.get(cache_key)

    if long_url is None:
        try:
            short = ShortURL.objects.get(code=code)
        except ShortURL.DoesNotExist:
            raise Http404("Short link not found")

        if short.is_expired():
            raise Http404("Short link has expired")

        long_url = short.long_url
        r.set(cache_key, long_url, ex=settings.REDIRECT_CACHE_TTL_SECONDS)

    event = {
        "code": code,
        "timestamp": timezone.now().isoformat(),
        "referrer": request.META.get("HTTP_REFERER", "")[:512],
        "ip_hash": hash_ip(client_ip(request)),
        "user_agent": request.META.get("HTTP_USER_AGENT", "")[:512],
    }
    publish_click_event(event)

    return HttpResponseRedirect(long_url)


def dashboard(request):
    session_key = _ensure_session(request)
    links = ShortURL.objects.filter(session_key=session_key)

    r = get_redis()
    link_data = []
    for link in links:
        total = r.get(f"{STATS_TOTAL_KEY}{link.code}")
        total_clicks = int(total) if total is not None else link.clicks.count()
        link_data.append({"link": link, "total_clicks": total_clicks})

    return render(request, "shortener/dashboard.html", {"link_data": link_data})


def _stats_payload(code):
    short = ShortURL.objects.filter(code=code).first()
    if short is None:
        return None

    r = get_redis()

    total = r.get(f"{STATS_TOTAL_KEY}{code}")
    total_clicks = int(total) if total is not None else short.clicks.count()

    daily_raw = r.hgetall(f"{STATS_DAILY_KEY}{code}")
    if daily_raw:
        daily = sorted((day, int(count)) for day, count in daily_raw.items())
    else:
        from django.db.models.functions import TruncDate
        from django.db.models import Count

        rows = (
            short.clicks.annotate(day=TruncDate("timestamp"))
            .values("day")
            .annotate(count=Count("id"))
            .order_by("day")
        )
        daily = [(row["day"].isoformat(), row["count"]) for row in rows]

    referrers_raw = r.zrevrange(f"{STATS_REFERRER_KEY}{code}", 0, 9, withscores=True)
    if referrers_raw:
        top_referrers = [(ref or "(direct)", int(score)) for ref, score in referrers_raw]
    else:
        from django.db.models import Count

        rows = (
            short.clicks.values("referrer")
            .annotate(count=Count("id"))
            .order_by("-count")[:10]
        )
        top_referrers = [(row["referrer"] or "(direct)", row["count"]) for row in rows]

    return {
        "short": short,
        "total_clicks": total_clicks,
        "daily": daily,
        "top_referrers": top_referrers,
    }


def stats(request, code):
    payload = _stats_payload(code)
    if payload is None:
        raise Http404("Short link not found")
    return render(request, "shortener/stats.html", payload)


def api_stats(request, code):
    payload = _stats_payload(code)
    if payload is None:
        return JsonResponse({"error": "not found"}, status=404)

    short = payload["short"]
    return JsonResponse(
        {
            "code": short.code,
            "long_url": short.long_url,
            "short_url": short.short_url(),
            "created_at": short.created_at.isoformat(),
            "total_clicks": payload["total_clicks"],
            "daily": [{"date": d, "count": c} for d, c in payload["daily"]],
            "top_referrers": [{"referrer": r, "count": c} for r, c in payload["top_referrers"]],
        }
    )
