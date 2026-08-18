from django.contrib import admin

from .models import Click, ShortURL


@admin.register(ShortURL)
class ShortURLAdmin(admin.ModelAdmin):
    list_display = ("code", "long_url", "created_at", "expires_at", "session_key")
    search_fields = ("code", "long_url")


@admin.register(Click)
class ClickAdmin(admin.ModelAdmin):
    list_display = ("short_url", "timestamp", "referrer", "ip_hash")
    list_filter = ("timestamp",)
