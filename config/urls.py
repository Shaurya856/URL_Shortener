from django.contrib import admin
from django.urls import path

from shortener import views

urlpatterns = [
    path("admin/", admin.site.urls),
    path("", views.home, name="home"),
    path("shorten", views.shorten, name="shorten"),
    path("dashboard", views.dashboard, name="dashboard"),
    path("stats/<str:code>", views.stats, name="stats"),
    path("api/stats/<str:code>", views.api_stats, name="api_stats"),
    path("<str:code>", views.redirect_short_url, name="redirect_short_url"),
]
