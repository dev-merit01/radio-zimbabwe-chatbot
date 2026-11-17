from django.urls import path
from . import views

urlpatterns = [
    path("stations", views.stations),
    path("resolve/<slug:station>/<str:code>", views.resolve),
    path("campaign/<str:token>", views.campaign),
]
