from django.urls import path
from .views import activate

urlpatterns = [
    path("activate/", activate, name="airvote_licence_activate"),
]
