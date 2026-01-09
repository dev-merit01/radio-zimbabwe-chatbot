from django.urls import path

from .views import login_view, register_view, logout_view, switch_station, clear_station_switch


app_name = 'accounts'

urlpatterns = [
    path('login/', login_view, name='login'),
    path('register/', register_view, name='register'),
    path('logout/', logout_view, name='logout'),
    path('switch-station/', switch_station, name='switch_station'),
    path('clear-station-switch/', clear_station_switch, name='clear_station_switch'),
]
