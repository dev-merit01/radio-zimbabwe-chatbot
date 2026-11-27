from django.urls import path

from .staff_views import staff_accounts, staff_home
from .views import login_view, register_view, logout_view, switch_station, clear_station_switch, profile_view


app_name = 'accounts'

urlpatterns = [
    path("staff/", staff_home, name="staff_home"),
    path("staff/accounts/", staff_accounts, name="staff_accounts"),
    path('profile/', profile_view, name='profile'),
    path('login/', login_view, name='login'),
    path('register/', register_view, name='register'),
    path('logout/', logout_view, name='logout'),
    path('switch-station/', switch_station, name='switch_station'),
    path('clear-station-switch/', clear_station_switch, name='clear_station_switch'),
]
