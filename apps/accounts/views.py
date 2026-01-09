from django.contrib import messages
from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.decorators import login_required, user_passes_test
from django.shortcuts import redirect, render
from django.urls import reverse
from django.http import JsonResponse

from .forms import RegistrationForm, LoginForm
from .models import Station


def login_view(request):
    if request.user.is_authenticated:
        return redirect('dashboard')

    next_url = request.GET.get('next') or ''

    if request.method == 'POST':
        form = LoginForm(request.POST)
        if form.is_valid():
            username = form.cleaned_data['username']
            password = form.cleaned_data['password']
            user = authenticate(request, username=username, password=password)
            if user is not None:
                login(request, user)
                return redirect(next_url or 'dashboard')
            messages.error(request, 'Invalid username or password.')
    else:
        form = LoginForm()

    return render(request, 'accounts/login.html', {
        'form': form,
        'next': next_url,
    })


def register_view(request):
    if request.user.is_authenticated:
        return redirect('dashboard')

    next_url = request.GET.get('next') or ''

    if request.method == 'POST':
        form = RegistrationForm(request.POST)
        if form.is_valid():
            user = form.save()
            login(request, user)
            return redirect(next_url or 'dashboard')
    else:
        form = RegistrationForm()

    return render(request, 'accounts/register.html', {
        'form': form,
        'next': next_url,
    })


@login_required
def logout_view(request):
    logout(request)
    return redirect('accounts:login')


@login_required
@user_passes_test(lambda u: u.is_superuser)
def switch_station(request):
    """
    Allow superusers to switch to a different station.
    Affects both dashboard AND admin views.
    """
    if request.method == 'POST':
        station = request.POST.get('station')
        valid_stations = [s[0] for s in Station.choices]
        
        if station in valid_stations:
            request.session['switched_station'] = station
            # Get display name
            station_display = dict(Station.choices).get(station, station)
            messages.success(request, f'✅ Switched to {station_display}')
        else:
            messages.error(request, 'Invalid station selected.')
        
        # Redirect back to where user came from, or dashboard
        next_url = request.POST.get('next') or request.META.get('HTTP_REFERER') or 'dashboard'
        return redirect(next_url)
    
    # GET request - return JSON list of stations
    return JsonResponse({
        'current_station': request.session.get('switched_station', ''),
        'stations': [
            {'value': s[0], 'label': s[1]}
            for s in Station.choices
        ]
    })


@login_required
@user_passes_test(lambda u: u.is_superuser)
def clear_station_switch(request):
    """Clear the station switch and return to user's default station."""
    if 'switched_station' in request.session:
        del request.session['switched_station']
        messages.success(request, '✅ Returned to your default station.')
    
    next_url = request.GET.get('next') or request.META.get('HTTP_REFERER') or 'dashboard'
    return redirect(next_url)
