from django import forms
from django.contrib.auth import get_user_model
from django.contrib.auth.password_validation import validate_password
from django.core.exceptions import ValidationError
from django.db import transaction

from .models import AccountProfile, station_choices


User = get_user_model()


class RegistrationForm(forms.Form):
    first_name = forms.CharField(max_length=150)
    last_name = forms.CharField(max_length=150)
    username = forms.CharField(max_length=150)
    station = forms.ChoiceField(choices=(), required=True)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["station"].choices = station_choices(active_only=True)
    password = forms.CharField(widget=forms.PasswordInput)
    confirm_password = forms.CharField(widget=forms.PasswordInput)

    def clean_username(self):
        username = (self.cleaned_data.get("username") or "").strip()
        if not username:
            raise forms.ValidationError("Username is required.")
        if User.objects.filter(username__iexact=username).exists():
            raise forms.ValidationError("That username is already taken.")
        return username

    def clean(self):
        cleaned = super().clean()
        password = cleaned.get("password")
        confirm_password = cleaned.get("confirm_password")
        if password and confirm_password and password != confirm_password:
            self.add_error("confirm_password", "Passwords do not match.")
        if password:
            candidate = User(
                username=cleaned.get("username", ""),
                first_name=cleaned.get("first_name", ""),
                last_name=cleaned.get("last_name", ""),
            )
            try:
                validate_password(password, candidate)
            except ValidationError as exc:
                self.add_error("password", exc)
        return cleaned

    @transaction.atomic
    def save(self) -> User:
        user = User.objects.create_user(
            is_active=False,
            username=self.cleaned_data["username"],
            password=self.cleaned_data["password"],
            first_name=self.cleaned_data["first_name"],
            last_name=self.cleaned_data["last_name"],
        )
        # profile is auto-created via signal, but we update station here
        AccountProfile.objects.update_or_create(
            user=user,
            defaults={"station": self.cleaned_data["station"]},
        )
        return user


class LoginForm(forms.Form):
    username = forms.CharField(max_length=150)
    password = forms.CharField(widget=forms.PasswordInput)
    chart_show = forms.ChoiceField(choices=(), required=False)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        from apps.voting.models import ChartShow

        stations = station_choices(active_only=True)
        station_ids = [slug for slug, _ in stations]
        grouped = {slug: [] for slug in station_ids}
        rows = ChartShow.objects.filter(
            station__in=station_ids, kind__in=("weekly", "year_end"),
            is_archived=False,
        ).order_by("name", "pk").values_list("station", "pk", "name")
        for station, show_id, show_name in rows:
            grouped[station].append((str(show_id), show_name))
        self.fields["chart_show"].choices = [
            ("", "Choose your chart show"),
            *[(name, grouped[slug]) for slug, name in stations if grouped[slug]],
        ]


class ProfileForm(forms.ModelForm):
    class Meta:
        model = User
        fields = ("first_name", "last_name", "email")
