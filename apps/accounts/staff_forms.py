from django import forms
from django.contrib.auth import get_user_model
from django.contrib.auth.password_validation import validate_password
from django.core.exceptions import ValidationError
from django.db import transaction

from apps.voting.models import ChartShow, ReviewAudit
from .models import AccountProfile, BroadcastStation, PresenterAssignment, StaffAccess
from .staff_policy import DEFAULTS, PRIVILEGES, Role, permitted


class StaffAccountForm(forms.Form):
    username = forms.CharField(min_length=3, max_length=150)
    password = forms.CharField(
        min_length=12,
        max_length=128,
        strip=False,
        widget=forms.PasswordInput(attrs={"autocomplete": "new-password"}),
    )
    stations = forms.ModelMultipleChoiceField(
        queryset=BroadcastStation.objects.none(), widget=forms.CheckboxSelectMultiple
    )
    programme = forms.ModelChoiceField(queryset=ChartShow.objects.none())
    privileges = forms.MultipleChoiceField(
        required=False, widget=forms.CheckboxSelectMultiple
    )
    network_access = forms.BooleanField(
        required=False, label="All active stations, including future stations"
    )
    active = forms.BooleanField(required=False, initial=True, label="Account enabled")

    def __init__(self, *args, actor, role, scope, target=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.role, self.actor, self.target = role, actor, target
        self.fields["stations"].queryset = scope
        if role != Role.ADMIN or not actor.is_superuser:
            del self.fields["network_access"]
        else:
            self.fields["stations"].required = False
        if role == Role.PRESENTER:
            del self.fields["stations"], self.fields["privileges"]
            self.fields["programme"].queryset = ChartShow.objects.filter(
                station__in=scope.values("slug"),
                is_archived=False,
                kind__in=("weekly", "year_end"),
            )
        else:
            del self.fields["programme"]
            if role == Role.DIRECTOR:
                del self.fields["stations"]
            choices = [
                (key, label)
                for key, label in PRIVILEGES.items()
                if permitted(actor, key) and (key != "staff" or role == Role.ADMIN)
            ]
            self.fields["privileges"].choices = choices
            self.initial["privileges"] = [
                key for key, _ in choices if key in DEFAULTS[role]
            ]
        if target:
            self.fields["password"].required = False
            self.fields[
                "password"
            ].help_text = "Leave blank to keep the current password."
            self.initial.update(
                network_access=target.network_access,
                username=target.user.username,
                active=target.user.is_active,
                privileges=target.privileges,
                stations=list(target.stations.all()),
            )
            if role == Role.PRESENTER:
                assignment = getattr(target.user, "presenter_assignment", None)
                self.initial["programme"] = (
                    assignment.chart_show_id if assignment else None
                )

    def clean(self):
        data = super().clean()
        username = data.get("username", "")
        users = get_user_model().objects.filter(username__iexact=username)
        if self.target:
            users = users.exclude(pk=self.target.user_id)
        if users.exists():
            self.add_error("username", "This username is already in use.")
        if self.role == Role.MANAGER and len(data.get("stations", [])) != 1:
            self.add_error("stations", "Choose one station for this manager.")
        if (
            self.role == Role.ADMIN
            and not data.get("network_access")
            and not data.get("stations")
        ):
            self.add_error("stations", "Select stations or grant network access.")
        candidate = get_user_model()(username=username)
        if username:
            try:
                candidate._meta.get_field("username").run_validators(username)
            except ValidationError as exc:
                self.add_error("username", exc)
        if data.get("password"):
            try:
                validate_password(data["password"], candidate)
            except ValidationError as exc:
                self.add_error("password", exc)
        return data

    @transaction.atomic
    def save(self):
        data = self.cleaned_data
        user = self.target.user if self.target else get_user_model()()
        user.username, user.is_active = data["username"], data["active"]
        if data.get("password"):
            user.set_password(data["password"])
        user.save()
        grant, _ = StaffAccess.objects.update_or_create(
            user=user,
            defaults={
                "role": self.role,
                "privileges": data.get("privileges", []),
                "network_access": data.get("network_access", False),
            },
        )
        if self.role == Role.PRESENTER:
            show = data["programme"]
            stations = [show.station]
            PresenterAssignment.objects.update_or_create(
                user=user, defaults={"chart_show": show}
            )
        else:
            stations = [station.slug for station in data.get("stations", [])]
        grant.stations.set(stations)
        profile, _ = AccountProfile.objects.get_or_create(user=user)
        if stations:
            profile.station = stations[0]
            profile.save(update_fields=["station"])
        ReviewAudit.objects.create(
            station=profile.station,
            actor=self.actor,
            action="staff_access_saved",
            details={
                "user_id": user.pk,
                "role": self.role,
                "stations": stations,
                "privileges": grant.privileges,
                "active": user.is_active,
            },
        )
        return grant
