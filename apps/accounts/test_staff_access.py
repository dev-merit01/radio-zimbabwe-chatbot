from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings
from django.urls import reverse

from apps.accounts.models import BroadcastStation, StaffAccess, PresenterAssignment
from apps.voting.models import ChartShow


@override_settings(SECURE_SSL_REDIRECT=False)
class StaffAccessTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.admin = get_user_model().objects.create_superuser("owner")
        cls.home = reverse("accounts:staff_home")
        cls.accounts = reverse("accounts:staff_accounts")
        cls.station = BroadcastStation.objects.get(pk="radio_zimbabwe")
        cls.show = ChartShow.objects.get(station=cls.station.pk, code="G20")
        cls.other = ChartShow.objects.get(station="national_fm", code="N20")

    def create(self, role, privileges=(), **values):
        self.client.force_login(self.admin)
        data = dict(
            username="new_" + role,
            password="Example-local-Testing394!",
            stations=[self.station.pk],
            privileges=privileges,
            active="on",
        )
        data.update(values)
        response = self.client.post(self.accounts + "?role=" + role, data)
        self.assertRedirects(response, self.accounts, fetch_redirect_response=False)
        user = get_user_model().objects.get(username=data["username"])
        self.assertFalse(user.is_staff or user.is_superuser)
        self.client.logout()
        self.client.post(
            reverse("accounts:login"),
            {"username": user.username, "password": data["password"]},
        )
        self.assertEqual(str(user.pk), self.client.session.get("_auth_user_id"))
        return user

    def test_station_manager_scope_through_direct_links_and_exports(self):
        self.create("manager", ["export"])
        page = self.client.get(self.home)
        self.assertContains(page, self.show.name)
        self.assertNotContains(page, self.other.name)
        self.assertEqual(
            self.client.get(self.home + f"?show={self.other.pk}&export=1").status_code,
            404,
        )
        url = self.home + f"?show={self.show.pk}"
        self.assertContains(
            self.client.get(url + "&export=1"), "Rank,Artist,Title,Votes"
        )
        self.assertEqual(self.client.post(url, {"action": "publish"}).status_code, 403)
        self.assertEqual(self.client.get(self.accounts).status_code, 403)
        for path in [
            "/api/workspace/intelligence",
            "/api/workspace/audit",
            "/api/workspace/health",
            "/api/workspace/shows",
            "/api/licensing/status",
            "/admin/",
            "/licensing/activate/",
        ]:
            self.assertEqual(self.client.get(path).status_code, 403, path)

    def test_director_sees_new_stations_and_configurable_actions(self):
        self.create("director", ["shows"])
        new = BroadcastStation.objects.create(
            slug="new", name="New Network Station", is_active=True
        )
        self.assertContains(self.client.get(self.home), new.name)
        self.assertContains(self.client.get(self.home), self.other.name)
        url = self.home + f"?show={self.other.pk}"
        self.assertEqual(
            self.client.post(
                url, dict(action="shows", presenter="Morning team", daily_limit=7)
            ).status_code,
            302,
        )
        self.other.refresh_from_db()
        self.assertEqual(self.other.daily_limit, 7)
        self.assertEqual(self.client.get(url + "&export=1").status_code, 403)

    def test_admin_cannot_escalate_or_grant_outside_scope(self):
        self.create("admin", ["presenters", "staff", "export"])
        for role in ("admin", "director"):
            self.assertEqual(
                self.client.get(self.accounts + "?role=" + role).status_code, 403
            )
        data = dict(
            username="delegated",
            password="Example-local-Testing394!",
            active="on",
            stations=["national_fm"],
            privileges=["export"],
        )
        self.client.post(self.accounts + "?role=manager", data)
        self.assertFalse(get_user_model().objects.filter(username="delegated").exists())
        data.update(stations=[self.station.pk], privileges=["publish"])
        self.client.post(self.accounts + "?role=manager", data)
        self.assertFalse(get_user_model().objects.filter(username="delegated").exists())
        data.update(privileges=["export"])
        self.client.post(self.accounts + "?role=manager", data)
        row = StaffAccess.objects.get(user__username="delegated")
        self.assertEqual(row.privileges, ["export"])
        self.assertContains(self.client.get(self.accounts), "delegated")
        self.assertFalse(row.network_access)

    def test_managed_presenter_is_bound_to_assigned_show(self):
        user = self.create("presenter", programme=self.show.pk)
        self.assertEqual(
            PresenterAssignment.objects.get(user=user).chart_show, self.show
        )
        self.client.logout()
        other = ChartShow.objects.get(station=self.station.pk, code="GP20")
        self.client.post(
            reverse("accounts:login"),
            {
                "username": user.username,
                "password": "Example-local-Testing394!",
                "chart_show": other.pk,
            },
        )
        self.assertEqual(self.client.session["airvote_chart_show_id"], self.show.pk)
        self.assertEqual(self.client.get(self.home).status_code, 403)

    def test_super_admin_edits_existing_grant_and_can_disable_account(self):
        user = self.create("manager")
        self.client.force_login(self.admin)
        self.client.post(
            self.accounts + f"?edit={user.staff_access.pk}",
            {
                "username": user.username,
                "stations": ["national_fm"],
                "privileges": ["publish"],
            },
        )
        user.refresh_from_db()
        self.assertFalse(user.is_active)
        self.assertEqual(user.staff_access.privileges, ["publish"])
        self.assertTrue(user.check_password("Example-local-Testing394!"))

    def test_defaults_and_privilege_changes_apply_to_open_sessions(self):
        self.client.force_login(self.admin)
        for role, expected in (
            ("manager", {"review", "export", "archives"}),
            ("director", {"export", "archives"}),
        ):
            page = self.client.get(self.accounts + "?role=" + role)
            self.assertEqual(set(page.context["form"]["privileges"].value()), expected)
        user = self.create("manager", ["export", "presenters"])
        self.assertContains(self.client.get(self.accounts), "Presenter")
        user.staff_access.privileges = []
        user.staff_access.save()
        self.assertEqual(self.client.get(self.accounts).status_code, 403)
        self.assertEqual(
            self.client.get(self.home + f"?show={self.show.pk}&export=1").status_code,
            403,
        )

    def test_admin_network_grant_still_excludes_sensitive_controls(self):
        self.create("admin", ["staff"], network_access="on", stations=[])
        self.assertContains(self.client.get(self.home), self.other.name)
        for path in (
            "/api/workspace/shows/multiplier",
            "/api/workspace/station-password",
            "/api/workspace/intelligence/save",
            "/api/workspace/companies/create",
        ):
            self.assertEqual(self.client.post(path, {}).status_code, 403)
        self.assertNotContains(
            self.client.get(self.home + f"?show={self.other.pk}"), "multiplier"
        )

    def test_show_selection_is_checked_on_read_write_and_delegation(self):
        other_show = ChartShow.objects.get(station=self.station.pk, code="GP20")
        user = self.create(
            "manager",
            ["shows", "presenters"],
            selected_shows_only="true",
            shows=[self.show.pk],
            role_description="Weekend producer",
        )
        self.assertContains(self.client.get(self.home), "Weekend producer")
        self.assertNotContains(self.client.get(self.home), other_show.name)
        url = self.home + f"?show={other_show.pk}"
        self.assertEqual(self.client.get(url).status_code, 404)
        self.assertEqual(
            self.client.post(url, {"action": "shows", "daily_limit": 10}).status_code,
            404,
        )
        data = {
            "username": "outside",
            "password": "Test-account-38924!",
            "programme": other_show.pk,
            "active": "on",
        }
        self.client.post(self.accounts + "?role=presenter", data)
        self.assertFalse(get_user_model().objects.filter(username="outside").exists())
        self.assertTrue(user.staff_access.selected_shows_only)

    def test_readonly_presenter_and_director(self):
        self.create("presenter", programme=self.show.pk)
        self.assertEqual(
            self.client.post(
                "/api/workspace/eligibility/create", {}, content_type="application/json"
            ).status_code,
            403,
        )
        self.assertEqual(self.client.get("/api/workspace/export").status_code, 403)
        self.assertEqual(self.client.get("/api/chart/archives").status_code, 403)
        self.create("director")
        url = self.home + f"?show={self.other.pk}"
        self.assertContains(self.client.get(url), "Live chart")
        for action in ("publish", "shows", "review"):
            self.assertEqual(self.client.post(url, {"action": action}).status_code, 403)

    def test_reject_shows_from_unselected_station_and_empty_selection(self):
        self.client.force_login(self.admin)
        data = {
            "username": "wrong-scope",
            "password": "Test-account-38924!",
            "stations": [self.station.pk],
            "selected_shows_only": "true",
            "active": "on",
        }
        for selected in ([], [self.other.pk]):
            response = self.client.post(
                self.accounts + "?role=manager", {**data, "shows": selected}
            )
            self.assertTrue(response.context["form"].errors.get("shows"))
        self.assertFalse(
            get_user_model().objects.filter(username="wrong-scope").exists()
        )
