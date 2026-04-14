from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from sa_costs_web.app import create_app
from sa_costs_web.calculator import BridgeData, calculate_period_summary
from sa_costs_web.config import WebConfig
from sa_costs_web.db import CostsRepository
from werkzeug.security import check_password_hash, generate_password_hash


class RepositoryMigrationTests(unittest.TestCase):
    def test_repository_migrates_period_columns(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "legacy.sqlite3"
            conn = sqlite3.connect(db_path)
            conn.executescript(
                """
                CREATE TABLE users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT NOT NULL UNIQUE,
                    password_hash TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE billing_periods (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    starts_on TEXT NOT NULL UNIQUE,
                    notes TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """
            )
            conn.close()

            CostsRepository(db_path)

            conn = sqlite3.connect(db_path)
            period_columns = [row[1] for row in conn.execute("PRAGMA table_info(billing_periods)").fetchall()]
            user_columns = [row[1] for row in conn.execute("PRAGMA table_info(users)").fetchall()]
            charge_columns = [row[1] for row in conn.execute("PRAGMA table_info(charge_rules)").fetchall()]
            conn.close()

            self.assertIn("utility_measured_kwh", period_columns)
            self.assertIn("has_inverter_data_issue", period_columns)
            self.assertIn("billing_source", period_columns)
            self.assertIn("role", user_columns)
            self.assertIn("enabled", user_columns)
            self.assertIn("show_on_dashboard", charge_columns)


class CalculatorTests(unittest.TestCase):
    def test_calculate_period_summary_prefers_company_meter_reading(self) -> None:
        summary = calculate_period_summary(
            {
                "id": 1,
                "name": "Abril 2026",
                "effective_start": "2026-04-01",
                "effective_end": "2026-04-30",
                "utility_measured_kwh": 150,
                "billing_source": "utility",
            },
            [
                {"iso": "2026-04-03T00:00:00-03:00", "grid_kwh": 40, "load_kwh": 55, "solar_pv_kwh": 20},
                {"iso": "2026-04-12T00:00:00-03:00", "grid_kwh": 60, "load_kwh": 65, "solar_pv_kwh": 24},
            ],
            tariff_bands=[
                {
                    "id": 1,
                    "scope": "period",
                    "position": 1,
                    "from_kwh": 0,
                    "to_kwh": None,
                    "price_per_kwh": 2,
                }
            ],
            fixed_charges=[],
            tax_rules=[],
        )

        self.assertEqual(summary["inverter_consumption_kwh"], 100.0)
        self.assertEqual(summary["utility_consumption_kwh"], 150.0)
        self.assertEqual(summary["billing_consumption_kwh"], 150.0)
        self.assertEqual(summary["consumption_source"], "utility")
        self.assertEqual(summary["consumption_difference_kwh"], 50.0)
        self.assertEqual(summary["energy_cost"], 300.0)
        self.assertEqual(summary["selected_variant"]["total"], 300.0)
        self.assertEqual(summary["alternate_variant"]["total"], 200.0)
        self.assertEqual(summary["inverter_load_kwh"], 120.0)
        self.assertEqual(summary["solar_pv_kwh"], 44.0)
        self.assertEqual(summary["load_variant"]["total"], 240.0)
        self.assertEqual(summary["solar_savings_total"], -60.0)
        self.assertEqual(len(summary["daily_energy_cost_breakdown"]), 30)
        self.assertEqual(summary["daily_energy_cost_breakdown"][2]["billed_kwh"], 60.0)
        self.assertEqual(summary["daily_energy_cost_breakdown"][11]["billed_kwh"], 90.0)
        self.assertEqual(summary["daily_energy_cost_breakdown"][-1]["cumulative_energy_cost"], 300.0)
        self.assertTrue(summary["has_missing_days"])
        self.assertTrue(summary["has_inverter_issue"])


class WebAppTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp_dir.name) / "energy_costs.sqlite3"
        self.config = WebConfig(
            bridge_url="http://bridge.local",
            bind_host="127.0.0.1",
            bind_port=8890,
            db_path=self.db_path,
            secret_key="test-secret",
            log_level="INFO",
            http_timeout=1.0,
        )
        self.app = create_app(self.config)
        self.app.testing = True

        with self.app.app_context():
            repo = self.app.extensions["repo"]
            self.user_id = repo.create_user("admin", generate_password_hash("admin123"), role="admin")

        self.client = self.app.test_client()
        with self.client.session_transaction() as session:
            session["user_id"] = self.user_id

    def tearDown(self) -> None:
        self.tmp_dir.cleanup()

    def test_save_period_persists_company_meter_reading_and_manual_issue_flag(self) -> None:
        response = self.client.post(
            "/periods/save",
            data={
                "name": "Factura abril 2026",
                "starts_on": "2026-04-01",
                "utility_measured_kwh": "143.5",
                "has_inverter_data_issue": "on",
                "billing_source": "utility",
                "notes": "Lectura del medidor de la compania",
            },
        )

        self.assertEqual(response.status_code, 302)

        with self.app.app_context():
            period = self.app.extensions["repo"].list_billing_periods()[0]

        self.assertEqual(period["name"], "Factura abril 2026")
        self.assertEqual(period["utility_measured_kwh"], 143.5)
        self.assertEqual(period["has_inverter_data_issue"], 1)
        self.assertEqual(period["billing_source"], "utility")

    def test_dashboard_and_period_detail_show_charts_and_measurement_status(self) -> None:
        with self.app.app_context():
            repo = self.app.extensions["repo"]
            previous_period_id = repo.save_billing_period(
                period_id=None,
                name="Factura mayo 2025",
                starts_on="2025-05-01",
                utility_measured_kwh=120.0,
                has_inverter_data_issue=False,
                billing_source="utility",
                notes="Periodo historico",
            )
            repo.save_tariff_band(
                band_id=None,
                scope="period",
                billing_period_id=previous_period_id,
                position=1,
                label="General",
                from_kwh=0.0,
                to_kwh=None,
                price_per_kwh=1.5,
            )
            repo.save_charge_rule(
                rule_id=None,
                scope="period",
                billing_period_id=previous_period_id,
                position=1,
                kind="fixed",
                section="service",
                name="Cargo fijo",
                alias="cargo_fijo",
                expression=None,
                amount=30.0,
                show_on_dashboard=True,
                enabled=True,
            )
            period_id = repo.save_billing_period(
                period_id=None,
                name="Factura abril 2026",
                starts_on="2026-04-01",
                utility_measured_kwh=150.0,
                has_inverter_data_issue=False,
                billing_source="utility",
                notes="Periodo con lectura real",
            )
            repo.save_tariff_band(
                band_id=None,
                scope="period",
                billing_period_id=period_id,
                position=1,
                label="General",
                from_kwh=0.0,
                to_kwh=None,
                price_per_kwh=2.0,
            )
            repo.save_charge_rule(
                rule_id=None,
                scope="period",
                billing_period_id=period_id,
                position=1,
                kind="fixed",
                section="service",
                name="Cargo fijo",
                alias="cargo_fijo",
                expression=None,
                amount=40.0,
                show_on_dashboard=True,
                enabled=True,
            )

        bridge_data = BridgeData(
            status={
                "connected": True,
                "last_message_at": "2026-04-11T00:00:00+00:00",
                "topic": "daily-data",
            },
            points=[
                {"iso": "2026-04-05T00:00:00-03:00", "grid_kwh": 50.0, "load_kwh": 76.0, "solar_pv_kwh": 18.0},
                {"iso": "2026-04-09T00:00:00-03:00", "grid_kwh": 70.0, "load_kwh": 84.0, "solar_pv_kwh": 22.0},
            ],
        )

        with patch("sa_costs_web.app.fetch_bridge_data", return_value=bridge_data):
            dashboard_response = self.client.get("/")
            period_response = self.client.get(f"/periods/{period_id}")
            periods_response = self.client.get("/periods")
            manifest_response = self.client.get("/manifest.webmanifest")
            sw_response = self.client.get("/sw.js")
            switch_response = self.client.post(
                f"/periods/{period_id}/billing-source",
                data={"billing_source": "inverter"},
                follow_redirects=True,
            )

        dashboard_html = dashboard_response.get_data(as_text=True)
        period_html = period_response.get_data(as_text=True)
        periods_html = periods_response.get_data(as_text=True)
        manifest_html = manifest_response.get_data(as_text=True)
        sw_js = sw_response.get_data(as_text=True)
        switch_html = switch_response.get_data(as_text=True)

        self.assertEqual(dashboard_response.status_code, 200)
        self.assertEqual(period_response.status_code, 200)
        self.assertEqual(periods_response.status_code, 200)
        self.assertEqual(manifest_response.status_code, 200)
        self.assertEqual(sw_response.status_code, 200)
        self.assertEqual(switch_response.status_code, 200)
        self.assertIn('"display": "standalone"', manifest_html)
        self.assertIn("CACHE_NAME", sw_js)
        self.assertIn("Consumo por periodo", dashboard_html)
        self.assertIn("Costos por periodo", dashboard_html)
        self.assertIn("Precio por franja", dashboard_html)
        self.assertIn("Cargos fijos visibles", dashboard_html)
        self.assertIn("Variacion anual", dashboard_html)
        self.assertIn("Factura mayo 2025", dashboard_html)
        self.assertIn("Load", dashboard_html)
        self.assertIn("Solar PV", dashboard_html)
        self.assertIn("echarts.min.js", dashboard_html)
        self.assertIn("data-echart", dashboard_html)
        self.assertIn("Integridad inversor", dashboard_html)
        self.assertIn("Barras", dashboard_html)
        self.assertIn("Area", dashboard_html)
        self.assertIn("Revisar", dashboard_html)
        self.assertIn("Tomado de la compania", dashboard_html)
        self.assertIn("metric-card-money", dashboard_html)
        self.assertIn("metric-card-kwh", dashboard_html)
        self.assertIn("Mediciones del inversor", period_html)
        self.assertIn("Faltantes detectados", period_html)
        self.assertIn("Marcar error de medicion del inversor", period_html)
        self.assertIn("Lectura cargada manualmente", period_html)
        self.assertIn("Carga total del inversor", period_html)
        self.assertIn("Generacion solar", period_html)
        self.assertIn("Factura teorica sin solar", period_html)
        self.assertIn("Ahorro solar estimado", period_html)
        self.assertIn("Con red", period_html)
        self.assertIn("Costo de energia por dia", period_html)
        self.assertIn("Costo diario de energia", period_html)
        self.assertIn("Red y Solar PV por dia", period_html)
        self.assertIn("Usado para calcular", period_html)
        self.assertIn("Con medicion", period_html)
        self.assertIn("Marcar error de medicion del inversor en este periodo", periods_html)
        self.assertIn("Tomado de la red del inversor", switch_html)

    def test_viewer_cannot_access_management_and_can_change_password(self) -> None:
        with self.app.app_context():
            repo = self.app.extensions["repo"]
            viewer_id = repo.create_user("viewer", generate_password_hash("viewer123"), role="viewer")
            period_id = repo.save_billing_period(
                period_id=None,
                name="Factura mayo 2026",
                starts_on="2026-05-01",
                utility_measured_kwh=None,
                has_inverter_data_issue=False,
                billing_source="inverter",
                notes="Solo lectura",
            )

        viewer_client = self.app.test_client()
        with viewer_client.session_transaction() as session:
            session["user_id"] = viewer_id

        bridge_data = BridgeData(
            status={"connected": True, "last_message_at": "2026-04-11T00:00:00+00:00"},
            points=[],
        )
        with patch("sa_costs_web.app.fetch_bridge_data", return_value=bridge_data):
            periods_response = viewer_client.get("/periods", follow_redirects=True)
            settings_response = viewer_client.get("/settings", follow_redirects=True)
            detail_response = viewer_client.get(f"/periods/{period_id}")
            password_response = viewer_client.post(
                "/account/password",
                data={
                    "current_password": "viewer123",
                    "new_password": "viewer456",
                    "confirm_password": "viewer456",
                },
                follow_redirects=True,
            )

        self.assertEqual(periods_response.status_code, 200)
        self.assertEqual(settings_response.status_code, 200)
        self.assertEqual(detail_response.status_code, 200)
        self.assertEqual(password_response.status_code, 200)
        self.assertIn("solo tiene acceso de visualizacion", periods_response.get_data(as_text=True))
        self.assertIn("solo tiene acceso de visualizacion", settings_response.get_data(as_text=True))
        self.assertNotIn("Guardar cambios", detail_response.get_data(as_text=True))
        self.assertNotIn('href="/settings"', detail_response.get_data(as_text=True))
        self.assertNotIn('href="/periods"', detail_response.get_data(as_text=True))
        self.assertIn("Contrasena actualizada", password_response.get_data(as_text=True))

        with self.app.app_context():
            updated_user = self.app.extensions["repo"].get_user_by_username("viewer")
        self.assertIsNotNone(updated_user)
        self.assertTrue(check_password_hash(str(updated_user["password_hash"]), "viewer456"))

    def test_admin_can_disable_other_user_and_disabled_user_cannot_login(self) -> None:
        with self.app.app_context():
            repo = self.app.extensions["repo"]
            viewer_id = repo.create_user("viewer2", generate_password_hash("viewer123"), role="viewer")

        disable_response = self.client.post(
            f"/account/users/{viewer_id}/toggle-enabled",
            data={"enabled": "0"},
            follow_redirects=True,
        )
        self.assertEqual(disable_response.status_code, 200)
        self.assertIn("Usuario deshabilitado", disable_response.get_data(as_text=True))

        login_client = self.app.test_client()
        login_response = login_client.post(
            "/login",
            data={"username": "viewer2", "password": "viewer123"},
            follow_redirects=True,
        )
        self.assertEqual(login_response.status_code, 200)
        self.assertIn("Usuario o contrasena incorrectos", login_response.get_data(as_text=True))

        disabled_session_client = self.app.test_client()
        with disabled_session_client.session_transaction() as session:
            session["user_id"] = viewer_id
        redirect_response = disabled_session_client.get("/", follow_redirects=True)
        self.assertEqual(redirect_response.status_code, 200)
        self.assertIn("usuario esta deshabilitado", redirect_response.get_data(as_text=True))


if __name__ == "__main__":
    unittest.main()
