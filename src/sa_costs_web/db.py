from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterator


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


class CostsRepository:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        conn = self._connect()
        try:
            with conn:
                yield conn
        finally:
            conn.close()

    def _init_db(self) -> None:
        with self._connection() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT NOT NULL UNIQUE,
                    role TEXT NOT NULL DEFAULT 'admin' CHECK(role IN ('admin', 'viewer')),
                    enabled INTEGER NOT NULL DEFAULT 1,
                    password_hash TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS billing_periods (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    starts_on TEXT NOT NULL UNIQUE,
                    utility_measured_kwh REAL,
                    has_inverter_data_issue INTEGER NOT NULL DEFAULT 0,
                    billing_source TEXT NOT NULL DEFAULT 'inverter' CHECK(billing_source IN ('inverter', 'utility')),
                    notes TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS tariff_bands (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scope TEXT NOT NULL CHECK(scope IN ('default', 'period')),
                    billing_period_id INTEGER,
                    position INTEGER NOT NULL DEFAULT 0,
                    label TEXT NOT NULL DEFAULT '',
                    from_kwh REAL NOT NULL,
                    to_kwh REAL,
                    price_per_kwh REAL NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY (billing_period_id) REFERENCES billing_periods(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS charge_rules (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scope TEXT NOT NULL CHECK(scope IN ('default', 'period')),
                    billing_period_id INTEGER,
                    position INTEGER NOT NULL DEFAULT 0,
                    kind TEXT NOT NULL CHECK(kind IN ('tax', 'fixed')),
                    section TEXT NOT NULL DEFAULT 'tax' CHECK(section IN ('service', 'tax')),
                    name TEXT NOT NULL,
                    alias TEXT,
                    expression TEXT,
                    amount REAL,
                    show_on_dashboard INTEGER NOT NULL DEFAULT 0,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY (billing_period_id) REFERENCES billing_periods(id) ON DELETE CASCADE
                );
                """
            )
            user_columns = {str(row["name"]) for row in conn.execute("PRAGMA table_info(users)").fetchall()}
            if "role" not in user_columns:
                conn.execute(
                    "ALTER TABLE users ADD COLUMN role TEXT NOT NULL DEFAULT 'admin'"
                )
            if "enabled" not in user_columns:
                conn.execute(
                    "ALTER TABLE users ADD COLUMN enabled INTEGER NOT NULL DEFAULT 1"
                )
            conn.execute(
                """
                UPDATE users
                SET role = 'admin'
                WHERE role IS NULL OR TRIM(role) = ''
                """
            )
            conn.execute(
                """
                UPDATE users
                SET enabled = 1
                WHERE enabled IS NULL
                """
            )
            billing_period_columns = {
                str(row["name"]) for row in conn.execute("PRAGMA table_info(billing_periods)").fetchall()
            }
            if "utility_measured_kwh" not in billing_period_columns:
                conn.execute("ALTER TABLE billing_periods ADD COLUMN utility_measured_kwh REAL")
            if "has_inverter_data_issue" not in billing_period_columns:
                conn.execute(
                    "ALTER TABLE billing_periods ADD COLUMN has_inverter_data_issue INTEGER NOT NULL DEFAULT 0"
                )
            if "billing_source" not in billing_period_columns:
                conn.execute(
                    "ALTER TABLE billing_periods ADD COLUMN billing_source TEXT NOT NULL DEFAULT 'inverter'"
                )
            conn.execute(
                """
                UPDATE billing_periods
                SET billing_source = CASE
                    WHEN utility_measured_kwh IS NOT NULL THEN 'utility'
                    ELSE 'inverter'
                END
                WHERE billing_source IS NULL OR TRIM(billing_source) = ''
                """
            )
            columns = {str(row["name"]) for row in conn.execute("PRAGMA table_info(charge_rules)").fetchall()}
            if "section" not in columns:
                conn.execute("ALTER TABLE charge_rules ADD COLUMN section TEXT")
            conn.execute(
                """
                UPDATE charge_rules
                SET section = CASE
                    WHEN kind = 'fixed' THEN 'service'
                    ELSE 'tax'
                END
                WHERE section IS NULL OR TRIM(section) = ''
                """
            )
            if "alias" not in columns:
                conn.execute("ALTER TABLE charge_rules ADD COLUMN alias TEXT")
            if "show_on_dashboard" not in columns:
                conn.execute(
                    "ALTER TABLE charge_rules ADD COLUMN show_on_dashboard INTEGER NOT NULL DEFAULT 0"
                )

    @staticmethod
    def _row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
        return dict(row) if row is not None else None

    def user_count(self) -> int:
        with self._connection() as conn:
            row = conn.execute("SELECT COUNT(*) AS total FROM users").fetchone()
        return int(row["total"]) if row is not None else 0

    def create_user(self, username: str, password_hash: str, *, role: str = "admin") -> int:
        now = utc_now()
        with self._connection() as conn:
            cursor = conn.execute(
                """
                INSERT INTO users (username, role, enabled, password_hash, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (username, role, 1, password_hash, now),
            )
            return int(cursor.lastrowid)

    def get_user_by_username(self, username: str) -> dict[str, Any] | None:
        with self._connection() as conn:
            row = conn.execute(
                "SELECT * FROM users WHERE username = ?",
                (username,),
            ).fetchone()
        return self._row_to_dict(row)

    def get_user_by_id(self, user_id: int) -> dict[str, Any] | None:
        with self._connection() as conn:
            row = conn.execute(
                "SELECT * FROM users WHERE id = ?",
                (user_id,),
            ).fetchone()
        return self._row_to_dict(row)

    def list_users(self) -> list[dict[str, Any]]:
        with self._connection() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM users
                ORDER BY username ASC, id ASC
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def update_user_password(self, user_id: int, password_hash: str) -> None:
        with self._connection() as conn:
            conn.execute(
                """
                UPDATE users
                SET password_hash = ?
                WHERE id = ?
                """,
                (password_hash, user_id),
            )

    def update_user_enabled(self, user_id: int, enabled: bool) -> None:
        with self._connection() as conn:
            conn.execute(
                """
                UPDATE users
                SET enabled = ?
                WHERE id = ?
                """,
                (1 if enabled else 0, user_id),
            )

    def list_billing_periods(self, *, ascending: bool = False) -> list[dict[str, Any]]:
        direction = "ASC" if ascending else "DESC"
        with self._connection() as conn:
            rows = conn.execute(
                f"""
                SELECT *
                FROM billing_periods
                ORDER BY starts_on {direction}, id {direction}
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def get_billing_period(self, period_id: int) -> dict[str, Any] | None:
        with self._connection() as conn:
            row = conn.execute(
                "SELECT * FROM billing_periods WHERE id = ?",
                (period_id,),
            ).fetchone()
        return self._row_to_dict(row)

    def save_billing_period(
        self,
        *,
        period_id: int | None,
        name: str,
        starts_on: str,
        utility_measured_kwh: float | None,
        has_inverter_data_issue: bool,
        billing_source: str,
        notes: str,
    ) -> int:
        now = utc_now()
        has_inverter_data_issue_int = 1 if has_inverter_data_issue else 0
        with self._connection() as conn:
            if period_id is None:
                cursor = conn.execute(
                    """
                    INSERT INTO billing_periods (
                        name, starts_on, utility_measured_kwh, has_inverter_data_issue, billing_source, notes, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        name,
                        starts_on,
                        utility_measured_kwh,
                        has_inverter_data_issue_int,
                        billing_source,
                        notes,
                        now,
                        now,
                    ),
                )
                return int(cursor.lastrowid)

            conn.execute(
                """
                UPDATE billing_periods
                SET name = ?, starts_on = ?, utility_measured_kwh = ?, has_inverter_data_issue = ?, billing_source = ?, notes = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    name,
                    starts_on,
                    utility_measured_kwh,
                    has_inverter_data_issue_int,
                    billing_source,
                    notes,
                    now,
                    period_id,
                ),
            )
            return period_id

    def update_billing_source(self, period_id: int, billing_source: str) -> None:
        with self._connection() as conn:
            conn.execute(
                """
                UPDATE billing_periods
                SET billing_source = ?, updated_at = ?
                WHERE id = ?
                """,
                (billing_source, utc_now(), period_id),
            )

    def delete_billing_period(self, period_id: int) -> None:
        with self._connection() as conn:
            conn.execute("DELETE FROM billing_periods WHERE id = ?", (period_id,))

    def find_latest_period_with_tariff_bands_before(
        self,
        starts_on: str,
        *,
        exclude_period_id: int | None = None,
    ) -> dict[str, Any] | None:
        sql = """
            SELECT DISTINCT bp.*
            FROM billing_periods bp
            JOIN tariff_bands tb
              ON tb.scope = 'period'
             AND tb.billing_period_id = bp.id
            WHERE bp.starts_on < ?
        """
        params: list[Any] = [starts_on]
        if exclude_period_id is not None:
            sql += " AND bp.id != ?"
            params.append(exclude_period_id)
        sql += " ORDER BY bp.starts_on DESC, bp.id DESC LIMIT 1"

        with self._connection() as conn:
            row = conn.execute(sql, params).fetchone()
        return self._row_to_dict(row)

    def find_latest_period_with_charge_rules_before(
        self,
        starts_on: str,
        *,
        kind: str,
        exclude_period_id: int | None = None,
    ) -> dict[str, Any] | None:
        sql = """
            SELECT DISTINCT bp.*
            FROM billing_periods bp
            JOIN charge_rules cr
              ON cr.scope = 'period'
             AND cr.kind = ?
             AND cr.billing_period_id = bp.id
            WHERE bp.starts_on < ?
        """
        params: list[Any] = [kind, starts_on]
        if exclude_period_id is not None:
            sql += " AND bp.id != ?"
            params.append(exclude_period_id)
        sql += " ORDER BY bp.starts_on DESC, bp.id DESC LIMIT 1"

        with self._connection() as conn:
            row = conn.execute(sql, params).fetchone()
        return self._row_to_dict(row)

    def list_tariff_bands(self, *, scope: str, billing_period_id: int | None = None) -> list[dict[str, Any]]:
        with self._connection() as conn:
            if scope == "default":
                rows = conn.execute(
                    """
                    SELECT *
                    FROM tariff_bands
                    WHERE scope = 'default'
                    ORDER BY position ASC, from_kwh ASC, id ASC
                    """
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT *
                    FROM tariff_bands
                    WHERE scope = 'period' AND billing_period_id = ?
                    ORDER BY position ASC, from_kwh ASC, id ASC
                    """,
                    (billing_period_id,),
                ).fetchall()
        return [dict(row) for row in rows]

    def save_tariff_band(
        self,
        *,
        band_id: int | None,
        scope: str,
        billing_period_id: int | None,
        position: int,
        label: str,
        from_kwh: float,
        to_kwh: float | None,
        price_per_kwh: float,
    ) -> int:
        now = utc_now()
        with self._connection() as conn:
            if band_id is None:
                cursor = conn.execute(
                    """
                    INSERT INTO tariff_bands (
                        scope, billing_period_id, position, label, from_kwh, to_kwh, price_per_kwh, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (scope, billing_period_id, position, label, from_kwh, to_kwh, price_per_kwh, now, now),
                )
                return int(cursor.lastrowid)

            conn.execute(
                """
                UPDATE tariff_bands
                SET position = ?, label = ?, from_kwh = ?, to_kwh = ?, price_per_kwh = ?, updated_at = ?
                WHERE id = ?
                """,
                (position, label, from_kwh, to_kwh, price_per_kwh, now, band_id),
            )
            return band_id

    def delete_tariff_band(self, band_id: int) -> None:
        with self._connection() as conn:
            conn.execute("DELETE FROM tariff_bands WHERE id = ?", (band_id,))

    def copy_tariff_bands_to_period(self, *, billing_period_id: int, source_bands: list[dict[str, Any]]) -> int:
        if not source_bands:
            return 0

        now = utc_now()
        copied = 0
        with self._connection() as conn:
            for band in source_bands:
                conn.execute(
                    """
                    INSERT INTO tariff_bands (
                        scope, billing_period_id, position, label, from_kwh, to_kwh, price_per_kwh, created_at, updated_at
                    )
                    VALUES ('period', ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        billing_period_id,
                        int(band.get("position") or 0),
                        str(band.get("label") or ""),
                        float(band.get("from_kwh") or 0.0),
                        float(band["to_kwh"]) if band.get("to_kwh") is not None else None,
                        float(band.get("price_per_kwh") or 0.0),
                        now,
                        now,
                    ),
                )
                copied += 1
        return copied

    def copy_charge_rules_to_period(
        self,
        *,
        billing_period_id: int,
        source_rules: list[dict[str, Any]],
        kind: str,
    ) -> int:
        if not source_rules:
            return 0

        now = utc_now()
        copied = 0
        with self._connection() as conn:
            for rule in source_rules:
                conn.execute(
                    """
                    INSERT INTO charge_rules (
                        scope, billing_period_id, position, kind, section, name, alias, expression, amount, show_on_dashboard, enabled, created_at, updated_at
                    )
                    VALUES ('period', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        billing_period_id,
                        int(rule.get("position") or 0),
                        kind,
                        str(rule.get("section") or ("service" if kind == "fixed" else "tax")),
                        str(rule.get("name") or ""),
                        str(rule["alias"]) if rule.get("alias") is not None else None,
                        str(rule["expression"]) if rule.get("expression") is not None else None,
                        float(rule["amount"]) if rule.get("amount") is not None else None,
                        1 if rule.get("show_on_dashboard", 0) else 0,
                        1 if rule.get("enabled", 1) else 0,
                        now,
                        now,
                    ),
                )
                copied += 1
        return copied

    def list_charge_rules(
        self,
        *,
        scope: str,
        kind: str | None = None,
        billing_period_id: int | None = None,
    ) -> list[dict[str, Any]]:
        clauses = ["scope = ?"]
        params: list[Any] = [scope]

        if scope == "period":
            clauses.append("billing_period_id = ?")
            params.append(billing_period_id)
        if kind is not None:
            clauses.append("kind = ?")
            params.append(kind)

        sql = f"""
            SELECT *
            FROM charge_rules
            WHERE {' AND '.join(clauses)}
            ORDER BY position ASC, id ASC
        """

        with self._connection() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [dict(row) for row in rows]

    def save_charge_rule(
        self,
        *,
        rule_id: int | None,
        scope: str,
        billing_period_id: int | None,
        position: int,
        kind: str,
        section: str,
        name: str,
        alias: str | None,
        expression: str | None,
        amount: float | None,
        show_on_dashboard: bool,
        enabled: bool,
    ) -> int:
        now = utc_now()
        enabled_int = 1 if enabled else 0
        show_on_dashboard_int = 1 if show_on_dashboard else 0
        with self._connection() as conn:
            if rule_id is None:
                cursor = conn.execute(
                    """
                    INSERT INTO charge_rules (
                        scope, billing_period_id, position, kind, section, name, alias, expression, amount, show_on_dashboard, enabled, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        scope,
                        billing_period_id,
                        position,
                        kind,
                        section,
                        name,
                        alias,
                        expression,
                        amount,
                        show_on_dashboard_int,
                        enabled_int,
                        now,
                        now,
                    ),
                )
                return int(cursor.lastrowid)

            conn.execute(
                """
                UPDATE charge_rules
                SET position = ?, section = ?, name = ?, alias = ?, expression = ?, amount = ?, show_on_dashboard = ?, enabled = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    position,
                    section,
                    name,
                    alias,
                    expression,
                    amount,
                    show_on_dashboard_int,
                    enabled_int,
                    now,
                    rule_id,
                ),
            )
            return rule_id

    def delete_charge_rule(self, rule_id: int) -> None:
        with self._connection() as conn:
            conn.execute("DELETE FROM charge_rules WHERE id = ?", (rule_id,))

    def get_effective_tariff_bands(self, billing_period_id: int) -> list[dict[str, Any]]:
        period_bands = self.list_tariff_bands(scope="period", billing_period_id=billing_period_id)
        if period_bands:
            return period_bands
        return self.list_tariff_bands(scope="default")

    def get_effective_fixed_charges(self, billing_period_id: int) -> list[dict[str, Any]]:
        period_rules = self.list_charge_rules(scope="period", kind="fixed", billing_period_id=billing_period_id)
        if period_rules:
            return period_rules
        return self.list_charge_rules(scope="default", kind="fixed")

    def get_effective_tax_rules(self, billing_period_id: int) -> list[dict[str, Any]]:
        period_rules = self.list_charge_rules(scope="period", kind="tax", billing_period_id=billing_period_id)
        if period_rules:
            return period_rules
        return self.list_charge_rules(scope="default", kind="tax")
