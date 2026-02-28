"""
freshness_check.py — Lightweight data freshness monitor for candata.

Queries each pipeline table in Supabase to determine how fresh the data is,
generates a report (stdout table + JSON file), and optionally sends an email
alert when any table exceeds its maximum allowed staleness.

Usage:
    python monitoring/freshness_check.py
"""

from __future__ import annotations

import json
import os
import smtplib
import sys
from datetime import date, datetime, timezone
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Bootstrap — make sure candata_shared is importable even when running
# this script directly from the repo root.
# ---------------------------------------------------------------------------
_REPO_ROOT = Path(__file__).resolve().parent.parent
_SHARED_SRC = _REPO_ROOT / "shared" / "python" / "src"
if str(_SHARED_SRC) not in sys.path:
    sys.path.insert(0, str(_SHARED_SRC))

from candata_shared.db import get_supabase_client  # noqa: E402

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

FRESHNESS_CONFIG: dict[str, dict[str, Any]] = {
    "cmhc_housing": {
        "expected_frequency_days": 30,
        "max_stale_days": 45,
        "date_column": "year/month",
        "description": "CMHC Housing Starts/Completions by CMA",
    },
    "nhpi": {
        "expected_frequency_days": 30,
        "max_stale_days": 45,
        "date_column": "year/month",
        "description": "New Housing Price Index",
    },
    "building_permits": {
        "expected_frequency_days": 30,
        "max_stale_days": 50,
        "date_column": "year/month",
        "description": "Building Permits by Municipality",
    },
    "trade_flows": {
        "expected_frequency_days": 30,
        "max_stale_days": 60,
        "date_column": "ref_date",
        "description": "StatCan Trade Flows",
    },
    "comtrade_flows": {
        "expected_frequency_days": 365,
        "max_stale_days": 400,
        "date_column": "period_year",
        "description": "UN Comtrade Bilateral Trade",
    },
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _query_table_freshness(
    table: str,
    cfg: dict[str, Any],
) -> dict[str, Any]:
    """Return freshness info for a single table."""
    supabase = get_supabase_client()
    today = date.today()

    result: dict[str, Any] = {
        "table": table,
        "description": cfg["description"],
        "record_count": 0,
        "latest_date": None,
        "days_since_latest": None,
        "max_stale_days": cfg["max_stale_days"],
        "is_stale": False,
    }

    try:
        # --- record count (use head(0) trick — Supabase returns count) ---
        count_resp = (
            supabase.table(table)
            .select("*", count="exact")
            .limit(0)
            .execute()
        )
        result["record_count"] = count_resp.count or 0

        if result["record_count"] == 0:
            return result

        # --- latest date ---
        date_col = cfg["date_column"]

        if date_col == "year/month":
            row = (
                supabase.table(table)
                .select("year,month")
                .order("year", desc=True)
                .order("month", desc=True)
                .limit(1)
                .execute()
            ).data
            if row:
                latest = date(row[0]["year"], row[0]["month"], 1)
                result["latest_date"] = latest.isoformat()

        elif date_col == "ref_year/ref_month":
            row = (
                supabase.table(table)
                .select("ref_year,ref_month")
                .order("ref_year", desc=True)
                .order("ref_month", desc=True)
                .limit(1)
                .execute()
            ).data
            if row:
                latest = date(row[0]["ref_year"], row[0]["ref_month"], 1)
                result["latest_date"] = latest.isoformat()

        elif date_col == "period_year":
            row = (
                supabase.table(table)
                .select("period_year")
                .order("period_year", desc=True)
                .limit(1)
                .execute()
            ).data
            if row:
                latest = date(row[0]["period_year"], 1, 1)
                result["latest_date"] = latest.isoformat()

        else:
            # Assume a regular date / timestamp column name
            row = (
                supabase.table(table)
                .select(date_col)
                .order(date_col, desc=True)
                .limit(1)
                .execute()
            ).data
            if row:
                raw = str(row[0][date_col])[:10]
                latest = date.fromisoformat(raw)
                result["latest_date"] = latest.isoformat()

        # --- staleness ---
        if result["latest_date"]:
            latest_date = date.fromisoformat(result["latest_date"])
            days_since = (today - latest_date).days
            result["days_since_latest"] = days_since
            result["is_stale"] = days_since > cfg["max_stale_days"]

    except Exception as exc:
        result["error"] = str(exc)

    return result


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------


def generate_report() -> list[dict[str, Any]]:
    """Query all tables and return a list of freshness dicts."""
    return [
        _query_table_freshness(table, cfg)
        for table, cfg in FRESHNESS_CONFIG.items()
    ]


def print_report_table(report: list[dict[str, Any]]) -> None:
    """Pretty-print the report as an aligned text table."""
    header = (
        f"{'Table':<20} {'Description':<42} {'Records':>8} "
        f"{'Latest Date':>12} {'Days Ago':>9} {'Max':>5} {'Stale?':>7}"
    )
    sep = "-" * len(header)
    print()
    print(header)
    print(sep)

    for row in report:
        stale_flag = "YES" if row["is_stale"] else ""
        if row.get("error"):
            stale_flag = "ERROR"
        latest = row["latest_date"] or "—"
        days = row["days_since_latest"] if row["days_since_latest"] is not None else "—"
        print(
            f"{row['table']:<20} {row['description']:<42} "
            f"{row['record_count']:>8} {latest:>12} {str(days):>9} "
            f"{row['max_stale_days']:>5} {stale_flag:>7}"
        )

    print(sep)
    print()


def write_json_report(report: list[dict[str, Any]], path: Path) -> None:
    """Write the report list to a JSON file."""
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "tables": report,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=str) + "\n")
    print(f"Report written to {path}")


# ---------------------------------------------------------------------------
# Email alerting
# ---------------------------------------------------------------------------


def _send_email_alert(report: list[dict[str, Any]]) -> None:
    """Send an email alert listing stale tables, if SMTP env vars are set."""
    to_addr = os.environ.get("ALERT_EMAIL_TO")
    from_addr = os.environ.get("ALERT_EMAIL_FROM")
    smtp_host = os.environ.get("SMTP_HOST")
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_user = os.environ.get("SMTP_USER")
    smtp_pass = os.environ.get("SMTP_PASS")

    missing = [
        name
        for name, val in [
            ("ALERT_EMAIL_TO", to_addr),
            ("ALERT_EMAIL_FROM", from_addr),
            ("SMTP_HOST", smtp_host),
        ]
        if not val
    ]
    if missing:
        print(
            f"⚠ Email alert skipped — missing env var(s): {', '.join(missing)}"
        )
        return

    stale_tables = [r for r in report if r["is_stale"]]
    if not stale_tables:
        return

    lines = ["The following candata tables are stale:\n"]
    for t in stale_tables:
        lines.append(
            f"  • {t['table']}: last data {t['latest_date']} "
            f"({t['days_since_latest']} days ago, max {t['max_stale_days']})"
        )
    lines.append("\nPlease run the relevant pipeline(s) to refresh the data.")

    body = "\n".join(lines)

    msg = MIMEText(body, "plain")
    msg["Subject"] = f"[candata] {len(stale_tables)} stale table(s) detected"
    msg["From"] = from_addr  # type: ignore[arg-type]
    msg["To"] = to_addr  # type: ignore[arg-type]

    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:  # type: ignore[arg-type]
            server.ehlo()
            server.starttls()
            server.ehlo()
            if smtp_user and smtp_pass:
                server.login(smtp_user, smtp_pass)
            server.sendmail(from_addr, [to_addr], msg.as_string())  # type: ignore[arg-type,list-item]
        print(f"✉ Alert email sent to {to_addr}")
    except Exception as exc:
        print(f"⚠ Failed to send alert email: {exc}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    print("=== candata data freshness check ===")

    report = generate_report()

    # 1. Formatted stdout table
    print_report_table(report)

    # 2. JSON file
    json_path = Path(__file__).resolve().parent / "freshness_report.json"
    write_json_report(report, json_path)

    # 3. Email alert
    stale = [r for r in report if r["is_stale"]]
    if stale:
        print(f"🚨 {len(stale)} table(s) are stale!")
        _send_email_alert(report)
    else:
        print("✅ All tables are within acceptable freshness thresholds.")


if __name__ == "__main__":
    main()
