import argparse
import json
import os
from datetime import datetime
from zoneinfo import ZoneInfo

import gspread

from ctm_combined_metrics import (
    CTM_REPORT_TIMEZONE,
    TEAM_ID,
    build_combined_rows,
    fetch_calls,
    fetch_utilization_payload,
    get_api_credentials,
    load_agents_with_fallback,
    validate_date,
)

EXPECTED_HEADERS = [
    "Date",
    "User name",
    "User email",
    "first_time_caller",
    "transfer_count",
    "Inbound calls",
    "Inbound minutes",
    "Hold time",
    "Last updated",
]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Fetch CTM metrics and upsert them into Google Sheets."
    )
    parser.add_argument("start_date", nargs="?")
    parser.add_argument("end_date", nargs="?")
    parser.add_argument(
        "--days-ago",
        type=int,
        default=0,
        help="Use a relative day offset in CTM report timezone when no explicit dates are provided. 0=today, 1=yesterday.",
    )
    parser.add_argument("--team-id", default=TEAM_ID)
    parser.add_argument("--timezone-label", default="EST")
    parser.add_argument("--interval", default="hour")
    parser.add_argument("--statistic", default="occupancy")
    parser.add_argument("--view-by", default="agent")
    parser.add_argument(
        "--refresh-calls-cache",
        action="store_true",
        help="Ignore any existing cached calls file for this date range and fetch again.",
    )
    return parser.parse_args()


def today_in_report_timezone():
    now_local = datetime.now(ZoneInfo(CTM_REPORT_TIMEZONE))
    return now_local.date().isoformat()


def relative_date_in_report_timezone(days_ago):
    now_local = datetime.now(ZoneInfo(CTM_REPORT_TIMEZONE))
    return (now_local.date()).fromordinal(now_local.date().toordinal() - days_ago).isoformat()


def resolve_dates(args):
    start_date = args.start_date or relative_date_in_report_timezone(args.days_ago)
    end_date = args.end_date or start_date
    start_date = validate_date(start_date).isoformat()
    end_date = validate_date(end_date).isoformat()
    if start_date > end_date:
        raise SystemExit("start_date must be on or before end_date.")
    return start_date, end_date


def get_sheet_config():
    sheet_id = os.getenv("GOOGLE_SHEET_ID")
    worksheet_name = os.getenv("GOOGLE_SHEET_TAB")
    service_account_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")

    if not sheet_id:
        raise SystemExit("Missing GOOGLE_SHEET_ID environment variable.")
    if not worksheet_name:
        raise SystemExit("Missing GOOGLE_SHEET_TAB environment variable.")
    if not service_account_json:
        raise SystemExit("Missing GOOGLE_SERVICE_ACCOUNT_JSON environment variable.")

    return sheet_id, worksheet_name, json.loads(service_account_json)


def open_worksheet():
    sheet_id, worksheet_name, service_account_info = get_sheet_config()
    client = gspread.service_account_from_dict(service_account_info)
    spreadsheet = client.open_by_key(sheet_id)
    try:
        worksheet = spreadsheet.worksheet(worksheet_name)
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows=1000, cols=20)
    return worksheet


def ensure_headers(worksheet):
    current_headers = worksheet.row_values(1)
    if current_headers != EXPECTED_HEADERS:
        worksheet.update("A1:I1", [EXPECTED_HEADERS])


def normalize_date_key(value):
    value = (value or "").strip()
    if not value:
        return ""

    if " - " in value:
        parts = value.split(" - ", 1)
        normalized_parts = [normalize_date_key(part) for part in parts]
        if all(normalized_parts):
            return " - ".join(normalized_parts)

    if "/" in value:
        parts = value.split("/")
        if len(parts) == 3:
            try:
                month = int(parts[0])
                day = int(parts[1])
                year = int(parts[2])
                return f"{month:02d}/{day:02d}/{year:04d}"
            except ValueError:
                pass

    if "-" in value:
        parts = value.split("-")
        if len(parts) == 3:
            try:
                year = int(parts[0])
                month = int(parts[1])
                day = int(parts[2])
                return f"{month:02d}/{day:02d}/{year:04d}"
            except ValueError:
                pass

    return value


def row_to_sheet_values(row):
    return [
        row["date"],
        row["user_name"],
        row["user_email"],
        row["first_time_caller"],
        row["transfer_count"],
        row["inbound_calls"],
        row["inbound_minutes"],
        row["hold_time"],
        row["last_updated"],
    ]


def load_existing_index(worksheet):
    records = worksheet.get_all_values()
    index = {}
    for row_number, values in enumerate(records[1:], start=2):
        if len(values) < 3:
            continue
        date_value = normalize_date_key(values[0])
        email_value = values[2].strip().lower()
        if date_value and email_value:
            index[(date_value, email_value)] = row_number
    return index


def upsert_rows(worksheet, rows):
    existing_index = load_existing_index(worksheet)
    updates = []
    appends = []

    for row in rows:
        key = (normalize_date_key(row["date"]), row["user_email"].lower())
        values = row_to_sheet_values(row)
        existing_row = existing_index.get(key)
        if existing_row:
            updates.append((existing_row, values))
        else:
            appends.append(values)

    for row_number, values in updates:
        worksheet.update(f"A{row_number}:I{row_number}", [values])

    if appends:
        worksheet.append_rows(appends, value_input_option="USER_ENTERED")

    return len(updates), len(appends)


def main():
    args = parse_args()
    start_date, end_date = resolve_dates(args)

    credentials = get_api_credentials()
    agents = load_agents_with_fallback(credentials)
    print(f"Found {len(agents)} alliance agents")

    payload, final_url = fetch_utilization_payload(
        start_date,
        end_date,
        credentials,
        args.team_id,
        args.timezone_label,
        args.interval,
        args.statistic,
        args.view_by,
    )
    print(f"Fetched utilization payload from {final_url}")

    calls = fetch_calls(
        start_date,
        end_date,
        credentials,
        refresh_cache=args.refresh_calls_cache,
    )
    print(f"Fetched {len(calls)} inbound calls in range")

    rows = build_combined_rows(start_date, end_date, agents, payload, calls)
    worksheet = open_worksheet()
    ensure_headers(worksheet)
    updated, appended = upsert_rows(worksheet, rows)
    print(f"Google Sheets sync complete. Updated {updated} rows, appended {appended} rows.")


if __name__ == "__main__":
    main()
