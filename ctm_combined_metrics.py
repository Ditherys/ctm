import argparse
import csv
import json
import os
import time
from datetime import datetime, time as dt_time
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from requests.auth import HTTPBasicAuth

API_HOST = "https://api.calltrackingmetrics.com"
API_KEY = "a341882d847fd790082ce05f378824a3e321f669"
API_SECRET = "74db3c6ae35b0afd9e1d9a02cf358671e1fe"
ACCOUNT_ID = "341882"
AGENTS_FILE = "alliance_agents_with_email.csv"
OUTPUT_FILE = "ctm_combined_metrics.csv"
CALLS_CACHE_TEMPLATE = "calls_cache_{start}_to_{end}.json"
EMAIL_DOMAIN = "@allianceglobalsolutions.com"
TEAM_ID = "5457"
CTM_REPORT_TIMEZONE = "America/New_York"
MAX_RETRIES = 5


def parse_args():
    parser = argparse.ArgumentParser(
        description="Export combined CTM metrics for alliance agents."
    )
    parser.add_argument("start_date", help="Start date in YYYY-MM-DD")
    parser.add_argument("end_date", help="End date in YYYY-MM-DD")
    parser.add_argument("--output", default=OUTPUT_FILE)
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


def validate_date(date_string):
    try:
        return datetime.strptime(date_string, "%Y-%m-%d").date()
    except ValueError as exc:
        raise SystemExit(f"Invalid date '{date_string}'. Use YYYY-MM-DD.") from exc


def date_to_epoch(date_string, end_of_day):
    day = validate_date(date_string)
    local_time = dt_time(23, 59, 59) if end_of_day else dt_time(0, 0, 0)
    local_dt = datetime.combine(day, local_time, tzinfo=ZoneInfo(CTM_REPORT_TIMEZONE))
    return int(local_dt.timestamp())


def seconds_to_hms(value):
    total_seconds = int(round(float(value or 0)))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def format_sheet_date(start_date, end_date):
    start = validate_date(start_date)
    end = validate_date(end_date)
    if start == end:
        return start.strftime("%m/%d/%Y")
    return f"{start.strftime('%m/%d/%Y')} - {end.strftime('%m/%d/%Y')}"


def current_run_timestamp():
    now_local = datetime.now(ZoneInfo(CTM_REPORT_TIMEZONE))
    return now_local.strftime("%m/%d/%Y %I:%M:%S %p")


def get_env_or_default(name, default_value):
    return os.getenv(name, default_value)


def get_api_credentials():
    return {
        "api_host": get_env_or_default("CTM_API_HOST", API_HOST),
        "api_key": get_env_or_default("CTM_API_KEY", API_KEY),
        "api_secret": get_env_or_default("CTM_API_SECRET", API_SECRET),
        "account_id": get_env_or_default("CTM_ACCOUNT_ID", ACCOUNT_ID),
    }


def api_get(path, params, credentials):
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with requests.Session() as session:
                session.trust_env = False
                response = session.get(
                    f"{credentials['api_host']}{path}",
                    auth=HTTPBasicAuth(credentials["api_key"], credentials["api_secret"]),
                    params=params,
                    timeout=120,
                )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            last_error = exc
            if attempt == MAX_RETRIES:
                raise
            time.sleep(attempt * 2)
    raise last_error


def fetch_all_pages_by_cursor(path, credentials, params=None, data_key=None):
    items = []
    cursor = None
    batch_number = 1

    while True:
        query = dict(params or {})
        if cursor:
            query["after"] = cursor
        print(f"Fetching {data_key} batch {batch_number}...")
        payload = api_get(path, query, credentials)
        batch = payload.get(data_key or "", [])
        if not batch:
            break

        items.extend(batch)
        print(f"Fetched {len(items)} total {data_key} so far")
        cursor = payload.get("after")
        if not payload.get("next_page") or not cursor:
            break
        batch_number += 1

    return items


def load_agents():
    rows = []
    path = Path(AGENTS_FILE)
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            email = (row.get("email") or "").strip().lower()
            if not email.endswith(EMAIL_DOMAIN):
                continue
            rows.append(
                {
                    "id": (row.get("id") or "").strip(),
                    "agent": (row.get("agent") or "").strip(),
                    "email": email,
                }
            )
    return sorted(rows, key=lambda item: item["agent"].lower())


def get_calls_cache_path(start_date, end_date):
    return Path(CALLS_CACHE_TEMPLATE.format(start=start_date, end=end_date))


def load_calls_cache(start_date, end_date, refresh_cache=False):
    cache_path = get_calls_cache_path(start_date, end_date)
    if refresh_cache or not cache_path.exists():
        return None

    print(f"Loading calls from cache: {cache_path.name}")
    with cache_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def save_calls_cache(start_date, end_date, calls):
    cache_path = get_calls_cache_path(start_date, end_date)
    print(f"Saving calls cache to {cache_path.name}...")
    with cache_path.open("w", encoding="utf-8") as handle:
        json.dump(calls, handle)


def fetch_calls(start_date, end_date, credentials, refresh_cache=False):
    cached_calls = load_calls_cache(start_date, end_date, refresh_cache=refresh_cache)
    if cached_calls is not None:
        return cached_calls

    print("Fetching inbound calls from CTM...")
    calls = fetch_all_pages_by_cursor(
        f"/api/v1/accounts/{credentials['account_id']}/calls.json",
        credentials=credentials,
        params={
            "per_page": 50,
            "start_date": start_date,
            "end_date": end_date,
            "direction": "inbound",
        },
        data_key="calls",
    )
    save_calls_cache(start_date, end_date, calls)
    return calls


def fetch_utilization_payload(
    start_date,
    end_date,
    credentials,
    team_id,
    timezone_label,
    interval,
    statistic,
    view_by,
):
    params = {
        "start_time": date_to_epoch(start_date, end_of_day=False),
        "end_time": date_to_epoch(end_date, end_of_day=True),
        "team_id": team_id,
        "timezone": timezone_label,
        "interval": interval,
        "statistic": statistic,
        "view_by": view_by,
    }
    with requests.Session() as session:
        session.trust_env = False
        response = session.get(
            f"{credentials['api_host']}/api/v1/accounts/{credentials['account_id']}/agents/utilization.json",
            auth=HTTPBasicAuth(credentials["api_key"], credentials["api_secret"]),
            params=params,
            timeout=120,
        )
    response.raise_for_status()
    return response.json(), response.url


def build_metric_map(payload, metric_name):
    users = payload.get("users") or {}
    metric_rows = (payload.get("metrics") or {}).get(metric_name) or []
    results = {}
    for row in metric_rows:
        user_id = str(row.get("user_id") or "").strip()
        user = users.get(user_id) or {}
        email = (user.get("email") or "").strip().lower()
        if email:
            results[email] = row
    return results


def calculate_calls_metrics(calls, agents):
    counts = {agent["email"]: 0 for agent in agents}
    transferred_counts = {agent["email"]: 0 for agent in agents}
    id_to_email = {agent["id"]: agent["email"] for agent in agents if agent["id"]}

    print("Calculating first-time caller and transferred counts...")
    for call in calls:
        agent_email = (((call.get("agent") or {}).get("email")) or "").strip().lower()
        if (call.get("direction") or "").strip().lower() != "inbound":
            continue

        if agent_email in counts and call.get("is_new_caller"):
            counts[agent_email] += 1

        if not call.get("is_new_caller"):
            continue

        seen_transfer_from = set()
        for transfer in call.get("transfers") or []:
            from_id = (transfer.get("from") or "").strip()
            from_email = id_to_email.get(from_id)
            if from_email:
                seen_transfer_from.add(from_email)

        for from_email in seen_transfer_from:
            transferred_counts[from_email] += 1

    return counts, transferred_counts


def build_combined_rows(
    start_date,
    end_date,
    agents,
    payload,
    calls,
):
    first_time_counts, transferred_counts = calculate_calls_metrics(calls, agents)
    inbound_map = build_metric_map(payload, "inbound_calls")
    hold_map = build_metric_map(payload, "hold_time")

    rows = []
    report_date = format_sheet_date(start_date, end_date)
    last_updated = current_run_timestamp()
    for agent in agents:
        inbound = inbound_map.get(agent["email"]) or {}
        hold = hold_map.get(agent["email"]) or {}
        rows.append(
            {
                "date": report_date,
                "user_name": agent["agent"],
                "user_email": agent["email"],
                "first_time_caller": first_time_counts.get(agent["email"], 0),
                "transfer_count": transferred_counts.get(agent["email"], 0),
                "inbound_calls": inbound.get("count", 0),
                "inbound_minutes": seconds_to_hms(inbound.get("total", 0)),
                "hold_time": seconds_to_hms(hold.get("total", 0)),
                "last_updated": last_updated,
            }
        )

    rows.sort(
        key=lambda row: (-row["first_time_caller"], -row["transfer_count"], row["user_name"].lower())
    )
    return rows


def write_rows_to_csv(rows, output_file):
    output_path = resolve_output_path(output_file)
    print(f"Saving results to {output_path.name}...")
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "date",
                "user_name",
                "user_email",
                "first_time_caller",
                "transfer_count",
                "inbound_calls",
                "inbound_minutes",
                "hold_time",
                "last_updated",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)
    return output_path


def resolve_output_path(output_file):
    primary = Path(output_file)
    try:
        with primary.open("w", newline="", encoding="utf-8"):
            pass
        return primary
    except PermissionError:
        fallback = primary.with_name(f"{primary.stem}_new{primary.suffix}")
        print(f"{primary.name} is locked, using {fallback.name} instead.")
        return fallback


def main():
    args = parse_args()
    start_date = validate_date(args.start_date).isoformat()
    end_date = validate_date(args.end_date).isoformat()
    if start_date > end_date:
        raise SystemExit("start_date must be on or before end_date.")

    credentials = get_api_credentials()
    agents = load_agents()
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
    output_path = write_rows_to_csv(rows, args.output)

    print(f"Saved to {output_path.name}")
    for row in rows:
        print(
            f"{row['date']}, {row['user_name']}, {row['user_email']}, "
            f"{row['first_time_caller']}, {row['transfer_count']}, "
            f"{row['inbound_calls']}, {row['inbound_minutes']}, "
            f"{row['hold_time']}, {row['last_updated']}"
        )


if __name__ == "__main__":
    main()
