"""
WIP cross-reference module.

Joins Trevor CSV data against the live TSEG WIP Google Sheet to surface
which orders are stuck in which blocker category (Objections, Missing Meter
Information, Gas Deleted, Switch Issues, etc.) and which supplier owns them.

Also computes cohort intelligence — grouping orders by age since they entered
bill setup — to highlight ageing patterns across the pipeline.
"""

import os
import json
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from pathlib import Path
from datetime import date

from utils import (
    detect_column,
    compute_days_elapsed,
    rag_status,
    ColumnNotFoundError,
    normalise_tseg_id,
    normalise_tseg_series,
)
from tseg_api import get_contract

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

CREDENTIALS_PATH = Path(__file__).parent / "credentials.json"

# Tabs to skip when reading the WIP sheet — completed orders aren't relevant,
# and "WIP Overview" is a summary tab that doesn't contain order rows.
SKIP_TABS = ["completed wip", "wip overview"]

# Column name patterns for the WIP Google Sheet (separate from Trevor patterns in utils.py)
PATTERNS = {
    "tseg_id":        ["TSEG ID", "tseg id", "tseg_id", "bill_payment_reference", "payment_reference"],
    "order_id":       ["order_id", "orderid"],
    "address":        ["Address 1", "address", "property_line1"],
    "provider":       ["Supplier", "supplier", "provider", "bill_provider"],
    "reason":         ["Notes", "notes", "reason", "comment", "status", "issue"],
    "supply_status":  ["Supply Status", "supply_status", "supply status"],
    "services_start": ["Services Start", "services_start", "services start", "service start"],
}


def detect_col(headers, field, required=False):
    """Local column detection for WIP sheets — exact match first, then substring."""
    patterns = PATTERNS[field]
    for pattern in patterns:
        for h in headers:
            if h.strip() == pattern:
                return h
    for pattern in patterns:
        for h in headers:
            if pattern.lower() in h.lower():
                return h
    return None


def get_wip_data(wip_url: str) -> list[dict]:
    """Read all active tabs from the WIP Google Sheet and normalise into flat rows.
    Each row carries its source tab name so we can identify the blocker category."""
    # Local file for dev, GOOGLE_CREDENTIALS env var for Railway production
    env_creds = os.environ.get("GOOGLE_CREDENTIALS")
    if env_creds:
        info = json.loads(env_creds)
        creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    elif CREDENTIALS_PATH.exists():
        creds = Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=SCOPES)
    else:
        raise RuntimeError(
            "No Google credentials found. Set the GOOGLE_CREDENTIALS env var "
            "or place credentials.json in the project root."
        )
    client = gspread.authorize(creds)
    sheet = client.open_by_url(wip_url)

    all_rows = []
    for ws in sheet.worksheets():
        tab_name = ws.title.strip()
        if tab_name.lower() in SKIP_TABS:
            continue

        try:
            records = ws.get_all_records()
        except Exception:
            # Fallback for sheets with duplicate/empty headers — gspread chokes on these
            raw = ws.get_all_values()
            if not raw:
                continue
            headers_row = raw[0]
            seen = {}
            clean_headers = []
            for h in headers_row:
                h = h.strip()
                if not h:
                    h = f"_col_{len(clean_headers)}"
                if h in seen:
                    seen[h] += 1
                    h = f"{h}_{seen[h]}"
                else:
                    seen[h] = 0
                clean_headers.append(h)
            records = [dict(zip(clean_headers, row)) for row in raw[1:] if any(row)]

        df = pd.DataFrame(records)
        headers = df.columns.tolist()

        tseg_col          = detect_col(headers, "tseg_id")
        order_col         = detect_col(headers, "order_id")
        address_col       = detect_col(headers, "address")
        provider_col      = detect_col(headers, "provider")
        reason_col        = detect_col(headers, "reason")
        supply_status_col = detect_col(headers, "supply_status")
        services_start_col = detect_col(headers, "services_start")

        if not tseg_col:
            continue

        for _, row in df.iterrows():
            raw_tseg = row.get(tseg_col, "")
            tseg_val = normalise_tseg_id(raw_tseg)
            if not tseg_val:
                continue
            all_rows.append({
                "wip_tseg_id":        tseg_val,
                "wip_order_id":       str(row.get(order_col, "")).strip() if order_col else "",
                "wip_address":        str(row.get(address_col, "")).strip() if address_col else "",
                "wip_provider":       str(row.get(provider_col, "")).strip() if provider_col else "",
                "wip_reason":         str(row.get(reason_col, "")).strip() if reason_col else "",
                "wip_supply_status":  str(row.get(supply_status_col, "")).strip() if supply_status_col else "",
                "wip_services_start": str(row.get(services_start_col, "")).strip() if services_start_col else "",
                "wip_tab":            tab_name,
            })

    return all_rows


def _clean_tseg_id(val):
    """Legacy helper kept for backwards compatibility — delegates to the
    canonical normalise_tseg_id from utils."""
    return normalise_tseg_id(val) or ""


def run_wip_check(trevor_df: pd.DataFrame, wip_url: str) -> dict:
    """Main entry point — cross-references Trevor export against WIP sheet.
    Returns summary metrics, per-supplier breakdowns, cohort analysis, and the
    merged row data for the frontend table."""

    # Field mapping: True = required, False = optional (gracefully absent)
    trevor_fields = {
        "tseg_id":       True,
        "order_id":      True,
        "address":       False,
        "provider":      False,
        "updated_at":    True,
        "status":        True,
        "order_started": False,
    }

    col_map = {}
    for field, required in trevor_fields.items():
        match = detect_column(trevor_df.columns, field, required=required)
        if match:
            col_map[field] = match

    tseg_col         = col_map["tseg_id"]
    order_col        = col_map["order_id"]
    updated_col      = col_map["updated_at"]
    status_col       = col_map["status"]
    address_col      = col_map.get("address")
    provider_col     = col_map.get("provider")
    order_started_col = col_map.get("order_started")

    trevor = trevor_df.copy()

    # Normalise TSEG IDs for join — canonical 10-digit zero-padded form.
    # Also overwrite the source column so the value displayed in the UI matches
    # what gets pushed to Google Sheets.
    trevor[tseg_col] = normalise_tseg_series(trevor[tseg_col])
    trevor["_join_key"] = trevor[tseg_col]
    trevor["days_elapsed"] = compute_days_elapsed(trevor, updated_col)
    trevor["rag"] = trevor["days_elapsed"].apply(rag_status)

    # Cohort assignment based on order_started_bill_setups_at.
    # Groups orders into 0-30 / 30-60 / 60+ day buckets to surface ageing patterns.
    # Falls back gracefully if the column isn't present in the Trevor export.
    if order_started_col and order_started_col in trevor.columns:
        parsed_started = pd.to_datetime(trevor[order_started_col], dayfirst=True, errors="coerce")
        today = pd.Timestamp(date.today())
        trevor["cohort_days"] = (today - parsed_started).dt.days.fillna(-1).astype(int)
        trevor["cohort"] = trevor["cohort_days"].apply(
            lambda d: "0-30" if 0 <= d <= 30 else ("30-60" if 31 <= d <= 60 else ("60+" if d > 60 else ""))
        )
    else:
        trevor["cohort_days"] = -1
        trevor["cohort"] = ""

    wip_rows = get_wip_data(wip_url)
    wip_df   = pd.DataFrame(wip_rows) if wip_rows else pd.DataFrame(
        columns=["wip_tseg_id", "wip_order_id", "wip_address", "wip_provider", "wip_reason",
                 "wip_supply_status", "wip_services_start", "wip_tab"]
    )

    if not wip_df.empty:
        # wip_df rows already have normalised TSEG IDs from get_wip_data
        wip_df["_join_key"] = wip_df["wip_tseg_id"].astype(str)

    # Left join preserves all Trevor rows — unmatched orders get empty WIP fields
    keep = [c for c in [order_col, tseg_col, address_col, provider_col, updated_col, status_col,
                         "days_elapsed", "rag", "cohort_days", "cohort"] if c]
    merged = trevor[keep + ["_join_key"]].merge(
        wip_df, on="_join_key", how="left"
    ).drop(columns=["_join_key"])
    merged = merged.fillna("")

    # ── TSEG API enrichment ────────────────────────────────────────────────
    # Call the live TSEG GET /wholesale/contracts endpoint for every row.
    # Every call sleeps 0.2s inside get_contract() so we don't hammer the API.
    # Errors are swallowed per-row so a single failure can't crash the batch.
    tseg_service_names  = []
    tseg_order_statuses = []
    tseg_service_starts = []
    tseg_errors         = []
    for tid in merged[tseg_col].astype(str).tolist():
        info = get_contract(tid)
        tseg_service_names.append(info.get("tseg_service_name", ""))
        tseg_order_statuses.append(info.get("tseg_order_status", ""))
        tseg_service_starts.append(info.get("tseg_service_start", ""))
        tseg_errors.append(info.get("tseg_error", ""))

    merged["tseg_service_name"]  = tseg_service_names
    merged["tseg_order_status"]  = tseg_order_statuses
    merged["tseg_service_start"] = tseg_service_starts
    merged["tseg_error"]         = tseg_errors

    all_cols = [c for c in keep if c in merged.columns] + \
               ["wip_tab", "wip_reason", "wip_provider", "wip_supply_status", "wip_services_start",
                "tseg_service_name", "tseg_order_status", "tseg_service_start"]

    result = merged[all_cols].copy()

    # cohort_days and cohort are used for filtering/charts but hidden from the table
    out_cols = [c for c in all_cols if c not in ("cohort_days", "cohort")]

    tabs_present = [t for t in result["wip_tab"].unique() if t and t != ""]

    # Objection breakdown — ranked by supplier, used for the orange bar chart
    objection_rows = result[result["wip_tab"] == "Objections"].copy()
    objection_by_supplier = []
    if not objection_rows.empty and provider_col:
        grp = objection_rows.groupby(provider_col).agg(
            count=(provider_col, "count"),
            orders=(order_col, lambda x: list(x)),
        ).reset_index()
        grp = grp.sort_values("count", ascending=False)
        objection_by_supplier = [
            {
                "supplier": row[provider_col],
                "count":    int(row["count"]),
                "orders":   row["orders"],
            }
            for _, row in grp.iterrows()
            if row[provider_col]
        ]

    # Supplier heat map — shows full WIP tab breakdown per supplier
    supplier_wip_breakdown = []
    if provider_col and provider_col in result.columns:
        wip_only = result[result["wip_tab"] != ""]
        if not wip_only.empty:
            grp = wip_only.groupby([provider_col, "wip_tab"]).size().reset_index(name="count")
            for supplier, sub in grp.groupby(provider_col):
                supplier_wip_breakdown.append({
                    "supplier": supplier,
                    "total":    int(sub["count"].sum()),
                    "by_tab":  {row["wip_tab"]: int(row["count"]) for _, row in sub.iterrows()},
                })
            supplier_wip_breakdown.sort(key=lambda x: x["total"], reverse=True)

    # Cohort intelligence — per-bucket summary for the frontend cards.
    # Each cohort surfaces: order count, most common blocker, worst supplier, avg age.
    cohort_summary = []
    for cohort_label in ["0-30", "30-60", "60+"]:
        cohort_rows = result[result["cohort"] == cohort_label]
        count = len(cohort_rows)
        if count == 0:
            cohort_summary.append({
                "cohort": cohort_label,
                "count": 0,
                "top_blocker": "",
                "top_supplier": "",
                "avg_days": 0,
            })
            continue

        # Most common WIP blocker type
        wip_tabs = cohort_rows[cohort_rows["wip_tab"] != ""]["wip_tab"]
        top_blocker = wip_tabs.mode().iloc[0] if not wip_tabs.empty else ""

        # Supplier with most issues
        top_supplier = ""
        if provider_col and provider_col in cohort_rows.columns:
            suppliers = cohort_rows[cohort_rows[provider_col] != ""][provider_col]
            if not suppliers.empty:
                top_supplier = suppliers.mode().iloc[0]

        # Average cohort_days
        valid_days = cohort_rows[cohort_rows["cohort_days"] >= 0]["cohort_days"]
        avg_days = round(valid_days.mean(), 1) if not valid_days.empty else 0

        cohort_summary.append({
            "cohort": cohort_label,
            "count": count,
            "top_blocker": top_blocker,
            "top_supplier": top_supplier,
            "avg_days": avg_days,
        })

    # Blocker type counts per cohort — feeds the stacked bar chart on the frontend
    cohort_blocker_breakdown = {}
    for cohort_label in ["0-30", "30-60", "60+"]:
        cohort_rows = result[(result["cohort"] == cohort_label) & (result["wip_tab"] != "")]
        if not cohort_rows.empty:
            cohort_blocker_breakdown[cohort_label] = cohort_rows["wip_tab"].value_counts().to_dict()
        else:
            cohort_blocker_breakdown[cohort_label] = {}

    return {
        "summary": {
            "total":          len(result),
            "matched_to_wip": int((result["wip_tab"] != "").sum()),
            "objections":     int((result["wip_tab"] == "Objections").sum()),
            "unmatched":      int((result["wip_tab"] == "").sum()),
        },
        "tabs":                      tabs_present,
        "objection_by_supplier":     objection_by_supplier,
        "supplier_wip_breakdown":    supplier_wip_breakdown,
        "cohort_summary":            cohort_summary,
        "cohort_blocker_breakdown":  cohort_blocker_breakdown,
        "columns":                   out_cols,
        "rows":                      result.to_dict(orient="records"),
        "tseg_id_col":               tseg_col,
        "order_id_col":              order_col,
        "address_col":               address_col or "",
        "provider_col":              provider_col or "",
        "updated_col":               updated_col,
        "status_col":                status_col,
    }


def run_wip_check_live(wip_url: str) -> dict:
    """V2 entry point — Homebox data is read directly from the Trevor WIP sheet
    (WIP_SHEET_URL env var) instead of from a CSV upload. The TSEG WIP sheet URL
    is still passed in by the caller (Tom's live sheet)."""
    homebox_url = os.environ.get("WIP_SHEET_URL", "").strip()
    if not homebox_url:
        raise RuntimeError("WIP_SHEET_URL env var is not set — cannot load Homebox WIP data.")
    # Local import avoids a circular dep at module load
    from sheets import read_sheet_as_df
    trevor_df = read_sheet_as_df(homebox_url)
    if trevor_df.empty:
        raise RuntimeError("Homebox WIP Trevor sheet is empty — check the sheet URL and sharing permissions.")
    return run_wip_check(trevor_df, wip_url)
