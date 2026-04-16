import pandas as pd
from datetime import date

from utils import (
    detect_column,
    compute_days_elapsed,
    rag_status,
    ColumnNotFoundError,
    classify_fuel,
    normalise_tseg_series,
)

AWAITING_KEYWORDS = ["feedback"]

# Minimum days elapsed for an order stuck in 'REGISTERING' to be flagged
# as a breach — the COO cares specifically about 10d+ registering stalls.
REGISTERING_BREACH_DAYS = 10

REQUIRED_FIELDS = {
    "order_id":    True,
    "tseg_id":     True,
    "updated_at":  True,
    "status":      True,
}

OPTIONAL_FIELDS = {
    "address":       False,
    "provider":      False,
    "issue":         False,
    "mprn":          False,
    "bill_name":     False,
    "order_started": False,
}


def _cohort_label(days):
    """Map age-in-days onto the same 0-30 / 30-60 / 60+ buckets used
    by the WIP module so the two modules stay aligned."""
    if days is None or days < 0:
        return ""
    if days <= 30:
        return "0-30"
    if days <= 60:
        return "30-60"
    return "60+"


def _enrich_with_tseg_api(awaiting: pd.DataFrame, tseg_col: str) -> pd.DataFrame:
    """Parallel TSEG API enrichment — mirrors the wip_checker pattern.
    Fires up to 20 concurrent workers; each get_contract() call sleeps 0.2s.
    Per-row errors are swallowed so one bad ID can't crash the batch."""
    # Local imports keep the CSV-upload path free of the optional requests dep
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from tseg_api import get_contract

    tseg_ids = awaiting[tseg_col].astype(str).tolist()
    api_results = [None] * len(tseg_ids)

    with ThreadPoolExecutor(max_workers=20) as executor:
        future_to_idx = {
            executor.submit(get_contract, tid): i
            for i, tid in enumerate(tseg_ids)
        }
        for future in as_completed(future_to_idx):
            idx = future_to_idx[future]
            try:
                api_results[idx] = future.result()
            except Exception:
                api_results[idx] = {
                    "tseg_service_name": "", "tseg_order_status": "Not found",
                    "tseg_service_start": "", "tseg_error": "Worker failed",
                }

    awaiting = awaiting.copy()
    awaiting["tseg_service_name"]  = [r.get("tseg_service_name", "")  for r in api_results]
    awaiting["tseg_order_status"]  = [r.get("tseg_order_status", "")  for r in api_results]
    awaiting["tseg_service_start"] = [r.get("tseg_service_start", "") for r in api_results]
    return awaiting


def run_sla_check(df: pd.DataFrame, enrich_tseg: bool = False) -> dict:
    """Main SLA breach detector.

    When enrich_tseg=True each awaiting order is enriched with a live TSEG
    API contract lookup so the frontend can surface REGISTERING breaches
    (the COO's 10d+ registering stall metric).  The CSV-upload /run-sla
    endpoint calls this with enrich_tseg=False to preserve its existing
    behaviour and avoid hitting the TSEG API unnecessarily."""

    col_map = {}
    for field, required in {**REQUIRED_FIELDS, **OPTIONAL_FIELDS}.items():
        # The shared "address" pattern list is too permissive — "property" matches
        # property_mprn before property_line1. For the SLA CSV export the COO
        # specifically wants property_line1, so we override the pattern list
        # locally without touching the shared module.
        if field == "address":
            match = detect_column(
                df.columns, field,
                patterns=["property_line1", "address1", "address", "addr", "postcode"],
                required=required,
            )
        else:
            match = detect_column(df.columns, field, required=required)
        if match:
            col_map[field] = match

    order_col         = col_map["order_id"]
    tseg_col          = col_map["tseg_id"]
    updated_col       = col_map["updated_at"]
    status_col        = col_map["status"]
    address_col       = col_map.get("address")
    provider_col      = col_map.get("provider")
    issue_col         = col_map.get("issue")
    mprn_col          = col_map.get("mprn")
    bill_name_col     = col_map.get("bill_name")
    order_started_col = col_map.get("order_started")

    df = df.copy()
    # Canonical 10-digit zero-padded TSEG IDs — also drives the API lookup
    df[tseg_col] = normalise_tseg_series(df[tseg_col])
    df["_status_lower"] = df[status_col].astype(str).str.lower().str.strip()

    awaiting = df[df["_status_lower"].apply(
        lambda s: any(k in s for k in AWAITING_KEYWORDS)
    )].copy()

    awaiting["days_elapsed"] = compute_days_elapsed(awaiting, updated_col)
    awaiting["rag"] = awaiting["days_elapsed"].apply(rag_status)

    # Fuel tag — only present if Trevor exports the MPRN column.
    # When MPRN is missing the fuel column is omitted entirely so we don't
    # mislead anyone with a hard-coded "Elec only" assumption.
    if mprn_col:
        awaiting["fuel"] = awaiting[mprn_col].apply(classify_fuel)

    # Cohort — aligned with the WIP module's 0-30 / 30-60 / 60+ buckets.
    # Only computed when the Trevor export includes order_started_bill_setups_at.
    if order_started_col and order_started_col in awaiting.columns:
        parsed_started = pd.to_datetime(awaiting[order_started_col], dayfirst=True, errors="coerce")
        today = pd.Timestamp(date.today())
        awaiting["cohort_days"] = (today - parsed_started).dt.days.fillna(-1).astype(int)
        awaiting["cohort"] = awaiting["cohort_days"].apply(_cohort_label)
    else:
        awaiting["cohort_days"] = -1
        awaiting["cohort"] = ""

    # ── TSEG API enrichment (optional) ────────────────────────────────
    registering_breach_count = 0
    if enrich_tseg and not awaiting.empty:
        awaiting = _enrich_with_tseg_api(awaiting, tseg_col)
        awaiting["registering_breach"] = (
            (awaiting["tseg_order_status"].astype(str).str.upper().str.strip() == "REGISTERING")
            & (awaiting["days_elapsed"] >= REGISTERING_BREACH_DAYS)
        )
        registering_breach_count = int(awaiting["registering_breach"].sum())
    else:
        awaiting["tseg_service_name"]  = ""
        awaiting["tseg_order_status"]  = ""
        awaiting["tseg_service_start"] = ""
        awaiting["registering_breach"] = False

    awaiting = awaiting.sort_values("days_elapsed", ascending=False)

    # Bill name (e.g. "Octopus - Gas") is preferred over plain supplier name
    # in the table because it lets the ops team spot gas bills on elec-only
    # properties at a glance — those are typically what need to be deleted.
    # The supplier breakdown chart further down still groups by provider so
    # the per-supplier rollup remains intact.
    table_provider_col = bill_name_col or provider_col

    # Default visible columns — the comprehensive CSV export is assembled on
    # the frontend and can use any of the fields carried on each row.
    out_cols = []
    for c in [order_col, tseg_col, address_col, table_provider_col, issue_col, updated_col, status_col]:
        if c and c in awaiting.columns and c not in out_cols:
            out_cols.append(c)
    out_cols += ["days_elapsed", "rag"]
    if enrich_tseg:
        out_cols += ["tseg_order_status", "tseg_service_name"]
    if mprn_col:
        out_cols.append("fuel")

    # Keep all enrichment + cohort + underlying column data on every row so
    # the frontend CSV export can pull everything it needs without a second
    # server round-trip.
    carry_cols = [order_col, tseg_col, address_col, provider_col, bill_name_col,
                  updated_col, status_col, order_started_col, mprn_col, issue_col]
    carry_cols = [c for c in carry_cols if c and c in awaiting.columns]
    extra_cols = ["days_elapsed", "rag", "fuel", "cohort", "cohort_days",
                  "tseg_service_name", "tseg_order_status", "tseg_service_start",
                  "registering_breach"]
    all_row_cols = list(dict.fromkeys(carry_cols + [c for c in extra_cols if c in awaiting.columns]))
    result = awaiting[all_row_cols].copy().fillna("")

    total_awaiting = len(result)
    breached       = int((result["rag"] == "breached").sum())
    at_risk        = int((result["rag"] == "at_risk").sum())
    ok             = int((result["rag"] == "ok").sum())

    # Supplier breakdown is computed from the full `awaiting` frame so it works
    # regardless of whether the table column is bill_name or provider.
    supplier_breakdown = []
    if provider_col and provider_col in awaiting.columns:
        for supplier, group in awaiting.groupby(provider_col):
            if not supplier:
                continue
            supplier_breakdown.append({
                "supplier": supplier,
                "breached": int((group["rag"] == "breached").sum()),
                "at_risk":  int((group["rag"] == "at_risk").sum()),
                "ok":       int((group["rag"] == "ok").sum()),
                "total":    len(group),
            })
        supplier_breakdown.sort(key=lambda x: x["total"], reverse=True)

    return {
        "summary": {
            "total_awaiting":      total_awaiting,
            "breached":            breached,
            "at_risk":             at_risk,
            "ok":                  ok,
            "registering_breach":  registering_breach_count,
        },
        "supplier_breakdown": supplier_breakdown,
        "columns":            out_cols,
        "rows":               result.to_dict(orient="records"),
        # Column metadata — lets the frontend build a consistent CSV export
        # without having to re-detect columns itself.
        "col_meta": {
            "order_id":             order_col,
            "tseg_id":              tseg_col,
            "address":              address_col or "",
            "provider":             provider_col or "",
            "bill_name":            bill_name_col or "",
            "status":               status_col,
            "updated_at":           updated_col,
        },
        "enriched": bool(enrich_tseg),
    }
