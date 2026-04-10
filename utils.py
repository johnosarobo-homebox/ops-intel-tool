"""
Shared utilities used across all analysis modules.

Handles column auto-detection (Trevor exports use inconsistent headers),
RAG/SLA status classification, and data cleaning.
"""

import pandas as pd
from datetime import date


# ── Column detection ─────────────────────────────────────────────────────────

# Maps logical field names to known header variations across Trevor/TSEG exports.
# Detection is case-insensitive and uses substring matching, so adding a pattern
# like "payment" will match "bill_payment_reference" automatically.
PATTERNS = {
    "tseg_id":       ["bill_payment_reference", "payment_reference", "tseg", "payment", "reference", "account", "payref", "pay_ref"],
    "order_id":      ["order_id", "orderid", "order id", "order no", "orderno"],
    "address":       ["address", "property", "postcode", "addr"],
    "mprn":          ["mprn", "gas meter", "gas_meter", "meter point ref"],
    "mpan":          ["mpan", "elec meter", "elec_meter", "electricity meter"],
    "fuel_type":     ["fuel", "fuel type", "fueltype", "service type"],
    "updated_at":    ["updated_at", "state_updated", "bill_state_updated"],
    "status":        ["bill_state", "state", "status"],
    "provider":      ["provider", "supplier", "bill_provider"],
    "order_started": ["order_started_bill_setups_at", "order_started", "started_bill_setups"],
    "issue":         ["issue", "lifecycle_issue", "lifecycle issue"],
    "business_type": ["business_type", "businesstype", "business type"],
    "gas":           ["gas_assigned", "gas assigned", "gas_supplier", "gas"],
}


class ColumnNotFoundError(Exception):
    """Raised when a required column cannot be detected."""
    pass


def detect_column(headers, field, patterns=None, required=True):
    """
    Scan headers for the first match against the pattern list for a given field.
    Returns the matched header name, or None if not found.
    Raises ColumnNotFoundError if required and not found.
    """
    if patterns is None:
        patterns = PATTERNS.get(field, [])
    headers_lower = {h: h.lower() for h in headers}
    for pattern in patterns:
        for original, lower in headers_lower.items():
            if pattern in lower:
                return original
    if required:
        raise ColumnNotFoundError(
            f"Could not detect column for '{field}'. "
            f"Patterns searched: {patterns}. "
            f"Headers found: {list(headers)}. "
            f"Please rename the relevant column so it contains one of: {patterns}"
        )
    return None


def detect_all_columns(df, source_label, fields):
    """
    Run detection for all requested fields against a dataframe's headers.
    Returns a dict mapping field name -> actual column name.
    """
    mapping = {}
    for field, required in fields.items():
        match = detect_column(df.columns, field, required=required)
        if match:
            mapping[field] = match
    return mapping


# ── TSEG ID normalisation ────────────────────────────────────────────────────

def normalise_tseg_id(raw_id):
    """Canonical TSEG ID normalisation — apply to every TSEG ID before any join
    or API call. Handles:
      • float exports from Trevor (e.g. 1234567890.0)
      • leading zero stripping by Google Sheets (e.g. 234567890 -> 0234567890)
      • 8-digit IDs that should be 10 digits
      • whitespace and stray characters
    Returns a 10-character zero-padded string, or None if the input is empty/invalid.
    """
    try:
        s = str(raw_id).strip()
        if not s or s.lower() in ("nan", "none", ""):
            return None
        return str(int(float(s))).zfill(10)
    except (ValueError, TypeError):
        return None


def normalise_tseg_series(series):
    """Vectorised version for pandas Series — returns a Series of normalised IDs
    with empty strings in place of None so the column can still be joined."""
    return series.apply(lambda v: normalise_tseg_id(v) or "")


# ── RAG / SLA helpers ────────────────────────────────────────────────────────

def compute_days_elapsed(df, date_col):
    """Parse a date column and return days elapsed since today.
    Uses dayfirst=True because Trevor exports use DD/MM/YYYY format."""
    parsed = pd.to_datetime(df[date_col], dayfirst=True, errors="coerce")
    today = pd.Timestamp(date.today())
    return (today - parsed).dt.days.fillna(0).astype(int)


def rag_status(days):
    """Classify days elapsed into RAG status.
    Thresholds align with Homebox SLA policy: 8+ = breached, 6-7 = at risk."""
    if days >= 8:
        return "breached"
    if days >= 6:
        return "at_risk"
    return "ok"


# ── NaN cleaning ─────────────────────────────────────────────────────────────

def clean_nan_in_rows(rows):
    """Replace NaN values with empty strings before JSON serialisation.
    pandas .to_dict() can leave float NaN values which aren't valid JSON."""
    for row in rows:
        for k, v in row.items():
            if isinstance(v, float) and v != v:
                row[k] = ""
    return rows
