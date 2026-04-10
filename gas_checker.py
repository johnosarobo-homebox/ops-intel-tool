import pandas as pd
import argparse
import sys
from datetime import date

from utils import (
    detect_column,
    detect_all_columns,
    ColumnNotFoundError,
    PATTERNS,
    normalise_tseg_series,
    classify_fuel,
)


# ── Flag labels (V2) ─────────────────────────────────────────────────────────

FLAG_BTR    = "Elec only (BTR)"
FLAG_ERROR  = "Gas error"
FLAG_REVIEW = "Review manually"
FLAG_OK     = "Gas ok"

# Treated as "no MPRN" — i.e. no gas meter recorded against this property
NULL_MPRN_VALUES = {"unknown", "n/a", "none", "null", "not found", "-", "", "nan"}

# Treated as "no gas supplier assigned"
NULL_GAS_VALUES = {"unknown", "n/a", "none", "null", "not found", "-", "", "nan", "0"}


def _is_blank(value, blanks):
    if pd.isna(value):
        return True
    return str(value).strip().lower() in blanks


def is_electricity_only(mprn_value):
    """Returns True when the MPRN cell is empty / unknown / placeholder."""
    return _is_blank(mprn_value, NULL_MPRN_VALUES)


def is_gas_assigned(gas_value):
    """Returns True when a gas supplier has been assigned to the property."""
    return not _is_blank(gas_value, NULL_GAS_VALUES)


def classify_row(row, mprn_col, gas_col, business_type_col):
    """V2 four-flag classifier.

    Rules (evaluated top-down):
      • business_type == 'build-to-rent' → Elec only (BTR)   (BTR sites are
        intentionally electricity-only by design — not an error)
      • mprn null AND gas assigned       → Gas error          (a gas supplier
        has been assigned but no MPRN exists — definite data problem)
      • mprn present AND gas assigned    → Review manually    (both flagged —
        could be legitimate or could be a duplicate switch — needs human eye)
      • mprn null (gas not assigned)     → Review manually    (incomplete data —
        we can't confidently say the property is gas-free without a human check)
      • mprn present                     → Gas ok
    """
    btype = ""
    if business_type_col:
        raw = row.get(business_type_col)
        if not pd.isna(raw):
            btype = str(raw).strip().lower()
    if btype == "build-to-rent":
        return FLAG_BTR

    mprn_blank   = is_electricity_only(row.get(mprn_col)) if mprn_col else True
    gas_assigned = is_gas_assigned(row.get(gas_col)) if gas_col else False

    if mprn_blank and gas_assigned:
        return FLAG_ERROR
    if (not mprn_blank) and gas_assigned:
        return FLAG_REVIEW
    if mprn_blank:
        # MPRN missing — incomplete data, never auto-pass as Gas ok
        return FLAG_REVIEW
    return FLAG_OK


def run_gas_check_v2(df: pd.DataFrame) -> dict:
    """Single-source gas check — reads everything from the Trevor Gas-checker
    sheet (which already contains all relevant columns: bill_payment_reference,
    mprn, gas_assigned, business_type, etc.). No separate TSEG file needed.

    Returns a dict shaped for the FastAPI response (summary / columns / rows).
    """
    fields = {
        "tseg_id":       True,
        "order_id":      False,
        "address":       False,
        "mprn":          True,
        "gas":           False,
        "business_type": False,
    }
    col_map = detect_all_columns(df, "Gas checker sheet", fields)

    tseg_col          = col_map["tseg_id"]
    mprn_col          = col_map["mprn"]
    gas_col           = col_map.get("gas")
    business_type_col = col_map.get("business_type")
    order_col         = col_map.get("order_id")
    address_col       = col_map.get("address")

    df = df.copy()
    df[tseg_col] = normalise_tseg_series(df[tseg_col])

    df["flag"] = df.apply(
        lambda r: classify_row(r, mprn_col, gas_col, business_type_col),
        axis=1,
    )

    # At-a-glance fuel tag — surfaces "Has gas" / "Elec only" alongside the flag
    # so the ops team can immediately see which properties have gas without
    # having to interpret the rule-based flag.
    df["fuel"] = df[mprn_col].apply(classify_fuel)

    out_cols = [c for c in [order_col, tseg_col, address_col, mprn_col, gas_col, business_type_col, "fuel", "flag"] if c]
    out_cols = list(dict.fromkeys(out_cols))
    result = df[out_cols].copy().fillna("")

    counts = result["flag"].value_counts().to_dict()
    return {
        "summary": {
            "total":     int(len(result)),
            "elec_btr":  int(counts.get(FLAG_BTR, 0)),
            "gas_error": int(counts.get(FLAG_ERROR, 0)),
            "review":    int(counts.get(FLAG_REVIEW, 0)),
            "gas_ok":    int(counts.get(FLAG_OK, 0)),
        },
        "columns": result.columns.tolist(),
        "rows":    result.to_dict(orient="records"),
    }


# ── Legacy two-source flagger — preserved for the existing /run-gas endpoint ─

def flag_order(row, homebox_map, tseg_map):
    """Legacy 3-flag classifier still used by the CSV-upload /run-gas endpoint.
    The four-flag V2 classifier above (classify_row / run_gas_check_v2) replaces
    this when reading directly from the Trevor Gas-checker sheet."""
    hb_mprn_col = homebox_map.get("mprn")
    tseg_mprn_col = tseg_map.get("mprn")

    hb_mprn = row.get(hb_mprn_col) if hb_mprn_col else None
    tseg_mprn = row.get(tseg_mprn_col) if tseg_mprn_col else None

    hb_elec_only = is_electricity_only(hb_mprn)

    if tseg_mprn_col:
        tseg_elec_only = is_electricity_only(tseg_mprn)
        if hb_elec_only and tseg_elec_only:
            return "electricity only - action required"
        elif hb_elec_only and not tseg_elec_only:
            return "mprn mismatch - review manually"
        else:
            return "gas confirmed - ok"
    else:
        if hb_elec_only:
            return "electricity only - action required"
        return "gas confirmed - ok"


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="TSEG gas fuel checker - flags electricity-only orders."
    )
    parser.add_argument("--homebox", required=True, help="Path to Homebox Trevor export CSV")
    parser.add_argument("--tseg",    required=True, help="Path to TSEG supplier CSV")
    parser.add_argument("--output",  default=None,  help="Output CSV path (optional)")
    args = parser.parse_args()

    output_path = args.output or f"flagged_orders_{date.today()}.csv"

    # ── Load files ────────────────────────────────────────────────────────────
    print(f"\nLoading Homebox file: {args.homebox}")
    try:
        hb = pd.read_csv(args.homebox)
    except Exception as e:
        print(f"  ERROR reading Homebox file: {e}")
        sys.exit(1)

    print(f"Loading TSEG file:    {args.tseg}")
    try:
        tseg = pd.read_csv(args.tseg)
    except Exception as e:
        print(f"  ERROR reading TSEG file: {e}")
        sys.exit(1)

    print(f"\n  Homebox: {len(hb)} rows, {len(hb.columns)} columns")
    print(f"  TSEG:    {len(tseg)} rows, {len(tseg.columns)} columns")

    # ── Detect columns ────────────────────────────────────────────────────────
    hb_fields = {
        "tseg_id":   True,
        "order_id":  True,
        "address":   False,
        "mprn":      True,
        "mpan":      False,
        "fuel_type": False,
    }
    tseg_fields = {
        "tseg_id": True,
        "mprn":    False,
    }

    try:
        print(f"\n  Detecting columns in Homebox file...")
        hb_map = detect_all_columns(hb, "Homebox file", hb_fields)
        for field, col in hb_map.items():
            print(f"    '{field}' -> '{col}'")

        print(f"\n  Detecting columns in TSEG file...")
        tseg_map = detect_all_columns(tseg, "TSEG file", tseg_fields)
        for field, col in tseg_map.items():
            print(f"    '{field}' -> '{col}'")
    except ColumnNotFoundError as e:
        print(f"\n  ERROR: {e}")
        sys.exit(1)

    # ── Normalise join key ────────────────────────────────────────────────────
    print("\n  Normalising TSEG ID columns...")
    hb["_join_key"]   = hb[hb_map["tseg_id"]].astype(str).str.strip().str.upper()
    tseg["_join_key"] = tseg[tseg_map["tseg_id"]].astype(str).str.strip().str.upper()

    # ── Join ──────────────────────────────────────────────────────────────────
    print("  Joining on TSEG ID...")
    tseg_cols = ["_join_key"] + [v for k, v in tseg_map.items() if k != "tseg_id" and v]
    merged = hb.merge(
        tseg[tseg_cols].rename(columns={v: f"tseg_{v}" for k, v in tseg_map.items() if k != "tseg_id" and v}),
        on="_join_key",
        how="left"
    )

    tseg_map_merged = {k: f"tseg_{v}" for k, v in tseg_map.items() if k != "tseg_id" and v}

    print(f"  Matched {len(merged)} rows | {len(tseg) - len(merged[merged['_join_key'].isin(tseg['_join_key'])])} TSEG orders not in Homebox file")

    # ── Flag ──────────────────────────────────────────────────────────────────
    print("  Flagging orders...")
    merged["flag"] = merged.apply(
        lambda row: flag_order(row, hb_map, tseg_map_merged), axis=1
    )

    # ── Build output ──────────────────────────────────────────────────────────
    output_cols = []
    for field, col in hb_map.items():
        if col and col in merged.columns:
            output_cols.append(col)

    for field, col in tseg_map_merged.items():
        if col and col in merged.columns:
            output_cols.append(col)

    output_cols.append("flag")
    output_cols = list(dict.fromkeys(output_cols))

    result = merged[output_cols].copy()
    result.to_csv(output_path, index=False)

    # ── Summary ───────────────────────────────────────────────────────────────
    total          = len(result)
    action_needed  = (result["flag"] == "electricity only - action required").sum()
    gas_ok         = (result["flag"] == "gas confirmed - ok").sum()
    review         = (result["flag"] == "mprn mismatch - review manually").sum()

    print(f"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  TSEG Gas Checker - Complete
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Total orders processed : {total}
  Electricity only       : {action_needed}  (action required)
  Gas confirmed          : {gas_ok}  (ok)
  MPRN mismatch          : {review}  (review manually)

  Output saved to: {output_path}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
""")


if __name__ == "__main__":
    main()
