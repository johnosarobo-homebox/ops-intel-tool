import json
import os

import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import pandas as pd
from pathlib import Path

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

CREDENTIALS_PATH = Path(__file__).parent / "credentials.json"


def get_client():
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
    return gspread.authorize(creds)


def push_to_sheets(df: pd.DataFrame, sheet_url: str) -> str:
    client = get_client()
    spreadsheet = client.open_by_url(sheet_url)
    tab_name = datetime.now().strftime("%Y-%m-%d %H:%M")
    worksheet = spreadsheet.add_worksheet(title=tab_name, rows=len(df) + 10, cols=len(df.columns) + 2)

    # Convert values to native Python types to avoid gspread serialization issues
    header = df.columns.tolist()
    rows = df.astype(object).where(df.notna(), "").values.tolist()

    # If a TSEG-ID-like column is present, prefix values with a leading apostrophe
    # so Google Sheets stores them as text and never strips leading zeros.
    tseg_like = {"bill_payment_reference", "tseg_id", "TSEG ID"}
    tseg_idx = [i for i, h in enumerate(header) if h in tseg_like or "payment_reference" in h.lower()]
    if tseg_idx:
        for r in rows:
            for i in tseg_idx:
                v = r[i]
                if v not in ("", None) and not str(v).startswith("'"):
                    r[i] = "'" + str(v)

    worksheet.update([header] + rows)
    worksheet.freeze(rows=1)
    return tab_name


def read_sheet_as_df(sheet_url: str, tab_title: str | None = None) -> pd.DataFrame:
    """Read a Google Sheet (by URL) into a pandas DataFrame.
    If tab_title is None, reads the first tab. All values are read as strings —
    callers must coerce types as needed (e.g. via normalise_tseg_id).
    """
    client = get_client()
    spreadsheet = client.open_by_url(sheet_url)
    ws = spreadsheet.worksheet(tab_title) if tab_title else spreadsheet.sheet1

    raw = ws.get_all_values()
    if not raw:
        return pd.DataFrame()

    # De-duplicate / fill blank header cells so pandas doesn't choke
    headers_row = raw[0]
    seen = {}
    clean_headers = []
    for h in headers_row:
        h = (h or "").strip()
        if not h:
            h = f"_col_{len(clean_headers)}"
        if h in seen:
            seen[h] += 1
            h = f"{h}_{seen[h]}"
        else:
            seen[h] = 0
        clean_headers.append(h)

    data_rows = [row for row in raw[1:] if any(c.strip() for c in row)]
    # Pad short rows so DataFrame construction is even
    width = len(clean_headers)
    data_rows = [(row + [""] * width)[:width] for row in data_rows]

    return pd.DataFrame(data_rows, columns=clean_headers)
