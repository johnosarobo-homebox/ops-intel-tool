"""
FastAPI server for Homebox Ops Intel.

Serves the single-page frontend and exposes the analysis endpoints:
  CSV-upload (legacy, kept as manual fallback):
    /run-gas, /run-sla, /run-wip
  Live (V2 — reads from Trevor Google Sheets directly):
    /run-gas-live, /run-sla-live, /run-wip-live

All processing is in-memory per request — no data is persisted server-side.
"""

import io
import os
import json
import asyncio
import pandas as pd
import uvicorn
import webbrowser
import threading

from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from datetime import date
from pathlib import Path

from gas_checker import flag_order, run_gas_check_v2
from sheets import push_to_sheets, read_sheet_as_df
from sla_checker import run_sla_check
from wip_checker import run_wip_check, run_wip_check_live
from utils import detect_all_columns, ColumnNotFoundError, clean_nan_in_rows

BASE_DIR = Path(__file__).parent

# Guard against oversized uploads — 20 MB covers the largest Trevor exports
MAX_FILE_SIZE = 20 * 1024 * 1024

# ── Live-run progress tracking ──────────────────────────────────────────────
# Keyed by the job_id the client generates before kicking off a live run.
# Each entry:  {stage: str, current: int, total: int, done: bool, error: str}
# Dict writes are atomic under the GIL so no explicit lock is needed for the
# simple assignments we do here.  Entries are left in place after the run
# completes — the tool's usage volume is low enough that this is negligible.
PROGRESS_STORE: dict = {}


def _init_progress(job_id: str, stage: str = "Starting...") -> None:
    PROGRESS_STORE[job_id] = {
        "stage": stage, "current": 0, "total": 0, "done": False, "error": "",
    }


def _update_progress(job_id: str, **fields) -> None:
    if job_id in PROGRESS_STORE:
        PROGRESS_STORE[job_id].update(fields)


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:8000",
        "http://127.0.0.1:8000",
    ],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")


@app.get("/")
def root():
    return FileResponse(str(BASE_DIR / "static" / "index.html"))


# ── Server-Sent Events progress stream ──────────────────────────────────────
# The client opens this stream alongside its POST to /run-sla-live or
# /run-wip-live and reads progress updates until the job reports done=True.
# We poll PROGRESS_STORE every 500ms.  If the job_id hasn't been registered
# yet we return a safe default — the POST handler initialises the entry on
# its first line so the race window is tiny.

@app.get("/progress/{job_id}")
async def progress_stream(job_id: str):
    async def generator():
        default = {"stage": "Waiting...", "current": 0, "total": 0, "done": False, "error": ""}
        last_payload = None
        # Safety cap — an SSE stream should never run longer than 15 minutes.
        # At 500ms polling that's 1800 iterations. If the job never completes
        # the client should see no further updates and close its side.
        for _ in range(1800):
            state = PROGRESS_STORE.get(job_id, default)
            payload = json.dumps(state)
            if payload != last_payload:
                yield f"data: {payload}\n\n"
                last_payload = payload
            if state.get("done"):
                break
            await asyncio.sleep(0.5)

    return StreamingResponse(
        generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


async def read_csv(upload: UploadFile, label: str = "file") -> pd.DataFrame:
    """Read an uploaded CSV into a DataFrame with size validation."""
    contents = await upload.read()
    if len(contents) > MAX_FILE_SIZE:
        raise HTTPException(400, f"{label} exceeds maximum size of {MAX_FILE_SIZE // (1024*1024)} MB.")
    try:
        return pd.read_csv(io.BytesIO(contents))
    except Exception as e:
        raise HTTPException(400, f"Could not read {label}: {e}")


def run_gas_check(hb: pd.DataFrame, ts: pd.DataFrame):
    hb_fields = {
        "tseg_id":   True,
        "order_id":  True,
        "address":   False,
        "mprn":      True,
        "mpan":      False,
        "fuel_type": False,
    }
    tseg_fields = {"tseg_id": True, "mprn": False}

    hb_map   = detect_all_columns(hb, "Homebox file", hb_fields)
    tseg_map = detect_all_columns(ts, "TSEG file", tseg_fields)

    hb["_join_key"] = hb[hb_map["tseg_id"]].astype(str).str.strip().str.upper()
    ts["_join_key"] = ts[tseg_map["tseg_id"]].astype(str).str.strip().str.upper()

    tseg_extra   = [v for k, v in tseg_map.items() if k != "tseg_id" and v]
    tseg_renamed = {v: f"tseg_{v}" for v in tseg_extra}

    merged = hb.merge(
        ts[["_join_key"] + tseg_extra].rename(columns=tseg_renamed),
        on="_join_key", how="left"
    )

    tseg_map_merged = {k: f"tseg_{v}" for k, v in tseg_map.items() if k != "tseg_id" and v}
    merged["flag"] = merged.apply(lambda row: flag_order(row, hb_map, tseg_map_merged), axis=1)

    output_cols = list(dict.fromkeys(
        [c for c in [
            hb_map.get("order_id"), hb_map.get("tseg_id"), hb_map.get("address"),
            hb_map.get("mprn"), hb_map.get("mpan"), hb_map.get("fuel_type"),
        ] if c] + list(tseg_map_merged.values()) + ["flag"]
    ))
    output_cols = [c for c in output_cols if c in merged.columns]
    return merged[output_cols].copy().fillna("")


@app.post("/run-gas")
async def run_gas(
    homebox:   UploadFile = File(...),
    tseg:      UploadFile = File(...),
    sheet_url: str = Form(default=""),
):
    hb = await read_csv(homebox, "Homebox file")
    ts = await read_csv(tseg, "TSEG file")

    try:
        result = run_gas_check(hb, ts)
    except ColumnNotFoundError as e:
        raise HTTPException(400, str(e))

    total     = len(result)
    elec_only = int((result["flag"] == "electricity only - action required").sum())
    gas_ok    = int((result["flag"] == "gas confirmed - ok").sum())
    mismatch  = int((result["flag"] == "mprn mismatch - review manually").sum())

    sheet_tab = sheet_error = None
    if sheet_url.strip():
        try:
            sheet_tab = push_to_sheets(result, sheet_url.strip())
        except Exception as e:
            sheet_error = str(e)

    return {
        "summary":     {"total": total, "elec_only": elec_only, "gas_ok": gas_ok, "mismatch": mismatch},
        "columns":     result.columns.tolist(),
        "rows":        result.to_dict(orient="records"),
        "sheet_tab":   sheet_tab,
        "sheet_error": sheet_error,
    }


@app.post("/run-sla")
async def run_sla(
    homebox:   UploadFile = File(...),
    sheet_url: str = Form(default=""),
):
    df = await read_csv(homebox, "Homebox file")

    try:
        result = run_sla_check(df)
    except ColumnNotFoundError as e:
        raise HTTPException(400, str(e))
    except Exception as e:
        raise HTTPException(400, str(e))

    if sheet_url.strip() and result["rows"]:
        try:
            out_df = pd.DataFrame(result["rows"])[result["columns"]]
            sheet_tab = push_to_sheets(out_df, sheet_url.strip())
            result["sheet_tab"] = sheet_tab
        except Exception as e:
            result["sheet_error"] = str(e)

    clean_nan_in_rows(result["rows"])

    return result


@app.post("/run-wip")
async def run_wip(
    homebox:   UploadFile = File(...),
    wip_url:   str = Form(...),
    sheet_url: str = Form(default=""),
):
    df = await read_csv(homebox, "Homebox file")

    try:
        result = run_wip_check(df, wip_url)
    except ColumnNotFoundError as e:
        raise HTTPException(400, str(e))
    except Exception as e:
        raise HTTPException(400, str(e))

    if sheet_url.strip() and result["rows"]:
        try:
            out_df    = pd.DataFrame(result["rows"])
            sheet_tab = push_to_sheets(out_df, sheet_url.strip())
            result["sheet_tab"] = sheet_tab
        except Exception as e:
            result["sheet_error"] = str(e)

    clean_nan_in_rows(result["rows"])

    return result


# ── V2 LIVE ENDPOINTS ────────────────────────────────────────────────────────
# These read directly from the Trevor Google Sheets (URLs are in env vars).
# CSV-upload endpoints above are preserved as manual fallbacks.

def _require_env(name: str) -> str:
    val = os.environ.get(name, "").strip()
    if not val:
        raise HTTPException(500, f"Environment variable {name} is not configured.")
    return val


@app.post("/run-gas-live")
async def run_gas_live(sheet_url: str = Form(default="")):
    url = _require_env("GAS_SHEET_URL")
    try:
        df = read_sheet_as_df(url)
    except Exception as e:
        raise HTTPException(502, f"Could not read Gas Trevor sheet: {e}")
    if df.empty:
        raise HTTPException(400, "Gas Trevor sheet returned no rows.")
    try:
        result = run_gas_check_v2(df)
    except ColumnNotFoundError as e:
        raise HTTPException(400, str(e))

    if sheet_url.strip() and result["rows"]:
        try:
            out_df = pd.DataFrame(result["rows"])[result["columns"]]
            result["sheet_tab"] = push_to_sheets(out_df, sheet_url.strip())
        except Exception as e:
            result["sheet_error"] = str(e)

    clean_nan_in_rows(result["rows"])
    return result


@app.post("/run-sla-live")
def run_sla_live(
    sheet_url: str = Form(default=""),
    job_id:    str = Form(default=""),
):
    """Sync handler — FastAPI runs this in a worker thread so the async SSE
    progress endpoint can serve updates concurrently on the event loop."""
    # Initialise the progress entry on the very first line so the SSE stream
    # (which may already be open) has something to read.
    if job_id:
        _init_progress(job_id, "Loading Trevor data...")

    url = _require_env("SLA_SHEET_URL")
    try:
        df = read_sheet_as_df(url)
    except Exception as e:
        if job_id:
            _update_progress(job_id, done=True, error=str(e))
        raise HTTPException(502, f"Could not read SLA Trevor sheet: {e}")
    if df.empty:
        if job_id:
            _update_progress(job_id, done=True, error="SLA Trevor sheet returned no rows.")
        raise HTTPException(400, "SLA Trevor sheet returned no rows.")

    # Progress callback closure — fires (0, total), then every 10 completed
    # workers, then (total, total).  Stage text is regenerated each tick so
    # the frontend shows accurate counts without needing a second channel.
    def _on_api_progress(current: int, total: int):
        if not job_id:
            return
        _update_progress(
            job_id,
            stage=f"Calling TSEG API ({current} / {total})...",
            current=current, total=total,
        )

    # Stage callback — lets run_sla_check flip the indicator label when it
    # transitions from the TSEG API phase into the WIP presence check.
    def _on_stage(text: str):
        if not job_id:
            return
        _update_progress(job_id, stage=text, current=0, total=0)

    # WIP presence check is best-effort — if the URL is missing we still run
    # the SLA check (the frontend just hides the WIP TAB column in that case).
    wip_url_env = os.environ.get("WIP_SHEET_URL", "").strip() or None

    try:
        result = run_sla_check(
            df,
            enrich_tseg=True,
            progress_cb=_on_api_progress,
            wip_url=wip_url_env,
            stage_cb=_on_stage,
        )
    except ColumnNotFoundError as e:
        if job_id:
            _update_progress(job_id, done=True, error=str(e))
        raise HTTPException(400, str(e))
    except Exception as e:
        if job_id:
            _update_progress(job_id, done=True, error=str(e))
        raise HTTPException(400, str(e))

    if sheet_url.strip() and result["rows"]:
        try:
            out_df = pd.DataFrame(result["rows"])[result["columns"]]
            result["sheet_tab"] = push_to_sheets(out_df, sheet_url.strip())
        except Exception as e:
            result["sheet_error"] = str(e)

    clean_nan_in_rows(result["rows"])

    if job_id:
        _update_progress(
            job_id,
            stage=f"Done — {len(result['rows'])} orders loaded",
            done=True,
        )
    return result


@app.post("/run-wip-live")
def run_wip_live(
    wip_url:   str = Form(...),
    sheet_url: str = Form(default=""),
    job_id:    str = Form(default=""),
):
    """V2 WIP cross-reference. Homebox WIP data is read from WIP_SHEET_URL env var.
    The TSEG WIP sheet URL still comes from the user (Tom's live sheet).
    Each matched order is enriched with a live TSEG API call.

    Sync handler — FastAPI runs this in a worker thread so the SSE progress
    endpoint can serve updates concurrently on the event loop.
    """
    if job_id:
        _init_progress(job_id, "Loading Trevor data...")

    homebox_url = os.environ.get("WIP_SHEET_URL", "").strip()
    if not homebox_url:
        if job_id:
            _update_progress(job_id, done=True, error="WIP_SHEET_URL env var is not set.")
        raise HTTPException(500, "WIP_SHEET_URL env var is not set — cannot load Homebox WIP data.")

    try:
        trevor_df = read_sheet_as_df(homebox_url)
    except Exception as e:
        if job_id:
            _update_progress(job_id, done=True, error=str(e))
        raise HTTPException(502, f"Could not read Homebox WIP Trevor sheet: {e}")

    if trevor_df.empty:
        if job_id:
            _update_progress(job_id, done=True, error="Homebox WIP Trevor sheet is empty.")
        raise HTTPException(400, "Homebox WIP Trevor sheet is empty — check the sheet URL and sharing permissions.")

    # Stage 2 — reading the TSEG WIP sheet (happens inside run_wip_check,
    # before API enrichment).  The stage flips to 'Calling TSEG API...' as
    # soon as the first progress callback tick arrives from the enrichment
    # ThreadPoolExecutor, so no extra hook is needed between these phases.
    if job_id:
        _update_progress(job_id, stage="Reading TSEG WIP sheet...", current=0, total=0)

    def _on_api_progress(current: int, total: int):
        if not job_id:
            return
        _update_progress(
            job_id,
            stage=f"Calling TSEG API ({current} / {total})...",
            current=current, total=total,
        )

    try:
        result = run_wip_check(trevor_df, wip_url, progress_cb=_on_api_progress)
    except ColumnNotFoundError as e:
        if job_id:
            _update_progress(job_id, done=True, error=str(e))
        raise HTTPException(400, str(e))
    except RuntimeError as e:
        if job_id:
            _update_progress(job_id, done=True, error=str(e))
        raise HTTPException(500, str(e))
    except Exception as e:
        if job_id:
            _update_progress(job_id, done=True, error=str(e))
        raise HTTPException(400, str(e))

    if sheet_url.strip() and result["rows"]:
        try:
            out_df    = pd.DataFrame(result["rows"])
            result["sheet_tab"] = push_to_sheets(out_df, sheet_url.strip())
        except Exception as e:
            result["sheet_error"] = str(e)

    clean_nan_in_rows(result["rows"])

    if job_id:
        _update_progress(
            job_id,
            stage=f"Done — {len(result['rows'])} orders loaded",
            done=True,
        )
    return result


@app.post("/download-gas")
async def download_gas(
    homebox: UploadFile = File(...),
    tseg:    UploadFile = File(...),
):
    hb = await read_csv(homebox, "Homebox file")
    ts = await read_csv(tseg, "TSEG file")

    try:
        result = run_gas_check(hb, ts)
    except ColumnNotFoundError as e:
        raise HTTPException(400, str(e))

    buf = io.StringIO()
    result.to_csv(buf, index=False)
    buf.seek(0)
    return StreamingResponse(
        io.BytesIO(buf.getvalue().encode()),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=gas_flagged_{date.today()}.csv"}
    )


def open_browser():
    webbrowser.open("http://localhost:8000")


if __name__ == "__main__":
    threading.Timer(1.5, open_browser).start()
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=False)
