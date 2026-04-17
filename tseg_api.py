"""
TSEG production API client.

Wraps the GET /wholesale/contracts/{tseg_id} endpoint and exposes a single
helper, get_contract(tseg_id), that:
  • injects today's date into the request body
  • applies a 0.2s rate-limit between successive calls (cooperative — caller
    just calls in a loop)
  • normalises errors so a single bad ID never crashes a batch run
  • flattens the response services array into a compact summary used by the
    WIP cross-reference UI

Environment variables required:
  TSEG_API_KEY     — production key supplied by Tom
  TSEG_BASE_URL    — defaults to https://api.durham.cloud
"""

import os
import time
from datetime import date
from typing import Optional

import requests


DEFAULT_BASE_URL = "https://api.durham.cloud"
RATE_LIMIT_SECONDS = 0.2
REQUEST_TIMEOUT = 15


def _get_base_url() -> str:
    return os.environ.get("TSEG_BASE_URL", DEFAULT_BASE_URL).rstrip("/")


def _get_headers() -> dict:
    api_key = os.environ.get("TSEG_API_KEY", "")
    return {
        "API-KEY":      api_key,
        "Accept":       "application/json",
        "Content-Type": "application/json",
    }


def get_contract(tseg_id: str, today: Optional[str] = None, sleep: bool = True) -> dict:
    """Fetch the contract for a single normalised TSEG ID.

    Returns a dict with these keys (always — never raises):
      tseg_service_name   — first service name (or "")
      tseg_order_status   — first service order_status, or "Not found" on 404,
                            or "Error" on any other failure
      tseg_service_start  — first service start_date (or "")
      tseg_services       — full list of {name, order_status, start_date}
      tseg_error          — error message if the call failed (else "")
    """
    if not tseg_id:
        return _empty("No TSEG ID")

    url = f"{_get_base_url()}/wholesale/contracts/{tseg_id}"
    body = {"date": today or date.today().isoformat()}

    try:
        resp = requests.get(
            url,
            headers=_get_headers(),
            json=body,
            timeout=REQUEST_TIMEOUT,
        )
    except requests.RequestException as e:
        if sleep:
            time.sleep(RATE_LIMIT_SECONDS)
        return _empty(f"Request failed: {e}", status="Error")

    if sleep:
        time.sleep(RATE_LIMIT_SECONDS)

    if resp.status_code == 404:
        return _empty("Not found", status="Not found")
    if resp.status_code >= 400:
        return _empty(f"HTTP {resp.status_code}", status="Error")

    try:
        payload = resp.json()
    except ValueError:
        return _empty("Invalid JSON", status="Error")

    services = payload.get("services") or []
    if not isinstance(services, list):
        services = []

    flat = []
    for svc in services:
        if not isinstance(svc, dict):
            continue
        flat.append({
            "name":         str(svc.get("name") or ""),
            "order_status": str(svc.get("order_status") or ""),
            "start_date":   str(svc.get("start_date") or ""),
            "supplier":     str(svc.get("supplier") or ""),
        })

    if flat:
        first = flat[0]
        return {
            "tseg_service_name":  first["name"],
            "tseg_order_status":  first["order_status"] or "Unknown",
            "tseg_service_start": first["start_date"],
            "tseg_services":      flat,
            "tseg_error":         "",
        }
    return _empty("No services", status="Unknown")


def _empty(error_msg: str, status: str = "Not found") -> dict:
    return {
        "tseg_service_name":  "",
        "tseg_order_status":  status,
        "tseg_service_start": "",
        "tseg_services":      [],
        "tseg_error":         error_msg,
    }
