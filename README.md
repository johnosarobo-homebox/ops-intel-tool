# homebox Ops Intel

Internal operational intelligence tool for the Homebox Sheffield ops team. Automates TSEG order monitoring, SLA breach detection and supplier performance tracking — replacing manual daily cross-checks across Trevor, the TSEG WIP sheet and Filament that previously consumed up to half a working day.

Built with Python, FastAPI and pandas. Runs in the browser — no installation required per user once deployed.

---

## Modules

### Gas checker
Reads Homebox meter data directly from the Trevor Gas-checker Google Sheet (or via manual CSV upload) and flags properties that need attention.

- **Load from Trevor** button pulls live data from `GAS_SHEET_URL` — no CSV export needed
- Manual CSV upload preserved as fallback
- Auto-detects column names across different Trevor/TSEG export formats
- TSEG IDs normalised to canonical 10-digit zero-padded form before any join
- Four flags:
  - **Elec only (BTR)** — build-to-rent property, electricity-only by design (navy)
  - **Gas error** — MPRN missing but gas supplier assigned (red)
  - **Review manually** — both MPRN and gas supplier present (amber)
  - **Gas ok** — no further action needed (green)
- Per-flag counts in metric cards
- Download results as CSV or push to a dated Google Sheets tab (TSEG ID columns are written as text so leading zeros survive)

### SLA monitor
Filters all orders in "awaiting feedback" status and calculates days elapsed since last update.

- **Load from Trevor** button pulls live data from `SLA_SHEET_URL`
- Manual CSV upload preserved as fallback
- RAG-rated: **Breached** (8+ days, red), **At risk** (6-7 days, amber), **Within SLA** (green)
- Days elapsed shown as large coloured numbers for at-a-glance scanning
- Supplier breakdown with bar chart showing which providers have the most breaches
- Search by order ID with real-time filtering
- Sortable columns — click any header, defaults to worst-first by days elapsed

### WIP cross-reference
Three-source cross-reference: Homebox Trevor data, the live TSEG WIP Google Sheet, and the live TSEG production API. Every matched order is enriched with the current contract status from TSEG.

**Sources:**
1. **Homebox** — read from `WIP_SHEET_URL` (Trevor sheet) or via manual CSV upload
2. **TSEG WIP sheet** — live Google Sheet URL pasted by the user. The `WIP Overview` tab is skipped. Tabs read: Objections, Missing Meter Information, Gas Deleted, Switch Issues, Missing Tenant Details, API Order Errors, ET Requests
3. **TSEG API** — `GET /wholesale/contracts/{tseg_id}` called for every matched order. 0.2s rate-limited. Errors handled per-row so a single bad ID can't crash the batch

All three sources are joined on the canonical normalised TSEG ID (10-digit zero-padded).

**Default table view:** ORDER ID · PROPERTY · BILL PROVIDER · DAYS ELAPSED · RAG · WIP TAB. Each row has an expand arrow that reveals three side-by-side zones:
- **Homebox** (navy header) — TSEG ID, bill state, bill state updated, cohort
- **TSEG WIP sheet** (amber header) — wip tab, reason, provider, supply status, services start
- **TSEG API live** (green header) — service name, order status (coloured badge: ACTIVE/OBJECTED/REGISTERING/NOT ORDERED), service start

Only one row can be expanded at a time.

**Other features:**
- Objections ranked by supplier with horizontal orange bar chart
- Supplier WIP heat map showing blocker breakdown per supplier
- **Cohort intelligence** — orders grouped by age based on `order_started_bill_setups_at`: **0–30 days**, **30–60 days**, **60+ days**
  - Three summary cards showing order count, top blocker, top supplier and average days elapsed per cohort
  - Pie chart showing cohort distribution (Homebox orange shades)
  - Stacked bar chart showing blocker type breakdown per cohort
  - Cohort filter buttons that combine with existing blocker type filters (e.g. Objections + 60+ days)
- Filter by blocker type using tab pills
- **"Not on WIP" filter** — isolates orders with no WIP match, highlighted in red when viewing All
- Search by order ID across all columns
- Sortable columns with days elapsed defaulting to worst-first
- Loading spinner on the run button while TSEG API enrichment is in progress
- Last-refreshed timestamp displayed in the navbar

---

## UI features

- Light/dark mode toggle saved to localStorage
- Homebox brand orange (#F05A28) accent throughout
- Four global metric cards: Orders processed, SLA breached, At risk, Active objections
- Tab badges showing breach count (SLA) and matched count (WIP)
- "Load from Trevor" buttons on every tab for live Google Sheets sources
- Drag-and-drop CSV upload zones preserved as a manual fallback on every tab
- Flat design with 0.5px borders, no gradients or shadows
- Real-time search and column sorting on SLA and WIP tables
- Last-refreshed timestamp shown in navbar
- Loading spinner on the run button during long-running operations (e.g. TSEG API batch calls)
- Proper error messages for failed uploads, missing columns and API errors

---

## Project structure

```
homebox-tools/
├── app.py                 # FastAPI server — routes, file validation, startup
├── gas_checker.py         # Gas fuel error detection (4-flag classifier + CLI)
├── sla_checker.py         # SLA breach monitoring
├── wip_checker.py         # WIP cross-reference (3-source: Trevor + WIP sheet + TSEG API)
├── tseg_api.py            # TSEG production API client (rate-limited, error-tolerant)
├── sheets.py              # Google Sheets read/write integration
├── utils.py               # Shared — column detection, TSEG ID normalisation, RAG logic
├── build_presentation.py  # Standalone PPTX pitch deck generator
├── requirements.txt       # Python dependencies
├── .gitignore
└── static/
    └── index.html         # Single-page frontend (dark/light theme)
```

---

## Setup

### Prerequisites
- Python 3.10+
- Google Cloud service account with Sheets and Drive API access (for Sheets push and WIP sync features)

### Install

```bash
pip install -r requirements.txt
```

### Google credentials

**Option A — Environment variable (recommended for deployment):**
```bash
export GOOGLE_CREDENTIALS='{"type":"service_account","project_id":"..."}'
```

**Option B — Local file (development only):**
Place `credentials.json` in the project root. This file is gitignored.

### Environment variables

For the V2 live data sources and TSEG API enrichment:

```bash
# TSEG production API (provided by Tom)
export TSEG_API_KEY=...
export TSEG_API_SECRET=...
export TSEG_BASE_URL=https://api.durham.cloud

# Trevor live Google Sheets — drive the "Load from Trevor" buttons
export GAS_SHEET_URL=https://docs.google.com/spreadsheets/d/.../edit
export SLA_SHEET_URL=https://docs.google.com/spreadsheets/d/.../edit
export WIP_SHEET_URL=https://docs.google.com/spreadsheets/d/.../edit
```

The CSV-upload paths still work without any of these set — they're only required for the live source buttons.

### Run

```bash
python app.py
```

Opens automatically at `http://localhost:8000`.

---

## Usage

Each module supports a **live mode** (recommended) and a **manual CSV upload** fallback.

1. **Gas checker**
   - Live: click **Load from Trevor** to pull directly from `GAS_SHEET_URL`
   - Manual: upload Homebox Trevor export + TSEG supplier export and click "Run analysis"
2. **SLA monitor**
   - Live: click **Load from Trevor** to pull directly from `SLA_SHEET_URL`
   - Manual: upload Homebox Trevor export and click "Run analysis"
3. **WIP cross-reference**
   - Live: paste the TSEG WIP Google Sheets URL and click **Load from Trevor + TSEG API**. Homebox data is read from `WIP_SHEET_URL` and every matched order is enriched with a live TSEG API call
   - Manual: upload Homebox Trevor export, paste the WIP Google Sheets URL, and click "Run analysis"

All modules auto-detect column names so they work across different export formats without manual renaming. TSEG IDs are normalised to a canonical 10-digit zero-padded form before any join or API call.

### CLI (gas checker only)

```bash
python gas_checker.py --homebox homebox_export.csv --tseg tseg_data.csv --output flagged.csv
```

---

## Tech stack

| Component | Purpose |
|---|---|
| Python 3.12 | Language |
| FastAPI | Web framework |
| pandas | Data processing |
| gspread + google-auth | Google Sheets read/write integration |
| requests | TSEG production API client |
| Chart.js (CDN) | Cohort pie chart and stacked bar chart |

---

## Security

- All processing is in-memory per request — no data stored server-side
- Google credentials loaded from environment variable in production
- CORS restricted to localhost by default
- File upload size limited to 20 MB
