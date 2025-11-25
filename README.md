# Python FastAPI Backend

Simple scraping API built with FastAPI, httpx, BeautifulSoup, and Playwright.

## Requirements
- Python 3.10+
- pip
- Playwright browsers (installed via command below)

## Setup (local)
1) Create and activate a virtualenv
```
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\\Scripts\\activate
```

2) Install dependencies and browsers
```
pip install -r requirements.txt
python -m playwright install --with-deps
```

3) Run the API (dev)
```
uvicorn app:app --host 0.0.0.0 --port 8010 --reload
```
Open http://localhost:8010/docs for Swagger UI.

## Environment variables (optional)
- `PLAYWRIGHT_PROXY` — proxy for all outbound HTTP/Playwright traffic
  - example: `http://user:pass@host:port`
- `MAPS_TIME_BUDGET_MS` — max time budget for Google Maps scraping (default 25000)

## Production (Gunicorn)
```
gunicorn -k uvicorn.workers.UvicornWorker -w 2 -b 0.0.0.0:8000 app:app
```

## Key endpoints
- `POST /api/scrape/url` — scrape a single website
- `POST /api/scrape/search` — search web and scrape results
- `POST /api/scrape/collect` — collect website origins from search
- `POST /api/scrape/maps` — collect listings from Google Maps URL
- `POST /api/scrape/import-csv` — import CSV/XLSX rows (flexible headers)
- `GET  /api/scrape/results` — list results (or `?format=csv` to export)
- `POST /api/scrape/clear` — clear in-memory results

## Notes
- Results are stored in memory (no database). Restart clears data.
- For best Maps reliability in cloud, consider setting `PLAYWRIGHT_PROXY`.
