# Generic Data Ingestion Service

A Python service (FastAPI + SQLAlchemy + HTTPX) that pulls data from third-party REST APIs and persists it, without being written for any single API. You describe a source through a config — endpoint, auth, pagination style — and the service handles the rest.

## Why generic

Most ingestion scripts are one-offs: hardcode the URL, hardcode the pagination logic, hardcode the field names. That breaks the moment you need a second source. Here the pagination logic, auth, and data extraction are all driven by the request config, so adding a new source is a POST request, not a code change.

## What it handles

**Pagination** — four strategies, picked per source via config:
- page number (`?page=1`, `?page=2` — JSONPlaceholder, GitHub-style)
- offset/limit (`?offset=0&limit=20` or `?skip=0&limit=20` — DummyJSON, PokeAPI)
- cursor / next-URL in the JSON body (`info.next`, `meta.cursor`)
- RFC 5988 `Link` headers (`Link: <url>; rel="next"`)
- or no pagination at all, for single-page sources

**Resilience** — retries with exponential backoff on 429/500/502/503/504 and timeouts, honors `Retry-After` when the server sends one, and a configurable delay between page requests so you don't hammer someone's API.

**Data extraction** — dot-notation path into the response (`results`, `data.items`, `products`, etc.), or if you don't specify one, it tries to auto-detect where the list of records lives.

**Storage** — records are stored as raw JSON (`raw_payload`) rather than forced into a rigid schema, since different APIs return different shapes and I didn't want the pipeline to break every time an upstream field changes. Job runs are tracked separately with status (`PENDING` → `RUNNING` → `COMPLETED`/`FAILED`), record counts, and error messages.

## Project layout

```
intentwise-data-ingestion/
├── app/
│   ├── main.py                  # FastAPI app, lifespan/startup
│   ├── api/routes.py            # /ingest, /jobs, /data, /demo endpoints
│   ├── ingestion/
│   │   ├── engine.py            # orchestrates a run: paginate, extract, save
│   │   ├── client.py            # async HTTP client with retry/backoff
│   │   └── pagination.py        # the pagination strategies
│   ├── storage/database.py      # SQLAlchemy models + storage layer
│   └── schemas/ingestion.py     # pydantic request/response models
├── tests/
├── requirements.txt
├── .env.example
└── README.md
```

## Running it

Needs Python 3.9+.

```bash
git clone <repository_url>
cd intentwise-data-ingestion
python -m venv venv
source venv/bin/activate   # venv\Scripts\Activate.ps1 on Windows

pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

Then:
- Swagger docs at http://localhost:8000/docs
- Health check at http://localhost:8000/

## Trying it out

Run the tests:
```bash
python -m pytest tests/ -v
```

Or hit one of the four built-in demo sources, which exercise different pagination styles against real public APIs:

```bash
curl -X POST "http://localhost:8000/api/v1/demo/ingest/rick_and_morty"     # cursor/next-URL
curl -X POST "http://localhost:8000/api/v1/demo/ingest/dummy_json"        # offset/limit (skip & limit)
curl -X POST "http://localhost:8000/api/v1/demo/ingest/jsonplaceholder"   # page number
curl -X POST "http://localhost:8000/api/v1/demo/ingest/pokeapi"           # offset/limit
```

Or point it at any API of your own:

```bash
curl -X POST "http://localhost:8000/api/v1/ingest" -H "Content-Type: application/json" -d '{
  "name": "my_custom_api",
  "endpoint_url": "https://api.example.com/v1/data",
  "method": "GET",
  "headers": { "User-Agent": "Custom-Ingestor" },
  "params": { "category": "electronics" },
  "auth": { "type": "api_key_header", "key": "X-API-Key", "value": "your_secret_key" },
  "pagination": {
    "strategy": "offset_limit",
    "offset_param": "offset",
    "limit_param": "limit",
    "limit_value": 20
  },
  "data_key": "data.items",
  "max_pages": 5,
  "max_records": 100
}'
```

Then check what got stored:
```
GET http://localhost:8000/api/v1/jobs
GET http://localhost:8000/api/v1/data?source_name=rick_and_morty_characters
```

## Design decisions worth explaining

**Strategy pattern for pagination.** Every API paginates differently, so pagination logic lives in small, swappable classes (`PageNumberPaginationStrategy`, `OffsetLimitPaginationStrategy`, `CursorPaginationStrategy`, `LinkHeaderPaginationStrategy`) behind a common interface. Adding a new pagination style later just means implementing `BasePaginationStrategy`, not touching the engine.

**Repository pattern for storage.** `AbstractStorage` keeps the ingestion engine from knowing anything about SQLite specifically. It's SQLite now because it's zero-setup for local evaluation, but swapping in Postgres or an S3-backed sink later is just a new class implementing the same interface.

**Storing raw JSON instead of a fixed schema.** Real APIs add and remove fields over time, and forcing a rigid table per source felt like it'd break constantly. Records are stored as `raw_payload` (JSON text) plus indexed metadata (`source_name`, `job_id`, `record_index`, `ingested_at`) so you can query and filter without caring about the internal shape.

**Retry/backoff in the client.** Transient failures (429s, 5xx, timeouts) are common when hitting live APIs. The client retries with exponential backoff and respects `Retry-After` when a server sends one, instead of hammering it.

## Tradeoffs

- DB writes are synchronous even though the ingestion loop is async — for SQLite this avoids locking issues, at the cost of not being the most "correctly async" thing possible.
- Jobs run inline within the request/background task rather than on a separate worker queue. Fine for the scale this needs to handle; wouldn't be for millions of records.
- Auth is validated up front — if `auth.type` isn't `none`, the config needs a `value` (and `key`, for header/query auth), or the job fails immediately with a clear error rather than quietly sending an unauthenticated request.

## If I had more time

- A real worker queue (Celery/Temporal + Redis) instead of running jobs inline
- An S3/data-lake sink for larger volumes
- Optional JSON Schema validation on ingested records before they're persisted
- Basic metrics (latency, retry counts, records/sec) — right now you only get logs

## Note on AI tool usage

I used an AI coding agent (Google Antigravity) for a lot of the scaffolding — the schema definitions, pagination strategy skeletons, and the initial test suite.

One thing worth flagging: the first pass at `CursorPaginationStrategy` assumed cursor values would always be plain tokens (e.g. `cursor: "eyJpZCI6MTB9"`) appended as `?cursor=<token>`. That broke against the real Rick & Morty API, where `info.next` is actually a full URL (`https://rickandmortyapi.com/api/character?page=2`), not a token — sending it as `?cursor=https://...` just failed. Found this while testing against the live API rather than mocks. Fixed it by checking whether the extracted cursor value looks like a full URL and, if so, parsing its query params out directly instead of trying to shove the whole URL into a `cursor` param.
