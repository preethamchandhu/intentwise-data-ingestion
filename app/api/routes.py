from typing import Dict, List, Optional
from fastapi import APIRouter, BackgroundTasks, HTTPException, Query

from app.ingestion.engine import IngestionEngine
from app.schemas.ingestion import (
    AuthConfig,
    AuthType,
    DemoSourceResponse,
    IngestedRecordResponse,
    IngestionJobResponse,
    IngestionRequestConfig,
    PaginationConfig,
    PaginationType,
)
from app.storage.database import DatabaseStorage

router = APIRouter(prefix="/api/v1", tags=["Ingestion Service"])
storage = DatabaseStorage()
engine = IngestionEngine(storage=storage)

# public APIs for the demo endpoints, picked to cover the different pagination styles
DEMO_SOURCES: Dict[str, DemoSourceResponse] = {
    "rick_and_morty": DemoSourceResponse(
        id="rick_and_morty",
        name="Rick & Morty Characters API",
        description="Public API demonstrating Cursor / URL-based next page pagination in JSON body (info.next).",
        endpoint_url="https://rickandmortyapi.com/api/character",
        pagination_style="Cursor / URL (info.next)",
        sample_config=IngestionRequestConfig(
            name="rick_and_morty_characters",
            endpoint_url="https://rickandmortyapi.com/api/character",
            data_key="results",
            pagination=PaginationConfig(
                strategy=PaginationType.CURSOR,
                cursor_path="info.next"
            ),
            max_pages=3,
            max_records=40
        )
    ),
    "dummy_json": DemoSourceResponse(
        id="dummy_json",
        name="DummyJSON Products API",
        description="E-Commerce API demonstrating Offset/Limit pagination using 'skip' and 'limit' query parameters.",
        endpoint_url="https://dummyjson.com/products",
        pagination_style="Offset / Limit (skip & limit)",
        sample_config=IngestionRequestConfig(
            name="dummy_json_products",
            endpoint_url="https://dummyjson.com/products",
            data_key="products",
            pagination=PaginationConfig(
                strategy=PaginationType.OFFSET_LIMIT,
                offset_param="skip",
                limit_param="limit",
                limit_value=10
            ),
            max_pages=3,
            max_records=30
        )
    ),
    "jsonplaceholder": DemoSourceResponse(
        id="jsonplaceholder",
        name="JSONPlaceholder Posts API",
        description="Public REST API returning a root JSON array of post records with page number pagination.",
        endpoint_url="https://jsonplaceholder.typicode.com/posts",
        pagination_style="Page Number (_page & _limit)",
        sample_config=IngestionRequestConfig(
            name="jsonplaceholder_posts",
            endpoint_url="https://jsonplaceholder.typicode.com/posts",
            pagination=PaginationConfig(
                strategy=PaginationType.PAGE_NUMBER,
                page_param="_page",
                limit_param="_limit",
                limit_value=10
            ),
            max_pages=3,
            max_records=30
        )
    ),
    "pokeapi": DemoSourceResponse(
        id="pokeapi",
        name="PokeAPI Pokemon Species",
        description="Open Data API demonstrating Offset & Limit pagination for nested data array.",
        endpoint_url="https://pokeapi.co/api/v2/pokemon",
        pagination_style="Offset / Limit",
        sample_config=IngestionRequestConfig(
            name="pokeapi_pokemon",
            endpoint_url="https://pokeapi.co/api/v2/pokemon",
            data_key="results",
            pagination=PaginationConfig(
                strategy=PaginationType.OFFSET_LIMIT,
                offset_param="offset",
                limit_param="limit",
                limit_value=20
            ),
            max_pages=2,
            max_records=40
        )
    )
}


@router.post("/ingest", response_model=IngestionJobResponse, summary="Trigger Ingestion")
async def trigger_ingestion(config: IngestionRequestConfig):
    job_result = await engine.run_ingestion(config)
    return job_result


@router.get("/jobs", response_model=List[IngestionJobResponse], summary="List Ingestion Jobs")
def list_jobs(
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0)
):
    return storage.list_jobs(limit=limit, offset=offset)


@router.get("/jobs/{job_id}", response_model=IngestionJobResponse, summary="Get Job Details")
def get_job_details(job_id: str):
    job = storage.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job with ID '{job_id}' not found.")
    return job


@router.get("/data", response_model=List[IngestedRecordResponse], summary="Query Ingested Data Records")
def get_ingested_records(
    source_name: Optional[str] = Query(default=None, description="Filter by source name"),
    job_id: Optional[str] = Query(default=None, description="Filter by specific job ID"),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0)
):
    return storage.get_records(
        source_name=source_name,
        job_id=job_id,
        limit=limit,
        offset=offset
    )


@router.get("/demo/sources", response_model=List[DemoSourceResponse], summary="List Pre-configured Public API Demos")
def list_demo_sources():
    return list(DEMO_SOURCES.values())


@router.post("/demo/ingest/{source_id}", response_model=IngestionJobResponse, summary="Trigger Demo Ingestion")
async def trigger_demo_ingestion(source_id: str):
    if source_id not in DEMO_SOURCES:
        raise HTTPException(
            status_code=404,
            detail=f"Demo source '{source_id}' not found. Available sources: {list(DEMO_SOURCES.keys())}"
        )

    demo_config = DEMO_SOURCES[source_id].sample_config
    job_result = await engine.run_ingestion(demo_config)
    return job_result
