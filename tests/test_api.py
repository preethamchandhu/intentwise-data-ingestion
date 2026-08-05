from unittest.mock import AsyncMock, patch
import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.schemas.ingestion import IngestionJobResponse, JobStatus
from datetime import datetime, timezone

from app.storage.database import DatabaseStorage

client = TestClient(app)


@pytest.fixture(autouse=True)
def setup_db():
    DatabaseStorage().init_db()



def test_root_endpoint():
    response = client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert "docs_url" in data


def test_list_demo_sources():
    response = client.get("/api/v1/demo/sources")
    assert response.status_code == 200
    demos = response.json()
    assert len(demos) >= 3
    source_ids = [d["id"] for d in demos]
    assert "rick_and_morty" in source_ids
    assert "dummy_json" in source_ids
    assert "jsonplaceholder" in source_ids


def test_list_jobs_empty():
    response = client.get("/api/v1/jobs")
    assert response.status_code == 200
    assert isinstance(response.json(), list)


def test_query_data_empty():
    response = client.get("/api/v1/data")
    assert response.status_code == 200
    assert isinstance(response.json(), list)


@patch("app.api.routes.engine.run_ingestion")
def test_trigger_ingestion_endpoint(mock_run_ingestion):
    mock_run_ingestion.return_value = IngestionJobResponse(
        job_id="test-job-123",
        source_name="test_api_source",
        endpoint_url="https://api.example.com/data",
        status=JobStatus.COMPLETED,
        records_ingested=5,
        created_at=datetime.now(timezone.utc),
        completed_at=datetime.now(timezone.utc)
    )

    payload = {
        "name": "test_api_source",
        "endpoint_url": "https://api.example.com/data",
        "pagination": {
            "strategy": "none"
        },
        "max_pages": 1,
        "max_records": 10
    }

    response = client.post("/api/v1/ingest", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["job_id"] == "test-job-123"
    assert data["status"] == "COMPLETED"
    assert data["records_ingested"] == 5
