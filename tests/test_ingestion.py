import pytest
from unittest.mock import AsyncMock, patch

from app.ingestion.client import IngestionClient
from app.ingestion.engine import IngestionEngine, extract_records
from app.ingestion.pagination import (
    CursorPaginationStrategy,
    LinkHeaderPaginationStrategy,
    NoPaginationStrategy,
    OffsetLimitPaginationStrategy,
    PageNumberPaginationStrategy,
    PaginationFactory,
    resolve_json_path,
)
from app.schemas.ingestion import (
    AuthConfig,
    AuthType,
    IngestionRequestConfig,
    JobStatus,
    PaginationConfig,
    PaginationType,
)
from app.storage.database import DatabaseStorage


def test_resolve_json_path():
    data = {
        "info": {
            "next": "https://api.com?page=2",
            "meta": {"count": 100}
        },
        "results": [{"id": 1}, {"id": 2}]
    }
    assert resolve_json_path(data, "info.next") == "https://api.com?page=2"
    assert resolve_json_path(data, "info.meta.count") == 100
    assert resolve_json_path(data, "results.0.id") == 1
    assert resolve_json_path(data, "non_existent") is None


def test_extract_records():
    # Test nested key
    data = {"data": {"items": [{"id": 1}, {"id": 2}]}}
    records = extract_records(data, "data.items")
    assert len(records) == 2

    # Test auto-detect root array
    array_data = [{"id": 10}, {"id": 20}]
    assert extract_records(array_data) == array_data

    # Test auto-detect 'results' key
    dict_data = {"results": [{"id": 100}]}
    assert extract_records(dict_data) == [{"id": 100}]


def test_page_number_pagination():
    config = PaginationConfig(
        strategy=PaginationType.PAGE_NUMBER,
        page_param="page",
        start_page=1
    )
    strategy = PaginationFactory.get_strategy(config)
    assert strategy.get_initial_params() == {"page": 1}

    next_params, has_more = strategy.get_next_params(
        response_data={}, response_headers={}, records_count=10
    )
    assert has_more is True
    assert next_params == {"page": 2}

    # Stop when records_count is 0
    _, has_more_empty = strategy.get_next_params(
        response_data={}, response_headers={}, records_count=0
    )
    assert has_more_empty is False


def test_offset_limit_pagination():
    config = PaginationConfig(
        strategy=PaginationType.OFFSET_LIMIT,
        offset_param="skip",
        limit_param="limit",
        limit_value=10
    )
    strategy = PaginationFactory.get_strategy(config)
    assert strategy.get_initial_params() == {"skip": 0, "limit": 10}

    next_params, has_more = strategy.get_next_params(
        response_data={}, response_headers={}, records_count=10
    )
    assert has_more is True
    assert next_params == {"skip": 10, "limit": 10}

    # Stop when returned count is less than limit_value
    _, has_more_partial = strategy.get_next_params(
        response_data={}, response_headers={}, records_count=5
    )
    assert has_more_partial is False


def test_cursor_pagination_full_url():
    config = PaginationConfig(
        strategy=PaginationType.CURSOR,
        cursor_path="info.next"
    )
    strategy = PaginationFactory.get_strategy(config)

    response_data = {
        "info": {
            "next": "https://rickandmortyapi.com/api/character?page=2"
        }
    }
    next_params, has_more = strategy.get_next_params(
        response_data=response_data, response_headers={}, records_count=20
    )
    assert has_more is True
    assert next_params == {"page": "2"}


def test_link_header_pagination():
    config = PaginationConfig(strategy=PaginationType.LINK_HEADER)
    strategy = PaginationFactory.get_strategy(config)

    headers = {
        "Link": '<https://api.github.com/user/repos?page=2&per_page=10>; rel="next"'
    }
    next_params, has_more = strategy.get_next_params(
        response_data=[], response_headers=headers, records_count=10
    )
    assert has_more is True
    assert next_params == {"page": "2", "per_page": "10"}


def test_auth_header_injection():
    client = IngestionClient()

    headers, params = client._prepare_headers_and_params(
        {}, {}, AuthConfig(type=AuthType.BEARER, value="secret-token")
    )
    assert headers["Authorization"] == "Bearer secret-token"

    headers, params = client._prepare_headers_and_params(
        {}, {}, AuthConfig(type=AuthType.API_KEY_HEADER, key="X-API-Key", value="abc123")
    )
    assert headers["X-API-Key"] == "abc123"

    headers, params = client._prepare_headers_and_params(
        {}, {}, AuthConfig(type=AuthType.API_KEY_QUERY, key="api_key", value="abc123")
    )
    assert params["api_key"] == "abc123"


def test_auth_missing_value_raises():
    client = IngestionClient()
    with pytest.raises(ValueError, match="auth.value"):
        client._prepare_headers_and_params({}, {}, AuthConfig(type=AuthType.BEARER, value=None))


def test_auth_missing_key_raises():
    client = IngestionClient()
    with pytest.raises(ValueError, match="auth.key"):
        client._prepare_headers_and_params(
            {}, {}, AuthConfig(type=AuthType.API_KEY_HEADER, key=None, value="abc123")
        )


@pytest.mark.asyncio
async def test_engine_run_ingestion_mocked(tmp_path):
    db_file = tmp_path / "test.db"
    storage = DatabaseStorage()
    with patch("app.storage.database.engine", storage._session_factory.kw["bind"]):
        storage.init_db()

    config = IngestionRequestConfig(
        name="test_source",
        endpoint_url="https://api.example.com/items",
        pagination=PaginationConfig(
            strategy=PaginationType.PAGE_NUMBER,
            page_param="page",
            start_page=1
        ),
        max_pages=2,
        max_records=15
    )

    mock_client = AsyncMock()
    # Page 1 returns 10 items, Page 2 returns 10 items
    mock_client.fetch_page.side_effect = [
        ([{"id": i} for i in range(1, 11)], {}),
        ([{"id": i} for i in range(11, 21)], {})
    ]

    engine = IngestionEngine(storage=storage)

    with patch("app.ingestion.engine.IngestionClient", return_value=mock_client):
        job = await engine.run_ingestion(config)

    assert job.status == JobStatus.COMPLETED
    assert job.records_ingested == 15  # Capped at max_records=15

    stored_records = storage.get_records(source_name="test_source", job_id=job.job_id)
    assert len(stored_records) == 15
