from enum import Enum
from typing import Any, Dict, List, Optional
from datetime import datetime
from pydantic import BaseModel, Field, HttpUrl, ConfigDict


class PaginationType(str, Enum):
    PAGE_NUMBER = "page_number"
    OFFSET_LIMIT = "offset_limit"
    CURSOR = "cursor"
    LINK_HEADER = "link_header"
    NONE = "none"


class AuthType(str, Enum):
    NONE = "none"
    BEARER = "bearer"
    API_KEY_HEADER = "api_key_header"
    API_KEY_QUERY = "api_key_query"


class JobStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class PaginationConfig(BaseModel):
    strategy: PaginationType = Field(
        default=PaginationType.NONE,
        description="Pagination strategy to use"
    )
    page_param: str = Field(
        default="page",
        description="Query parameter name for page number (e.g. 'page', 'p')"
    )
    start_page: int = Field(
        default=1,
        description="Initial page number"
    )
    offset_param: str = Field(
        default="offset",
        description="Query parameter name for offset (e.g. 'offset', 'skip')"
    )
    limit_param: str = Field(
        default="limit",
        description="Query parameter name for page size limit (e.g. 'limit', 'per_page', 'size')"
    )
    limit_value: int = Field(
        default=20,
        description="Number of records per page request"
    )
    cursor_param: str = Field(
        default="cursor",
        description="Query parameter name used to pass cursor to next page request"
    )
    cursor_path: Optional[str] = Field(
        default=None,
        description="Dot-notation path to extract next cursor from response JSON (e.g. 'info.next', 'meta.next_cursor')"
    )


class AuthConfig(BaseModel):
    type: AuthType = Field(default=AuthType.NONE, description="Authentication mechanism")
    key: Optional[str] = Field(default=None, description="Header name or Query param name for API key")
    value: Optional[str] = Field(default=None, description="API Key value or Bearer token")


class IngestionRequestConfig(BaseModel):
    name: str = Field(..., description="Unique human-readable identifier for the data source")
    endpoint_url: str = Field(..., description="Target API endpoint URL")
    method: str = Field(default="GET", description="HTTP method (GET, POST)")
    headers: Dict[str, str] = Field(default_factory=dict, description="Custom HTTP headers")
    params: Dict[str, Any] = Field(default_factory=dict, description="Base query parameters")
    auth: AuthConfig = Field(default_factory=AuthConfig, description="Authentication settings")
    pagination: PaginationConfig = Field(default_factory=PaginationConfig, description="Pagination strategy configuration")
    data_key: Optional[str] = Field(
        default=None,
        description="Dot-notation path to extract records array from response (e.g. 'results', 'data.items'). None for root array."
    )
    max_pages: int = Field(default=5, description="Safety limit for maximum pages to ingest")
    max_records: int = Field(default=100, description="Safety limit for maximum records to ingest")
    rate_limit_delay: float = Field(default=0.1, description="Delay between requests in seconds")


class IngestionJobResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    job_id: str
    source_name: str
    endpoint_url: str
    status: JobStatus
    records_ingested: int
    error_message: Optional[str] = None
    created_at: datetime
    completed_at: Optional[datetime] = None


class IngestedRecordResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    job_id: str
    source_name: str
    record_index: int
    raw_data: Any
    ingested_at: datetime


class DemoSourceResponse(BaseModel):
    id: str
    name: str
    description: str
    endpoint_url: str
    pagination_style: str
    sample_config: IngestionRequestConfig
