from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Tuple
from urllib.parse import parse_qs, urlparse

from app.schemas.ingestion import PaginationConfig, PaginationType


def resolve_json_path(data: Any, path: str) -> Any:
    """Walk a dotted path like 'info.next' or 'data.items' through nested dicts/lists."""
    if not path or not data:
        return None

    current = data
    parts = path.split(".")
    for part in parts:
        if isinstance(current, dict) and part in current:
            current = current[part]
        elif isinstance(current, list) and part.isdigit():
            idx = int(part)
            if 0 <= idx < len(current):
                current = current[idx]
            else:
                return None
        else:
            return None
    return current


class BasePaginationStrategy(ABC):
    def __init__(self, config: PaginationConfig):
        self.config = config

    @abstractmethod
    def get_initial_params(self) -> Dict[str, Any]:
        """Params for the first request."""
        pass

    @abstractmethod
    def get_next_params(
        self,
        response_data: Any,
        response_headers: Dict[str, str],
        records_count: int
    ) -> Tuple[Dict[str, Any], bool]:
        """Look at the last response and decide the next page's params (or stop)."""
        pass


class PageNumberPaginationStrategy(BasePaginationStrategy):
    def __init__(self, config: PaginationConfig):
        super().__init__(config)
        self.current_page = config.start_page

    def get_initial_params(self) -> Dict[str, Any]:
        return {self.config.page_param: self.current_page}

    def get_next_params(
        self,
        response_data: Any,
        response_headers: Dict[str, str],
        records_count: int
    ) -> Tuple[Dict[str, Any], bool]:
        if records_count == 0:
            return {}, False

        self.current_page += 1
        return {self.config.page_param: self.current_page}, True


class OffsetLimitPaginationStrategy(BasePaginationStrategy):
    def __init__(self, config: PaginationConfig):
        super().__init__(config)
        self.current_offset = 0

    def get_initial_params(self) -> Dict[str, Any]:
        return {
            self.config.offset_param: self.current_offset,
            self.config.limit_param: self.config.limit_value
        }

    def get_next_params(
        self,
        response_data: Any,
        response_headers: Dict[str, str],
        records_count: int
    ) -> Tuple[Dict[str, Any], bool]:
        if records_count == 0 or records_count < self.config.limit_value:
            return {}, False

        self.current_offset += self.config.limit_value
        return {
            self.config.offset_param: self.current_offset,
            self.config.limit_param: self.config.limit_value
        }, True


class CursorPaginationStrategy(BasePaginationStrategy):
    def get_initial_params(self) -> Dict[str, Any]:
        return {}

    def get_next_params(
        self,
        response_data: Any,
        response_headers: Dict[str, str],
        records_count: int
    ) -> Tuple[Dict[str, Any], bool]:
        if records_count == 0 or not self.config.cursor_path:
            return {}, False

        cursor_val = resolve_json_path(response_data, self.config.cursor_path)
        if not cursor_val:
            return {}, False

        # cursor value might be a raw token, or a full next-page URL (Rick & Morty does this)
        if isinstance(cursor_val, str) and (cursor_val.startswith("http://") or cursor_val.startswith("https://")):
            parsed_url = urlparse(cursor_val)
            query_params = parse_qs(parsed_url.query)
            flat_params = {k: v[0] if len(v) == 1 else v for k, v in query_params.items()}
            return flat_params, True

        return {self.config.cursor_param: cursor_val}, True


class LinkHeaderPaginationStrategy(BasePaginationStrategy):
    def get_initial_params(self) -> Dict[str, Any]:
        return {}

    def get_next_params(
        self,
        response_data: Any,
        response_headers: Dict[str, str],
        records_count: int
    ) -> Tuple[Dict[str, Any], bool]:
        if records_count == 0:
            return {}, False

        # Case insensitive header check for Link header RFC 5988
        link_header = None
        for k, v in response_headers.items():
            if k.lower() == "link":
                link_header = v
                break

        if not link_header:
            return {}, False

        # RFC 5988: Link: <url>; rel="next", <url>; rel="last"
        next_url = None
        for part in link_header.split(","):
            sections = part.split(";")
            if len(sections) >= 2 and 'rel="next"' in sections[1].strip():
                next_url = sections[0].strip().lstrip("<").rstrip(">")
                break

        if not next_url:
            return {}, False

        parsed_url = urlparse(next_url)
        query_params = parse_qs(parsed_url.query)
        flat_params = {k: v[0] if len(v) == 1 else v for k, v in query_params.items()}
        return flat_params, True


class NoPaginationStrategy(BasePaginationStrategy):
    def get_initial_params(self) -> Dict[str, Any]:
        return {}

    def get_next_params(
        self,
        response_data: Any,
        response_headers: Dict[str, str],
        records_count: int
    ) -> Tuple[Dict[str, Any], bool]:
        return {}, False


class PaginationFactory:
    @staticmethod
    def get_strategy(config: PaginationConfig) -> BasePaginationStrategy:
        if config.strategy == PaginationType.PAGE_NUMBER:
            return PageNumberPaginationStrategy(config)
        elif config.strategy == PaginationType.OFFSET_LIMIT:
            return OffsetLimitPaginationStrategy(config)
        elif config.strategy == PaginationType.CURSOR:
            return CursorPaginationStrategy(config)
        elif config.strategy == PaginationType.LINK_HEADER:
            return LinkHeaderPaginationStrategy(config)
        else:
            return NoPaginationStrategy(config)
