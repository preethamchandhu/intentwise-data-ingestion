import asyncio
import logging
from typing import Any, List, Optional

from app.ingestion.client import IngestionClient
from app.ingestion.pagination import PaginationFactory, resolve_json_path
from app.schemas.ingestion import (
    IngestionJobResponse,
    IngestionRequestConfig,
    JobStatus,
)
from app.storage.database import AbstractStorage, DatabaseStorage

logger = logging.getLogger(__name__)


def extract_records(response_json: Any, data_key: Optional[str] = None) -> List[Any]:
    """Pull the list of records out of a response, either via data_key or by guessing."""
    if response_json is None:
        return []

    if data_key:
        extracted = resolve_json_path(response_json, data_key)
        if isinstance(extracted, list):
            return extracted
        elif extracted is not None:
            return [extracted]
        return []

    # no data_key given, try to guess where the list is
    if isinstance(response_json, list):
        return response_json
    elif isinstance(response_json, dict):
        for common_key in ["results", "data", "items", "products", "posts"]:
            if common_key in response_json and isinstance(response_json[common_key], list):
                return response_json[common_key]
        # couldn't find a list, treat the whole object as one record
        return [response_json]

    return []


class IngestionEngine:
    def __init__(self, storage: Optional[AbstractStorage] = None):
        self.storage = storage or DatabaseStorage()

    async def run_ingestion(self, config: IngestionRequestConfig) -> IngestionJobResponse:
        job = self.storage.create_job(config)
        job_id = job.job_id

        self.storage.update_job(job_id, JobStatus.RUNNING, 0)
        logger.info(f"Started ingestion job {job_id} for source '{config.name}' at {config.endpoint_url}")

        client = IngestionClient()
        strategy = PaginationFactory.get_strategy(config.pagination)

        total_records = 0
        pages_processed = 0
        current_pagination_params = strategy.get_initial_params()
        has_more = True

        try:
            while has_more and pages_processed < config.max_pages:
                merged_params = dict(config.params)
                merged_params.update(current_pagination_params)

                logger.debug(f"Job {job_id}: fetching page {pages_processed + 1}, params={merged_params}")

                json_body, resp_headers = await client.fetch_page(
                    url=config.endpoint_url,
                    method=config.method,
                    headers=config.headers,
                    params=merged_params,
                    auth=config.auth
                )

                records = extract_records(json_body, config.data_key)
                records_count = len(records)

                # trim the batch if it would push us past max_records
                records_to_save = records
                if config.max_records and (total_records + records_count) > config.max_records:
                    needed = config.max_records - total_records
                    records_to_save = records[:needed]
                    records_count = len(records_to_save)

                if records_to_save:
                    self.storage.save_records(
                        job_id=job_id,
                        source_name=config.name,
                        records=records_to_save,
                        start_index=total_records
                    )
                    total_records += records_count
                    self.storage.update_job(job_id, JobStatus.RUNNING, total_records)

                pages_processed += 1

                if config.max_records and total_records >= config.max_records:
                    logger.info(f"Job {job_id}: hit max_records ({config.max_records}), stopping")
                    break

                current_pagination_params, has_more = strategy.get_next_params(
                    response_data=json_body,
                    response_headers=resp_headers,
                    records_count=records_count
                )

                if has_more and config.rate_limit_delay > 0:
                    await asyncio.sleep(config.rate_limit_delay)

            completed_job = self.storage.update_job(
                job_id=job_id,
                status=JobStatus.COMPLETED,
                records_ingested=total_records
            )
            logger.info(f"Job {job_id} completed - {total_records} records over {pages_processed} page(s)")
            return completed_job

        except Exception as exc:
            logger.error(f"Job {job_id} failed: {exc}", exc_info=True)
            failed_job = self.storage.update_job(
                job_id=job_id,
                status=JobStatus.FAILED,
                records_ingested=total_records,
                error_message=str(exc)
            )
            return failed_job
