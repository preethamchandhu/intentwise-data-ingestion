import json
import os
import uuid
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import Column, DateTime, ForeignKey, Integer, String, Text, create_engine
from sqlalchemy.orm import declarative_base, relationship, sessionmaker, Session

from app.schemas.ingestion import (
    IngestedRecordResponse,
    IngestionJobResponse,
    IngestionRequestConfig,
    JobStatus,
)

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./ingestion.db")

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class IngestionJobModel(Base):
    __tablename__ = "ingestion_jobs"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    source_name = Column(String, nullable=False, index=True)
    endpoint_url = Column(String, nullable=False)
    status = Column(String, nullable=False, default=JobStatus.PENDING.value, index=True)
    records_ingested = Column(Integer, default=0)
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    completed_at = Column(DateTime, nullable=True)

    records = relationship("IngestedRecordModel", back_populates="job", cascade="all, delete-orphan")


class IngestedRecordModel(Base):
    __tablename__ = "ingested_records"

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(String, ForeignKey("ingestion_jobs.id"), nullable=False, index=True)
    source_name = Column(String, nullable=False, index=True)
    record_index = Column(Integer, nullable=False)
    raw_payload = Column(Text, nullable=False)  # Stores JSON serialized payload
    ingested_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    job = relationship("IngestionJobModel", back_populates="records")


class AbstractStorage(ABC):
    @abstractmethod
    def create_job(self, config: IngestionRequestConfig) -> IngestionJobResponse:
        pass

    @abstractmethod
    def update_job(
        self,
        job_id: str,
        status: JobStatus,
        records_ingested: int,
        error_message: Optional[str] = None
    ) -> IngestionJobResponse:
        pass

    @abstractmethod
    def save_records(
        self,
        job_id: str,
        source_name: str,
        records: List[Any],
        start_index: int = 0
    ) -> int:
        pass

    @abstractmethod
    def get_job(self, job_id: str) -> Optional[IngestionJobResponse]:
        pass

    @abstractmethod
    def list_jobs(self, limit: int = 50, offset: int = 0) -> List[IngestionJobResponse]:
        pass

    @abstractmethod
    def get_records(
        self,
        source_name: Optional[str] = None,
        job_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[IngestedRecordResponse]:
        pass


class DatabaseStorage(AbstractStorage):
    def __init__(self, db_session: Optional[Session] = None):
        self._session_factory = SessionLocal
        self._custom_session = db_session
        self.init_db()

    def init_db(self):
        Base.metadata.create_all(bind=engine)

    def _get_session(self) -> Session:
        if self._custom_session:
            return self._custom_session
        return self._session_factory()

    def create_job(self, config: IngestionRequestConfig) -> IngestionJobResponse:
        session = self._get_session()
        try:
            job_model = IngestionJobModel(
                id=str(uuid.uuid4()),
                source_name=config.name,
                endpoint_url=config.endpoint_url,
                status=JobStatus.PENDING.value,
                records_ingested=0,
                created_at=datetime.now(timezone.utc)
            )
            session.add(job_model)
            session.commit()
            session.refresh(job_model)
            return self._to_job_response(job_model)
        finally:
            if not self._custom_session:
                session.close()

    def update_job(
        self,
        job_id: str,
        status: JobStatus,
        records_ingested: int,
        error_message: Optional[str] = None
    ) -> IngestionJobResponse:
        session = self._get_session()
        try:
            job_model = session.query(IngestionJobModel).filter_by(id=job_id).first()
            if not job_model:
                raise ValueError(f"Job with id {job_id} not found")

            job_model.status = status.value
            job_model.records_ingested = records_ingested
            if error_message:
                job_model.error_message = error_message
            if status in (JobStatus.COMPLETED, JobStatus.FAILED):
                job_model.completed_at = datetime.now(timezone.utc)

            session.commit()
            session.refresh(job_model)
            return self._to_job_response(job_model)
        finally:
            if not self._custom_session:
                session.close()

    def save_records(
        self,
        job_id: str,
        source_name: str,
        records: List[Any],
        start_index: int = 0
    ) -> int:
        if not records:
            return 0

        session = self._get_session()
        try:
            record_models = []
            for idx, rec in enumerate(records):
                payload_str = json.dumps(rec, default=str) if not isinstance(rec, str) else rec
                record_models.append(
                    IngestedRecordModel(
                        job_id=job_id,
                        source_name=source_name,
                        record_index=start_index + idx,
                        raw_payload=payload_str,
                        ingested_at=datetime.now(timezone.utc)
                    )
                )
            session.bulk_save_objects(record_models)
            session.commit()
            return len(records)
        finally:
            if not self._custom_session:
                session.close()

    def get_job(self, job_id: str) -> Optional[IngestionJobResponse]:
        session = self._get_session()
        try:
            job_model = session.query(IngestionJobModel).filter_by(id=job_id).first()
            return self._to_job_response(job_model) if job_model else None
        finally:
            if not self._custom_session:
                session.close()

    def list_jobs(self, limit: int = 50, offset: int = 0) -> List[IngestionJobResponse]:
        session = self._get_session()
        try:
            jobs = (
                session.query(IngestionJobModel)
                .order_by(IngestionJobModel.created_at.desc())
                .offset(offset)
                .limit(limit)
                .all()
            )
            return [self._to_job_response(j) for j in jobs]
        finally:
            if not self._custom_session:
                session.close()

    def get_records(
        self,
        source_name: Optional[str] = None,
        job_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[IngestedRecordResponse]:
        session = self._get_session()
        try:
            query = session.query(IngestedRecordModel)
            if source_name:
                query = query.filter(IngestedRecordModel.source_name == source_name)
            if job_id:
                query = query.filter(IngestedRecordModel.job_id == job_id)

            records = query.order_by(IngestedRecordModel.id.asc()).offset(offset).limit(limit).all()
            return [self._to_record_response(r) for r in records]
        finally:
            if not self._custom_session:
                session.close()

    @staticmethod
    def _to_job_response(model: IngestionJobModel) -> IngestionJobResponse:
        return IngestionJobResponse(
            job_id=model.id,
            source_name=model.source_name,
            endpoint_url=model.endpoint_url,
            status=JobStatus(model.status),
            records_ingested=model.records_ingested,
            error_message=model.error_message,
            created_at=model.created_at,
            completed_at=model.completed_at
        )

    @staticmethod
    def _to_record_response(model: IngestedRecordModel) -> IngestedRecordResponse:
        try:
            parsed_payload = json.loads(model.raw_payload)
        except Exception:
            parsed_payload = model.raw_payload

        return IngestedRecordResponse(
            id=model.id,
            job_id=model.job_id,
            source_name=model.source_name,
            record_index=model.record_index,
            raw_data=parsed_payload,
            ingested_at=model.ingested_at
        )
