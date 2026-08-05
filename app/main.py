from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.api.routes import router as api_router
from app.storage.database import DatabaseStorage


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: initialize database tables
    storage = DatabaseStorage()
    storage.init_db()
    yield
    # Shutdown logic if needed


app = FastAPI(
    title="Generic Data Ingestion Service",
    description=(
        "A generic, extensible data ingestion service capable of pulling data from arbitrary "
        "REST APIs with custom pagination, authentication, resilience backoff retries, "
        "and structured database persistence."
    ),
    version="1.0.0",
    lifespan=lifespan
)

# Enable CORS for cross-origin access (useful for frontend integrations)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router)


@app.get("/", summary="Health check & Service Information")
def root():
    return JSONResponse(
        content={
            "service": "Generic Data Ingestion Service",
            "version": "1.0.0",
            "status": "healthy",
            "docs_url": "/docs",
            "redoc_url": "/redoc",
            "endpoints": {
                "trigger_ingest": "POST /api/v1/ingest",
                "list_jobs": "GET /api/v1/jobs",
                "get_job": "GET /api/v1/jobs/{job_id}",
                "get_data": "GET /api/v1/data",
                "list_demos": "GET /api/v1/demo/sources",
                "trigger_demo": "POST /api/v1/demo/ingest/{source_id}"
            }
        }
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
