"""FastAPI application entry point.

    uvicorn app.main:app --reload
    open http://127.0.0.1:8000/docs
"""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import RedirectResponse

from app.api.routes import init_runtime, router
from app.logging_setup import configure_logging


@asynccontextmanager
async def lifespan(_: FastAPI):
    configure_logging("INFO")
    init_runtime()
    yield


app = FastAPI(
    title="SmartDialer",
    description="Safety-first progressive/predictive dialer prototype. Time is virtual: "
                "use POST /dialer/cycle or POST /time/advance to make things happen.",
    version="1.0.0",
    lifespan=lifespan,
)
app.include_router(router)


@app.get("/", include_in_schema=False)
def root() -> RedirectResponse:
    """The bare address opens the interactive API docs instead of a 404."""
    return RedirectResponse(url="/docs")
