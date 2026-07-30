"""
Query API entrypoint.

Real endpoints as of Task 8 — GET /options/bars, GET /underlying/bars,
GET /contracts — live in service/api/routes.py. This module just builds
the FastAPI app, health check, and wires the router in.
"""

import logging

from fastapi import FastAPI

from service.api.routes import router
from service.config.settings import get_settings
from service.db.session import check_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("api")

app = FastAPI(
    title="Options Data Service",
    description="Query API for TastyTrade-sourced options market data. "
    "See PLAN.md for the full project plan.",
    version="0.1.0",
)
app.include_router(router)


@app.get("/health")
async def health() -> dict:
    db_ok = await check_connection()
    settings = get_settings()
    return {
        "status": "ok" if db_ok else "degraded",
        "database": "ok" if db_ok else "unreachable",
        "tracked_tickers": [t.ticker for t in settings.tickers],
    }


@app.get("/")
async def root() -> dict:
    return {
        "service": "options-data-service",
        "endpoints": ["/options/bars", "/underlying/bars", "/contracts"],
        "health_check": "/health",
        "docs": "/docs",
    }
