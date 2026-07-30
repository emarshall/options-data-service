"""
Lightweight, opt-in API key auth for the query API.

Per PLAN.md Task 8's own framing: this service is meant for local/private-
network use (a home-server deployment), not a public API — so auth is
opt-in, not mandatory. If `API_KEY` is unset (the default), every request
is allowed through. If it's set, every request must send it back in an
`X-API-Key` header.
"""

from fastapi import Header, HTTPException, status

from service.config.settings import get_settings


async def require_api_key(x_api_key: str | None = Header(default=None)) -> None:
    settings = get_settings()
    if not settings.api_key:
        return  # no key configured — API is open, by design (see module docstring)
    if x_api_key != settings.api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing or invalid X-API-Key header.",
        )
