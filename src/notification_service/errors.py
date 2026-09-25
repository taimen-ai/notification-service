"""The error envelope: ``{"error": {"code", "message", "details"}}``, as in the Control Plane."""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from platform_auth import EnforcementError


class ApiError(Exception):
    status = 400
    code = "bad_request"

    def __init__(
        self, message: str = "", *, code: str | None = None, details: dict[str, Any] | None = None
    ) -> None:
        super().__init__(message or self.code)
        self.message = message or self.code
        if code:
            self.code = code
        self.details = details or {}


class NotFound(ApiError):
    status = 404
    code = "not_found"


class Conflict(ApiError):
    status = 409
    code = "conflict"


class Unprocessable(ApiError):
    status = 422
    code = "validation_error"


class Unavailable(ApiError):
    status = 503
    code = "dependency_unavailable"


def envelope(code: str, message: str, details: dict[str, Any] | None = None) -> dict[str, Any]:
    return {"error": {"code": code, "message": message, "details": details or {}}}


def install(app: FastAPI) -> None:
    @app.exception_handler(ApiError)
    async def _api_error(_: Request, exc: ApiError) -> JSONResponse:
        return JSONResponse(envelope(exc.code, exc.message, exc.details), status_code=exc.status)

    @app.exception_handler(EnforcementError)
    async def _denied(_: Request, exc: EnforcementError) -> JSONResponse:
        # The SDK's deny contract: a stable code only, the reason stays in audit.
        headers = {"WWW-Authenticate": "Bearer"} if exc.http_status == 401 else None
        return JSONResponse(
            envelope(exc.code, exc.code), status_code=exc.http_status, headers=headers
        )

    @app.exception_handler(RequestValidationError)
    async def _invalid(_: Request, exc: RequestValidationError) -> JSONResponse:
        errors = [
            {"loc": list(err.get("loc", ())), "msg": str(err.get("msg", ""))}
            for err in exc.errors()
        ]
        return JSONResponse(
            envelope("validation_error", "Request is invalid", {"errors": errors}),
            status_code=422,
        )
