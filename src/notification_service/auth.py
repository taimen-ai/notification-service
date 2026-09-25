"""Authentication: IAM tokens verified by platform-auth-sdk (ADR-0030).

The service is a resource server for exactly one audience. Scopes of that
audience gate the three kinds of callers: senders (``notifications:send``),
recipients reading their own inbox and settings (``notifications:read``), and
organization administrators (``notifications:admin``).
"""

from __future__ import annotations

from typing import Annotated

from fastapi import Depends, Request
from platform_auth import TokenVerifier, TrustedAuthContext, VerificationUnavailable, parse_bearer

SCOPE_SEND = "notifications:send"
SCOPE_READ = "notifications:read"
SCOPE_ADMIN = "notifications:admin"


async def authenticate(request: Request) -> TrustedAuthContext:
    verifier: TokenVerifier | None = request.app.state.verifier
    if verifier is None:
        raise VerificationUnavailable("verifier_not_configured")
    token = parse_bearer(request.headers.get("authorization"))
    return await verifier.verify(token, correlation_id=request.headers.get("x-request-id", ""))


def require(*any_of: str):  # type: ignore[no-untyped-def]
    async def dependency(
        ctx: Annotated[TrustedAuthContext, Depends(authenticate)],
    ) -> TrustedAuthContext:
        ctx.require_scope(*any_of)
        return ctx

    return dependency


Sender = Annotated[TrustedAuthContext, Depends(require(SCOPE_SEND))]
SenderOrAdmin = Annotated[TrustedAuthContext, Depends(require(SCOPE_SEND, SCOPE_ADMIN))]
Reader = Annotated[TrustedAuthContext, Depends(require(SCOPE_READ))]
Admin = Annotated[TrustedAuthContext, Depends(require(SCOPE_ADMIN))]
