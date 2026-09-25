"""Contract: a decision from Telegram against the code of IAM and the Control Plane.

- The subject of the one-decision token is ``approval:<uuid>`` exactly as the
  core parses it (``control_plane.infrastructure.auth.iam.decision_purpose``,
  CP-ADR-0070), and the request the adapter makes is the one that token is
  good for; ``Idempotency-Key`` is the header the core's write flow reads.
- The IAM requests are IAM's own request models (``iam_service.channels.schemas``
  of the neighbouring checkout), its answers parse into what the adapter keeps,
  and the audience and scope the service asks for are IAM's defaults.
"""

from __future__ import annotations

import importlib.util
import json
import re
import uuid
from pathlib import Path
from typing import Any

import httpx
import pytest
from control_plane.api.v1.schemas import ApprovalOut
from control_plane.api.write_flow import IDEMPOTENCY_HEADER
from control_plane.domain.errors import AuthorizationError
from control_plane.infrastructure.auth.iam import SCOPE_DECIDE, decision_purpose
from platform_auth import StaticKeySet, TokenVerifier, VerifierConfig
from platform_auth.testing import SigningKey

from notification_service.config import Settings
from notification_service.decisions import ControlPlaneApprovals, IamChannelLinks, purpose_ref
from notification_service.telegram_bot import TelegramWebhook

from conftest import ISSUER
from telegram_fakes import IAM_SCHEMAS, NOW, StaticTokens, iam_schemas

IAM_ROOT = IAM_SCHEMAS.parents[1]


def iam_module(path: Path, name: str) -> Any:
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


needs_iam = pytest.mark.skipif(
    iam_schemas() is None, reason="iam-service checkout is not next to this repository"
)


# --- Control Plane -------------------------------------------------------------


@pytest.mark.parametrize("verb", ["approve", "reject"])
async def test_decision_request_is_what_the_core_accepts(verb: str) -> None:
    approval_id = uuid.uuid4()
    seen: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        body = ApprovalOut(
            id=approval_id,
            tenant_id=uuid.uuid4(),
            workspace_id=None,
            task_id=None,
            artifact_id=None,
            requested_by_principal_id=uuid.uuid4(),
            status="approved" if verb == "approve" else "rejected",
            gate=True,
            required_role_id=None,
            assigned_principal_id=None,
            decision_by_principal_id=uuid.uuid4(),
            decision_at=NOW,
            comment="",
            version=2,
            outcome_status=None,
            created_at=NOW,
            updated_at=NOW,
        )
        return httpx.Response(200, json=body.model_dump(mode="json", by_alias=True))

    approvals = ControlPlaneApprovals(
        "https://cp.test", None, transport=httpx.MockTransport(handle)
    )
    decided = await approvals.decide(
        approval_id, approve=verb == "approve", token="t", idempotency_key="4711"
    )

    (request,) = seen
    # The core's own check of a channel token against this very request.
    assert (
        decision_purpose(
            {"purpose_ref": purpose_ref(approval_id)}, f"{request.method} {request.url.path}"
        )
        == f"approval:{approval_id}"
    )
    assert request.headers[IDEMPOTENCY_HEADER] == "4711"
    assert request.headers["authorization"] == "Bearer t"
    # What the service records of the decision is read from ``ApprovalOut``.
    outcome = TelegramWebhook._outcome_of(decided, channel="telegram")
    assert outcome["status"] == decided["status"]
    assert outcome["by"] == decided["decisionByPrincipalId"]
    assert outcome["at"] == decided["decisionAt"]


def test_purpose_names_one_approval_only() -> None:
    mine, other = uuid.uuid4(), uuid.uuid4()
    with pytest.raises(AuthorizationError):
        decision_purpose(
            {"purpose_ref": purpose_ref(mine)}, f"POST /api/v1/approvals/{other}:approve"
        )
    with pytest.raises(AuthorizationError):
        decision_purpose({"purpose_ref": purpose_ref(mine)}, f"GET /api/v1/approvals/{mine}")


# --- IAM -------------------------------------------------------------------------


@needs_iam
def test_service_asks_iam_for_its_default_audience_and_scope() -> None:
    iam_config = iam_module(IAM_ROOT / "config.py", "_iam_config")
    defaults = iam_config.Settings()
    ours = Settings()
    assert ours.iam_channel_audience == defaults.channel_audience
    assert ours.iam_channel_scope == defaults.channel_scope
    # The token IAM issues for a press is the core's decision scope.
    assert defaults.channel_assertion_scope == SCOPE_DECIDE
    assert defaults.channel_assertion_audience == "control-plane"


@needs_iam
def test_adapter_paths_are_iam_routes() -> None:
    routes = (IAM_ROOT / "channels" / "routes.py").read_text()
    for path in ("channel-links:confirm", "channel-assertions:exchange"):
        assert f'"/api/v1/tenants/{{tenant_id}}/{path}"' in routes


@needs_iam
async def test_adapter_requests_and_answers_are_iam_models() -> None:
    schemas = iam_schemas()
    assert schemas is not None
    tenant_id, principal_id = uuid.uuid4(), uuid.uuid4()
    approval_id = uuid.uuid4()
    seen: list[tuple[str, dict[str, Any]]] = []

    def handle(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        seen.append((request.url.path, body))
        if request.url.path.endswith(":confirm"):
            parsed = schemas.ChannelLinkConfirm.model_validate(body)
            answer = schemas.ChannelLinkView(
                linkId=uuid.uuid4(),
                principalId=principal_id,
                channel=parsed.channel,
                status="active",
                linkedAt=NOW,
            )
        else:
            parsed = schemas.ChannelAssertionExchange.model_validate(body)
            answer = schemas.ChannelAssertionToken(
                accessToken="decide-token",
                expiresIn=60,
                audience="control-plane",
                scope=[SCOPE_DECIDE],
                sessionId=uuid.uuid4(),
                principalId=principal_id,
                purposeRef=parsed.purpose_ref,
            )
        return httpx.Response(200, json=answer.model_dump(mode="json", by_alias=True))

    key = SigningKey.generate("contract")
    service_token = key.issue(
        ttl_seconds=60,
        issuer=ISSUER,
        audience="iam",
        tenant_id=tenant_id,
        subject=uuid.uuid4(),
        scopes=["iam:channel-links"],
        principal_type="service_account",
    )
    links = IamChannelLinks(
        "https://iam.test",
        StaticTokens(service_token),  # type: ignore[arg-type]
        TokenVerifier(
            StaticKeySet(key.public_pem, key_id=key.key_id),
            VerifierConfig(issuer=ISSUER, audience="iam"),
        ),
        transport=httpx.MockTransport(handle),
    )
    linked = await links.confirm("telegram", "code-from-web", "1001")
    token = await links.exchange(tenant_id, "telegram", "1001", purpose_ref(approval_id))
    await links.aclose()

    assert linked.tenant_id == tenant_id and linked.iam_principal_id == principal_id
    assert token == "decide-token"
    assert [path for path, _ in seen] == [
        f"/api/v1/tenants/{tenant_id}/channel-links:confirm",
        f"/api/v1/tenants/{tenant_id}/channel-assertions:exchange",
    ]
    # IAM's own pattern for a Telegram account and for the purpose reference.
    routes = (IAM_ROOT / "channels" / "routes.py").read_text()
    subject = re.search(r'"telegram": re\.compile\(r"(.+?)"\)', routes)
    assert subject is not None and re.match(subject.group(1), seen[0][1]["externalSubject"])
    assert seen[1][1]["purposeRef"] == f"approval:{approval_id}"
