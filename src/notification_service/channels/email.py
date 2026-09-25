"""The ``email`` channel: plain-text mail over SMTP, or only a log line.

``log`` mode is for stands without a mail relay (staging): the message is
rendered exactly as it would be sent and written to the log instead.
"""

from __future__ import annotations

import asyncio
import logging
import smtplib
import ssl
from email.message import EmailMessage
from email.utils import make_msgid

from notification_service.channels.base import (
    OutboundMessage,
    PermanentDeliveryError,
    RecipientUnreachable,
    SendResult,
    TransientDeliveryError,
)
from notification_service.config import Settings

logger = logging.getLogger("notification_service.email")


def _one_line(value: str) -> str:
    return " ".join(value.split())


def render(message: OutboundMessage, *, sender: str) -> EmailMessage:
    mail = EmailMessage()
    mail["From"] = sender
    mail["To"] = message.address
    mail["Subject"] = _one_line(message.title)
    mail["Message-ID"] = make_msgid(idstring=str(message.delivery_id))
    lines = [message.body] if message.body else []
    links = [f"{_one_line(str(link.get('label') or ''))}: {link['url']}" for link in message.links]
    if links:
        lines += ["", *links]
    mail.set_content("\n".join(lines) + "\n")
    return mail


class EmailChannel:
    name = "email"
    push = True
    needs_address = True

    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    async def send(self, message: OutboundMessage) -> SendResult:
        if not message.address:
            raise PermanentDeliveryError("no_address")
        mail = render(message, sender=self._settings.email_from)
        if self._settings.email_mode == "log":
            logger.info(
                "email not sent (log mode): delivery=%s to=%s subject=%r",
                message.delivery_id,
                message.address,
                mail["Subject"],
            )
            return SendResult(external_id=mail["Message-ID"])
        await asyncio.to_thread(self._send_smtp, mail)
        return SendResult(external_id=mail["Message-ID"])

    def _send_smtp(self, mail: EmailMessage) -> None:
        s = self._settings
        try:
            with smtplib.SMTP(s.smtp_host, s.smtp_port, timeout=s.smtp_timeout_seconds) as smtp:
                if s.smtp_starttls:
                    smtp.starttls(context=ssl.create_default_context())
                if s.smtp_username:
                    smtp.login(s.smtp_username, s.smtp_password.get_secret_value())
                smtp.send_message(mail)
        except smtplib.SMTPRecipientsRefused as exc:
            codes = [code for code, _ in exc.recipients.values()]
            if codes and all(code >= 500 for code in codes):
                raise RecipientUnreachable(f"smtp_recipient_refused: {codes[0]}") from exc
            raise TransientDeliveryError(f"smtp_recipient_deferred: {codes}") from exc
        except smtplib.SMTPResponseException as exc:
            # Only the code: the server's text may echo the message back.
            if exc.smtp_code >= 500:
                raise PermanentDeliveryError(f"smtp_rejected: {exc.smtp_code}") from exc
            raise TransientDeliveryError(f"smtp_deferred: {exc.smtp_code}") from exc
        except (smtplib.SMTPException, OSError) as exc:
            raise TransientDeliveryError(f"smtp_unavailable: {type(exc).__name__}") from exc
