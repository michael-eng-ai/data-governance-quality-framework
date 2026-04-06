"""SLA breach alerting.

Why a dedicated alerting module: decouples notification delivery from
quality check execution. Supports multiple channels (webhook, email)
and can be extended with new channels without modifying the quality engine.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import httpx

from src.config import get_settings
from src.models.quality import CheckStatus, FreshnessResult, ValidationResult

logger = logging.getLogger("data_governance")


class AlertDeliveryError(Exception):
    """Raised when an alert cannot be delivered to any configured channel."""


class AlertManager:
    """Manages alert delivery for quality check failures and SLA breaches.

    Why webhook-first: webhooks integrate with Slack, PagerDuty, OpsGenie,
    and custom systems without requiring provider-specific SDKs. Email is
    a fallback for environments without webhook infrastructure.
    """

    def __init__(
        self,
        webhook_url: str | None = None,
        email_config: dict[str, Any] | None = None,
    ) -> None:
        self._webhook_url = webhook_url
        self._email_config = email_config or {}

    @classmethod
    def from_settings(cls) -> AlertManager:
        """Create AlertManager from application settings.

        Why a factory: avoids coupling AlertManager construction to the
        settings module, enabling easier testing with explicit parameters.
        """
        settings = get_settings()
        email_config: dict[str, Any] = {}

        if settings.alert_email_smtp_host:
            email_config = {
                "smtp_host": settings.alert_email_smtp_host,
                "smtp_port": settings.alert_email_smtp_port,
                "from_address": settings.alert_email_from,
                "to_address": settings.alert_email_to,
            }

        return cls(
            webhook_url=settings.alert_webhook_url,
            email_config=email_config,
        )

    def send_quality_alert(self, result: ValidationResult) -> bool:
        """Send an alert for a failed quality check result.

        Args:
            result: The failed ValidationResult to alert on.

        Returns:
            True if alert was delivered to at least one channel.
        """
        if result.overall_status not in (CheckStatus.FAILED, CheckStatus.ERROR):
            return False

        payload = self._build_quality_payload(result)
        return self._deliver_alert(payload)

    def send_freshness_alert(self, freshness_result: FreshnessResult) -> bool:
        """Send an alert for a freshness SLA breach.

        Args:
            freshness_result: The failed FreshnessResult to alert on.

        Returns:
            True if alert was delivered to at least one channel.
        """
        if freshness_result.status != CheckStatus.FAILED:
            return False

        payload = self._build_freshness_payload(freshness_result)
        return self._deliver_alert(payload)

    def _build_quality_payload(self, result: ValidationResult) -> dict[str, Any]:
        """Build a webhook payload from a quality check result."""
        failed_check_names = [
            c.check_name
            for c in result.schema_checks + result.quality_checks
            if c.status == CheckStatus.FAILED
        ]

        return {
            "alert_type": "quality_check_failure",
            "severity": "critical" if result.failed_checks > 3 else "warning",
            "table": f"{result.schema_name}.{result.table_name}",
            "contract_version": result.contract_version,
            "status": result.overall_status.value,
            "total_checks": result.total_checks,
            "failed_checks": result.failed_checks,
            "failed_check_names": failed_check_names[:10],
            "duration_seconds": result.duration_seconds,
            "run_id": str(result.run_id),
            "timestamp": result.executed_at.isoformat(),
        }

    def _build_freshness_payload(self, result: FreshnessResult) -> dict[str, Any]:
        """Build a webhook payload from a freshness check result."""
        return {
            "alert_type": "freshness_sla_breach",
            "severity": "critical",
            "table": f"{result.schema_name}.{result.table_name}",
            "timestamp_column": result.timestamp_column,
            "actual_delay_minutes": result.actual_delay_minutes,
            "max_delay_minutes": result.max_delay_minutes,
            "message": result.message,
            "timestamp": result.checked_at.isoformat(),
        }

    def _deliver_alert(self, payload: dict[str, Any]) -> bool:
        """Attempt to deliver an alert via configured channels.

        Why try all channels: if the primary channel (webhook) fails,
        the alert still reaches the team via email. Silent alert
        failures are worse than duplicate alerts.
        """
        delivered = False

        if self._webhook_url:
            delivered = self._send_webhook(payload) or delivered

        if self._email_config.get("smtp_host"):
            delivered = self._send_email(payload) or delivered

        if not delivered:
            logger.warning(
                "alert_delivery_failed",
                extra={
                    "event": "alert_delivery_failed",
                    "payload": json.dumps(payload, default=str),
                    "reason": "No channels configured or all channels failed",
                },
            )

        return delivered

    def _send_webhook(self, payload: dict[str, Any]) -> bool:
        """Send alert via webhook (Slack, PagerDuty, etc)."""
        if not self._webhook_url:
            return False

        try:
            response = httpx.post(
                self._webhook_url,
                json=payload,
                timeout=10.0,
                headers={"Content-Type": "application/json"},
            )
            response.raise_for_status()

            logger.info(
                "webhook_alert_sent",
                extra={
                    "event": "webhook_alert_sent",
                    "alert_type": payload.get("alert_type"),
                    "table": payload.get("table"),
                    "status_code": response.status_code,
                },
            )
            return True

        except httpx.HTTPError as exc:
            logger.error(
                "webhook_alert_failed",
                extra={
                    "event": "webhook_alert_failed",
                    "alert_type": payload.get("alert_type"),
                    "error": str(exc),
                },
            )
            return False

    def _send_email(self, payload: dict[str, Any]) -> bool:
        """Send alert via email (SMTP).

        Note: In production this would use smtplib or an async email library.
        The implementation logs the attempt for demonstration purposes.
        """
        smtp_host = self._email_config.get("smtp_host")
        from_address = self._email_config.get("from_address")
        to_address = self._email_config.get("to_address")

        if not all([smtp_host, from_address, to_address]):
            return False

        try:
            import smtplib
            from email.mime.text import MIMEText

            subject = (
                f"[Data Governance Alert] {payload.get('alert_type', 'unknown')} "
                f"- {payload.get('table', 'unknown')}"
            )
            body = json.dumps(payload, indent=2, default=str)

            msg = MIMEText(body)
            msg["Subject"] = subject
            msg["From"] = from_address
            msg["To"] = to_address

            smtp_port = self._email_config.get("smtp_port", 587)
            with smtplib.SMTP(smtp_host, smtp_port, timeout=10) as server:
                server.starttls()
                server.send_message(msg)

            logger.info(
                "email_alert_sent",
                extra={
                    "event": "email_alert_sent",
                    "alert_type": payload.get("alert_type"),
                    "table": payload.get("table"),
                    "to": to_address,
                },
            )
            return True

        except Exception as exc:
            logger.error(
                "email_alert_failed",
                extra={
                    "event": "email_alert_failed",
                    "alert_type": payload.get("alert_type"),
                    "error": str(exc),
                },
            )
            return False
