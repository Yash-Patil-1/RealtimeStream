"""
RealtimeStream — Alerting & Notification System

Sends anomaly alerts through multiple channels:
  - Webhook (Slack, generic HTTP)
  - Email (SMTP)
  - Console / log output

Designed to be called by the Silver layer's AnomalyDetector when
batch-level anomalies are detected.
"""

from __future__ import annotations

import json
import logging
import os
import smtplib
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.mime.text import MIMEText
from typing import Any, Dict, List, Optional

logger = logging.getLogger("alerting")


# ─── Alert Data Model ─────────────────────────────────────────────────


@dataclass
class Alert:
    """Immutable alert object produced by the anomaly detection pipeline."""

    alert_id: str
    event_id: str
    anomaly_type: str
    anomaly_score: float
    event_type: str
    timestamp: str
    detected_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    response_time_ms: Optional[int] = None
    error_code: Optional[int] = None
    message: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "alert_id": self.alert_id,
            "event_id": self.event_id,
            "anomaly_type": self.anomaly_type,
            "anomaly_score": self.anomaly_score,
            "event_type": self.event_type,
            "timestamp": self.timestamp,
            "detected_at": self.detected_at,
            "response_time_ms": self.response_time_ms,
            "error_code": self.error_code,
            "message": self.message or self._default_message(),
            "metadata": self.metadata,
        }

    def _default_message(self) -> str:
        if self.anomaly_type == "error_rate_spike":
            return (
                f"Error rate spike detected: anomaly_score={self.anomaly_score:.2f}, "
                f"event_type={self.event_type}"
            )
        if self.anomaly_type == "slow_response":
            rt = self.response_time_ms or 0
            return (
                f"Slow response detected: {rt}ms, anomaly_score={self.anomaly_score:.2f}, "
                f"event_id={self.event_id}"
            )
        return (
            f"Anomaly detected: type={self.anomaly_type}, "
            f"score={self.anomaly_score:.2f}, event_id={self.event_id}"
        )


# ─── Channel Configuration ────────────────────────────────────────────


ALERTING_CONFIG_DEFAULTS = {
    "webhook_url": os.getenv("ALERT_WEBHOOK_URL", ""),
    "webhook_timeout": int(os.getenv("ALERT_WEBHOOK_TIMEOUT", "10")),
    "slack_channel": os.getenv("ALERT_SLACK_CHANNEL", "#anomaly-alerts"),
    "slack_username": os.getenv("ALERT_SLACK_USERNAME", "RealtimeStream Bot"),
    "smtp_host": os.getenv("ALERT_SMTP_HOST", ""),
    "smtp_port": int(os.getenv("ALERT_SMTP_PORT", "587")),
    "smtp_user": os.getenv("ALERT_SMTP_USER", ""),
    "smtp_password": os.getenv("ALERT_SMTP_PASSWORD", ""),
    "email_from": os.getenv("ALERT_EMAIL_FROM", "alerts@realtimestream.dev"),
    "email_to": os.getenv("ALERT_EMAIL_TO", ""),
    "use_tls": os.getenv("ALERT_SMTP_USE_TLS", "true").lower() == "true",
    "min_alert_score": float(os.getenv("ALERT_MIN_SCORE", "0.0")),
    "batch_alerts": os.getenv("ALERT_BATCH", "true").lower() == "true",
    "max_alerts_per_batch": int(os.getenv("ALERT_MAX_BATCH", "50")),
}


# ─── Channel Implementations ──────────────────────────────────────────


class AlertChannel:
    """Base class for alert notification channels."""

    def send(self, alert: Alert) -> bool:
        raise NotImplementedError

    def send_batch(self, alerts: List[Alert]) -> int:
        """Send multiple alerts. Returns count of successful sends."""
        success = 0
        for alert in alerts:
            if self.send(alert):
                success += 1
        return success

    @property
    def name(self) -> str:
        return self.__class__.__name__.replace("Channel", "").lower()


class WebhookChannel(AlertChannel):
    """Send alerts via HTTP POST to a configurable webhook URL.

    Supports:
      - Generic JSON webhooks
      - Slack webhooks (formatted as Slack Block Kit messages)
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = {**ALERTING_CONFIG_DEFAULTS, **(config or {})}

    def send(self, alert: Alert) -> bool:
        url = self.config.get("webhook_url", "")
        if not url:
            logger.debug("No webhook URL configured, skipping")
            return False

        payload = self._format_payload(alert)
        return self._post(url, payload)

    def send_batch(self, alerts: List[Alert]) -> int:
        url = self.config.get("webhook_url", "")
        if not url:
            return 0

        if self._is_slack_webhook(url):
            payload = self._format_slack_blocks(alerts)
        else:
            payload = [self._format_payload(a) for a in alerts]

        return len(alerts) if self._post(url, payload) else 0

    def _format_payload(self, alert: Alert) -> Dict[str, Any]:
        return alert.to_dict()

    def _format_slack_block(self, alert: Alert) -> Dict[str, Any]:
        """Format a single alert as a Slack Block Kit message."""
        color = (
            "#FF0000"
            if alert.anomaly_score >= 0.8
            else "#FFA500" if alert.anomaly_score >= 0.5
            else "#FFFF00"
        )
        return {
            "channel": self.config.get("slack_channel", "#anomaly-alerts"),
            "username": self.config.get("slack_username", "RealtimeStream Bot"),
            "attachments": [
                {
                    "color": color,
                    "title": f"🚨 {alert.anomaly_type.replace('_', ' ').title()}",
                    "fields": [
                        {"title": "Alert ID", "value": alert.alert_id, "short": True},
                        {"title": "Event ID", "value": alert.event_id, "short": True},
                        {"title": "Score", "value": str(alert.anomaly_score), "short": True},
                        {"title": "Event Type", "value": alert.event_type, "short": True},
                        {"title": "Message", "value": alert._default_message(), "short": False},
                    ],
                    "footer": "RealtimeStream Anomaly Detector",
                    "ts": datetime.now(timezone.utc).timestamp(),
                }
            ],
        }

    def _format_slack_blocks(self, alerts: List[Alert]) -> Dict[str, Any]:
        """Format multiple alerts as a Slack Block Kit message (batch)."""
        fields = []
        for alert in alerts[:10]:  # Slack limit ~10 fields
            fields.append({"title": alert.alert_id, "value": alert._default_message(), "short": False})

        return {
            "channel": self.config.get("slack_channel", "#anomaly-alerts"),
            "username": self.config.get("slack_username", "RealtimeStream Bot"),
            "attachments": [
                {
                    "color": "#FF0000",
                    "title": f"🚨 {len(alerts)} Anomaly Alert(s) Detected",
                    "fields": fields,
                    "footer": "RealtimeStream Anomaly Detector",
                    "ts": datetime.now(timezone.utc).timestamp(),
                }
            ],
        }

    @staticmethod
    def _is_slack_webhook(url: str) -> bool:
        return "hooks.slack.com" in url.lower()

    def _post(self, url: str, data: Any) -> bool:
        try:
            import urllib.request as request
            import urllib.error as error

            body = json.dumps(data, default=str).encode("utf-8")
            req = request.Request(
                url,
                data=body,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with request.urlopen(req, timeout=self.config.get("webhook_timeout", 10)):
                return True
        except ImportError:
            logger.warning("urllib not available for webhook alert")
            return False
        except error.HTTPError as e:
            logger.error(f"Webhook HTTP error {e.code}: {e.reason}")
            return False
        except Exception as e:
            logger.error(f"Webhook send failed: {e}")
            return False


class EmailChannel(AlertChannel):
    """Send alerts via SMTP email."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = {**ALERTING_CONFIG_DEFAULTS, **(config or {})}

    def send(self, alert: Alert) -> bool:
        to = self.config.get("email_to", "")
        if not to:
            logger.debug("No email recipient configured, skipping")
            return False

        subject = f"🚨 RealtimeStream Alert: {alert.anomaly_type.replace('_', ' ').title()}"
        body = self._format_email_body(alert)
        return self._send_email(subject, body, to)

    def send_batch(self, alerts: List[Alert]) -> int:
        to = self.config.get("email_to", "")
        if not to:
            return 0

        subject = f"🚨 RealtimeStream: {len(alerts)} Anomaly Alert(s)"
        body_lines = [
            f"RealtimeStream detected {len(alerts)} anomaly alert(s).\n",
            f"{'='*60}\n",
        ]
        for alert in alerts:
            body_lines.append(f"• {alert.alert_id}: {alert._default_message()}")
        body = "\n".join(body_lines)

        return len(alerts) if self._send_email(subject, body, to) else 0

    def _format_email_body(self, alert: Alert) -> str:
        lines = [
            "RealtimeStream — Anomaly Alert",
            "=" * 40,
            "",
            f"Alert ID:      {alert.alert_id}",
            f"Event ID:      {alert.event_id}",
            f"Event Type:    {alert.event_type}",
            f"Anomaly Type:  {alert.anomaly_type}",
            f"Anomaly Score: {alert.anomaly_score:.3f}",
            f"Timestamp:     {alert.timestamp}",
            f"Detected At:   {alert.detected_at}",
        ]
        if alert.response_time_ms is not None:
            lines.append(f"Response Time: {alert.response_time_ms}ms")
        if alert.error_code is not None:
            lines.append(f"Error Code:    {alert.error_code}")
        lines.extend(["", "Message:", alert._default_message(), "", "=" * 40])
        return "\n".join(lines)

    def _send_email(self, subject: str, body: str, to: str) -> bool:
        host = self.config.get("smtp_host", "")
        if not host:
            logger.debug("No SMTP host configured, email alert skipped")
            return False

        try:
            msg = MIMEText(body)
            msg["Subject"] = subject
            msg["From"] = self.config.get("email_from", "alerts@realtimestream.dev")
            msg["To"] = to

            with smtplib.SMTP(host, self.config.get("smtp_port", 587)) as server:
                if self.config.get("use_tls", True):
                    server.starttls()
                user = self.config.get("smtp_user", "")
                password = self.config.get("smtp_password", "")
                if user and password:
                    server.login(user, password)
                server.send_message(msg)

            logger.info(f"Email alert sent to {to}: {subject}")
            return True
        except smtplib.SMTPException as e:
            logger.error(f"SMTP error sending alert: {e}")
            return False
        except Exception as e:
            logger.error(f"Email send failed: {e}")
            return False


class ConsoleChannel(AlertChannel):
    """Log alerts to the console / application logs.

    This is the default channel and always active.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = {**ALERTING_CONFIG_DEFAULTS, **(config or {})}

    def send(self, alert: Alert) -> bool:
        logger.warning(
            f"ALERT [{alert.anomaly_type}] score={alert.anomaly_score:.2f} "
            f"event_id={alert.event_id} — {alert._default_message()}"
        )
        print(
            json.dumps(alert.to_dict(), default=str),
            file=sys.stderr,
        )
        return True

    def send_batch(self, alerts: List[Alert]) -> int:
        logger.warning(f"ALERT BATCH: {len(alerts)} anomaly alert(s)")
        for alert in alerts:
            self.send(alert)
        return len(alerts)


# ─── Channel Registry ──────────────────────────────────────────────────

CHANNEL_REGISTRY: Dict[str, type] = {
    "webhook": WebhookChannel,
    "email": EmailChannel,
    "console": ConsoleChannel,
}


# ─── AlertManager ─────────────────────────────────────────────────────


class AlertManager:
    """Central alert orchestration.

    Dispatches anomaly alerts to all configured channels (webhook, email,
    console). Filters by minimum score and handles batching.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = {**ALERTING_CONFIG_DEFAULTS, **(config or {})}
        self.channels: List[AlertChannel] = self._init_channels()

    def _init_channels(self) -> List[AlertChannel]:
        """Initialize enabled channels based on config."""
        channels: List[AlertChannel] = [ConsoleChannel(self.config)]

        if self.config.get("webhook_url"):
            channels.append(WebhookChannel(self.config))

        if self.config.get("smtp_host") and self.config.get("email_to"):
            channels.append(EmailChannel(self.config))

        logger.info(
            f"AlertManager initialized with {len(channels)} channel(s): "
            f"{[c.name for c in channels]}"
        )
        return channels

    def send_alert(self, alert: Alert) -> int:
        """Send a single alert through all configured channels.

        Returns:
            Number of channels that successfully delivered the alert.
        """
        if alert.anomaly_score < self.config.get("min_alert_score", 0.0):
            logger.debug(f"Alert score {alert.anomaly_score} below threshold, skipping")
            return 0

        success = 0
        for channel in self.channels:
            try:
                if channel.send(alert):
                    success += 1
            except Exception as e:
                logger.error(f"Channel {channel.name} failed: {e}")
        return success

    def send_alerts(self, alerts: List[Alert]) -> Dict[str, int]:
        """Send multiple alerts through all configured channels.

        Supports batching: if batch_alerts is True, alerts are sent
        as a group to each channel (channel-level batching).

        Args:
            alerts: List of Alert objects to dispatch.

        Returns:
            Dict mapping channel names to count of successfully sent alerts.
        """
        if not alerts:
            return {}

        # Filter by min score
        min_score = self.config.get("min_alert_score", 0.0)
        filtered = [a for a in alerts if a.anomaly_score >= min_score]

        if not filtered:
            return {}

        # Respect max batch size
        max_batch = self.config.get("max_alerts_per_batch", 50)
        batch = filtered[:max_batch]

        if len(filtered) > max_batch:
            logger.info(f"Alert batch truncated: {len(filtered)} -> {max_batch}")

        results: Dict[str, int] = {}
        use_batch = self.config.get("batch_alerts", True)

        for channel in self.channels:
            try:
                if use_batch:
                    sent = channel.send_batch(batch)
                else:
                    sent = sum(1 for a in batch if channel.send(a))
                results[channel.name] = sent
                logger.debug(f"Channel '{channel.name}' sent {sent}/{len(batch)} alerts")
            except Exception as e:
                logger.error(f"Channel '{channel.name}' batch failed: {e}")
                results[channel.name] = 0

        return results

    def channel_summary(self) -> List[Dict[str, Any]]:
        """Return a summary of configured channels and their status."""
        return [
            {
                "name": c.name,
                "type": c.__class__.__name__,
                "configured": True,
            }
            for c in self.channels
        ]


# ─── Convenience: one-shot alert ──────────────────────────────────────


def send_anomaly_alert(
    alert_data: Dict[str, Any],
    config: Optional[Dict[str, Any]] = None,
) -> int:
    """Quick one-shot helper to send a single alert.

    Args:
        alert_data: Dict with keys matching Alert constructor
            (event_id, anomaly_type, anomaly_score, event_type, timestamp).
        config: Optional overrides for AlertManager config.

    Returns:
        Number of channels that successfully delivered the alert.
    """
    manager = AlertManager(config)
    alert = Alert(
        alert_id=alert_data.get(
            "alert_id",
            f"alert-{alert_data['event_id']}-{int(datetime.now(timezone.utc).timestamp())}",
        ),
        event_id=alert_data["event_id"],
        anomaly_type=alert_data["anomaly_type"],
        anomaly_score=alert_data["anomaly_score"],
        event_type=alert_data.get("event_type", "unknown"),
        timestamp=alert_data.get("timestamp", datetime.now(timezone.utc).isoformat()),
        response_time_ms=alert_data.get("response_time_ms"),
        error_code=alert_data.get("error_code"),
        message=alert_data.get("message"),
        metadata=alert_data.get("metadata", {}),
    )
    return manager.send_alert(alert)
