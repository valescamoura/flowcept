"""Parser utilities for Academy Redis MONITOR events."""

from __future__ import annotations

import pickle
import re
from dataclasses import dataclass
from typing import Any

from flowcept.commons.utils import replace_non_serializable


@dataclass
class AcademyRedisMonitorEvent:
    """Normalized event extracted from Redis MONITOR output."""

    command: str
    redis_time: float | None = None
    db: int | None = None
    client: str | None = None
    operation: str | None = None
    queue_key: str | None = None
    destination_uid: str | None = None
    raw_payload_text: str | None = None
    payload_bytes: bytes | None = None
    academy_message: Any | None = None
    parse_error: str | None = None

    def to_metadata(self) -> dict[str, Any]:
        """Return metadata safe for JSON-like persistence."""
        return {
            "source": "academy_redis_monitor",
            "redis_time": self.redis_time,
            "db": self.db,
            "client": self.client,
            "operation": self.operation,
            "queue_key": self.queue_key,
            "destination_uid": self.destination_uid,
            "raw_payload_text": self.raw_payload_text,
            "parse_error": self.parse_error,
        }


@dataclass
class AcademyRedisOperationalEvent:
    """Operational Academy Redis metadata extracted from MONITOR output."""

    kind: str
    command: str
    redis_time: float | None = None
    db: int | None = None
    client: str | None = None
    operation: str | None = None
    key: str | None = None
    uid: str | None = None
    value: str | None = None

    def to_metadata(self) -> dict[str, Any]:
        """Return metadata safe for JSON-like persistence."""
        return {
            "source": "academy_redis_monitor",
            "kind": self.kind,
            "redis_time": self.redis_time,
            "db": self.db,
            "client": self.client,
            "operation": self.operation,
            "key": self.key,
            "uid": self.uid,
            "value": self.value,
        }


class AcademyRedisMonitorParser:
    """Parse Redis MONITOR entries produced by redis-py."""

    QUEUE_PREFIX = "queue:"
    AGENT_PREFIX = "agent:"
    HEARTBEAT_PREFIX = "heartbeat:"

    def parse(self, monitor_message: dict[str, Any] | str) -> AcademyRedisMonitorEvent | None:
        """Parse one Redis MONITOR message.

        Parameters
        ----------
        monitor_message
            Either the dictionary yielded by redis-py's monitor listener or a
            raw command string.

        Returns
        -------
        AcademyRedisMonitorEvent | None
            A normalized Academy queue event, or ``None`` if the command is not
            an Academy queue `RPUSH`.
        """
        command, redis_time, db, client = self._monitor_fields(monitor_message)

        parts = self._split_command(command)
        if len(parts) < 3:
            return None

        operation = parts[0].upper()
        queue_key = parts[1]
        if operation != "RPUSH" or not queue_key.startswith(self.QUEUE_PREFIX):
            return None

        raw_payload_text = parts[2]
        event = AcademyRedisMonitorEvent(
            command=command,
            redis_time=float(redis_time) if redis_time is not None else None,
            db=int(db) if db is not None else None,
            client=str(client) if client is not None else None,
            operation=operation,
            queue_key=queue_key,
            destination_uid=queue_key.removeprefix(self.QUEUE_PREFIX),
            raw_payload_text=raw_payload_text,
        )

        try:
            event.payload_bytes = self._payload_text_to_bytes(raw_payload_text)
            event.academy_message = pickle.loads(event.payload_bytes)
        except Exception as exc:
            event.parse_error = f"{type(exc).__name__}: {exc}"

        return event

    def parse_operational_event(
        self,
        monitor_message: dict[str, Any] | str,
    ) -> AcademyRedisOperationalEvent | None:
        """Parse Academy operational Redis metadata relevant for enrichment.

        The adapter currently keeps these events as contextual metadata rather
        than emitting them as FlowCept tasks.
        """
        command, redis_time, db, client = self._monitor_fields(monitor_message)
        parts = self._split_command(command)
        if len(parts) < 3:
            return None

        operation = parts[0].upper()
        key = parts[1]
        value = parts[2]
        if operation != "SET":
            return None

        if key.startswith(self.AGENT_PREFIX):
            kind = "academy_agent_registration"
            uid = key.removeprefix(self.AGENT_PREFIX)
        elif key.startswith(self.HEARTBEAT_PREFIX):
            kind = "academy_heartbeat"
            uid = key.removeprefix(self.HEARTBEAT_PREFIX)
        else:
            return None

        return AcademyRedisOperationalEvent(
            kind=kind,
            command=command,
            redis_time=float(redis_time) if redis_time is not None else None,
            db=int(db) if db is not None else None,
            client=str(client) if client is not None else None,
            operation=operation,
            key=key,
            uid=uid,
            value=value,
        )

    def _monitor_fields(self, monitor_message: dict[str, Any] | str) -> tuple[str, Any, Any, Any]:
        if isinstance(monitor_message, dict):
            command = str(monitor_message.get("command", ""))
            redis_time = monitor_message.get("time")
            db = monitor_message.get("db")
            client = monitor_message.get("client_address") or monitor_message.get("client")
        else:
            command = str(monitor_message)
            redis_time = None
            db = None
            client = None
        return command, redis_time, db, client

    def _split_command(self, command: str) -> list[str]:
        parts: list[str] = []
        index = 0
        command_len = len(command)

        for _ in range(2):
            while index < command_len and command[index].isspace():
                index += 1
            if index >= command_len:
                return parts

            if command[index] == '"':
                token, index = self._read_quoted_token(command, index)
            else:
                start = index
                while index < command_len and not command[index].isspace():
                    index += 1
                token = command[start:index]
            parts.append(token)

        while index < command_len and command[index].isspace():
            index += 1
        if index < command_len:
            payload = command[index:]
            if payload.startswith('"') and payload.endswith('"'):
                payload = payload[1:-1]
            parts.append(payload)

        return parts

    def _read_quoted_token(self, command: str, start_index: int) -> tuple[str, int]:
        token: list[str] = []
        index = start_index + 1
        while index < len(command):
            char = command[index]
            if char == "\\" and index + 1 < len(command):
                token.append(command[index + 1])
                index += 2
                continue
            if char == '"':
                return "".join(token), index + 1
            token.append(char)
            index += 1
        return "".join(token), index

    def _payload_text_to_bytes(self, raw_payload_text: str) -> bytes:
        # Redis MONITOR renders binary payloads as escaped text. This conversion
        # handles common \xNN escape sequences while preserving byte values.
        escaped_payload = raw_payload_text
        if raw_payload_text.startswith("x80"):
            # Some MONITOR clients strip the leading backslashes from pickle
            # bytes, yielding x80x05... instead of \x80\x05....
            escaped_payload = re.sub(r"(?<!\\)x([0-9a-fA-F]{2})", r"\\x\1", raw_payload_text)
        unescaped = escaped_payload.encode("utf-8").decode("unicode_escape")
        return unescaped.encode("latin-1")


def academy_message_to_dict(message: Any) -> dict[str, Any]:
    """Convert an Academy Message object into a JSON-like dictionary."""
    body = message.get_body()
    body_kind = getattr(body, "kind", type(body).__name__)
    payload: dict[str, Any] = {
        "header": {
            "src": str(message.src),
            "dest": str(message.dest),
            "tag": str(message.tag),
            "label": str(message.label) if message.label is not None else None,
            "kind": message.header.kind,
        },
        "body_kind": body_kind,
    }

    if body_kind == "action-request":
        payload["action"] = body.action
        payload["pargs"] = replace_non_serializable(body.get_args())
        payload["kargs"] = replace_non_serializable(body.get_kwargs())
    elif body_kind == "action-response":
        payload["result"] = replace_non_serializable(body.get_result())
    elif body_kind == "error-response":
        exception = body.get_exception()
        payload["exception_type"] = type(exception).__name__
        payload["exception"] = str(exception)
    else:
        payload["body"] = replace_non_serializable(body.model_dump(mode="json"))

    return payload
