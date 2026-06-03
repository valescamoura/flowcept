"""Academy Redis MONITOR interceptor."""

from __future__ import annotations

import json
from pathlib import Path
from threading import Event, Thread
from time import sleep
from typing import Any
from uuid import uuid4

from flowcept.commons.flowcept_dataclasses.task_object import TaskObject
from flowcept.commons.utils import get_utc_now
from flowcept.commons.vocabulary import Status
from flowcept.flowcept_api.flowcept_controller import Flowcept
from flowcept.flowceptor.adapters.academy.academy_message_parser import (
    AcademyRedisMonitorEvent,
    AcademyRedisOperationalEvent,
    AcademyRedisMonitorParser,
    academy_message_to_dict,
)
from flowcept.flowceptor.adapters.base_interceptor import BaseInterceptor


class AcademyRedisMonitorInterceptor(BaseInterceptor):
    """Observe Academy Redis list traffic using Redis MONITOR."""

    def __init__(
        self,
        plugin_key: str | None = None,
        *,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: str | None = None,
        raw_event_log_path: str | None = None,
        max_messages: int | None = None,
        poll_sleep_seconds: float = 0.05,
        shutdown_drain_seconds: float = 1.0,
    ):
        super().__init__(plugin_key=plugin_key, kind="academy_redis_monitor")
        self._host = host
        self._port = port
        self._db = db
        self._password = password
        self._raw_event_log_path = Path(raw_event_log_path) if raw_event_log_path else None
        self._max_messages = max_messages
        self._poll_sleep_seconds = poll_sleep_seconds
        self._shutdown_drain_seconds = shutdown_drain_seconds
        self._parser = AcademyRedisMonitorParser()
        self._observer_thread: Thread | None = None
        self._stop_event = Event()
        self._observed_messages = 0
        self._agent_registry: dict[str, dict[str, Any]] = {}
        self._heartbeats: dict[str, float] = {}
        self._pending_requests: dict[str, dict[str, Any]] = {}

    def start(self, bundle_exec_id, check_safe_stops: bool = True) -> "AcademyRedisMonitorInterceptor":
        """Start FlowCept buffering and the Redis MONITOR observer thread."""
        super().start(bundle_exec_id, check_safe_stops=check_safe_stops)
        self._stop_event.clear()
        self._observer_thread = Thread(target=self.observe, daemon=True)
        self._observer_thread.start()
        return self

    def stop(self, check_safe_stops: bool = True) -> bool:
        """Stop the observer and flush FlowCept messages."""
        if self._shutdown_drain_seconds > 0:
            # Academy may publish shutdown messages while its manager context is
            # closing. Keep MONITOR alive briefly so those final Redis writes are
            # converted before FlowCept flushes and stops.
            sleep(self._shutdown_drain_seconds)
        self._stop_event.set()
        if self._observer_thread is not None:
            self._observer_thread.join(timeout=3)
        self._flush_pending_requests()
        super().stop(check_safe_stops=check_safe_stops)
        return True

    def observe(self):
        """Listen to Redis MONITOR output and process Academy queue writes."""
        import redis

        client = redis.Redis(
            host=self._host,
            port=self._port,
            db=self._db,
            password=self._password,
            decode_responses=True,
        )
        try:
            with client.monitor() as monitor:
                for message in monitor.listen():
                    if self._stop_event.is_set():
                        break
                    self.callback(message)
                    if self._max_messages is not None and self._observed_messages >= self._max_messages:
                        break
                    sleep(self._poll_sleep_seconds)
        finally:
            client.close()

    def callback(self, monitor_message: dict[str, Any] | str):
        """Handle one Redis MONITOR message."""
        operational_event = self._parser.parse_operational_event(monitor_message)
        if operational_event is not None:
            self._handle_operational_event(operational_event)
            return operational_event

        event = self._parser.parse(monitor_message)
        if event is None:
            return None

        self._observed_messages += 1
        self._write_raw_event(event)
        task_msg = self.prepare_task_msg(event)
        if task_msg is not None:
            self.intercept(task_msg.to_dict())
        return task_msg

    def prepare_task_msg(self, event: AcademyRedisMonitorEvent) -> TaskObject | None:
        """Convert Academy Redis events into collapsed FlowCept interactions."""
        if event.academy_message is None:
            return self._prepare_unparsed_event_task(event)

        msg = academy_message_to_dict(event.academy_message)
        body_kind = msg["body_kind"]
        tag = msg["header"]["tag"]
        bundle = self._message_bundle(event, msg)

        if body_kind.endswith("request"):
            self._pending_requests[tag] = bundle
            return None

        if body_kind.endswith("response"):
            request_bundle = self._pending_requests.pop(tag, None)
            return self._prepare_exchange_task(request_bundle, bundle)

        return self._prepare_exchange_task(bundle, None, pairing_status="unpaired_message")

    def _prepare_unparsed_event_task(self, event: AcademyRedisMonitorEvent) -> TaskObject:
        """Represent an observed Redis queue write that was not an Academy message."""
        task_obj = TaskObject()
        task_obj.task_id = f"academy-redis-monitor:{uuid4()}"
        task_obj.utc_timestamp = event.redis_time or get_utc_now()
        task_obj.subtype = "academy_redis_observation"
        task_obj.workflow_id = Flowcept.current_workflow_id
        task_obj.campaign_id = Flowcept.campaign_id
        task_obj.adapter_id = "academy_redis_monitor"
        task_obj.tags = ["academy", "redis", "message-stream"]
        task_obj.custom_metadata = event.to_metadata()
        task_obj.activity_id = "academy.redis.rpush"
        task_obj.status = Status.UNKNOWN
        task_obj.used = {
            "queue_key": event.queue_key,
            "destination_uid": event.destination_uid,
        }
        task_obj.stderr = event.parse_error
        return task_obj

    def _prepare_exchange_task(
        self,
        request_bundle: dict[str, Any] | None,
        response_bundle: dict[str, Any] | None,
        pairing_status: str | None = None,
    ) -> TaskObject:
        msg = self._message_from_pair(request_bundle, response_bundle)
        header = msg["header"]
        body_kind = msg["body_kind"]
        tag = header["tag"]

        task_obj = TaskObject()
        task_obj.task_id = f"academy-exchange:{tag}"
        task_obj.utc_timestamp = self._bundle_time(response_bundle) or self._bundle_time(request_bundle) or get_utc_now()
        task_obj.subtype = "academy_exchange_interaction"
        task_obj.workflow_id = Flowcept.current_workflow_id
        task_obj.campaign_id = Flowcept.campaign_id
        task_obj.adapter_id = "academy_redis_monitor"
        task_obj.tags = ["academy", "redis", "message-stream", "communication"]
        task_obj.activity_id = self._activity_id(msg)
        task_obj.source_agent_id = header["src"]
        task_obj.agent_id = header["dest"]
        task_obj.group_id = tag
        task_obj.submitted_at = self._bundle_time(request_bundle)
        task_obj.started_at = self._bundle_time(request_bundle)
        task_obj.ended_at = self._bundle_time(response_bundle)
        task_obj.status = self._status_for_pair(request_bundle, response_bundle)
        task_obj.used = self._used_for_request(request_bundle)
        task_obj.generated = self._generated_for_response(response_bundle)
        task_obj.stderr = self._stderr_for_response(response_bundle)
        task_obj.custom_metadata = {
            "semantic_record_type": "academy_exchange_interaction",
            "communication_layer": "academy_redis_exchange",
            "pairing_status": pairing_status or self._pairing_status(request_bundle, response_bundle),
            "academy_runtime": self._runtime_metadata_for_message(header),
            "communication": {
                "request": self._communication_metadata(request_bundle),
                "response": self._communication_metadata(response_bundle),
            },
        }
        return task_obj

    def _message_bundle(self, event: AcademyRedisMonitorEvent, msg: dict[str, Any]) -> dict[str, Any]:
        return {
            "redis": event.to_metadata(),
            "message": msg,
        }

    def _flush_pending_requests(self) -> None:
        pending = list(self._pending_requests.values())
        self._pending_requests.clear()
        for request_bundle in pending:
            task_msg = self._prepare_exchange_task(
                request_bundle,
                None,
                pairing_status="request_without_response",
            )
            self.intercept(task_msg.to_dict())

    def _message_from_pair(
        self,
        request_bundle: dict[str, Any] | None,
        response_bundle: dict[str, Any] | None,
    ) -> dict[str, Any]:
        if request_bundle is not None:
            return request_bundle["message"]
        if response_bundle is not None:
            return response_bundle["message"]
        raise ValueError("At least one Academy message bundle is required.")

    def _bundle_time(self, bundle: dict[str, Any] | None) -> float | None:
        if bundle is None:
            return None
        return bundle["redis"].get("redis_time")

    def _status_for_pair(
        self,
        request_bundle: dict[str, Any] | None,
        response_bundle: dict[str, Any] | None,
    ) -> Status:
        if response_bundle is None:
            return Status.SUBMITTED if request_bundle is not None else Status.UNKNOWN
        response_kind = response_bundle["message"]["body_kind"]
        if response_kind == "error-response":
            return Status.ERROR
        if response_kind.endswith("response"):
            return Status.FINISHED
        return Status.UNKNOWN

    def _pairing_status(
        self,
        request_bundle: dict[str, Any] | None,
        response_bundle: dict[str, Any] | None,
    ) -> str:
        if request_bundle is not None and response_bundle is not None:
            return "complete"
        if request_bundle is not None:
            return "request_without_response"
        if response_bundle is not None:
            return "response_without_request"
        return "unpaired_message"

    def _used_for_request(self, request_bundle: dict[str, Any] | None) -> dict[str, Any] | None:
        if request_bundle is None:
            return None
        msg = request_bundle["message"]
        if msg["body_kind"] == "action-request":
            return {
                "args": msg.get("pargs", []),
                "kwargs": msg.get("kargs", {}),
            }
        return {
            "request": msg.get("body", msg),
        }

    def _generated_for_response(self, response_bundle: dict[str, Any] | None) -> dict[str, Any] | None:
        if response_bundle is None:
            return None
        msg = response_bundle["message"]
        if msg["body_kind"] == "action-response":
            return {"result": msg.get("result")}
        if msg["body_kind"] == "error-response":
            return {
                "exception_type": msg.get("exception_type"),
                "exception": msg.get("exception"),
            }
        return {
            "response": msg.get("body", msg),
        }

    def _stderr_for_response(self, response_bundle: dict[str, Any] | None) -> str | None:
        if response_bundle is None:
            return None
        msg = response_bundle["message"]
        if msg["body_kind"] == "error-response":
            return msg.get("exception")
        return None

    def _communication_metadata(self, bundle: dict[str, Any] | None) -> dict[str, Any] | None:
        if bundle is None:
            return None
        return {
            "redis": bundle["redis"],
            "message": bundle["message"],
        }

    def _handle_operational_event(self, event: AcademyRedisOperationalEvent) -> None:
        if event.kind == "academy_agent_registration" and event.uid is not None and event.value is not None:
            mro = [item for item in event.value.split(",") if item]
            self._agent_registry[event.uid] = {
                "uid": event.uid,
                "class": mro[0] if mro else None,
                "mro": mro,
                "registered_at": event.redis_time,
            }
        elif event.kind == "academy_heartbeat" and event.uid is not None and event.value is not None:
            try:
                self._heartbeats[event.uid] = float(event.value)
            except ValueError:
                return

    def _runtime_metadata_for_message(self, header: dict[str, Any]) -> dict[str, Any]:
        return {
            "source": self._entity_runtime_metadata(header.get("src")),
            "destination": self._entity_runtime_metadata(header.get("dest")),
        }

    def _entity_runtime_metadata(self, identifier: str | None) -> dict[str, Any]:
        uid = self._uid_from_identifier(identifier)
        if uid is None:
            return {}

        metadata: dict[str, Any] = {"uid": uid}
        agent_metadata = self._lookup_by_uid_or_prefix(self._agent_registry, uid)
        if agent_metadata is not None:
            metadata["agent"] = agent_metadata

        heartbeat_timestamp = self._lookup_by_uid_or_prefix(self._heartbeats, uid)
        if heartbeat_timestamp is not None:
            metadata["heartbeat"] = {"timestamp": heartbeat_timestamp}

        return metadata

    def _uid_from_identifier(self, identifier: str | None) -> str | None:
        if not identifier or "<" not in identifier or ">" not in identifier:
            return None
        return identifier.split("<", maxsplit=1)[1].split(">", maxsplit=1)[0]

    def _lookup_by_uid_or_prefix(self, mapping: dict[str, Any], uid: str) -> Any | None:
        if uid in mapping:
            return mapping[uid]
        matches = [value for key, value in mapping.items() if key.startswith(uid)]
        if len(matches) == 1:
            return matches[0]
        return None

    def _activity_id(self, msg: dict[str, Any]) -> str:
        body_kind = msg["body_kind"]
        if body_kind == "action-request":
            return msg.get("action") or "academy.action"
        if body_kind in {"action-response", "error-response"}:
            return "academy.action"
        return f"academy.{body_kind}"

    def _write_raw_event(self, event: AcademyRedisMonitorEvent) -> None:
        if self._raw_event_log_path is None:
            return
        self._raw_event_log_path.parent.mkdir(parents=True, exist_ok=True)
        with self._raw_event_log_path.open("a") as f:
            f.write(json.dumps(event.to_metadata(), sort_keys=True) + "\n")
