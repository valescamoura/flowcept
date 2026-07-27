"""Codex session log interceptor."""

from __future__ import annotations

import json
import os
import re
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from time import sleep
from typing import Any

from flowcept.commons.flowcept_dataclasses.agent_object import AgentObject
from flowcept.commons.flowcept_dataclasses.task_object import TaskObject
from flowcept.commons.flowcept_dataclasses.workflow_object import WorkflowObject
from flowcept.commons.utils import get_utc_now
from flowcept.commons.vocabulary import PROV_AGENT, PROV_AGENT_LOOP, Status
from flowcept.flowceptor.adapters.base_interceptor import BaseInterceptor


JsonObject = dict[str, Any]

PLAN_SUMMARY_SECTION_NAMES = {
    "resumo",
    "summary",
}

PLAN_EVALUATION_SECTION_NAMES = {
    "verificacao",
    "verificacoes",
    "verification",
    "verifications",
    "validation",
    "validacao",
    "test",
    "tests",
    "testing",
    "teste",
    "testes",
    "evaluation",
    "avaliacao",
    "evaluation_criteria",
    "criterios_de_avaliacao",
    "criterios_avaliacao",
}


@dataclass
class ToolInvocationState:
    """State for a Codex function_call/function_call_output pair."""

    call_id: str
    task_id: str
    tool_name: str | None
    arguments: Any
    started_at: float | None
    response_item_id: str | None = None
    ended_at: float | None = None
    output: Any = None
    emitted: bool = False


@dataclass
class ModelInvocationState:
    """State for a Codex model invocation within a turn."""

    task_id: str
    turn_id: str
    workflow_id: str | None
    agent_id: str | None
    source_agent_id: str | None
    started_at: float | None
    prompt: str | None = None
    ai_model: JsonObject = field(default_factory=dict)
    messages: list[JsonObject] = field(default_factory=list)
    generated_messages: list[JsonObject] = field(default_factory=list)
    plans: list[JsonObject] = field(default_factory=list)
    response: str | None = None
    token_usage: JsonObject | None = None
    ended_at: float | None = None
    tools: dict[str, ToolInvocationState] = field(default_factory=dict)
    emitted: bool = False


@dataclass
class TurnState:
    """State for one Codex turn, mapped to a loop iteration."""

    task_id: str
    workflow_id: str | None
    agent_id: str | None
    source_agent_id: str | None
    started_at: float | None
    submitted_at: float | None = None
    ended_at: float | None = None
    prompt: str | None = None
    final_response: str | None = None
    metadata: JsonObject = field(default_factory=dict)
    mandates: list[JsonObject] = field(default_factory=list)
    messages: list[JsonObject] = field(default_factory=list)
    invocations: list[ModelInvocationState] = field(default_factory=list)
    current_invocation: ModelInvocationState | None = None
    emitted: bool = False


def _compact_dict(data: JsonObject) -> JsonObject:
    return {key: value for key, value in data.items() if value is not None}


def _epoch_seconds(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _parse_json_or_raw(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def _message_text(payload: JsonObject) -> str | None:
    content = payload.get("content") or []
    parts = []
    for item in content:
        if isinstance(item, dict):
            text = item.get("text")
            if text:
                parts.append(text)
    return "\n".join(parts) if parts else None


def _extract_proposed_plans(text: str | None) -> list[str]:
    if not text:
        return []
    return [
        match.group(1).strip()
        for match in re.finditer(r"<proposed_plan>(.*?)</proposed_plan>", text, flags=re.DOTALL)
        if match.group(1).strip()
    ]


def _normalize_plan_content(content: str | None) -> str | None:
    if content is None:
        return None
    return content.strip()


def _normalized_section_name(name: str) -> str:
    normalized = unicodedata.normalize("NFKD", name.strip().lower())
    ascii_name = "".join(char for char in normalized if not unicodedata.combining(char))
    return re.sub(r"[^a-z0-9]+", "_", ascii_name).strip("_")


def _parse_plan_content(content: str) -> JsonObject:
    title = None
    current_section = None
    sections: dict[str, list[str]] = {}
    raw_sections: dict[str, str] = {}

    for raw_line in content.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        heading = re.fullmatch(r"\*\*(.+?)\*\*", line)
        if heading:
            heading_text = heading.group(1).strip()
            if title is None:
                title = heading_text
                continue
            current_section = _normalized_section_name(heading_text)
            raw_sections[current_section] = heading_text
            sections.setdefault(current_section, [])
            continue
        if current_section is None:
            continue
        bullet = re.match(r"^[-*]\s+(.*)$", line)
        sections[current_section].append((bullet.group(1) if bullet else line).strip())

    return _compact_dict(
        {
            "title": title,
            "sections": {key: value for key, value in sections.items() if value},
            "section_names": raw_sections or None,
        }
    )


def _section_items(sections: JsonObject, aliases: set[str]) -> list[str]:
    items = []
    for name, section_items in sections.items():
        if name in aliases:
            items.extend(section_items)
    return items


def _plan_summary(plan: JsonObject) -> str | None:
    summary_items = _section_items(plan.get("sections") or {}, PLAN_SUMMARY_SECTION_NAMES)
    return "\n".join(summary_items) if summary_items else None


def _task_status(ended_at: float | None, output: Any = None) -> Status:
    if ended_at is None:
        return Status.UNKNOWN
    if isinstance(output, str):
        exit_codes = re.findall(r"Process exited with code (\d+)", output)
        if exit_codes and any(code != "0" for code in exit_codes):
            return Status.ERROR
        if '"interrupted":true' in output or "'interrupted': True" in output:
            return Status.ERROR
    if isinstance(output, dict) and (output.get("interrupted") or output.get("is_error")):
        return Status.ERROR
    return Status.FINISHED


def _entity(kind: str, **kwargs) -> JsonObject:
    return _compact_dict({"type": kind, **kwargs})


def _message(content: str | None, attributed_to: str | None, role: str | None = None, **kwargs) -> JsonObject:
    return _entity("message", content=content, attributed_to=attributed_to, role=role, **kwargs)


def _mandate(content: str | None, attributed_to: str | None, role: str | None = None, **kwargs) -> JsonObject:
    return _entity("mandate", content=content, attributed_to=attributed_to, role=role, **kwargs)


def _is_context_message(content: str | None) -> bool:
    if content is None:
        return False
    stripped = content.strip()
    return stripped.startswith("<environment_context>") or stripped.startswith("<permissions instructions>")


class CodexInterceptor(BaseInterceptor):
    """Interceptors Codex JSONL session logs into Flowcept provenance records."""

    def __init__(self, plugin_key: str = "codex"):
        super().__init__(plugin_key)
        self._observer: Any | None = None
        self._processed_lines: dict[str, int] = {}
        self._session_id: str | None = None
        self._codex_agent_id: str | None = None
        self._human_agent_id: str | None = None
        self._model_provider: str | None = None
        self._current_turn_id: str | None = None
        self._active_execution_plan_workflow_id: str | None = None
        self._pending_execution_plan_workflow_id: str | None = None
        self._pending_execution_plan_turn_id: str | None = None
        self._execution_plan_criteria: dict[str, list[JsonObject]] = {}
        self._turns: dict[str, TurnState] = {}
        self._emitted_records_count = 0

    def callback(self) -> int:
        """Read newly appended Codex JSONL events and emit Flowcept messages."""
        sleep(self.settings.watch_interval_sec)
        intercepted = 0
        for path in self._log_paths():
            intercepted += self._process_log(path)
        return intercepted

    def intercept(self, obj_msg: JsonObject):
        """Intercept and count emitted Codex provenance records."""
        super().intercept(obj_msg)
        self._emitted_records_count += 1

    def start(self, bundle_exec_id, check_safe_stops: bool = True) -> "CodexInterceptor":
        """Start observing Codex logs."""
        super().start(bundle_exec_id, check_safe_stops)
        self.observe()
        return self

    def stop(self, check_safe_stops: bool = True) -> bool:
        """Stop observing Codex logs."""
        self.logger.debug("Codex interceptor stopping...")
        if self._observer is not None:
            self._observer.stop()
            self._observer.join(timeout=1)
        try:
            self.callback()
            self._flush_open_records()
        except Exception as e:
            self.logger.exception(e)
        super().stop(check_safe_stops)
        self.logger.debug("Codex interceptor stopped.")
        return True

    def observe(self):
        """Observe Codex log files."""
        from watchdog.observers.polling import PollingObserver

        from flowcept.flowceptor.adapters.mlflow.interception_event_handler import (
            InterceptionEventHandler,
        )

        watch_path = Path(self.settings.file_path)
        parent_to_watch = watch_path if watch_path.is_dir() else watch_path.parent
        while not parent_to_watch.exists():
            self.logger.debug(f"I can't watch {parent_to_watch}, as it does not exist.")
            sleep(self.settings.watch_interval_sec)

        event_handler = InterceptionEventHandler(self, str(parent_to_watch), self.__class__.callback)
        self._observer = PollingObserver()
        self._observer.schedule(event_handler, str(parent_to_watch), recursive=self.settings.recursive)
        self._observer.start()
        sleep(0.2)
        self.logger.debug(f"Watching Codex logs under {parent_to_watch}")
        self.callback()

    def _log_paths(self) -> list[Path]:
        path = Path(self.settings.file_path)
        if path.is_file():
            return [path]
        if not path.exists():
            return []
        if path.is_dir():
            return sorted(item for item in path.rglob("*.jsonl") if item.is_file())
        return []

    def _process_log(self, path: Path) -> int:
        path_key = str(path.resolve())
        processed = self._processed_lines.get(path_key, 0)
        intercepted_before = self._emitted_records_count
        with path.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                if line_number <= processed:
                    continue
                line = line.strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError as exc:
                    self.logger.warning(f"Skipping invalid Codex JSONL line {line_number} in {path}: {exc}")
                    continue
                self._map_event(event, line_number, path)
                self._processed_lines[path_key] = line_number
        return self._emitted_records_count - intercepted_before

    def _map_event(self, event: JsonObject, line_number: int, path: Path):
        event_type = event.get("type")
        payload = event.get("payload") or {}
        timestamp = _epoch_seconds(event.get("timestamp"))
        if event_type == "session_meta":
            self._handle_session_meta(payload, timestamp, line_number, path)
        elif event_type == "turn_context":
            self._handle_turn_context(payload)
        elif event_type == "event_msg":
            self._handle_event_msg(payload, timestamp, line_number)
        elif event_type == "response_item":
            self._handle_response_item(payload, timestamp, line_number)

    def _handle_session_meta(self, payload: JsonObject, timestamp: float | None, line_number: int, path: Path):
        session_id = payload.get("session_id") or payload.get("id")
        if not session_id:
            return
        self._session_id = session_id
        self._codex_agent_id = f"codex:{session_id}"
        self._human_agent_id = f"human:{session_id}"
        self._model_provider = payload.get("model_provider")

        workflow_obj = WorkflowObject(workflow_id=session_id, name=payload.get("title") or session_id)
        workflow_obj.subtype = PROV_AGENT_LOOP.SESSION.value
        workflow_obj.started_at = _epoch_seconds(payload.get("timestamp")) or timestamp
        workflow_obj.status = Status.UNKNOWN
        workflow_obj.used = {
            "messages": [
                _entity(
                    "mandate",
                    content=(payload.get("base_instructions") or {}).get("text"),
                    attributed_to=self._human_agent_id,
                )
            ]
        }
        workflow_obj.custom_metadata = _compact_dict(
            {
                "cwd": payload.get("cwd"),
                "originator": payload.get("originator"),
                "cli_version": payload.get("cli_version"),
                "source": payload.get("source"),
                "thread_source": payload.get("thread_source"),
                "model_provider": payload.get("model_provider"),
                "log_path": str(path),
                "line_number": line_number,
            }
        )
        workflow_obj.workflow_description = "Codex session imported from a Codex JSONL transcript."
        self.send_workflow_message(workflow_obj)
        self.send_agent_message(self._agent_obj(self._codex_agent_id, payload.get("originator") or "Codex"))
        self.send_agent_message(self._agent_obj(self._human_agent_id, "Human"))

    def _handle_turn_context(self, payload: JsonObject):
        turn_id = payload.get("turn_id") or self._current_turn_id
        turn = self._turns.get(turn_id) if turn_id else None
        if not turn:
            return
        collaboration_mode = payload.get("collaboration_mode") or {}
        sandbox_policy = payload.get("sandbox_policy") or {}
        turn.metadata.update(
            _compact_dict(
                {
                    "turn_id": turn_id,
                    "cwd": payload.get("cwd"),
                    "model": payload.get("model"),
                    "effort": payload.get("effort"),
                    "approval_policy": payload.get("approval_policy"),
                    "sandbox_policy": sandbox_policy,
                    "collaboration_mode_kind": collaboration_mode.get("mode"),
                    "workspace_roots": payload.get("workspace_roots"),
                    "current_date": payload.get("current_date"),
                    "timezone": payload.get("timezone"),
                }
            )
        )
        for invocation in turn.invocations:
            self._apply_model_metadata(invocation, turn)

    def _handle_event_msg(self, payload: JsonObject, timestamp: float | None, line_number: int):
        payload_type = payload.get("type")
        if payload_type == "task_started":
            self._start_turn(payload, timestamp, line_number)
        elif payload_type == "user_message":
            self._set_turn_prompt(payload.get("message"), timestamp)
        elif payload_type == "agent_message":
            invocation = self._ensure_invocation(timestamp)
            if invocation:
                self._add_assistant_text(invocation, payload.get("message"), payload.get("phase"), timestamp)
        elif payload_type == "item_completed":
            self._handle_item_completed(payload, timestamp)
        elif payload_type == "token_count":
            self._close_invocation_with_tokens(payload, timestamp)
        elif payload_type == "task_complete":
            self._complete_turn(payload, timestamp, line_number)

    def _handle_response_item(self, payload: JsonObject, timestamp: float | None, line_number: int):
        metadata = payload.get("internal_chat_message_metadata_passthrough") or {}
        turn_id = metadata.get("turn_id")
        if turn_id in self._turns:
            self._current_turn_id = turn_id

        payload_type = payload.get("type")
        if payload_type == "message":
            self._handle_message(payload, timestamp, line_number)
        elif payload_type == "reasoning" and self.settings.include_reasoning:
            invocation = self._ensure_invocation(timestamp)
            if invocation:
                self._append_unique_message(
                    invocation.generated_messages,
                    _entity(
                        "observation",
                        content=payload.get("summary") or payload.get("content"),
                        encrypted_content=payload.get("encrypted_content"),
                        attributed_to=invocation.agent_id,
                        line_number=line_number,
                    ),
                )
        elif payload_type == "function_call":
            self._handle_function_call(payload, timestamp)
        elif payload_type == "function_call_output":
            self._handle_function_call_output(payload, timestamp)

    def _start_turn(self, payload: JsonObject, timestamp: float | None, line_number: int):
        turn_id = payload.get("turn_id")
        if not turn_id:
            return
        self._current_turn_id = turn_id
        started_at = _epoch_seconds(payload.get("started_at")) or timestamp
        turn = TurnState(
            task_id=turn_id,
            workflow_id=self._session_id,
            agent_id=self._codex_agent_id,
            source_agent_id=self._human_agent_id,
            started_at=started_at,
            metadata=_compact_dict(
                {
                    "turn_id": turn_id,
                    "collaboration_mode_kind": payload.get("collaboration_mode_kind"),
                    "model_context_window": payload.get("model_context_window"),
                    "line_number": line_number,
                }
            ),
        )
        self._turns[turn_id] = turn

    def _set_turn_prompt(self, prompt: str | None, timestamp: float | None):
        turn = self._current_turn()
        if not turn or prompt is None:
            return
        if _is_context_message(prompt):
            mandate = _mandate(prompt, attributed_to=turn.agent_id, role="user")
            self._append_unique_message(turn.mandates, mandate)
            if turn.current_invocation:
                self._append_unique_message(turn.current_invocation.messages, mandate)
            return
        turn.prompt = prompt
        turn.submitted_at = timestamp
        message = _message(prompt, attributed_to=turn.source_agent_id, role="user")
        self._append_unique_message(turn.messages, message)
        invocation = self._ensure_invocation(timestamp)
        if invocation and (invocation.prompt is None or _is_context_message(invocation.prompt)):
            invocation.prompt = prompt
            self._append_unique_message(invocation.messages, message)

    def _handle_message(self, payload: JsonObject, timestamp: float | None, line_number: int):
        role = payload.get("role")
        text = _message_text(payload)
        if not text:
            return
        turn = self._current_turn()
        if role == "user":
            self._set_turn_prompt(text, timestamp)
        elif role == "developer" and self.settings.include_developer_messages and turn:
            if _is_context_message(text):
                mandate = _mandate(text, attributed_to=turn.agent_id, role=role, line_number=line_number)
                self._append_unique_message(turn.mandates, mandate)
                if turn.current_invocation:
                    self._append_unique_message(turn.current_invocation.messages, mandate)
            else:
                message = _message(
                    text,
                    attributed_to=turn.source_agent_id,
                    role=role,
                    line_number=line_number,
                )
                self._append_unique_message(turn.messages, message)
                if turn.current_invocation:
                    self._append_unique_message(turn.current_invocation.messages, message)
        elif role == "assistant":
            invocation = self._ensure_invocation(timestamp)
            if invocation:
                self._add_assistant_text(invocation, text, payload.get("phase"), timestamp)

    def _handle_item_completed(self, payload: JsonObject, timestamp: float | None):
        item = payload.get("item") or {}
        if item.get("type") != "Plan":
            return
        invocation = self._ensure_invocation(timestamp)
        if invocation:
            self._register_plan(
                invocation,
                content=item.get("text"),
                item_id=item.get("id"),
                timestamp=timestamp,
            )

    def _add_assistant_text(
        self,
        invocation: ModelInvocationState,
        text: str | None,
        phase: str | None,
        timestamp: float | None,
    ):
        if not text:
            return
        message = _message(text, attributed_to=invocation.agent_id, role="assistant", phase=phase)
        for plan_text in _extract_proposed_plans(text):
            self._register_plan(invocation, content=plan_text, item_id=None, timestamp=timestamp)
        if phase == "final_answer":
            invocation.response = text
            invocation.ended_at = timestamp or invocation.ended_at
        else:
            self._append_unique_message(invocation.generated_messages, message)

    def _handle_function_call(self, payload: JsonObject, timestamp: float | None):
        invocation = self._ensure_invocation(timestamp)
        if not invocation:
            return
        call_id = payload.get("call_id") or payload.get("id")
        if not call_id:
            return
        tool_name = payload.get("name")
        invocation.tools[call_id] = ToolInvocationState(
            call_id=call_id,
            task_id=f"{invocation.task_id}:tool_invocation:{len(invocation.tools) + 1}",
            tool_name=tool_name,
            arguments=_parse_json_or_raw(payload.get("arguments")),
            started_at=timestamp,
            response_item_id=payload.get("id"),
        )

    def _handle_function_call_output(self, payload: JsonObject, timestamp: float | None):
        call_id = payload.get("call_id")
        invocation = self._find_invocation_for_call(call_id) or self._ensure_invocation(timestamp)
        if not invocation or not call_id:
            return
        tool = invocation.tools.get(call_id)
        if tool is None:
            tool = ToolInvocationState(
                call_id=call_id,
                task_id=f"{invocation.task_id}:tool_invocation:{len(invocation.tools) + 1}",
                tool_name=None,
                arguments=None,
                started_at=timestamp,
            )
            invocation.tools[call_id] = tool
        tool.output = _parse_json_or_raw(payload.get("output"))
        tool.ended_at = timestamp
        self._emit_tool(tool, invocation)

    def _close_invocation_with_tokens(self, payload: JsonObject, timestamp: float | None):
        invocation = self._ensure_invocation(timestamp)
        turn = self._current_turn()
        if not invocation or not turn:
            return
        info = payload.get("info") or {}
        usage = info.get("last_token_usage") or info
        invocation.token_usage = self._normalize_llm_usage(usage, invocation, info)
        invocation.ai_model["context_window"] = info.get("model_context_window") or invocation.ai_model.get(
            "context_window"
        )
        invocation.ended_at = timestamp

    def _complete_turn(self, payload: JsonObject, timestamp: float | None, line_number: int):
        turn_id = payload.get("turn_id") or self._current_turn_id
        turn = self._turns.get(turn_id) if turn_id else None
        if not turn:
            return
        turn.final_response = payload.get("last_agent_message")
        turn.ended_at = _epoch_seconds(payload.get("completed_at")) or timestamp
        turn.metadata.update(
            _compact_dict(
                {
                    "duration_ms": payload.get("duration_ms"),
                    "time_to_first_token_ms": payload.get("time_to_first_token_ms"),
                    "line_number_completed": line_number,
                }
            )
        )
        if turn.current_invocation:
            turn.current_invocation.response = turn.current_invocation.response or turn.final_response
            turn.current_invocation.ended_at = turn.current_invocation.ended_at or turn.ended_at
            self._emit_invocation(turn.current_invocation)
            turn.current_invocation = None
        self._emit_turn(turn)
        self._activate_pending_execution_plan(turn.task_id)
        self._current_turn_id = None

    def _ensure_invocation(self, timestamp: float | None) -> ModelInvocationState | None:
        turn = self._current_turn()
        if not turn:
            return None
        if turn.current_invocation:
            return turn.current_invocation
        invocation = ModelInvocationState(
            task_id=f"{turn.task_id}:ai_model_invocation:{len(turn.invocations) + 1}",
            turn_id=turn.task_id,
            workflow_id=self._active_workflow_id(turn.workflow_id),
            agent_id=turn.agent_id,
            source_agent_id=turn.source_agent_id,
            started_at=timestamp or turn.started_at,
            prompt=turn.prompt,
            messages=list(turn.mandates + turn.messages),
        )
        self._apply_model_metadata(invocation, turn)
        turn.invocations.append(invocation)
        turn.current_invocation = invocation
        return invocation

    def _apply_model_metadata(self, invocation: ModelInvocationState, turn: TurnState):
        invocation.ai_model.update(
            _compact_dict(
                {
                    "type": "ai_model",
                    "model": turn.metadata.get("model"),
                    "provider": self._workflow_provider(),
                    "effort": turn.metadata.get("effort"),
                    "context_window": turn.metadata.get("model_context_window"),
                }
            )
        )

    def _normalize_llm_usage(
        self, usage: JsonObject, invocation: ModelInvocationState, info: JsonObject
    ) -> JsonObject:
        return _compact_dict(
            {
                "model": invocation.ai_model.get("model"),
                "input_tokens": usage.get("input_tokens"),
                "output_tokens": usage.get("output_tokens"),
                "total_tokens": usage.get("total_tokens"),
                "cached_input_tokens": usage.get("cached_input_tokens"),
                "reasoning_output_tokens": usage.get("reasoning_output_tokens"),
                "model_context_window": info.get("model_context_window"),
                "token_count_source": "codex_token_count",
            }
        )

    def _emit_turn(self, turn: TurnState):
        if turn.emitted:
            return
        task = TaskObject()
        task.task_id = turn.task_id
        task.workflow_id = self._active_workflow_id(turn.workflow_id)
        task.agent_id = turn.agent_id
        task.source_agent_id = turn.source_agent_id
        task.subtype = PROV_AGENT_LOOP.LOOP_ITERATION.value
        task.activity_id = "loop_iteration"
        task.used = _compact_dict(
            {
                "messages": (turn.mandates + turn.messages) or None,
            }
        )
        task.generated = _compact_dict(
            {
                "messages": [
                    _entity(
                        "observation",
                        content=turn.final_response,
                        attributed_to=turn.agent_id,
                        role="assistant",
                    )
                ]
                if turn.final_response
                else None,
            }
        )
        task.submitted_at = turn.submitted_at
        task.started_at = turn.started_at
        task.ended_at = turn.ended_at
        task.status = _task_status(turn.ended_at)
        task.custom_metadata = _compact_dict(
            {
                **turn.metadata,
                "session_workflow_id": turn.workflow_id if task.workflow_id != turn.workflow_id else None,
                "execution_plan_id": self._active_execution_plan_workflow_id,
            }
        )
        task.enrich(self.plugin_key)
        self.intercept(task.to_dict())
        turn.emitted = True

    def _emit_invocation(self, invocation: ModelInvocationState):
        if invocation.emitted:
            return
        for tool in invocation.tools.values():
            if tool.ended_at is not None:
                self._emit_tool(tool, invocation)
        task = TaskObject()
        task.task_id = invocation.task_id
        task.workflow_id = invocation.workflow_id
        task.parent_task_id = invocation.turn_id
        task.agent_id = invocation.agent_id
        task.source_agent_id = invocation.source_agent_id
        task.subtype = PROV_AGENT.AI_MODEL_INVOCATION.value
        task.activity_id = "ai_model_invocation"
        task.used = _compact_dict(
            {
                "prompt": invocation.prompt or self._prompt_from_messages(invocation.messages),
                "messages": invocation.messages or None,
                "ai_model": invocation.ai_model or None,
            }
        )
        task.generated = _compact_dict(
            {
                "response": invocation.response,
                "messages": self._generated_invocation_messages(invocation) or None,
            }
        )
        task.started_at = invocation.started_at
        task.ended_at = invocation.ended_at
        task.status = _task_status(invocation.ended_at)
        task.custom_metadata = _compact_dict(
            {
                "turn_id": invocation.turn_id,
                "session_workflow_id": self._session_id if task.workflow_id != self._session_id else None,
                "execution_plan_id": task.workflow_id if task.workflow_id != self._session_id else None,
                "llm_usage": invocation.token_usage,
                "response_metadata": {
                    "model": invocation.ai_model.get("model"),
                    "provider": invocation.ai_model.get("provider"),
                    "effort": invocation.ai_model.get("effort"),
                    "context_window": invocation.ai_model.get("context_window"),
                },
            }
        )
        task.enrich(self.plugin_key)
        self.intercept(task.to_dict())
        invocation.emitted = True

    def _generated_invocation_messages(self, invocation: ModelInvocationState) -> list[JsonObject]:
        messages = []
        if invocation.response:
            messages.append(
                _entity(
                    "observation",
                    content=invocation.response,
                    attributed_to=invocation.agent_id,
                    role="assistant",
                )
            )
        messages.extend(invocation.generated_messages)
        messages.extend(invocation.plans)
        return messages

    def _register_plan(
        self,
        invocation: ModelInvocationState,
        content: str | None,
        item_id: str | None,
        timestamp: float | None,
    ):
        content = _normalize_plan_content(content)
        if not content:
            return
        for existing in invocation.plans:
            if existing.get("content") == content:
                return
        plan = _entity(
            "plan",
            content=content,
            item_id=item_id,
            attributed_to=invocation.agent_id,
            **_parse_plan_content(content),
        )
        invocation.plans.append(plan)
        self._emit_execution_plan(plan, invocation, timestamp)

    def _emit_execution_plan(
        self,
        plan: JsonObject,
        invocation: ModelInvocationState,
        timestamp: float | None,
    ):
        workflow_id = f"{invocation.task_id}:execution_plan:{len(invocation.plans)}"
        workflow = WorkflowObject(
            workflow_id=workflow_id,
            name="execution_plan",
        )
        workflow.parent_workflow_id = self._session_id or invocation.workflow_id
        workflow.agent_id = invocation.agent_id
        workflow.subtype = PROV_AGENT_LOOP.EXECUTION_PLAN.value
        workflow.started_at = timestamp or invocation.started_at
        workflow.ended_at = timestamp
        workflow.status = _task_status(workflow.ended_at)
        workflow.used = _compact_dict(
            {
                "messages": self._execution_plan_used_messages(invocation) or None,
                "ai_model": invocation.ai_model or None,
            }
        )
        workflow.generated = {"messages": self._execution_plan_generated_messages(plan)}
        workflow.custom_metadata = _compact_dict(
            {
                "turn_id": invocation.turn_id,
                "ai_model_invocation_id": invocation.task_id,
                "item_id": plan.get("item_id"),
            }
        )
        workflow.workflow_description = (
            _plan_summary(plan) or "Codex execution plan extracted from a proposed_plan block or Plan item."
        )
        self.send_workflow_message(workflow)
        self._pending_execution_plan_workflow_id = workflow_id
        self._pending_execution_plan_turn_id = invocation.turn_id
        self._emit_plan_evaluation(plan, invocation, workflow_id, timestamp)

    def _execution_plan_used_messages(self, invocation: ModelInvocationState) -> list[JsonObject]:
        messages = [message for message in invocation.messages if message.get("type") != "mandate"]
        prompt = invocation.prompt or self._prompt_from_messages(invocation.messages)
        if prompt:
            objective = _entity(
                "objective",
                content=prompt,
                attributed_to=invocation.source_agent_id,
                role="user",
            )
            if not any(message.get("type") == "objective" and message.get("content") == prompt for message in messages):
                messages.append(objective)
        return messages

    def _execution_plan_generated_messages(self, plan: JsonObject) -> list[JsonObject]:
        sections = plan.get("sections") or {}
        return [
            _entity(
                "plan",
                content=plan.get("content"),
                title=plan.get("title"),
                sections=sections or None,
                attributed_to=plan.get("attributed_to"),
            )
        ]

    def _emit_plan_evaluation(
        self,
        plan: JsonObject,
        invocation: ModelInvocationState,
        workflow_id: str,
        timestamp: float | None,
    ):
        sections = plan.get("sections") or {}
        criteria = _section_items(sections, PLAN_EVALUATION_SECTION_NAMES)
        if not criteria:
            return
        self._execution_plan_criteria[workflow_id] = [
            _entity(
                "evaluation_criteria",
                content=item,
                attributed_to=plan.get("attributed_to"),
                index=index,
            )
            for index, item in enumerate(criteria, start=1)
        ]
        task = TaskObject()
        task.task_id = f"{workflow_id}:evaluation"
        task.workflow_id = workflow_id
        task.parent_task_id = invocation.task_id
        task.agent_id = invocation.agent_id
        task.source_agent_id = invocation.source_agent_id
        task.subtype = PROV_AGENT_LOOP.EVALUATION.value
        task.activity_id = "evaluation"
        task.used = {
            "messages": self._execution_plan_criteria[workflow_id]
        }
        task.started_at = timestamp or invocation.started_at
        task.ended_at = timestamp
        task.status = _task_status(task.ended_at)
        task.custom_metadata = _compact_dict(
            {
                "turn_id": invocation.turn_id,
                "ai_model_invocation_id": invocation.task_id,
                "execution_plan_id": workflow_id,
                "item_id": plan.get("item_id"),
            }
        )
        task.enrich(self.plugin_key)
        self.intercept(task.to_dict())

    def _emit_tool(self, tool: ToolInvocationState, invocation: ModelInvocationState):
        if tool.emitted:
            return
        task = TaskObject()
        task.task_id = tool.task_id
        task.workflow_id = self._active_workflow_id(invocation.workflow_id)
        task.parent_task_id = invocation.task_id
        task.agent_id = invocation.agent_id
        task.source_agent_id = invocation.source_agent_id
        is_evaluation = self._is_test_command(tool.arguments)
        task.subtype = PROV_AGENT_LOOP.EVALUATION.value if is_evaluation else PROV_AGENT.TOOL_INVOCATION.value
        task.activity_id = "evaluation" if is_evaluation else tool.tool_name or "tool_invocation"
        task.used = _compact_dict(
            {
                "tool": _entity("tool", name=tool.tool_name) if not is_evaluation else None,
                "messages": self._tool_used_messages(tool, invocation, task.workflow_id, is_evaluation) or None,
            }
        )
        task.generated = _compact_dict(
            {
                "messages": [
                    _entity(
                        "observation",
                        content=tool.output,
                        attributed_to=invocation.agent_id,
                    )
                ]
                if tool.output is not None
                else None
            }
        )
        task.started_at = tool.started_at
        task.ended_at = tool.ended_at or tool.started_at
        task.status = _task_status(task.ended_at, tool.output)
        task.custom_metadata = _compact_dict(
            {
                "call_id": tool.call_id,
                "response_item_id": tool.response_item_id,
                "loop_iteration_id": invocation.turn_id,
                "session_workflow_id": self._session_id if task.workflow_id != self._session_id else None,
                "execution_plan_id": task.workflow_id if task.workflow_id != self._session_id else None,
                "tool_name": tool.tool_name if is_evaluation else None,
                "evaluation_criteria_match": self._evaluation_criteria_match(task.workflow_id)
                if is_evaluation
                else None,
            }
        )
        task.enrich(self.plugin_key)
        self.intercept(task.to_dict())
        tool.emitted = True

    def _flush_open_records(self):
        for turn in self._turns.values():
            for invocation in turn.invocations:
                for tool in invocation.tools.values():
                    if not tool.emitted:
                        self._emit_tool(tool, invocation)
                if not invocation.emitted:
                    self._emit_invocation(invocation)
            if not turn.emitted:
                turn.ended_at = turn.ended_at or get_utc_now()
                self._emit_turn(turn)

    def _current_turn(self) -> TurnState | None:
        if self._current_turn_id:
            return self._turns.get(self._current_turn_id)
        return None

    def _active_workflow_id(self, fallback_workflow_id: str | None) -> str | None:
        return self._active_execution_plan_workflow_id or fallback_workflow_id

    def _activate_pending_execution_plan(self, turn_id: str | None):
        if turn_id is None or self._pending_execution_plan_turn_id != turn_id:
            return
        self._active_execution_plan_workflow_id = self._pending_execution_plan_workflow_id
        self._pending_execution_plan_workflow_id = None
        self._pending_execution_plan_turn_id = None

    def _is_test_command(self, arguments: Any) -> bool:
        if isinstance(arguments, dict):
            command = arguments.get("cmd") or arguments.get("command")
        else:
            command = arguments
        return isinstance(command, str) and command.strip().startswith("test")

    def _tool_used_messages(
        self,
        tool: ToolInvocationState,
        invocation: ModelInvocationState,
        workflow_id: str | None,
        is_evaluation: bool,
    ) -> list[JsonObject]:
        messages = []
        if tool.arguments is not None:
            messages.append(
                _message(
                    tool.arguments,
                    attributed_to=invocation.agent_id,
                    role="assistant",
                )
            )
        if is_evaluation and workflow_id:
            messages.extend(self._execution_plan_criteria.get(workflow_id, []))
        return messages

    def _evaluation_criteria_match(self, workflow_id: str | None) -> str:
        if workflow_id and self._execution_plan_criteria.get(workflow_id):
            return "all_active_plan_criteria"
        return "unresolved"

    def _find_invocation_for_call(self, call_id: str | None) -> ModelInvocationState | None:
        if not call_id:
            return None
        for turn in self._turns.values():
            for invocation in turn.invocations:
                if call_id in invocation.tools:
                    return invocation
        return None

    def _workflow_provider(self) -> str | None:
        return self._model_provider

    def _prompt_from_messages(self, messages: list[JsonObject]) -> str | None:
        for message in reversed(messages):
            if message.get("role") == "user" and message.get("content"):
                return message["content"]
        return None

    def _append_unique_message(self, messages: list[JsonObject], message: JsonObject):
        for existing in messages:
            if existing.get("role") == message.get("role") and existing.get("content") == message.get("content"):
                return
        messages.append(message)

    def _agent_obj(self, agent_id: str, name: str) -> AgentObject:
        agent = AgentObject(agent_id=agent_id, name=name, workflow_id=self._session_id)
        agent.extra_metadata = {"source": "codex_jsonl"}
        agent.enrich()
        return agent
