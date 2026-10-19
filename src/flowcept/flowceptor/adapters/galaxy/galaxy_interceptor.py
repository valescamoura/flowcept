"""Galaxy interceptor module."""

import json
from datetime import datetime, timezone
from pathlib import Path
from threading import Event, Lock, Thread
from typing import Callable, Dict, List, Optional
from urllib.parse import urlparse

from bioblend.galaxy import GalaxyInstance

from flowcept.commons.flowcept_dataclasses.task_object import TaskObject
from flowcept.commons.flowcept_dataclasses.workflow_object import WorkflowObject
from flowcept.commons.utils import get_utc_now
from flowcept.commons.vocabulary import Status
from flowcept.flowceptor.adapters.base_interceptor import BaseInterceptor

ContextResolver = Callable[[Dict, Dict], Dict]
"""Receives the Galaxy invocation and its stored workflow and returns extra
context for the records, e.g. ``{"campaign_id": ..., "parent_workflow_id": ...}``."""

FINISHED_INVOCATION_STATES = {"failed", "cancelled"}
TERMINAL_JOB_STATES = {"ok", "error", "failed", "deleted", "skipped"}
JOB_STATUS = {"ok": Status.FINISHED, "error": Status.ERROR, "failed": Status.ERROR}


def invocation_workflow_id(galaxy_url: str, invocation_id: str) -> str:
    """Return the Flowcept workflow_id of a Galaxy invocation.

    Public so that the application that triggers the invocation can register
    the same workflow before the interceptor fills in its tasks.
    """
    return f"galaxy:{urlparse(galaxy_url).netloc}:invocation:{invocation_id}"


def job_task_id(galaxy_url: str, job_id: str) -> str:
    """Return the Flowcept task_id of a Galaxy job."""
    return f"galaxy:{urlparse(galaxy_url).netloc}:job:{job_id}"


class GalaxyInterceptor(BaseInterceptor):
    """Observe Galaxy workflow invocations through the Galaxy API.

    Each finished invocation becomes a workflow record and each of its jobs a
    task record. Invocations are polled every ``watch_interval_sec`` and
    intercepted once; their ids are kept in ``state_path``.
    """

    def __init__(self, plugin_key="galaxy", context_resolver: Optional[ContextResolver] = None):
        super().__init__(plugin_key)
        self.context_resolver = context_resolver
        self.gi = GalaxyInstance(self.settings.url, key=self.settings.api_key, verify=self.settings.verify)
        self._state_path = Path(self.settings.state_path)
        self._done = set(json.loads(self._state_path.read_text())) if self._state_path.exists() else set()
        self._intercepted_since_start: List[str] = []
        self._callback_lock = Lock()
        self._stop_event = Event()
        self._observer_thread: Thread = None

    def workflow_id_of(self, invocation_id: str) -> str:
        """Return the Flowcept workflow_id of a Galaxy invocation."""
        return invocation_workflow_id(self.settings.url, invocation_id)

    def task_id_of(self, job_id: str) -> str:
        """Return the Flowcept task_id of a Galaxy job."""
        return job_task_id(self.settings.url, job_id)

    def start(self, bundle_exec_id, check_safe_stops: bool = True) -> "GalaxyInterceptor":
        """Start polling the Galaxy API."""
        super().start(bundle_exec_id, check_safe_stops)
        self._stop_event.clear()
        self._observer_thread = Thread(target=self.observe, daemon=True)
        self._observer_thread.start()
        return self

    def stop(self, check_safe_stops: bool = True) -> bool:
        """Stop polling, intercept what finished meanwhile and persist the state."""
        self._stop_event.set()
        self._observer_thread.join()
        self.callback()
        super().stop(check_safe_stops)
        self._commit_state()
        return True

    def observe(self):
        """Poll the Galaxy API until stopped."""
        while not self._stop_event.is_set():
            self.callback()
            self._stop_event.wait(self.settings.watch_interval_sec)

    def callback(self) -> int:
        """Intercept the finished invocations not intercepted yet."""
        intercepted = 0
        with self._callback_lock:
            for invocation in self._list_invocations():
                invocation_id = invocation["id"]
                if invocation_id in self._done:
                    continue
                if self._intercept_invocation(invocation_id):
                    self._done.add(invocation_id)
                    self._intercepted_since_start.append(invocation_id)
                    intercepted += 1
        return intercepted

    def _list_invocations(self) -> List[Dict]:
        return self.gi.make_get_request(
            f"{self.gi.url}/invocations",
            params={"limit": self.settings.invocations_limit, "sort_by": "create_time", "sort_desc": True},
        ).json()

    def _intercept_invocation(self, invocation_id: str) -> bool:
        invocation = self.gi.invocations.show_invocation(invocation_id)
        jobs = self.gi.jobs.get_jobs(invocation_id=invocation_id)
        if not self._is_finished(invocation, jobs):
            return False

        stored_workflow = self.gi.workflows.show_workflow(invocation["workflow_id"], instance=True)
        context = self.context_resolver(invocation, stored_workflow) if self.context_resolver else {}
        workflow_id = self.workflow_id_of(invocation_id)

        job_details = [self.gi.jobs.show_job(job["id"], full_details=True) for job in jobs]
        producers = {
            output["id"]: self.task_id_of(job["id"]) for job in job_details for output in job["outputs"].values()
        }
        steps = {step["job_id"]: step for step in invocation["steps"] if step.get("job_id")}

        self.send_workflow_message(self._workflow_obj(workflow_id, invocation, stored_workflow, context))
        for job in job_details:
            task = self._task_obj(job, workflow_id, context, producers, steps.get(job["id"]))
            self.intercept(task.to_dict())
        return True

    @staticmethod
    def _is_finished(invocation: Dict, jobs: List[Dict]) -> bool:
        if invocation["state"] in FINISHED_INVOCATION_STATES:
            return True
        if invocation["state"] != "scheduled":
            return False
        return all(job["state"] in TERMINAL_JOB_STATES for job in jobs)

    def _workflow_obj(self, workflow_id: str, invocation: Dict, stored_workflow: Dict, context: Dict) -> WorkflowObject:
        wf = WorkflowObject()
        wf.workflow_id = workflow_id
        wf.name = stored_workflow["name"]
        wf.adapter_id = self.settings.key
        wf.campaign_id = context.get("campaign_id")
        wf.parent_workflow_id = context.get("parent_workflow_id")
        wf.used = {
            "stored_workflow_id": stored_workflow["id"],
            "workflow_version_id": invocation["workflow_id"],
            "history_id": invocation["history_id"],
            "inputs": _datasets(invocation.get("inputs", {})),
        }
        wf.generated = {"outputs": _datasets(invocation.get("outputs", {}))}
        wf.custom_metadata = {
            "galaxy_url": self.settings.url,
            "invocation_id": invocation["id"],
            "state": invocation["state"],
            "create_time": invocation["create_time"],
            "update_time": invocation["update_time"],
        }
        return wf

    def _task_obj(
        self, job: Dict, workflow_id: str, context: Dict, producers: Dict[str, str], step: Optional[Dict]
    ) -> TaskObject:
        metrics = {m["name"]: m["raw_value"] for m in self.gi.jobs.get_metrics(job["id"])}
        task = TaskObject()
        task.task_id = self.task_id_of(job["id"])
        task.workflow_id = workflow_id
        task.campaign_id = context.get("campaign_id")
        task.activity_id = job["tool_id"]
        task.adapter_id = self.settings.key
        task.utc_timestamp = get_utc_now()
        task.status = JOB_STATUS.get(job["state"], Status.UNKNOWN)
        task.submitted_at = _galaxy_time(job["create_time"])
        task.started_at = float(metrics["start_epoch"]) if "start_epoch" in metrics else None
        task.ended_at = float(metrics["end_epoch"]) if "end_epoch" in metrics else _galaxy_time(job["update_time"])
        task.used = {"params": job.get("params", {}), "inputs": _datasets(job["inputs"])}
        task.generated = {"outputs": _datasets(job["outputs"])}
        task.stdout = job.get("tool_stdout")
        task.stderr = job.get("tool_stderr")
        task.dependencies = sorted(
            {producers[i["id"]] for i in job["inputs"].values() if i["id"] in producers} - {task.task_id}
        )
        task.custom_metadata = {
            "galaxy_job_id": job["id"],
            "tool_version": job.get("tool_version"),
            "command_line": job.get("command_line"),
            "exit_code": job.get("exit_code"),
            "state": job["state"],
            "history_id": job.get("history_id"),
            "metrics": metrics,
        }
        if step is not None:
            task.custom_metadata["step"] = {
                "order_index": step.get("order_index"),
                "label": step.get("workflow_step_label"),
            }
        return task

    def _commit_state(self):
        """Persist the intercepted ids after the buffer was flushed by stop()."""
        if not self._intercepted_since_start:
            return
        tmp = self._state_path.with_suffix(self._state_path.suffix + ".tmp")
        tmp.write_text(json.dumps(sorted(self._done)))
        tmp.replace(self._state_path)
        self._intercepted_since_start = []


def _datasets(refs: Dict) -> Dict:
    """Keep the identifying fields of Galaxy dataset references."""
    return {
        name: {k: ref.get(k) for k in ("id", "src", "uuid", "label") if ref.get(k) is not None}
        for name, ref in refs.items()
    }


def _galaxy_time(value: str) -> float:
    """Convert a Galaxy ISO timestamp (UTC, no offset) to epoch seconds."""
    return datetime.fromisoformat(value).replace(tzinfo=timezone.utc).timestamp()
