import json
import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from flowcept import Flowcept, configs
from flowcept.flowceptor.adapters.galaxy.galaxy_interceptor import GalaxyInterceptor

HOST = "galaxy.test"

INVOCATION = {
    "id": "inv1",
    "workflow_id": "wfv1",
    "history_id": "h1",
    "state": "scheduled",
    "create_time": "2026-10-18T12:00:00.000000",
    "update_time": "2026-10-18T12:05:00.000000",
    "inputs": {"0": {"id": "d0", "src": "hda", "uuid": "u0", "label": "horizons"}},
    "outputs": {"report": {"id": "d2", "src": "hda", "uuid": "u2"}},
    "steps": [
        {"order_index": 0, "workflow_step_label": "horizons", "job_id": None},
        {"order_index": 1, "workflow_step_label": "download", "job_id": "j1"},
        {"order_index": 2, "workflow_step_label": "aggregate", "job_id": "j2"},
    ],
}

JOBS = {
    "j1": {
        "id": "j1",
        "tool_id": "seismic_download",
        "tool_version": "1.0",
        "state": "ok",
        "exit_code": 0,
        "create_time": "2026-10-18T12:00:01.000000",
        "update_time": "2026-10-18T12:01:00.000000",
        "command_line": "python seismic_download.py",
        "params": {"work_product_id": "osdu:wp:1"},
        "inputs": {"input": {"id": "d0", "src": "hda", "uuid": "u0"}},
        "outputs": {"out": {"id": "d1", "src": "hda", "uuid": "u1"}},
        "tool_stdout": "downloaded",
        "tool_stderr": "",
        "history_id": "h1",
    },
    "j2": {
        "id": "j2",
        "tool_id": "seismic_aggregate",
        "tool_version": "1.0",
        "state": "error",
        "exit_code": 1,
        "create_time": "2026-10-18T12:01:01.000000",
        "update_time": "2026-10-18T12:02:00.000000",
        "command_line": "python seismic_aggregate.py",
        "params": {},
        "inputs": {"input": {"id": "d1", "src": "hda", "uuid": "u1"}},
        "outputs": {"report": {"id": "d2", "src": "hda", "uuid": "u2"}},
        "history_id": "h1",
    },
}


class _Response:
    def __init__(self, body):
        self._body = body

    def json(self):
        return self._body


class FakeGalaxy:
    """Minimal stand-in for bioblend's GalaxyInstance."""

    url = f"https://{HOST}/api"

    def __init__(self, invocation_state="scheduled", job_states=None):
        self.invocation = dict(INVOCATION, state=invocation_state)
        self.jobs_by_id = {k: dict(v) for k, v in JOBS.items()}
        for job_id, state in (job_states or {}).items():
            self.jobs_by_id[job_id]["state"] = state
        self.invocations = self
        self.jobs = self
        self.workflows = self

    def make_get_request(self, url, params=None):
        assert url == f"{self.url}/invocations"
        return _Response([{"id": self.invocation["id"]}])

    def show_invocation(self, invocation_id):
        return self.invocation

    def get_jobs(self, invocation_id=None):
        return [{"id": j["id"], "state": j["state"]} for j in self.jobs_by_id.values()]

    def show_job(self, job_id, full_details=False):
        return self.jobs_by_id[job_id]

    def get_metrics(self, job_id):
        if job_id == "j1":
            return [
                {"name": "start_epoch", "raw_value": "1792411210"},
                {"name": "end_epoch", "raw_value": "1792411260"},
            ]
        return []

    def show_workflow(self, workflow_id, instance=None):
        assert instance is True
        return {"id": "sw1", "name": "seismic_pipeline"}


class TestGalaxyInterceptor(unittest.TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.state_path = self.dir / "state.json"
        self.buffer_path = self.dir / configs.DUMP_BUFFER_PATH
        self._cwd = os.getcwd()
        os.chdir(self.dir)
        configs.settings["adapters"]["galaxy"] = {
            "kind": "galaxy",
            "url": f"https://{HOST}",
            "api_key": "fake",
            "watch_interval_sec": 3600,
            "state_path": str(self.state_path),
        }

    def tearDown(self):
        configs.settings["adapters"].pop("galaxy", None)
        os.chdir(self._cwd)
        self.tmp.cleanup()

    def _interceptor(self, fake, context_resolver=None):
        interceptor = GalaxyInterceptor(context_resolver=context_resolver)
        interceptor.gi = fake
        return interceptor

    def _run_cycle(self, interceptor):
        """One offline cycle: start polling, stop, and let stop() dump the buffer."""
        interceptor.start(bundle_exec_id="test", check_safe_stops=False)
        interceptor.stop(check_safe_stops=False)

    def _records(self):
        if not self.buffer_path.exists():
            return []
        return Flowcept.read_buffer_file(str(self.buffer_path))

    def test_finished_invocation_becomes_workflow_and_tasks(self):
        def resolver(invocation, stored_workflow):
            assert invocation["id"] == "inv1" and stored_workflow["id"] == "sw1"
            return {"campaign_id": "sess-1", "parent_workflow_id": "chat:sess-1"}

        self._run_cycle(self._interceptor(FakeGalaxy(), resolver))
        records = self._records()

        workflows = [r for r in records if r["type"] == "workflow"]
        tasks = {r["task_id"]: r for r in records if r["type"] == "task"}
        assert len(workflows) == 1
        wf = workflows[0]
        assert wf["workflow_id"] == f"galaxy:{HOST}:invocation:inv1"
        assert wf["campaign_id"] == "sess-1"
        assert wf["parent_workflow_id"] == "chat:sess-1"
        assert wf["name"] == "seismic_pipeline"
        assert wf["used"]["stored_workflow_id"] == "sw1"
        assert wf["used"]["inputs"]["0"]["uuid"] == "u0"
        assert wf["generated"]["outputs"]["report"]["uuid"] == "u2"

        download = tasks[f"galaxy:{HOST}:job:j1"]
        aggregate = tasks[f"galaxy:{HOST}:job:j2"]
        assert download["workflow_id"] == wf["workflow_id"]
        assert download["campaign_id"] == "sess-1"
        assert download["activity_id"] == "seismic_download"
        assert download["status"] == "FINISHED"
        assert download["started_at"] == 1792411210.0
        assert download["ended_at"] == 1792411260.0
        assert download["used"]["params"] == {"work_product_id": "osdu:wp:1"}
        assert download["generated"]["outputs"]["out"]["uuid"] == "u1"
        assert download["custom_metadata"]["step"] == {"order_index": 1, "label": "download"}
        assert download["dependencies"] == []
        assert aggregate["status"] == "ERROR"
        assert aggregate["dependencies"] == [download["task_id"]]
        assert "started_at" not in aggregate

        assert json.loads(self.state_path.read_text()) == ["inv1"]

    def test_unfinished_invocation_is_skipped_until_its_jobs_finish(self):
        self._run_cycle(self._interceptor(FakeGalaxy(job_states={"j2": "running"})))
        assert self._records() == []
        assert not self.state_path.exists()

        self._run_cycle(self._interceptor(FakeGalaxy()))
        assert len([r for r in self._records() if r["type"] == "task"]) == 2

    def test_intercepted_invocation_is_not_repeated(self):
        self._run_cycle(self._interceptor(FakeGalaxy()))
        self._run_cycle(self._interceptor(FakeGalaxy()))
        assert len([r for r in self._records() if r["type"] == "workflow"]) == 1

    def test_without_resolver_records_have_no_campaign(self):
        self._run_cycle(self._interceptor(FakeGalaxy(invocation_state="failed")))
        wf = [r for r in self._records() if r["type"] == "workflow"][0]
        assert "campaign_id" not in wf
