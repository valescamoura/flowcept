"""SQLiteDAO tests: loading offline buffers and querying them with Mongo-style filters.

Run with a settings file where ``databases.sqlite.enabled: true`` and ``db_flush_mode: offline``.
"""

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

from flowcept import Flowcept, WorkflowObject, flowcept_task
from flowcept.commons.daos.docdb_dao.docdb_dao_base import DocumentDBDAO
from flowcept.commons.daos.docdb_dao.sqlite_dao import SQLiteDAO
from flowcept.configs import SQLITE_ENABLED


@flowcept_task(output_names="y")
def _add_one(x):
    return x + 1


@flowcept_task(output_names="z")
def _double(y):
    return y * 2


@unittest.skipIf(not SQLITE_ENABLED, "SQLite is disabled")
class SQLiteDAOTest(unittest.TestCase):
    def tearDown(self):
        instance = DocumentDBDAO._instance
        if instance is not None:
            instance.close()

    def test_get_instance_is_sqlite(self):
        assert isinstance(DocumentDBDAO.get_instance(), SQLiteDAO)
        assert Flowcept.db.liveness_test()

    def test_load_buffer_file_from_offline_capture(self):
        with TemporaryDirectory() as tmp:
            campaign_id = str(uuid4())
            with Flowcept(campaign_id=campaign_id, workflow_name="sqlite_load_test", start_persistence=False) as f:
                _double(_add_one(3))
                workflow_id = f.current_workflow_id
                f.dump_buffer(str(Path(tmp) / "buffer.jsonl"))
            # dump_buffer may append ids to the file name (project.dump_buffer settings).
            buffer_path = str(next(Path(tmp).glob("buffer*.jsonl")))
            loaded = Flowcept.load_buffer_file(buffer_path)
            assert loaded >= 2

            tasks = Flowcept.db.task_query(filter={"workflow_id": workflow_id})
            assert {t["activity_id"] for t in tasks} == {"_add_one", "_double"}
            assert all(t["campaign_id"] == campaign_id for t in tasks)
            double = next(t for t in tasks if t["activity_id"] == "_double")
            assert double["generated"]["z"] == 8

            # Loading the same buffer again updates instead of duplicating.
            Flowcept.load_buffer_file(buffer_path)
            assert len(Flowcept.db.task_query(filter={"workflow_id": workflow_id})) == 2

    def test_filter_operators(self):
        wf = str(uuid4())
        docs = [
            {
                "type": "task",
                "task_id": f"{wf}-a",
                "workflow_id": wf,
                "activity_id": "download",
                "status": "FINISHED",
                "started_at": 1.0,
                "ended_at": 3.0,
                "used": {"input": "seismic"},
                "dependencies": [],
            },
            {
                "type": "task",
                "task_id": f"{wf}-b",
                "workflow_id": wf,
                "activity_id": "visualize",
                "status": "ERROR",
                "started_at": 4.0,
                "ended_at": 9.0,
                "used": {"input": "horizon"},
                "dependencies": [f"{wf}-a"],
            },
            {
                "type": "task",
                "task_id": f"{wf}-c",
                "workflow_id": wf,
                "activity_id": "aggregate",
                "started_at": 10.0,
                "dependencies": [f"{wf}-a", f"{wf}-b"],
            },
        ]
        Flowcept.load_messages(docs)

        def ids(filter, **kwargs):
            return [t["task_id"][-1] for t in Flowcept.db.task_query(filter={"workflow_id": wf, **filter}, **kwargs)]

        assert ids({"status": "ERROR"}) == ["b"]
        assert ids({"status": {"$ne": "ERROR"}}) == ["a", "c"]  # $ne also matches missing fields
        assert ids({"status": {"$exists": False}}) == ["c"]
        assert ids({"status": {"$in": ["FINISHED", "ERROR"]}}) == ["a", "b"]
        assert ids({"status": {"$nin": ["FINISHED"]}}) == ["b", "c"]
        assert ids({"started_at": {"$gte": 4.0, "$lt": 10.0}}) == ["b"]
        assert ids({"used.input": "horizon"}) == ["b"]  # dot notation
        assert ids({"used.input": {"$regex": "^seis"}}) == ["a"]
        assert ids({"dependencies": f"{wf}-a"}) == ["b", "c"]  # scalar matches array elements
        assert ids({"$or": [{"activity_id": "download"}, {"activity_id": "aggregate"}]}) == ["a", "c"]
        assert ids({"$nor": [{"status": "ERROR"}, {"status": {"$exists": False}}]}) == ["a"]
        assert ids({"started_at": {"$not": {"$gt": 2.0}}}) == ["a"]
        assert ids({}, sort=[("started_at", -1)], limit=2) == ["c", "b"]

        projected = Flowcept.db.task_query(filter={"task_id": f"{wf}-a"}, projection=["activity_id", "used.input"])
        assert projected == [{"activity_id": "download", "used": {"input": "seismic"}}]

        with self.assertRaises(ValueError):
            Flowcept.db.task_query(filter={"status": {"$where": "1"}})

    def test_task_messages_are_merged(self):
        task_id = str(uuid4())
        Flowcept.load_messages([{"type": "task", "task_id": task_id, "status": "RUNNING", "used": {"a": 1}}])
        Flowcept.load_messages(
            [{"type": "task", "task_id": task_id, "status": "FINISHED", "used": {"b": 2}, "generated": {"c": 3}}]
        )
        task = Flowcept.db.task_query(filter={"task_id": task_id})[0]
        assert task["status"] == "FINISHED"
        assert task["used"] == {"a": 1, "b": 2}
        assert task["generated"] == {"c": 3}

    def test_workflow_records_are_merged(self):
        # e.g., a workflow registered when an execution starts and completed by an adapter later.
        workflow_id = str(uuid4())
        Flowcept.load_messages(
            [
                {
                    "type": "workflow",
                    "workflow_id": workflow_id,
                    "campaign_id": "c1",
                    "started_at": 1.0,
                    "interceptor_ids": ["i1"],
                }
            ]
        )
        Flowcept.load_messages(
            [
                {
                    "type": "workflow",
                    "workflow_id": workflow_id,
                    "name": "galaxy run",
                    "status": "FINISHED",
                    "interceptor_ids": ["i2"],
                }
            ]
        )
        wf = Flowcept.db.get_workflow_object(workflow_id)
        assert (wf.campaign_id, wf.started_at, wf.name) == ("c1", 1.0, "galaxy run")
        assert wf.interceptor_ids == ["i1", "i2"]

        wf_obj = WorkflowObject(workflow_id=workflow_id)
        wf_obj.custom_metadata = {"k": "v"}
        assert Flowcept.db.insert_or_update_workflow(wf_obj)
        assert Flowcept.db.get_workflow_object(workflow_id).name == "galaxy run"

    def test_agents_and_derived_summaries(self):
        campaign_id, agent_id = str(uuid4()), str(uuid4())
        Flowcept.load_messages(
            [
                {"type": "agent", "agent_id": agent_id, "name": "OmniAgent", "campaign_id": campaign_id},
                {"type": "workflow", "workflow_id": f"chat:{campaign_id}", "campaign_id": campaign_id, "name": "chat"},
                {
                    "type": "task",
                    "task_id": str(uuid4()),
                    "workflow_id": f"chat:{campaign_id}",
                    "campaign_id": campaign_id,
                    "agent_id": agent_id,
                    "activity_id": "OmniAgent.run",
                    "status": "FINISHED",
                    "started_at": 1.0,
                    "ended_at": 2.0,
                },
            ]
        )
        assert Flowcept.db.get_agent_object(agent_id).name == "OmniAgent"
        agents = [a for a in Flowcept.db.derive_agents() if a["agent_id"] == agent_id]
        assert agents and agents[0]["task_count"] == 1 and agents[0]["campaign_ids"] == [campaign_id]
        campaigns = [c for c in Flowcept.db.derive_campaigns() if c["campaign_id"] == campaign_id]
        assert campaigns and campaigns[0]["workflow_count"] == 1 and campaigns[0]["task_count"] == 1
        summary = Flowcept.db.task_summary({"campaign_id": campaign_id})
        assert summary["count"] == 1 and summary["status_counts"] == {"FINISHED": 1}

    def test_unsupported_operations_raise(self):
        assert Flowcept.db.query(filter={}, collection="objects") == []
        with self.assertRaises(NotImplementedError):
            Flowcept.db.task_query(filter={}, aggregation=[("max", "ended_at")])

    def test_online_mode_is_rejected(self):
        import flowcept.configs as configs

        original = configs.DB_FLUSH_MODE
        configs.DB_FLUSH_MODE = "online"
        try:
            with self.assertRaises(ValueError):
                configs.validate_config()
        finally:
            configs.DB_FLUSH_MODE = original

    def test_buffer_lines_roundtrip_as_json(self):
        # Documents are stored as JSON: what is loaded is what comes back.
        task_id = str(uuid4())
        message = {"type": "task", "task_id": task_id, "custom_metadata": {"nested": {"list": [1, "x", None]}}}
        Flowcept.load_messages([json.loads(json.dumps(message))])
        task = Flowcept.db.task_query(filter={"task_id": task_id})[0]
        assert task["custom_metadata"] == {"nested": {"list": [1, "x", None]}}

    def test_webservice_over_sqlite(self):
        """The REST API (used by the UI) serves data loaded into SQLite."""
        from fastapi.testclient import TestClient

        from flowcept.webservice.main import create_app

        campaign_id, agent_id = str(uuid4()), str(uuid4())
        chat_wf, run_wf = f"chat:{campaign_id}", f"run:{campaign_id}"
        Flowcept.load_messages(
            [
                {"type": "agent", "agent_id": agent_id, "name": "OmniAgent", "campaign_id": campaign_id},
                {"type": "workflow", "workflow_id": chat_wf, "campaign_id": campaign_id, "name": "chat"},
                {
                    "type": "workflow",
                    "workflow_id": run_wf,
                    "campaign_id": campaign_id,
                    "name": "run",
                    "parent_workflow_id": chat_wf,
                },
                {
                    "type": "task",
                    "task_id": f"{campaign_id}-turn",
                    "workflow_id": chat_wf,
                    "campaign_id": campaign_id,
                    "agent_id": agent_id,
                    "activity_id": "OmniAgent.run",
                    "status": "FINISHED",
                    "started_at": 1.0,
                    "ended_at": 2.0,
                },
                {
                    "type": "task",
                    "task_id": f"{campaign_id}-job1",
                    "workflow_id": run_wf,
                    "campaign_id": campaign_id,
                    "activity_id": "download",
                    "status": "FINISHED",
                    "started_at": 3.0,
                    "ended_at": 4.0,
                    "generated": {"outputs": {"out": {"uuid": "d1"}}},
                },
                {
                    "type": "task",
                    "task_id": f"{campaign_id}-job2",
                    "workflow_id": run_wf,
                    "campaign_id": campaign_id,
                    "activity_id": "visualize",
                    "status": "ERROR",
                    "started_at": 5.0,
                    "ended_at": 6.0,
                    "used": {"inputs": {"in": {"uuid": "d1"}}},
                    "dependencies": [f"{campaign_id}-job1"],
                },
            ]
        )
        client = TestClient(create_app())

        campaign = client.get(f"/api/v1/campaigns/{campaign_id}")
        assert campaign.status_code == 200, campaign.text
        workflows = client.get("/api/v1/workflows", params={"campaign_id": campaign_id}).json()
        assert {w["workflow_id"] for w in workflows["items"]} == {chat_wf, run_wf}
        tasks = client.get(f"/api/v1/tasks/by_workflow/{run_wf}").json()
        assert {t["activity_id"] for t in tasks["items"]} == {"download", "visualize"}
        assert client.get(f"/api/v1/agents/{agent_id}").status_code == 200
        assert client.get(f"/api/v1/workflows/{run_wf}/dataflow").status_code == 200
        assert client.get(f"/api/v1/workflows/{run_wf}/workflow_card").status_code == 200
        assert client.post("/api/v1/objects/query", json={"filter": {}}).json()["items"] == []
        # Deletes are not supported yet: DBAPI reports nothing deleted (same as LMDB).
        assert client.delete(f"/api/v1/workflows/{run_wf}").json() == {"deleted": {}}
        assert client.get(f"/api/v1/workflows/{run_wf}").status_code == 200
