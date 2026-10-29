"""lmdb_dao module.

This module provides the `LMDBDAO` class for interacting with an LMDB-backed database.
"""

from time import time
from typing import List, Dict

import lmdb
import json

from flowcept import WorkflowObject, AgentObject
from flowcept.commons.daos.docdb_dao.docdb_dao_base import DocumentDBDAO
from flowcept.commons.daos.docdb_dao.in_process_aggregations import InProcessAggregationsMixin
from flowcept.commons.flowcept_logger import FlowceptLogger
from flowcept.configs import PERF_LOG, LMDB_SETTINGS
from flowcept.flowceptor.consumers.consumer_utils import curate_dict_task_messages


class LMDBDAO(InProcessAggregationsMixin, DocumentDBDAO):
    """DocumentDBDAO implementation for interacting with LMDB.

    Provides methods for storing and retrieving task and workflow data.
    """

    _shared_handles = {}

    def __init__(self):
        # Avoid reopening LMDB for every DAO instance: lmdb can reject
        # opening the same environment path more than once per process.
        self._initialized = False
        self._path = None
        self._open()
        self._initialized = True
        self.logger = FlowceptLogger()

    def _open(self):
        """Open LMDB environment and databases."""
        path = LMDB_SETTINGS.get("path", "flowcept_lmdb")
        handle = LMDBDAO._shared_handles.get(path)
        if handle is None:
            env = lmdb.open(path, map_size=10**12, max_dbs=5)
            handle = {
                "env": env,
                "tasks_db": env.open_db(b"tasks"),
                "workflows_db": env.open_db(b"workflows"),
                "agents_db": env.open_db(b"agents"),
                "dashboards_db": env.open_db(b"dashboards"),
                "ref_count": 0,
            }
            LMDBDAO._shared_handles[path] = handle

        handle["ref_count"] += 1
        self._path = path
        self._env = handle["env"]
        self._tasks_db = handle["tasks_db"]
        self._workflows_db = handle["workflows_db"]
        self._agents_db = handle["agents_db"]
        self._dashboards_db = handle["dashboards_db"]
        self._initialized = True
        self._is_closed = False

    def insert_and_update_many_tasks(self, docs: List[Dict], indexing_key=None):
        """Insert or update multiple task documents in the LMDB database.

        Parameters
        ----------
        docs : list of dict
            A list of task documents to insert or update.
        indexing_key : str, optional
            Key used for indexing task messages.

        Returns
        -------
        bool
            True if the operation succeeds, False otherwise.
        """
        try:
            t0 = 0
            if PERF_LOG:
                t0 = time()
            indexed_buffer = curate_dict_task_messages(
                docs, indexing_key, t0, convert_times=False, keys_to_drop=["data"]
            )

            with self._env.begin(write=True, db=self._tasks_db) as txn:
                for key, value in indexed_buffer.items():
                    k, v = key.encode(), json.dumps(value).encode()
                    txn.put(k, v)
            return True
        except Exception as e:
            self.logger.exception(e)
            return False

    def insert_one_task(self, task_dict):
        """Insert a single task document.

        Parameters
        ----------
        task_dict : dict
            The task document to insert.

        Returns
        -------
        bool
            True if the operation succeeds, False otherwise.
        """
        try:
            with self._env.begin(write=True, db=self._tasks_db) as txn:
                k, v = task_dict.get("task_id").encode(), json.dumps(task_dict).encode()
                txn.put(k, v)
            return True
        except Exception as e:
            self.logger.exception(e)
            return False

    def insert_or_update_workflow(self, wf_obj: WorkflowObject):
        """Insert or update a workflow document.

        Parameters
        ----------
        wf_obj : WorkflowObject
            Workflow object to insert or update.

        Returns
        -------
        bool
            True if the operation succeeds, False otherwise.
        """
        try:
            _dict = wf_obj.to_dict()
            with self._env.begin(write=True, db=self._workflows_db) as txn:
                key = _dict.get("workflow_id").encode()
                value = json.dumps(_dict).encode()
                txn.put(key, value)
            return True
        except Exception as e:
            self.logger.exception(e)
            return False

    def save_workflow_domain_data_schema(self, workflow_id: str, fields: Dict):
        """Update selected workflow fields without replacing the full document."""
        try:
            with self._env.begin(write=True, db=self._workflows_db) as txn:
                key = workflow_id.encode()
                existing = txn.get(key)
                doc = json.loads(existing.decode()) if existing else {"workflow_id": workflow_id, "type": "workflow"}
                doc.update(fields)
                txn.put(key, json.dumps(doc).encode())
            return True
        except Exception as e:
            self.logger.exception(e)
            return False

    def insert_or_update_agent(self, agent_obj: AgentObject):
        """Insert or update an agent document.

        Parameters
        ----------
        agent_obj : AgentObject
            Agent object to insert or update.

        Returns
        -------
        bool
            True if the operation succeeds, False otherwise.
        """
        try:
            _dict = agent_obj.to_dict()
            with self._env.begin(write=True, db=self._agents_db) as txn:
                key = _dict.get("agent_id").encode()
                value = json.dumps(_dict).encode()
                txn.put(key, value)
            return True
        except Exception as e:
            self.logger.exception(e)
            return False

    def liveness_test(self) -> bool:
        """Return True when LMDB is enabled and the environment is open."""
        from flowcept.configs import LMDB_ENABLED

        if not LMDB_ENABLED:
            self.logger.warning("LMDB liveness check: LMDB_ENABLED is False — store is disabled.")
            return False
        try:
            with self._env.begin():
                pass
            return True
        except Exception as e:
            self.logger.error(f"LMDB liveness check failed: {e}")
            return False

    def delete_task_keys(self, key_name, keys_list: List[str]) -> bool:
        """Delete task documents by a key value list.

        When deleting by task_id, deletes keys directly. Otherwise, scans
        tasks and deletes matching entries.
        """
        if self._is_closed:
            self._open()
        if type(keys_list) is not list:
            keys_list = [keys_list]
        try:
            with self._env.begin(write=True, db=self._tasks_db) as txn:
                if key_name == "task_id":
                    for key in keys_list:
                        if key is None:
                            continue
                        txn.delete(str(key).encode())
                else:
                    cursor = txn.cursor()
                    for key, value in cursor:
                        entry = json.loads(value.decode())
                        if entry.get(key_name) in keys_list:
                            cursor.delete()
            return True
        except Exception as e:
            self.logger.exception(e)
            return False

    def delete_agents_with_filter(self, filter) -> bool:
        """Delete agent documents that match the specified filter."""
        if self._is_closed:
            self._open()
        try:
            with self._env.begin(write=True, db=self._agents_db) as txn:
                cursor = txn.cursor()
                for key, value in cursor:
                    entry = json.loads(value.decode())
                    if LMDBDAO._match_filter(entry, filter):
                        cursor.delete()
            return True
        except Exception as e:
            self.logger.exception(e)
            return False

    def count_tasks(self) -> int:
        """Count number of docs in tasks collection."""
        if self._is_closed:
            self._open()
        try:
            with self._env.begin(db=self._tasks_db) as txn:
                return txn.stat().get("entries", 0)
        except Exception as e:
            self.logger.exception(e)
            return -1

    def count_workflows(self) -> int:
        """Count number of docs in workflows collection."""
        if self._is_closed:
            self._open()
        try:
            with self._env.begin(db=self._workflows_db) as txn:
                return txn.stat().get("entries", 0)
        except Exception as e:
            self.logger.exception(e)
            return -1

    @staticmethod
    def _match_filter(entry, filter):
        """Check if an entry matches a Mongo-style filter dict.

        Supports: ``$and``, ``$or``, ``$eq``, ``$ne``, ``$gt``, ``$gte``,
        ``$lt``, ``$lte``, ``$in``, ``$nin``, and plain equality.
        """
        from flowcept.commons.daos.docdb_dao.docdb_dao_utils import ALLOWED_FILTER_OPERATORS

        _field_ops = {
            "$eq": lambda v, o: v == o,
            "$ne": lambda v, o: v != o,
            "$gt": lambda v, o: v is not None and v > o,
            "$gte": lambda v, o: v is not None and v >= o,
            "$lt": lambda v, o: v is not None and v < o,
            "$lte": lambda v, o: v is not None and v <= o,
            "$in": lambda v, o: v in o,
            "$nin": lambda v, o: v not in o,
        }

        if not filter:
            return True

        for key, value in filter.items():
            if key == "$or":
                if not any(LMDBDAO._match_filter(entry, clause) for clause in value):
                    return False
            elif key == "$and":
                if not all(LMDBDAO._match_filter(entry, clause) for clause in value):
                    return False
            elif key.startswith("$"):
                if key not in ALLOWED_FILTER_OPERATORS:
                    raise ValueError(f"Unsupported filter operator: {key}")
            elif isinstance(value, dict):
                entry_val = entry.get(key)
                for op, op_val in value.items():
                    fn = _field_ops.get(op)
                    if fn is None or not fn(entry_val, op_val):
                        return False
            else:
                if entry.get(key) != value:
                    return False
        return True

    def query(
        self,
        filter=None,
        projection=None,
        limit=None,
        sort=None,
        aggregation=None,
        remove_json_unserializables=None,
        collection="tasks",
    ) -> List[Dict]:
        """Query data from LMDB.

        Parameters
        ----------
        filter : dict, optional
            Filter criteria.
        projection : dict, optional
            Fields to include or exclude.
        limit : int, optional
            Maximum number of results to return.
        sort : list, optional
            Sorting criteria.
        aggregation : list, optional
            Aggregation stages.
        remove_json_unserializables : bool, optional
            Remove JSON-unserializable fields.
        collection : str, optional
            Name of the collection ('tasks' or 'workflows'). Default is 'tasks'.

        Returns
        -------
        list of dict
            A list of queried documents.
        """
        if self._is_closed:
            self._open()

        if collection == "tasks":
            _db = self._tasks_db
        elif collection == "workflows":
            _db = self._workflows_db
        elif collection == "agents":
            _db = self._agents_db
        else:
            self.logger.warning(f"LMDB does not support collection '{collection}'. Returning None.")
            return None

        try:
            data = []
            with self._env.begin(db=_db) as txn:
                cursor = txn.cursor()
                for key, value in cursor:
                    entry = json.loads(value.decode())
                    if LMDBDAO._match_filter(entry, filter):
                        data.append(entry)
            return data
        except Exception as e:
            self.logger.exception(e)
            return None

    def task_query(
        self,
        filter=None,
        projection=None,
        limit=None,
        sort=None,
        aggregation=None,
        remove_json_unserializables=None,
    ):
        """Query tasks collection in the LMDB database.

        Parameters
        ----------
        filter : dict, optional
            Filter criteria for the query.
        projection : dict, optional
            Fields to include or exclude in the results.
        limit : int, optional
            Maximum number of results to return.
        sort : list of tuple, optional
            Sorting criteria. Example: [("field", "asc"), ("field", "desc")].
        aggregation : list, optional
            Aggregation pipeline stages for advanced queries.
        remove_json_unserializables : bool, optional
            Remove JSON-unserializable fields from the results.

        Returns
        -------
        list of dict
            A list of task documents that match the query criteria.
        """
        return self.query(
            collection="tasks",
            filter=filter,
            projection=projection,
            limit=limit,
            sort=sort,
            aggregation=aggregation,
            remove_json_unserializables=remove_json_unserializables,
        )

    def workflow_query(
        self,
        filter=None,
        projection=None,
        limit=None,
        sort=None,
        aggregation=None,
        remove_json_unserializables=None,
    ):
        """Query workflows collection in the LMDB database.

        Parameters
        ----------
        filter : dict, optional
            Filter criteria for the query.
        projection : dict, optional
            Fields to include or exclude in the results.
        limit : int, optional
            Maximum number of results to return.
        sort : list of tuple, optional
            Sorting criteria. Example: [("field", "asc"), ("field", "desc")].
        aggregation : list, optional
            Aggregation pipeline stages for advanced queries.
        remove_json_unserializables : bool, optional
            Remove JSON-unserializable fields from the results.

        Returns
        -------
        list of dict
            A list of workflow documents that match the query criteria.
        """
        return self.query(
            collection="workflows",
            filter=filter,
            projection=projection,
            limit=limit,
            sort=sort,
            aggregation=aggregation,
            remove_json_unserializables=remove_json_unserializables,
        )

    def agent_query(
        self,
        filter=None,
        projection=None,
        limit=None,
        sort=None,
        aggregation=None,
        remove_json_unserializables=None,
    ):
        """Query agents collection in the LMDB database."""
        return self.query(
            collection="agents",
            filter=filter,
            projection=projection,
            limit=limit,
            sort=sort,
            aggregation=aggregation,
            remove_json_unserializables=remove_json_unserializables,
        )

    def save_dashboard(self, dashboard: Dict) -> bool:
        """Insert or replace a dashboard document.

        Parameters
        ----------
        dashboard : dict
            Dashboard document; must contain ``dashboard_id``.

        Returns
        -------
        bool
            True on success.
        """
        if self._is_closed:
            self._open()
        try:
            with self._env.begin(write=True, db=self._dashboards_db) as txn:
                key = dashboard["dashboard_id"].encode()
                txn.put(key, json.dumps(dashboard).encode())
            return True
        except Exception as e:
            self.logger.exception(e)
            return False

    def get_dashboard(self, dashboard_id: str) -> Dict:
        """Get a dashboard document by id.

        Parameters
        ----------
        dashboard_id : str
            Dashboard identifier.

        Returns
        -------
        dict or None
            The dashboard document, or None when not found.
        """
        if self._is_closed:
            self._open()
        try:
            with self._env.begin(db=self._dashboards_db) as txn:
                value = txn.get(dashboard_id.encode())
                return json.loads(value.decode()) if value else None
        except Exception as e:
            self.logger.exception(e)
            return None

    def list_dashboards(self, filter: Dict = None) -> List[Dict]:
        """List dashboard documents, optionally filtered.

        Parameters
        ----------
        filter : dict, optional
            Key/value pairs to match against stored documents (equality only).

        Returns
        -------
        list of dict
            Matching dashboard documents.
        """
        if self._is_closed:
            self._open()
        try:
            results = []
            with self._env.begin(db=self._dashboards_db) as txn:
                cursor = txn.cursor()
                for _, value in cursor:
                    doc = json.loads(value.decode())
                    if filter is None or all(doc.get(k) == v for k, v in filter.items()):
                        results.append(doc)
            return results
        except Exception as e:
            self.logger.exception(e)
            return []

    def delete_dashboard(self, dashboard_id: str) -> bool:
        """Delete a dashboard document by id.

        Parameters
        ----------
        dashboard_id : str
            Dashboard identifier.

        Returns
        -------
        bool
            True when a document was deleted, False otherwise.
        """
        if self._is_closed:
            self._open()
        try:
            with self._env.begin(write=True, db=self._dashboards_db) as txn:
                return txn.delete(dashboard_id.encode())
        except Exception as e:
            self.logger.exception(e)
            return False

    def close(self):
        """Close lmdb."""
        if getattr(self, "_initialized"):
            super().close()
            setattr(self, "_initialized", False)
            path = self._path
            handle = LMDBDAO._shared_handles.get(path)
            if handle is not None:
                handle["ref_count"] -= 1
                if handle["ref_count"] <= 0:
                    handle["env"].close()
                    LMDBDAO._shared_handles.pop(path, None)
            self._is_closed = True

    def object_query(self, filter):
        """Query objects collection."""
        raise NotImplementedError

    def get_tasks_recursive(self, workflow_id, max_depth=999, mapping=None):
        """Get_tasks_recursive in LMDB."""
        raise NotImplementedError

    def dump_tasks_to_file_recursive(self, workflow_id, output_file="tasks.parquet", max_depth=999, mapping=None):
        """Dump_tasks_to_file_recursive in LMDB."""
        raise NotImplementedError

    def save_or_update_object(
        self,
        blob_obj,
        object,
        save_data_in_collection=False,
        pickle_=False,
        control_version=False,
    ):
        """Save object."""
        raise NotImplementedError

    def update_object_metadata(
        self,
        object_id,
        custom_metadata=None,
        tags=None,
        object_type=None,
        task_id=None,
        workflow_id=None,
        control_version=True,
    ):
        """Update object metadata only."""
        raise NotImplementedError

    def get_file_data(self, file_id):
        """Get file data."""
        raise NotImplementedError
