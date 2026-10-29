"""sqlite_dao module.

This module provides the `SQLiteDAO` class, a DocumentDBDAO backed by a single SQLite file.

SQLite is currently a local query/loading backend: documents get into it through
``Flowcept.load_buffer_file`` / ``Flowcept.load_messages`` (offline JSONL buffers), which run
the same message handling as the ``DocumentInserter``. Online persistence (the MQ consumer
writing to SQLite) is rejected by ``validate_config``.
"""

import json
import re
import sqlite3
from contextlib import closing
from pathlib import Path
from time import time
from typing import Dict, List

from flowcept import AgentObject, WorkflowObject
from flowcept.commons.daos.docdb_dao.docdb_dao_base import DocumentDBDAO
from flowcept.commons.daos.docdb_dao.docdb_dao_utils import get_nested, validate_filter
from flowcept.commons.daos.docdb_dao.in_process_aggregations import InProcessAggregationsMixin
from flowcept.commons.flowcept_logger import FlowceptLogger
from flowcept.configs import PERF_LOG, SQLITE_SETTINGS
from flowcept.flowceptor.consumers.consumer_utils import curate_dict_task_messages


class SQLiteDAO(InProcessAggregationsMixin, DocumentDBDAO):
    """DocumentDBDAO implementation backed by SQLite.

    Each collection is a table with the document id as primary key and the full document as
    JSON. Mongo-style filters are translated to SQL over ``json_extract``; the fields most used
    in filters are covered by expression indexes. Summaries and charts are computed in Python
    (``InProcessAggregationsMixin``), as in ``LMDBDAO``.
    """

    # collection -> id field
    _COLLECTIONS = {
        "tasks": "task_id",
        "workflows": "workflow_id",
        "agents": "agent_id",
        "dashboards": "dashboard_id",
    }

    # collection -> fields with an expression index
    _INDEXED_FIELDS = {
        "tasks": ["workflow_id", "campaign_id", "parent_task_id", "activity_id", "agent_id", "status"],
        "workflows": ["campaign_id", "parent_workflow_id"],
        "agents": ["campaign_id", "workflow_id"],
        "dashboards": ["dashboard_type"],
    }

    def __init__(self):
        self._initialized = False
        self.logger = FlowceptLogger()
        self._path = SQLITE_SETTINGS.get("path", "flowcept.sqlite")
        Path(self._path).parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            for collection, id_field in SQLiteDAO._COLLECTIONS.items():
                conn.execute(
                    f"CREATE TABLE IF NOT EXISTS {collection} ({id_field} TEXT PRIMARY KEY, doc TEXT NOT NULL)"
                )
                for field in SQLiteDAO._INDEXED_FIELDS[collection]:
                    conn.execute(
                        f"CREATE INDEX IF NOT EXISTS ix_{collection}_{field} "
                        f"ON {collection} ({SQLiteDAO._json_path_expr(field)})"
                    )
        self._initialized = True

    def _open_connection(self) -> sqlite3.Connection:
        """Open a new connection. Connections are per operation, so the DAO is thread-safe."""
        conn = sqlite3.connect(self._path)
        conn.create_function("REGEXP", 2, SQLiteDAO._regexp, deterministic=True)
        return conn

    def _connect(self):
        """Open a connection for a write transaction (``with self._connect() as conn``)."""
        return _Transaction(self._open_connection())

    @staticmethod
    def _regexp(pattern, value):
        return value is not None and re.search(pattern, str(value)) is not None

    # ------------------------------------------------------------------ #
    # Mongo-style filter -> SQL                                           #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _json_path(field: str) -> str:
        return "$" + "".join('."' + part.replace('"', '""') + '"' for part in field.split("."))

    @staticmethod
    def _json_path_expr(field: str) -> str:
        # Inlined (not bound) so that queries match the expression indexes.
        path = SQLiteDAO._json_path(field).replace("'", "''")
        return f"json_extract(doc, '{path}')"

    @staticmethod
    def _bind(value):
        if isinstance(value, (dict, list)):
            return json.dumps(value, separators=(",", ":"))
        return value

    @staticmethod
    def _eq_sql(field: str, value, params: List) -> str:
        """Equality with Mongo semantics: null matches missing, scalars match array elements."""
        expr = SQLiteDAO._json_path_expr(field)
        if value is None:
            return f"{expr} IS NULL"
        params.append(SQLiteDAO._bind(value))
        if isinstance(value, (dict, list)):
            return f"{expr} = json(?)"
        path = SQLiteDAO._json_path(field).replace("'", "''")
        params.append(SQLiteDAO._bind(value))
        return (
            f"({expr} = ? OR (json_type(doc, '{path}') = 'array' "
            f"AND EXISTS (SELECT 1 FROM json_each(doc, '{path}') WHERE value = ?)))"
        )

    @staticmethod
    def _field_ops_sql(field: str, ops: Dict, params: List) -> str:
        expr = SQLiteDAO._json_path_expr(field)
        clauses = []
        for op, value in ops.items():
            if op == "$eq":
                clauses.append(SQLiteDAO._eq_sql(field, value, params))
            elif op == "$ne":
                clauses.append(f"NOT IFNULL({SQLiteDAO._eq_sql(field, value, params)}, 0)")
            elif op in ("$gt", "$gte", "$lt", "$lte"):
                sql_op = {"$gt": ">", "$gte": ">=", "$lt": "<", "$lte": "<="}[op]
                params.append(SQLiteDAO._bind(value))
                clauses.append(f"{expr} {sql_op} ?")
            elif op in ("$in", "$nin"):
                inner = " OR ".join(SQLiteDAO._eq_sql(field, v, params) for v in value) if value else "0"
                clauses.append(f"({inner})" if op == "$in" else f"NOT IFNULL(({inner}), 0)")
            elif op == "$exists":
                path = SQLiteDAO._json_path(field).replace("'", "''")
                clauses.append(f"json_type(doc, '{path}') IS {'NOT ' if value else ''}NULL")
            elif op == "$regex":
                params.append(value)
                clauses.append(f"{expr} REGEXP ?")
            elif op == "$not":
                clauses.append(f"NOT IFNULL({SQLiteDAO._field_ops_sql(field, value, params)}, 0)")
            else:
                raise ValueError(f"Unsupported filter operator for field '{field}': {op}")
        return "(" + " AND ".join(clauses) + ")" if clauses else "1"

    @staticmethod
    def _filter_sql(filter: Dict, params: List) -> str:
        """Translate a Mongo-style filter (``docdb_dao_utils.ALLOWED_FILTER_OPERATORS``) to SQL."""
        if not filter:
            return "1"
        clauses = []
        for key, value in filter.items():
            if key in ("$and", "$or", "$nor"):
                parts = [SQLiteDAO._filter_sql(clause, params) for clause in value]
                if key == "$and":
                    clauses.append("(" + " AND ".join(parts) + ")" if parts else "1")
                elif key == "$or":
                    clauses.append("(" + " OR ".join(parts) + ")" if parts else "0")
                else:
                    clauses.append("NOT (" + " OR ".join(parts) + ")" if parts else "1")
            elif key.startswith("$"):
                raise ValueError(f"Unsupported top-level filter operator: {key}")
            elif isinstance(value, dict) and value and all(k.startswith("$") for k in value):
                clauses.append(SQLiteDAO._field_ops_sql(key, value, params))
            else:
                clauses.append(SQLiteDAO._eq_sql(key, value, params))
        return " AND ".join(clauses)

    # ------------------------------------------------------------------ #
    # Low-level document access                                          #
    # ------------------------------------------------------------------ #

    def _find(self, collection, filter=None, projection=None, limit=None, sort=None) -> List[Dict]:
        id_field = SQLiteDAO._COLLECTIONS[collection]
        validate_filter(filter)
        params: List = []
        sql = f"SELECT doc FROM {collection} WHERE {SQLiteDAO._filter_sql(filter, params)}"
        if sort:
            order = []
            for field, direction in sort:
                desc = direction in (-1, "desc", "DESC", "descending")
                order.append(f"{SQLiteDAO._json_path_expr(field)} {'DESC' if desc else 'ASC'}")
            sql += " ORDER BY " + ", ".join(order)
        else:
            sql += f" ORDER BY {id_field}"
        if limit:
            sql += f" LIMIT {int(limit)}"
        with closing(self._open_connection()) as conn:
            docs = [json.loads(row[0]) for row in conn.execute(sql, params)]
        if projection:
            docs = [SQLiteDAO._project(doc, projection) for doc in docs]
        return docs

    @staticmethod
    def _project(doc: Dict, projection) -> Dict:
        if isinstance(projection, dict):
            excluded = [f for f, v in projection.items() if not v]
            if excluded and len(excluded) == len(projection):
                out = dict(doc)
                for field in excluded:
                    out.pop(field, None)
                return out
            projection = [f for f, v in projection.items() if v]
        out: Dict = {}
        for field in projection:
            value = get_nested(doc, field)
            if value is None:
                continue
            target = out
            parts = field.split(".")
            for part in parts[:-1]:
                target = target.setdefault(part, {})
            target[parts[-1]] = value
        return out

    @staticmethod
    def _get(conn, collection, doc_id):
        id_field = SQLiteDAO._COLLECTIONS[collection]
        row = conn.execute(f"SELECT doc FROM {collection} WHERE {id_field} = ?", (doc_id,)).fetchone()
        return json.loads(row[0]) if row else None

    @staticmethod
    def _put(conn, collection, doc_id, doc):
        id_field = SQLiteDAO._COLLECTIONS[collection]
        conn.execute(
            f"INSERT OR REPLACE INTO {collection} ({id_field}, doc) VALUES (?, ?)",
            (doc_id, json.dumps(doc, default=str)),
        )

    # ------------------------------------------------------------------ #
    # Writes                                                             #
    # ------------------------------------------------------------------ #

    def insert_and_update_many_tasks(self, docs: List[Dict], indexing_key=None):
        """Insert or update task documents, merging with the stored ones like the MongoDB DAO.

        Parameters
        ----------
        docs : list of dict
            Task documents to insert or update.
        indexing_key : str, optional
            Key used for indexing task messages (task_id).

        Returns
        -------
        bool
            True if the operation succeeds, False otherwise.
        """
        try:
            if len(docs) == 0:
                return False
            t0 = time() if PERF_LOG else 0
            indexed_buffer = curate_dict_task_messages(
                docs, indexing_key, t0, convert_times=False, keys_to_drop=["data"]
            )
            with self._connect() as conn:
                for key, doc in indexed_buffer.items():
                    stored = SQLiteDAO._get(conn, "tasks", key)
                    if stored is not None:
                        # Same field-merging rules used to merge messages of one task in a buffer.
                        doc = curate_dict_task_messages([stored, doc], indexing_key, convert_times=False)[key]
                    SQLiteDAO._put(conn, "tasks", key, doc)
            return True
        except Exception as e:
            self.logger.exception(e)
            return False

    def insert_one_task(self, task_dict):
        """Insert (or replace) a single task document.

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
            with self._connect() as conn:
                SQLiteDAO._put(conn, "tasks", task_dict["task_id"], task_dict)
            return True
        except Exception as e:
            self.logger.exception(e)
            return False

    def insert_or_update_workflow(self, wf_obj: WorkflowObject):
        """Insert or update a workflow document with the MongoDB DAO semantics.

        Top-level fields are set (``$set``), ``interceptor_ids`` are appended (``$push``) and
        ``machine_info`` entries are merged.

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
            new = wf_obj.to_dict()
            workflow_id = new["workflow_id"]
            with self._connect() as conn:
                doc = SQLiteDAO._get(conn, "workflows", workflow_id) or {}
                interceptor_ids = new.pop("interceptor_ids", None)
                machine_info = new.pop("machine_info", None)
                doc.update(new)
                if interceptor_ids:
                    doc["interceptor_ids"] = doc.get("interceptor_ids", []) + list(interceptor_ids)
                if machine_info is not None:
                    doc["machine_info"] = {**doc.get("machine_info", {}), **machine_info}
                SQLiteDAO._put(conn, "workflows", workflow_id, doc)
            return True
        except Exception as e:
            self.logger.exception(e)
            return False

    def save_workflow_domain_data_schema(self, workflow_id: str, fields: Dict):
        """Update selected workflow fields without replacing the full document."""
        try:
            with self._connect() as conn:
                doc = SQLiteDAO._get(conn, "workflows", workflow_id) or {"workflow_id": workflow_id, "type": "workflow"}
                doc.update(fields)
                SQLiteDAO._put(conn, "workflows", workflow_id, doc)
            return True
        except Exception as e:
            self.logger.exception(e)
            return False

    def insert_or_update_agent(self, agent_obj: AgentObject):
        """Insert or update an agent document (top-level ``$set`` semantics).

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
            new = agent_obj.to_dict()
            with self._connect() as conn:
                doc = SQLiteDAO._get(conn, "agents", new["agent_id"]) or {}
                doc.update(new)
                SQLiteDAO._put(conn, "agents", new["agent_id"], doc)
            return True
        except Exception as e:
            self.logger.exception(e)
            return False

    def delete_agents_with_filter(self, filter) -> bool:
        """Delete agent documents that match the filter (not supported yet)."""
        # TODO: implement deletes (agents, tasks, workflows) for SQLite.
        raise NotImplementedError("Deletes are not supported by SQLiteDAO yet.")

    def liveness_test(self) -> bool:
        """Return True when SQLite is enabled and the database file can be queried."""
        from flowcept.configs import SQLITE_ENABLED

        if not SQLITE_ENABLED:
            self.logger.warning("SQLite liveness check: SQLITE_ENABLED is False — store is disabled.")
            return False
        try:
            with closing(self._open_connection()) as conn:
                conn.execute("SELECT 1").fetchone()
            return True
        except Exception as e:
            self.logger.error(f"SQLite liveness check failed: {e}")
            return False

    # ------------------------------------------------------------------ #
    # Queries                                                            #
    # ------------------------------------------------------------------ #

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
        """Query a collection with a Mongo-style filter.

        Parameters
        ----------
        filter : dict, optional
            Mongo-style filter (operators in ``docdb_dao_utils.ALLOWED_FILTER_OPERATORS``).
        projection : list or dict, optional
            Fields (dot notation allowed) to include, or a dict of fields to include/exclude.
        limit : int, optional
            Maximum number of results to return (0 or None means no limit).
        sort : list of tuple, optional
            ``[(field, order), ...]`` where order is 1/-1 or "asc"/"desc".
        aggregation : list, optional
            Not supported by SQLiteDAO.
        remove_json_unserializables : bool, optional
            Ignored: documents are stored as JSON.
        collection : str, optional
            'tasks', 'workflows' or 'agents' ('objects' is always empty for now). Default is 'tasks'.

        Returns
        -------
        list of dict
            The matching documents.
        """
        if aggregation is not None:
            # TODO: support aggregation for SQLite (only needed by the MongoDB pipeline API).
            raise NotImplementedError("Aggregations are not supported by SQLiteDAO yet.")
        if collection in ("objects", "object_history"):
            # TODO: support objects/blobs for SQLite. Until then no object can be stored, so reads are empty.
            return []
        if collection not in ("tasks", "workflows", "agents"):
            raise ValueError(f"SQLite stores tasks, workflows and agents; got collection '{collection}'.")
        return self._find(collection, filter=filter, projection=projection, limit=limit, sort=sort)

    def task_query(
        self,
        filter=None,
        projection=None,
        limit=None,
        sort=None,
        aggregation=None,
        remove_json_unserializables=None,
    ):
        """Query the tasks collection (see ``query``)."""
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
        """Query the workflows collection (see ``query``)."""
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
        """Query the agents collection (see ``query``)."""
        return self.query(
            collection="agents",
            filter=filter,
            projection=projection,
            limit=limit,
            sort=sort,
            aggregation=aggregation,
            remove_json_unserializables=remove_json_unserializables,
        )

    # ------------------------------------------------------------------ #
    # Dashboards (webservice)                                            #
    # ------------------------------------------------------------------ #

    def save_dashboard(self, dashboard: Dict) -> bool:
        """Insert or replace a dashboard document (must contain ``dashboard_id``)."""
        try:
            with self._connect() as conn:
                SQLiteDAO._put(conn, "dashboards", dashboard["dashboard_id"], dashboard)
            return True
        except Exception as e:
            self.logger.exception(e)
            return False

    def get_dashboard(self, dashboard_id: str) -> Dict:
        """Get a dashboard document by id, or None when not found."""
        with closing(self._open_connection()) as conn:
            return SQLiteDAO._get(conn, "dashboards", dashboard_id)

    def list_dashboards(self, filter: Dict = None) -> List[Dict]:
        """List dashboard documents, optionally filtered (Mongo-style filter)."""
        return self._find("dashboards", filter=filter)

    def delete_dashboard(self, dashboard_id: str) -> bool:
        """Delete a dashboard document by id; True when a document was deleted."""
        with self._connect() as conn:
            return conn.execute("DELETE FROM dashboards WHERE dashboard_id = ?", (dashboard_id,)).rowcount > 0

    # ------------------------------------------------------------------ #
    # Not supported yet                                                   #
    # ------------------------------------------------------------------ #

    def object_query(self, filter):
        """Query objects collection: always empty, objects cannot be stored in SQLite yet."""
        # TODO: support objects/blobs for SQLite.
        return []

    def get_tasks_recursive(self, workflow_id, max_depth=999, mapping=None):
        """Get tasks recursively (not supported yet)."""
        # TODO: implement for SQLite (MongoDBDAO._get_children_tasks_iterative is Mongo-specific).
        raise NotImplementedError("get_tasks_recursive is not supported by SQLiteDAO yet.")

    def dump_tasks_to_file_recursive(self, workflow_id, output_file="tasks.parquet", max_depth=999, mapping=None):
        """Dump tasks recursively (not supported yet)."""
        # TODO: implement for SQLite together with get_tasks_recursive.
        raise NotImplementedError("dump_tasks_to_file_recursive is not supported by SQLiteDAO yet.")

    def save_or_update_object(
        self,
        blob_obj,
        object,
        save_data_in_collection=False,
        pickle_=False,
        control_version=False,
    ):
        """Save object (not supported yet)."""
        # TODO: support objects/blobs for SQLite.
        raise NotImplementedError("Objects are not supported by SQLiteDAO yet.")

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
        """Update object metadata (not supported yet)."""
        # TODO: support objects/blobs for SQLite.
        raise NotImplementedError("Objects are not supported by SQLiteDAO yet.")

    def get_file_data(self, file_id):
        """Get file data (not supported yet)."""
        # TODO: support objects/blobs for SQLite.
        raise NotImplementedError("Objects are not supported by SQLiteDAO yet.")

    def close(self):
        """Close the DAO (connections are opened per operation)."""
        if getattr(self, "_initialized"):
            super().close()
            setattr(self, "_initialized", False)


class _Transaction:
    """Context manager: one connection, one transaction (BEGIN IMMEDIATE), always closed."""

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    def __enter__(self) -> sqlite3.Connection:
        self._conn.isolation_level = None
        self._conn.execute("BEGIN IMMEDIATE")
        return self._conn

    def __exit__(self, exc_type, exc, tb):
        try:
            self._conn.execute("ROLLBACK" if exc_type else "COMMIT")
        finally:
            self._conn.close()
