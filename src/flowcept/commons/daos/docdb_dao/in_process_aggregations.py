"""In-process aggregations shared by DocumentDBDAO implementations without a query engine.

The methods in ``InProcessAggregationsMixin`` only depend on the DAO query methods
(``query``, ``task_query``, ``workflow_query`` and ``agent_query``) and aggregate the
returned documents in Python. They were originally written for ``LMDBDAO`` and are
shared with ``SQLiteDAO``.
"""

from typing import Dict, List

import pandas as pd


class InProcessAggregationsMixin:
    """Summaries, charts and exports computed in Python over the DAO query results."""

    def to_df(self, collection="tasks", filter=None) -> pd.DataFrame:
        """Fetch data via ``query`` and return a DataFrame with optional MongoDB-style filtering.

        Args:
            collection (str, optional): Collection name. Should be tasks, workflows or agents
            filter (dict, optional): A dictionary representing the filter criteria.
                 Example: {"workflow_id": "123", "status": "completed"}

        Returns
        -------
         pd.DataFrame: A DataFrame containing the filtered data.
        """
        docs = self.query(collection=collection, filter=filter)
        return pd.DataFrame(docs)

    def task_summary(self, filter: Dict) -> Dict:
        """Summarize tasks via in-process aggregation (in-process path).

        Returns status counts, per-activity stats, and time range for tasks matching filter.
        """
        from flowcept.commons.daos.docdb_dao.docdb_dao_utils import _merge_summary_rows, _duration

        docs = (
            self.task_query(
                filter=filter,
                projection=["activity_id", "status", "started_at", "ended_at"],
            )
            or []
        )
        groups: Dict = {}
        for doc in docs:
            key = (doc.get("activity_id"), doc.get("status"))
            group = groups.setdefault(
                key,
                {
                    "activity_id": key[0],
                    "status": key[1],
                    "count": 0,
                    "durations": [],
                    "min_started_at": None,
                    "max_ended_at": None,
                },
            )
            group["count"] += 1
            dur = _duration(doc)
            if dur is not None:
                group["durations"].append(dur)
            started, ended = doc.get("started_at"), doc.get("ended_at")
            if isinstance(started, (int, float)):
                current = group["min_started_at"]
                group["min_started_at"] = started if current is None else min(current, started)
            if isinstance(ended, (int, float)):
                current = group["max_ended_at"]
                group["max_ended_at"] = ended if current is None else max(current, ended)
        rows = []
        for group in groups.values():
            durations = group.pop("durations")
            group["avg_duration"] = sum(durations) / len(durations) if durations else None
            group["min_duration"] = min(durations) if durations else None
            group["max_duration"] = max(durations) if durations else None
            group["sum_duration"] = sum(durations) if durations else None
            rows.append(group)
        return _merge_summary_rows(rows)

    def derive_campaigns(self, campaign_id: str = None) -> List[Dict]:
        """Derive campaign summaries via in-process aggregation (in-process path)."""
        from flowcept.commons.daos.docdb_dao.docdb_dao_utils import to_epoch

        campaigns: Dict = {}

        def _campaign(cid):
            return campaigns.setdefault(
                cid,
                {
                    "campaign_id": cid,
                    "workflow_count": 0,
                    "task_count": 0,
                    "users": set(),
                    "workflow_names": set(),
                    "first_ts": None,
                    "last_ts": None,
                },
            )

        def _expand(record, *values):
            for raw in values:
                val = to_epoch(raw)
                if val is None:
                    continue
                record["first_ts"] = val if record["first_ts"] is None else min(record["first_ts"], val)
                record["last_ts"] = val if record["last_ts"] is None else max(record["last_ts"], val)

        wf_filter = {"campaign_id": {"$exists": True, "$ne": None}}
        if campaign_id is not None:
            wf_filter["campaign_id"] = campaign_id
        for doc in self.workflow_query(filter=wf_filter) or []:
            if not doc.get("campaign_id"):
                continue
            record = _campaign(doc["campaign_id"])
            record["workflow_count"] += 1
            if doc.get("user"):
                record["users"].add(doc["user"])
            if doc.get("name"):
                record["workflow_names"].add(doc["name"])
            _expand(record, doc.get("utc_timestamp"))

        for doc in self.task_query(filter=wf_filter, projection=["campaign_id", "started_at", "ended_at"]) or []:
            if not doc.get("campaign_id"):
                continue
            record = _campaign(doc["campaign_id"])
            record["task_count"] += 1
            _expand(record, doc.get("started_at"), doc.get("ended_at"))

        results = []
        for record in campaigns.values():
            record["users"] = sorted(record["users"])
            record["workflow_names"] = sorted(record["workflow_names"])
            results.append(record)
        results.sort(
            key=lambda r: (1, r["last_ts"]) if r["last_ts"] is not None else (0, float("-inf")),
            reverse=True,
        )
        return results

    def derive_agents(self, filter: Dict = None) -> List[Dict]:
        """Derive agent summaries via in-process aggregation (in-process path)."""

        def _ts(val):
            if val is None:
                return None
            if isinstance(val, (int, float)):
                return float(val)
            from datetime import datetime as _dt

            if isinstance(val, _dt):
                return val.timestamp()
            if isinstance(val, str):
                try:
                    return _dt.fromisoformat(val.replace("Z", "+00:00")).timestamp()
                except Exception:
                    return None
            return None

        try:
            stored = self.agent_query(filter=filter or {}) or []
        except Exception as e:
            self.logger.error(f"Error querying stored agents: {e}")
            stored = []
        stored = [a for a in stored if a.get("agent_id") not in ("train_agent_id", "orchestrator_agent_id")]
        if not stored:
            return []

        agent_ids = [a["agent_id"] for a in stored if "agent_id" in a]
        docs = (
            self.task_query(
                filter={"agent_id": {"$in": agent_ids}},
                projection=[
                    "agent_id",
                    "activity_id",
                    "source_agent_id",
                    "campaign_id",
                    "workflow_id",
                    "registered_at",
                ],
            )
            or []
        )

        stats_map: Dict = {}
        for doc in docs:
            agent_id = doc.get("agent_id")
            if not agent_id:
                continue
            record = stats_map.setdefault(
                agent_id,
                {
                    "task_count": 0,
                    "activities": set(),
                    "source_agent_ids": set(),
                    "campaign_ids": set(),
                    "workflow_ids": set(),
                    "last_active": None,
                },
            )
            record["task_count"] += 1
            for key, field in (
                ("activities", "activity_id"),
                ("source_agent_ids", "source_agent_id"),
                ("campaign_ids", "campaign_id"),
                ("workflow_ids", "workflow_id"),
            ):
                if doc.get(field):
                    record[key].add(doc[field])
            ts = _ts(doc.get("registered_at"))
            if ts is not None:
                current = record["last_active"]
                record["last_active"] = ts if current is None else max(current, ts)
        for record in stats_map.values():
            for key in ("activities", "source_agent_ids", "campaign_ids", "workflow_ids"):
                record[key] = sorted(record[key])

        agents = []
        for sa in stored:
            agent_id = sa["agent_id"]
            stat = stats_map.get(
                agent_id,
                {
                    "task_count": 0,
                    "activities": [],
                    "source_agent_ids": [],
                    "campaign_ids": [],
                    "workflow_ids": [],
                    "last_active": None,
                },
            )
            if stat["task_count"] == 0:
                continue
            agents.append(
                {
                    "agent_id": agent_id,
                    "task_count": stat["task_count"],
                    "activities": stat["activities"],
                    "source_agent_ids": stat["source_agent_ids"],
                    "campaign_ids": stat["campaign_ids"],
                    "workflow_ids": stat["workflow_ids"],
                    "last_active": stat["last_active"],
                    "name": sa.get("name"),
                    "workflow_id": sa.get("workflow_id"),
                    "registered_at": _ts(sa.get("registered_at")),
                }
            )
        agents.sort(
            key=lambda a: (1, a["registered_at"]) if a["registered_at"] is not None else (0, float("-inf")),
            reverse=True,
        )
        return agents

    def telemetry_timeseries(
        self, filter: Dict, fields: List, x_field: str = "started_at", limit: int = 1000
    ) -> List[Dict]:
        """Extract plottable rows of dot-notated fields from tasks (in-process path)."""
        from flowcept.commons.daos.docdb_dao.docdb_dao_utils import get_nested

        top_level = sorted({f.split(".")[0] for f in fields} | {x_field.split(".")[0]})
        docs = (
            self.task_query(
                filter=filter,
                projection=["task_id", "activity_id"] + top_level,
                limit=limit,
            )
            or []
        )
        rows = []
        for doc in docs:
            row = {
                x_field: get_nested(doc, x_field),
                "task_id": doc.get("task_id"),
                "activity_id": doc.get("activity_id"),
            }  # noqa: E501
            row.update({f: get_nested(doc, f) for f in fields})
            rows.append(row)
        rows.sort(key=lambda r: (r[x_field] is None, r[x_field]))
        return rows

    def resolve_chart_data(self, data: Dict, context: Dict = None) -> Dict:
        """Resolve a declarative chart spec into plottable rows (in-process path)."""
        from collections import defaultdict

        from flowcept.commons.daos.docdb_dao.docdb_dao_utils import (
            _merge_context_filter,
            _metric_key,
            get_nested,
            to_epoch,
        )

        card_filter = data.get("filter") or {}
        query_filter = _merge_context_filter(card_filter, context)
        source = data.get("source", "tasks")
        limit = data.get("limit") or 1000
        group_by = data.get("group_by")
        metrics = data.get("metrics") or []
        x_field = data.get("x")
        y_fields = data.get("y") or []

        if source == "collection_sizes":
            # No bsonSize equivalent outside MongoDB; return empty.
            return {"rows": [], "count": 0}

        if group_by or metrics:
            metrics = metrics or [{"field": "", "agg": "count"}]
            has_elapsed = any(m.get("field") == "elapsed" for m in metrics)
            fields = sorted({m["field"] for m in metrics if m.get("field") and m["field"] != "elapsed"})
            elapsed_fields = ["started_at", "ended_at"] if has_elapsed else []
            top_level = sorted(
                {f.split(".")[0] for f in fields}
                | ({group_by.split(".")[0]} if group_by else set())
                | set(elapsed_fields)
            )
            docs = self.query(collection=source, filter=query_filter, projection=top_level or None) or []
            grouped: Dict = defaultdict(list)
            for doc in docs:
                grouped[get_nested(doc, group_by) if group_by else None].append(doc)
            out = []
            for key, group_docs in grouped.items():
                record = {group_by or "group": key}
                for metric in metrics:
                    field = metric.get("field", "")
                    agg = metric.get("agg", "count")
                    if field == "elapsed":
                        values = []
                        for d in group_docs:
                            s, e = to_epoch(d.get("started_at")), to_epoch(d.get("ended_at"))
                            if s is not None and e is not None:
                                values.append(e - s)
                    else:
                        values = [v for v in (get_nested(d, field) for d in group_docs) if isinstance(v, (int, float))]
                    mk = _metric_key(metric)
                    if agg == "count":
                        record[mk] = len(group_docs)
                    elif not values:
                        record[mk] = None
                    elif agg == "avg":
                        record[mk] = sum(values) / len(values)
                    elif agg == "sum":
                        record[mk] = sum(values)
                    elif agg == "min":
                        record[mk] = min(values)
                    elif agg == "max":
                        record[mk] = max(values)
                out.append(record)
            out.sort(key=lambda r: str(r.get(group_by or "group")))
            rows = out[:limit]
            return {"rows": rows, "count": len(rows)}

        if x_field and y_fields:
            rows = self.telemetry_timeseries(query_filter, fields=y_fields, x_field=x_field, limit=limit)
            return {"rows": rows[:limit], "count": len(rows[:limit])}

        sort_raw = data.get("sort")
        sort = None if not sort_raw else [(s["field"], s["order"]) for s in sort_raw]
        rows = self.query(collection=source, filter=query_filter, limit=limit, sort=sort) or []
        rows = rows[:limit]
        return {"rows": rows, "count": len(rows)}

    def dump_to_file(self, collection, filter, output_file, export_format, should_zip):
        """Dump collection data to a CSV or Parquet file, optionally zipped."""
        import os
        import zipfile
        from datetime import datetime

        df = self.to_df(collection, filter)

        if output_file is None:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"{collection}_{ts}.{export_format}"

        if export_format == "csv":
            df.to_csv(output_file, index=False)
        elif export_format == "parquet":
            df.to_parquet(output_file, index=False)
        else:
            raise ValueError(f"Unsupported format '{export_format}'. Use 'csv' or 'parquet'.")

        if should_zip:
            zip_path = output_file + ".zip"
            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
                zf.write(output_file, arcname=os.path.basename(output_file))
            os.remove(output_file)
