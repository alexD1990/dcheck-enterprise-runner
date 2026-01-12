from __future__ import annotations

import time
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from dcheck_enterprise_runner.io import append_jsonl, ensure_dir, write_json
from dcheck_enterprise_runner.planner import build_plan
from dcheck_enterprise_runner.redaction import redact_report_payload
from dcheck_enterprise_runner.spec import EnterpriseSpec


_SCHEMA_VERSION = "dcheck-enterprise-run-report/v1"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _status_counts(results: List[Dict[str, Any]]) -> Dict[str, int]:
    counts = {"ok": 0, "warning": 0, "error": 0, "fail": 0}
    for r in results:
        s = str(r.get("status", "")).lower()
        if s in counts:
            counts[s] += 1
    return counts


def _should_fail(counts: Dict[str, int], fail_on: List[str]) -> bool:
    fail_on_set = {s.lower() for s in fail_on}
    for status, n in counts.items():
        if status in fail_on_set and n > 0:
            return True
    return False


def _normalize_output_path(p: str) -> Path:
    """
    Databricks hint:
      - If user passes `dbfs:/...`, write via `/dbfs/...` from Python.
      - If user passes `/dbfs/...`, use as-is.
    """
    if p.startswith("dbfs:/"):
        return Path("/dbfs") / p[len("dbfs:/") :]
    return Path(p)


def run_from_spec(spec: EnterpriseSpec, audit: Dict[str, Any], resume: bool, limit: Optional[int]) -> int:
    out_dir = _normalize_output_path(spec.run.output_path)
    ensure_dir(out_dir)

    state_path = out_dir / "run_state.jsonl"
    summary_path = out_dir / "summary.json"

    jobs = build_plan(spec)
    if limit is not None:
        jobs = jobs[:limit]

    completed = set()
    if resume and state_path.exists():
        for line in state_path.read_text(encoding="utf-8").splitlines():
            try:
                rec = __import__("json").loads(line)
                if rec.get("status") == "completed":
                    completed.add(rec.get("table"))
            except Exception:
                continue

    # Summary object kept small in memory
    run_summary: Dict[str, Any] = {
        "schema_version": _SCHEMA_VERSION,
        "run": {
            "id": spec.run.id,
            "started_utc": _utc_now(),
            "output_path": str(out_dir),
            "fail_on": spec.run.fail_on,
            "continue_on_error": spec.run.continue_on_error,
            "allow_pii_output": spec.run.allow_pii_output,
        },
        "audit": audit,
        "tables_total": len(jobs),
        "tables_completed": 0,
        "tables_failed": 0,
        "tables_skipped": 0,
        "table_results": [],  # list of small per-table summaries only
    }

    # Import here to keep runner importable without Spark in unit tests
    from dcheck.api import dc  # type: ignore

    overall_failed = False

    for idx, job in enumerate(jobs, start=1):
        if job.name in completed:
            run_summary["tables_skipped"] += 1
            append_jsonl(state_path, {"ts_utc": _utc_now(), "table": job.name, "status": "skipped", "reason": "resume"})
            continue

        started = time.time()
        append_jsonl(state_path, {"ts_utc": _utc_now(), "table": job.name, "status": "started", "index": idx, "total": len(jobs)})

        table_out = out_dir / "tables" / _safe_table_filename(job.name)
        ensure_dir(table_out.parent)

        try:
            # dc() supports table auto-load when given a string. Provide table_name as well for preflight paths.
            report_obj = dc(job.name, table_name=job.name, modules=job.modules, config=job.config, render=False)

            payload = _serialize_report(report_obj, table_name=job.name, modules=job.modules, config=job.config, audit=audit)

            redaction_events: List[str] = []
            if not spec.run.allow_pii_output:
                payload, redaction_events = redact_report_payload(payload)
                if redaction_events:
                    payload.setdefault("redaction", {})
                    payload["redaction"].update(
                        {
                            "applied": True,
                            "reason": "allow_pii_output=false",
                            "paths": redaction_events[:200],  # cap to avoid huge files
                        }
                    )

            write_json(table_out, payload)

            counts = payload.get("summary", {}).get("status_counts") or _status_counts(payload.get("results", []))
            failed = _should_fail(counts, spec.run.fail_on)

            run_summary["tables_completed"] += 1
            run_summary["table_results"].append(
                {
                    "table": job.name,
                    "output_file": str(table_out),
                    "status_counts": counts,
                    "duration_sec": round(time.time() - started, 3),
                    "failed": bool(failed),
                    "redaction_applied": bool(redaction_events),
                }
            )

            append_jsonl(state_path, {"ts_utc": _utc_now(), "table": job.name, "status": "completed", "failed": bool(failed)})

            if failed:
                run_summary["tables_failed"] += 1
                overall_failed = True
                if not spec.run.continue_on_error:
                    break

        except Exception as e:
            overall_failed = True
            run_summary["tables_failed"] += 1
            append_jsonl(
                state_path,
                {"ts_utc": _utc_now(), "table": job.name, "status": "error", "error": f"{type(e).__name__}: {e}"},
            )
            run_summary["table_results"].append(
                {
                    "table": job.name,
                    "output_file": None,
                    "status_counts": {"error": 1},
                    "duration_sec": round(time.time() - started, 3),
                    "failed": True,
                    "exception": f"{type(e).__name__}: {e}",
                }
            )
            if not spec.run.continue_on_error:
                break

    run_summary["run"]["finished_utc"] = _utc_now()
    write_json(summary_path, run_summary)

    # Exit codes:
    # 0 -> ok/warnings only
    # 1 -> any status in fail_on occurred
    return 1 if overall_failed else 0


def _serialize_report(report_obj: Any, *, table_name: str, modules: List[str], config: Dict[str, Any], audit: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert DCheck report object into a stable JSON payload.
    Works even if underlying report class changes, as long as it exposes common attributes.
    """
    rows = getattr(report_obj, "rows", None)
    columns = getattr(report_obj, "columns", None)
    column_names = getattr(report_obj, "column_names", None)
    results = getattr(report_obj, "results", None)

    # results expected to be list of RuleResult-like objects
    results_json: List[Dict[str, Any]] = []
    if isinstance(results, list):
        for r in results:
            results_json.append(
                {
                    "name": getattr(r, "name", None),
                    "status": getattr(r, "status", None),
                    "message": getattr(r, "message", None),
                    "metrics": getattr(r, "metrics", None) or {},
                }
            )

    counts = _status_counts(results_json)

    return {
        "schema_version": _SCHEMA_VERSION,
        "table": {"name": table_name},
        "execution": {"modules": modules, "config": config},
        "dataset": {"rows": rows, "columns": columns, "column_names": list(column_names) if column_names else None},
        "summary": {"status_counts": counts},
        "results": results_json,
        "audit": audit,
        "generated_utc": _utc_now(),
    }


def _safe_table_filename(table_name: str) -> str:
    # make stable file name
    return table_name.replace("/", "_").replace(":", "_").replace(".", "__") + ".json"