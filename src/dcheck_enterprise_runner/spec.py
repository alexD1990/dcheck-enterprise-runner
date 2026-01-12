from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml


_ALLOWED_STATUSES = {"ok", "warning", "error", "fail"}


@dataclass
class RunSpec:
    id: str
    output_path: str
    fail_on: List[str] = field(default_factory=lambda: ["error"])
    continue_on_error: bool = True
    allow_pii_output: bool = False


@dataclass
class TableSpec:
    name: str
    modules: List[str] = field(default_factory=lambda: ["core_quality"])
    config: Dict[str, Any] = field(default_factory=dict)


@dataclass
class EnterpriseSpec:
    run: RunSpec
    tables: List[TableSpec]


class SpecError(ValueError):
    pass


def load_and_validate_spec(path: Path) -> EnterpriseSpec:
    data = _load_yaml(path)
    run = _parse_run(data.get("run", {}), path)
    tables = _parse_tables(data.get("tables", None), path)

    if not tables:
        raise SpecError(f"{path}: 'tables' must contain at least 1 table entry")

    return EnterpriseSpec(run=run, tables=tables)


def _load_yaml(path: Path) -> Dict[str, Any]:
    raw = path.read_text(encoding="utf-8")
    obj = yaml.safe_load(raw)
    if not isinstance(obj, dict):
        raise SpecError(f"{path}: YAML root must be a mapping/object")
    return obj


def _parse_run(run: Dict[str, Any], path: Path) -> RunSpec:
    if not isinstance(run, dict):
        raise SpecError(f"{path}: 'run' must be an object")

    run_id = run.get("id") or "dcheck-run"
    output_path = run.get("output_path") or run.get("output") or "./out"

    fail_on = run.get("fail_on", ["error"])
    if isinstance(fail_on, str):
        fail_on = [s.strip().lower() for s in fail_on.split(",") if s.strip()]
    if not isinstance(fail_on, list) or not all(isinstance(x, str) for x in fail_on):
        raise SpecError(f"{path}: 'run.fail_on' must be a list of strings or comma-separated string")

    fail_on_norm = [s.strip().lower() for s in fail_on if s.strip()]
    unknown = [s for s in fail_on_norm if s not in _ALLOWED_STATUSES]
    if unknown:
        raise SpecError(f"{path}: unknown statuses in run.fail_on: {unknown}. Allowed: {sorted(_ALLOWED_STATUSES)}")

    continue_on_error = bool(run.get("continue_on_error", True))
    allow_pii_output = bool(run.get("allow_pii_output", False))

    return RunSpec(
        id=str(run_id),
        output_path=str(output_path),
        fail_on=fail_on_norm or ["error"],
        continue_on_error=continue_on_error,
        allow_pii_output=allow_pii_output,
    )


def _parse_tables(tables: Any, path: Path) -> List[TableSpec]:
    if tables is None:
        raise SpecError(f"{path}: missing required 'tables' list")
    if not isinstance(tables, list):
        raise SpecError(f"{path}: 'tables' must be a list")

    out: List[TableSpec] = []
    for i, t in enumerate(tables, start=1):
        if not isinstance(t, dict):
            raise SpecError(f"{path}: tables[{i}] must be an object")
        name = t.get("name")
        if not name or not isinstance(name, str):
            raise SpecError(f"{path}: tables[{i}].name is required and must be a string")

        modules = t.get("modules", ["core_quality"])
        if isinstance(modules, str):
            modules = [s.strip() for s in modules.split(",") if s.strip()]
        if not isinstance(modules, list) or not all(isinstance(x, str) for x in modules):
            raise SpecError(f"{path}: tables[{i}].modules must be list[str] or comma-separated string")

        config = t.get("config", {})
        if config is None:
            config = {}
        if not isinstance(config, dict):
            raise SpecError(f"{path}: tables[{i}].config must be an object")

        out.append(TableSpec(name=name, modules=[m.strip() for m in modules if m.strip()], config=config))
    return out