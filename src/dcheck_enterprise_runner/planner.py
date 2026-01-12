from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

from dcheck_enterprise_runner.spec import EnterpriseSpec


@dataclass(frozen=True)
class TableJob:
    name: str
    modules: List[str]
    config: Dict[str, Any]


def build_plan(spec: EnterpriseSpec) -> List[TableJob]:
    jobs: List[TableJob] = []
    for t in spec.tables:
        jobs.append(TableJob(name=t.name, modules=t.modules, config=t.config))
    return jobs