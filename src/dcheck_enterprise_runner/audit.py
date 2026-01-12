from __future__ import annotations

import getpass
import os
import platform
import socket
from datetime import datetime, timezone
from importlib.metadata import PackageNotFoundError, version
from typing import Any, Dict


def _pkg_version(pkg: str) -> str:
    try:
        return version(pkg)
    except PackageNotFoundError:
        return "not-installed"


def build_audit_metadata() -> Dict[str, Any]:
    """
    Best-effort audit metadata.
    Avoids hard dependencies on Databricks SDK.
    """
    now = datetime.now(timezone.utc).isoformat()

    return {
        "timestamp_utc": now,
        "user": os.getenv("DATABRICKS_USER") or os.getenv("USER") or getpass.getuser(),
        "host": socket.gethostname(),
        "platform": platform.platform(),
        "python": platform.python_version(),
        "env": {
            "databricks_runtime_version": os.getenv("DATABRICKS_RUNTIME_VERSION"),
            "cluster_id": os.getenv("DATABRICKS_CLUSTER_ID"),
            "job_id": os.getenv("DATABRICKS_JOB_ID"),
        },
        "versions": {
            "dcheck": _pkg_version("dcheck"),
            "dcheck-gdpr": _pkg_version("dcheck-gdpr"),
            "dcheck-enterprise-runner": _pkg_version("dcheck-enterprise-runner"),
        },
    }

def collect_audit() -> Dict[str, Any]:
    """
    Backwards-compatible alias used by notebooks/examples.
    """
    return build_audit_metadata()
