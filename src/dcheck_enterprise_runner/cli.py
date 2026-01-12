from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Optional

import click

from dcheck_enterprise_runner.audit import build_audit_metadata
from dcheck_enterprise_runner.runner import run_from_spec
from dcheck_enterprise_runner.spec import load_and_validate_spec


@click.group()
def main() -> None:
    """DCheck Enterprise Runner."""


@main.command("run")
@click.option("--spec", "spec_path", type=click.Path(exists=True, dir_okay=False, path_type=Path), required=True)
@click.option("--output", "output_path", type=click.Path(file_okay=False, path_type=Path), required=False)
@click.option("--run-id", "run_id", type=str, required=False)
@click.option("--dry-run", is_flag=True, default=False)
@click.option("--resume", is_flag=True, default=False)
@click.option("--limit", type=int, required=False, help="Run only first N tables.")
@click.option(
    "--fail-on",
    type=str,
    required=False,
    help='Comma-separated statuses that should fail the run (e.g. "error,fail"). Overrides YAML.',
)
@click.option("--allow-pii-output", is_flag=True, default=False, help="Allow writing non-redacted output (DANGEROUS).")
def run_cmd(
    spec_path: Path,
    output_path: Optional[Path],
    run_id: Optional[str],
    dry_run: bool,
    resume: bool,
    limit: Optional[int],
    fail_on: Optional[str],
    allow_pii_output: bool,
) -> None:
    """
    Execute a DCheck run from a YAML spec and write JSON outputs.
    """
    spec = load_and_validate_spec(spec_path)

    # CLI overrides
    if output_path is not None:
        spec.run.output_path = str(output_path)
    if run_id is not None:
        spec.run.id = run_id
    if fail_on is not None:
        spec.run.fail_on = [s.strip().lower() for s in fail_on.split(",") if s.strip()]
    if allow_pii_output:
        spec.run.allow_pii_output = True

    audit = build_audit_metadata()

    if dry_run:
        click.echo("DRY RUN - plan only\n")
        click.echo(f"Run ID: {spec.run.id}")
        click.echo(f"Output: {spec.run.output_path}")
        click.echo(f"Fail-on: {spec.run.fail_on}")
        click.echo(f"Resume: {resume}")
        click.echo(f"Allow PII output: {spec.run.allow_pii_output}")
        click.echo(f"Tables: {len(spec.tables)}")
        if limit:
            click.echo(f"Limit: {limit}")
        click.echo("\nAudit (best-effort):")
        click.echo(json.dumps(audit, indent=2, sort_keys=True))
        click.echo("\nPlanned tables:")
        for i, t in enumerate(spec.tables[: limit or len(spec.tables)], start=1):
            click.echo(f"  {i}. {t.name}  modules={t.modules}")
        return

    rc = run_from_spec(spec=spec, audit=audit, resume=resume, limit=limit)
    raise SystemExit(rc)