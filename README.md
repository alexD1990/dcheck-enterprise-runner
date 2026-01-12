# DCheck Enterprise Runner (MVP)

Runs DCheck validations in batch mode from a YAML spec and writes machine-readable JSON outputs.
Designed for Databricks / Spark environments.

## Features (MVP0)
- YAML spec -> plan -> execute -> JSON output
- Runs DCheck via public API: `from dcheck.api import dc`
- Writes per-table JSON immediately (no huge in-memory aggregation)
- `summary.json` + `run_state.jsonl` (resume support)
- Schema versioning in JSON outputs
- Defensive redaction policy (default: do not allow raw PII output)
- Click-based CLI

## Install (local dev)
```bash
pip install -e .[dev]
```
## Run (dry-run)
```
dcheck-enterprise-runner run --spec specs/example.yml --output ./out --dry-run
```
## Run (execute)
```
dcheck-enterprise-runner run --spec specs/example.yml --output ./out
```
## Resume
```
dcheck-enterprise-runner run --spec specs/example.yml --output ./out --resume
```
## Databricks
### Install directly from GitHub (example):
```
%pip install --no-deps --no-cache-dir --force-reinstall "git+https://github.com/<ORG>/dcheck-enterprise-runner.git@main"
dbutils.library.restartPython()
```
### Then run:
```
!dcheck-enterprise-runner run --spec /dbfs/path/spec.yml --output /dbfs/path/out
```


## `specs/example.yml`
```yaml
run:
  id: "demo-run-2026-01-12"
  # Use /dbfs/... in Databricks if you want to write to DBFS
  output_path: "./out"
  fail_on: ["error"]         # allowed: ok, warning, error, fail
  continue_on_error: true
  allow_pii_output: false

tables:
  - name: "catalog.schema.table1"
    modules: ["core_quality", "gdpr"]
    config:
      gdpr:
        scan:
          sample_rows: 5000

  - name: "catalog.schema.table2"
    modules: ["gdpr"]
```