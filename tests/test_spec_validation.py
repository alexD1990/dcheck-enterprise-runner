from pathlib import Path
import tempfile

from dcheck_enterprise_runner.spec import load_and_validate_spec, SpecError

def test_spec_requires_tables():
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "spec.yml"
        p.write_text("run:\n  id: x\n  output_path: ./out\n", encoding="utf-8")
        try:
            load_and_validate_spec(p)
            assert False, "Expected SpecError"
        except SpecError:
            pass

def test_spec_parses_minimal():
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "spec.yml"
        p.write_text(
            "run:\n  id: x\n  output_path: ./out\n"
            "tables:\n"
            "  - name: cat.sch.tbl\n",
            encoding="utf-8",
        )
        spec = load_and_validate_spec(p)
        assert spec.run.id == "x"
        assert spec.tables[0].name == "cat.sch.tbl"
        assert spec.tables[0].modules == ["core_quality"]