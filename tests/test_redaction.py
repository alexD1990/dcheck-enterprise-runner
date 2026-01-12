from dcheck_enterprise_runner.redaction import redact_string, contains_obvious_pii

def test_contains_obvious_pii():
    assert contains_obvious_pii("a@b.com")
    assert contains_obvious_pii("01019012345")
    assert contains_obvious_pii("+4791122334")

def test_redact_string():
    s = "mail a@b.com fnr 01019012345 phone +4791122334"
    r = redact_string(s)
    assert "a@b.com" not in r
    assert "01019012345" not in r
    assert "+4791122334" not in r