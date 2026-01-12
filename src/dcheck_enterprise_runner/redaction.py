from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple

# Very defensive heuristics. This is NOT the primary masking implementation.
# It's a safety belt to avoid storing raw PII if a module accidentally leaks it.
_EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)
_PHONE_RE = re.compile(r"\+?\d[\d\s\-]{6,}\d")
_FNR_RE = re.compile(r"\b\d{11}\b")


def contains_obvious_pii(text: str) -> bool:
    if not text:
        return False
    return bool(_EMAIL_RE.search(text) or _PHONE_RE.search(text) or _FNR_RE.search(text))


def redact_string(text: str) -> str:
    """
    Heavy-handed redaction for safety belt usage.
    """
    if not isinstance(text, str):
        return text  # type: ignore[return-value]

    text = _EMAIL_RE.sub("[REDACTED_EMAIL]", text)
    text = _FNR_RE.sub("[REDACTED_FNR]", text)
    text = _PHONE_RE.sub("[REDACTED_PHONE]", text)
    return text


def redact_report_payload(payload: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
    """
    Redact likely PII from known high-risk fields (samples/findings/message-like strings).
    Returns (new_payload, redaction_events).
    """
    redactions: List[str] = []

    def _walk(x: Any, path: str) -> Any:
        # strings
        if isinstance(x, str):
            if contains_obvious_pii(x):
                redactions.append(path)
                return redact_string(x)
            return x

        # lists
        if isinstance(x, list):
            return [_walk(v, f"{path}[{i}]") for i, v in enumerate(x)]

        # dicts
        if isinstance(x, dict):
            out: Dict[str, Any] = {}
            for k, v in x.items():
                kp = f"{path}.{k}" if path else str(k)
                # prioritize redacting likely sample-bearing keys
                if str(k).lower() in {"sample", "samples", "findings", "matches", "examples", "value"}:
                    out[k] = _walk(v, kp)
                else:
                    out[k] = _walk(v, kp)
            return out

        return x

    new_payload = _walk(payload, "")  # type: ignore[assignment]
    assert isinstance(new_payload, dict)
    return new_payload, redactions