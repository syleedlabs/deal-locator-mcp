"""resolve_address 가 불완전한 부속지번 대장을 '단일'로 내보내지 않는지 검증.

배경(2026-10-24): 삼성동 157-27(3필지)이 '단일'로 응답됐다(test_attached_jibun_complete 참조).
여기서는 서버 응답 계약만 본다 — 판정 자체는 core.attached_jibun.classify_lot.
"""

from __future__ import annotations

from typing import Any, Optional

import pytest

from deal_locator import server as S
from deal_locator.core.attached_jibun import AttachedJibunIndex


class _Pipe:
    def __init__(self, idx: AttachedJibunIndex, direct: Optional[set[str]]) -> None:
        self._idx, self._direct, self.direct_calls = idx, direct, 0

    def get_bdong_code(self, gu: str, dong: str) -> str:
        return "10500"

    def load_attached_index(self, gu: str, dong: str) -> AttachedJibunIndex:
        return self._idx

    def lot_direct(self, gu: str, dong: str, bunji: str) -> Optional[set[str]]:
        self.direct_calls += 1
        return self._direct


@pytest.fixture(autouse=True)
def _isolate(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DEAL_LOCATOR_SERVICE_KEY", "test-key")
    S._cache.clear()


def _run(monkeypatch: pytest.MonkeyPatch, pipe: _Pipe, q: str = "강남구 삼성동 157-27") -> dict[str, Any]:
    monkeypatch.setattr(S, "get_pipeline", lambda: pipe)
    return S._resolve_address_payload(q)


PARTIAL = AttachedJibunIndex([{"main": "1-1", "attached": "1-2"}], complete=False, total_count=627)


def test_partial_index_upgraded_by_direct_query(monkeypatch: pytest.MonkeyPatch) -> None:
    d = _run(monkeypatch, _Pipe(PARTIAL, {"157-27", "157-29", "157-31"}))
    assert d["lot_structure"] == "다필지" and d["lot_source"] == "직접조회"
    assert d["lot_set"] == ["157-27", "157-29", "157-31"] and d["index_complete"] is False
    text = S._render_resolve_address(d)
    assert "다필지 대지 — 세트 157-27, 157-29, 157-31" in text and "직접 조회로 확인" in text


def test_partial_index_and_empty_direct_is_unconfirmed(monkeypatch: pytest.MonkeyPatch) -> None:
    d = _run(monkeypatch, _Pipe(PARTIAL, set()))
    assert d["lot_structure"] == "조회불완전" and d["lot_set"] == []
    text = S._render_resolve_address(d)
    assert "단일 필지" not in text and "단일이라는 뜻이 아님" in text


def test_partial_index_and_direct_failure_is_unconfirmed(monkeypatch: pytest.MonkeyPatch) -> None:
    d = _run(monkeypatch, _Pipe(PARTIAL, None))
    assert d["lot_structure"] == "조회불완전" and d["lot_source"] == "인덱스"


def test_empty_index_from_total_failure_is_not_single(monkeypatch: pytest.MonkeyPatch) -> None:
    d = _run(monkeypatch, _Pipe(AttachedJibunIndex(complete=False), None))
    assert d["lot_structure"] != "단일"


def test_legacy_unverified_cache_is_not_single(monkeypatch: pytest.MonkeyPatch) -> None:
    d = _run(monkeypatch, _Pipe(AttachedJibunIndex([], complete=None), set()))
    assert d["lot_structure"] == "조회불완전"


def test_complete_index_says_single_without_direct_call(monkeypatch: pytest.MonkeyPatch) -> None:
    pipe = _Pipe(AttachedJibunIndex([], complete=True, total_count=5), {"x"})
    d = _run(monkeypatch, pipe)
    assert d["lot_structure"] == "단일" and d["index_complete"] is True and pipe.direct_calls == 0


def test_new_payloads_match_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    jsonschema = pytest.importorskip("jsonschema")
    for pipe in (_Pipe(PARTIAL, {"157-27", "157-29"}), _Pipe(PARTIAL, set()), _Pipe(PARTIAL, None),
                 _Pipe(AttachedJibunIndex([], complete=True), None)):
        jsonschema.validate(_run(monkeypatch, pipe), S.RESOLVE_ADDRESS_OUTPUT_SCHEMA)


def test_schema_advertises_incomplete_state() -> None:
    props = S.RESOLVE_ADDRESS_OUTPUT_SCHEMA["properties"]
    assert "조회불완전" in props["lot_structure"]["enum"]
    assert {"index_complete", "lot_source"} <= set(props)
