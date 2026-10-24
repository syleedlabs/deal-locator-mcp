"""매칭(lookup)·match_explain 이 불완전한 부속지번 대장을 '단일'로 다루지 않는지 검증.

배경(2026-10-24): 대장 일부만 받은 인덱스에 157-27 이 없어 다필지 대지면적 완화가 빠지고,
match_explain 은 그 상태를 '(단일)'로 적었다.
"""

from __future__ import annotations

from typing import Any, Optional

import pandas as pd
import pytest

from deal_locator import server as S
from deal_locator.core import lookup as L
from deal_locator.core.attached_jibun import AttachedJibunIndex

from test_match_explain import _basis, _res, _tx  # noqa: E402 — 같은 디렉터리 픽스처 재사용

_COLS = ["대지위치", "도로명대지위치", "연면적_float", "대지면적_float", "건축년도_매칭"]


class _Pipe:
    _match_tolerance = 0.10

    def __init__(self, idx: Optional[AttachedJibunIndex], direct: Optional[set[str]]) -> None:
        pool = pd.DataFrame([{"대지위치": "서울특별시 강남구 삼성동 157-27", "도로명대지위치": "테헤란로 501",
                              "연면적_float": 50_000.0, "대지면적_float": 1_691.4, "건축년도_매칭": 2014}],
                            columns=_COLS)
        pool.attrs["complete"] = True
        self._pool, self._idx, self._direct = pool, idx, direct
        self._deals = pd.DataFrame([{
            "시군구": "서울특별시 강남구 삼성동", "지번": "1**", "해제사유발생일": "",
            "거래금액(만원)": "100000", "전용/연면적(㎡)": 111.0, "대지면적(㎡)": 222.0,
            "건축년도": 1999, "층": "", "건축물주용도": "업무", "용도지역": "일반상업",
            "매도": "법인", "매수": "법인", "계약년월": "202610", "계약일": 1, "유형": "일반", "지분구분": ""}])

    def load_pyoje_bon(self, gu: str, dong: str, bon: str) -> pd.DataFrame:
        return self._pool

    def load_attached_index(self, gu: str, dong: str) -> Optional[AttachedJibunIndex]:
        return self._idx

    def lot_direct(self, gu: str, dong: str, bunji: str) -> Optional[set[str]]:
        return self._direct

    def fetch_from_api_multi_month(self, months: int = 12, gus: Any = None) -> pd.DataFrame:
        return self._deals

    def normalize_columns(self, raw: pd.DataFrame, source: str = "api") -> pd.DataFrame:
        return raw


PARTIAL = AttachedJibunIndex([{"main": "1-1", "attached": "1-2"}], complete=False)


def _mc(pipe: _Pipe) -> dict[str, Any]:
    return L.lookup_deal(pipe, "강남구 삼성동 157-27", months=3).get("match_context") or {}


def test_partial_index_is_upgraded_for_matching() -> None:
    mc = _mc(_Pipe(PARTIAL, {"157-27", "157-29", "157-31"}))
    assert mc["multi_lot"] is True and mc["lot_set"] == ["157-27", "157-29", "157-31"]
    assert mc["lot_source"] == "직접조회" and mc["lot_index_complete"] is False


def test_partial_index_without_direct_evidence_stays_unconfirmed() -> None:
    mc = _mc(_Pipe(PARTIAL, set()))
    assert mc["multi_lot"] is False and mc["lot_index_complete"] is False


def test_complete_index_keeps_previous_behaviour() -> None:
    mc = _mc(_Pipe(AttachedJibunIndex([], complete=True), {"x"}))
    assert mc["multi_lot"] is False and mc["lot_index_complete"] is True and mc["lot_source"] == "인덱스"


def test_no_index_at_all_reports_unknown() -> None:
    mc = _mc(_Pipe(None, None))
    assert mc["lot_index_complete"] is None and mc["lot_set"] == ["157-27"]


def _explain(monkeypatch: pytest.MonkeyPatch, complete: Optional[bool]) -> str:
    monkeypatch.setenv("DEAL_LOCATOR_SERVICE_KEY", "test-key")
    S._cache.clear()
    res = _res(match_context={"anchor_bunji": "1128-1", "anchor_via": "직접", "lot_set": ["1128-1"],
                              "multi_lot": False, "lot_index_complete": complete})
    monkeypatch.setattr(S, "_cached_lookup", lambda address, months: res)
    d = S._match_explain_payload("구로구 구로동 1128-1", 12)
    assert d["lot_index_complete"] is complete
    return S._render_match_explain(d)


def test_match_explain_does_not_say_single_when_incomplete(monkeypatch: pytest.MonkeyPatch) -> None:
    text = _explain(monkeypatch, False)
    assert "(단일)" not in text and "필지 구성 미확인" in text


def test_match_explain_says_single_when_complete(monkeypatch: pytest.MonkeyPatch) -> None:
    assert "(단일)" in _explain(monkeypatch, True)


def test_match_explain_schema_accepts_new_field(monkeypatch: pytest.MonkeyPatch) -> None:
    jsonschema = pytest.importorskip("jsonschema")
    _explain(monkeypatch, False)
    jsonschema.validate(S._match_explain_payload("구로구 구로동 1128-1", 12), S.MATCH_EXPLAIN_OUTPUT_SCHEMA)
