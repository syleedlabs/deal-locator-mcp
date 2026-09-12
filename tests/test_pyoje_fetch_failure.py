"""표제부 조회 실패를 '표제부 없음'으로 오판하지 않는지 검증.

배경(2026-09-12 제보): 실재하는 서초구 방배동 910-15·우면동 72-8 이
deal_card_search 에서 계속 NOT_FOUND('번지 오타/신축/멸실 가능')로 나왔다.
원인은 데이터 부재가 아니라 표제부 페이지 수신이 조용히 끊긴 것이었고,
그 실패가 부재와 같은 응답으로 나갔다.

여기서 고정하는 계약:
  1) 'HTTP 200 + 빈 본문'과 파싱 실패는 재시도 대상이다 — 중단이 아니다.
  2) 마지막 페이지 전에 items 가 비면 부분수신(complete=False)으로 표시된다.
  3) 앵커풀이 불완전한데 앵커를 못 찾으면 API_오류지 표제부없음이 아니다.
  4) 풀이 온전한데 없으면 그때만 표제부없음 — 메시지는 기존 그대로.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pandas as pd
import pytest

from deal_locator import server as S
from deal_locator.core import lookup as L
from deal_locator.core.pipeline import RealEstateDataPipeline


# ── 1) 페이지 재시도 ────────────────────────────────────────────────

class _Resp:
    def __init__(self, status: int, text: str) -> None:
        self.status_code, self.text = status, text


_XML_OK = (
    '<response><header><resultCode>00</resultCode></header>'
    '<body><items><item><platPlc>서울특별시 서초구 우면동 72-8번지</platPlc>'
    '<newPlatPlc>서울특별시 서초구 바우뫼로7길 64-11</newPlatPlc>'
    '<totArea>478.26</totArea><platArea>308</platArea>'
    '<useAprDay>20040301</useAprDay></item></items>'
    '<totalCount>1</totalCount></body></response>'
)


def _page_fetcher(monkeypatch: pytest.MonkeyPatch, responses: list[Any]) -> list[int]:
    """requests.get 을 순서대로 responses 로 대체하고 호출 횟수를 돌려준다."""
    calls = [0]
    seq = list(responses)

    def fake_get(url: str, params: Any = None, timeout: Any = None) -> _Resp:
        calls[0] += 1
        item = seq.pop(0) if seq else _Resp(200, _XML_OK)
        if isinstance(item, Exception):
            raise item
        return item

    import requests

    monkeypatch.setattr(requests, "get", fake_get)
    monkeypatch.setattr("time.sleep", lambda *_: None)
    return calls


def test_empty_body_is_retried_not_aborted(monkeypatch: pytest.MonkeyPatch) -> None:
    """HTTP 200 + 빈 본문은 재시도한다 — 예전엔 여기서 페이지네이션이 끊겼다."""
    p = RealEstateDataPipeline()
    calls = _page_fetcher(monkeypatch, [_Resp(200, ""), _Resp(200, ""), _Resp(200, _XML_OK)])

    data = p._fetch_pyoje_page("http://x", {}, "서초구", "우면동", 1)

    assert data is not None, "빈 본문 뒤 성공 응답을 받아내야 한다"
    assert calls[0] == 3, "빈 본문 2회를 재시도하고 3회째에 성공해야 한다"


def test_parse_failure_is_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    """깨진 본문도 재시도 대상이다."""
    p = RealEstateDataPipeline()
    calls = _page_fetcher(monkeypatch, [_Resp(200, "<not-xml"), _Resp(200, _XML_OK)])

    assert p._fetch_pyoje_page("http://x", {}, "서초구", "우면동", 1) is not None
    assert calls[0] == 2


def test_page_gives_up_after_attempt_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    """계속 실패하면 None — 호출자는 이걸 부분수신으로 취급한다."""
    p = RealEstateDataPipeline()
    calls = _page_fetcher(monkeypatch, [_Resp(503, "err")] * 20)

    assert p._fetch_pyoje_page("http://x", {}, "서초구", "우면동", 1) is None
    assert calls[0] == p._PYOJE_PAGE_ATTEMPTS


# ── 2) 조회 실패 ≠ 부재 ─────────────────────────────────────────────

_PYOJE_COLS = ["대지위치", "도로명대지위치", "연면적_float", "대지면적_float", "건축년도_매칭"]


def _pool(complete: bool, rows: list[dict[str, Any]] | None = None) -> pd.DataFrame:
    df = pd.DataFrame(rows or [], columns=_PYOJE_COLS)
    df.attrs["complete"] = complete
    return df


def _deal_row(dong: str) -> dict[str, Any]:
    """앵커와 절대 안 붙는 통건물 거래 1건 — 앵커 분기만 보기 위한 들러리."""
    return {
        "시군구": f"서울특별시 서초구 {dong}", "지번": "1***", "해제사유발생일": "",
        "거래금액(만원)": "100000", "전용/연면적(㎡)": 111.0, "대지면적(㎡)": 222.0,
        "건축년도": 1999, "층": "", "건축물주용도": "제2종근린생활",
        "용도지역": "제3종일반주거", "매도": "개인", "매수": "개인",
        "계약년월": "202607", "계약일": 10, "유형": "일반", "지분구분": "",
    }


class _FakePipeline:
    """표제부만 흉내 낸다 — 실거래는 앵커와 안 붙는 1건으로 둬 앵커 분기만 본다."""

    _match_tolerance = 0.10

    def __init__(self, pool: pd.DataFrame, dong: str = "우면동") -> None:
        self._pool = pool
        self._deals = pd.DataFrame([_deal_row(dong)])

    def load_pyoje_bon(self, gu: str, dong: str, bon: str) -> pd.DataFrame:
        return self._pool

    def load_attached_index(self, gu: str, dong: str) -> None:
        return None

    def fetch_from_api_multi_month(self, months: int = 12, gus: Any = None) -> pd.DataFrame:
        return self._deals

    def normalize_columns(self, raw: pd.DataFrame, source: str = "api") -> pd.DataFrame:
        return raw


def test_incomplete_pool_reports_fetch_failure_not_absence() -> None:
    """앵커풀이 불완전하면 API_오류 — '번지 오타/신축/멸실' 문구가 나가면 안 된다."""
    res = L.lookup_deal(_FakePipeline(_pool(complete=False)), "서초구 우면동 72-8", months=3)

    assert res["status"] == "API_오류"
    assert "조회에 실패" in res["message"]
    assert "번지 오타" not in res["message"]


def test_complete_pool_still_reports_absence_verbatim() -> None:
    """풀이 온전한데 없으면 그때만 표제부없음 — 기존 메시지 바이트 그대로."""
    res = L.lookup_deal(
        _FakePipeline(_pool(complete=True), dong="방배동"),
        "서초구 방배동 99999-99", months=3,
    )

    assert res["status"] == "표제부없음"
    assert res["message"] == (
        "서초구 방배동 99999-99 — 표제부·매칭 실거래 모두 없음 (번지 오타/신축/멸실 가능)."
    )


# ── 3) 서버 계약: 조회 실패는 도구 실패(isError)이고 캐시되지 않는다 ──

def test_server_surfaces_fetch_failure_as_external_api_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """조회 실패는 EXTERNAL_API_ERROR·isError=true 로 나가야 한다.

    NOT_FOUND 로 나가면 LLM 이 '데이터가 없구나'로 읽고 발행을 접는다 —
    제보 사고가 정확히 그 경로였다.
    """
    monkeypatch.setattr(S, "_key_ok", lambda: True)
    monkeypatch.setattr(S, "_cached_lookup", lambda a, m: {
        "status": "API_오류",
        "message": L._pyoje_fetch_failed_msg("서초구", "우면동", "72-8"),
        "parsed": {"gu": "서초구", "dong": "우면동", "bunji": "72-8"},
    })

    d = S._deal_card_search_payload("서초구 우면동 72-8", 3)

    assert d["status"] == "EXTERNAL_API_ERROR"
    assert d["status"] in S._IS_ERROR_STATUSES, "도구 실패로 표시돼야 재시도된다"
    assert "조회에 실패" in d["message"]


def test_fetch_failure_is_not_cached() -> None:
    """일시 실패를 15분 캐시에 박으면 재시도해도 같은 오답이 나온다."""
    import inspect

    src = inspect.getsource(S._cached_lookup)
    assert 'res.get("status") != "API_오류"' in src
