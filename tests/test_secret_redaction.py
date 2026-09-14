"""응답으로 인증키 원문이 나가지 않는지 — 오류 경로까지.

외부 API 예외 문자열엔 '?serviceKey=<키>' 가 든 URL 이 섞일 수 있다. 이게 도구 응답에
실리면 키가 LLM 대화 기록에 남는다. 두 겹으로 막는다:
  1) 응답 문자열 관문(_scrub)에서 마스킹 — structuredContent·텍스트 둘 다
  2) 도구가 잡지 못한 예외는 원문을 클라이언트에 보내지 않음(mask_error_details)
"""

from __future__ import annotations

import asyncio
import json

import pandas as pd
import pytest
import requests

from deal_locator import server as S

_KEY = "TESTKEY_abc123%2Bxyz%3D%3D"  # 가짜 키


@pytest.fixture(autouse=True)
def _isolate(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(S, "_key_ok", lambda: True)
    S._cache.clear()
    yield
    S._cache.clear()


def _empty_pool() -> pd.DataFrame:
    df = pd.DataFrame(columns=["대지위치"])
    df.attrs["complete"] = True
    return df


class _ApiBoom:
    """실거래 조회가 requests 예외를 던진다 — 예외 문자열에 키가 든 URL."""

    _match_tolerance = 0.1

    def fetch_from_api_multi_month(self, *a, **k):
        raise requests.ConnectionError(f"Max retries exceeded with url: /x?serviceKey={_KEY}&LAWD_CD=11650")

    def load_attached_index(self, *a, **k):
        return None

    def load_pyoje_bon(self, *a, **k):
        return _empty_pool()


_TOOLS = [
    ("deal_card_search", lambda: S._deal_card_search_payload("서초구 우면동 72-8", 3), S._render_deal_card_search),
    ("deal_history", lambda: S._deal_history_payload("서초구 우면동 72-8", 3), S._render_deal_history),
    ("match_explain", lambda: S._match_explain_payload("서초구 우면동 72-8", 3), S._render_match_explain),
    ("area_scan", lambda: S._area_scan_payload("서초구 우면동", 0, 0, 3, "", 30), S._render_area_scan),
]


@pytest.mark.parametrize("name,payload,render", _TOOLS, ids=[t[0] for t in _TOOLS])
def test_api_error_response_has_no_key(monkeypatch: pytest.MonkeyPatch, name, payload, render) -> None:
    monkeypatch.setattr(S, "get_pipeline", lambda: _ApiBoom())

    d = payload()

    assert d["status"] == "EXTERNAL_API_ERROR", "전제: 외부 API 오류로 분류돼야 한다"
    assert _KEY not in json.dumps(d, ensure_ascii=False), f"{name}: structuredContent 에 키 원문"
    assert _KEY not in render(d), f"{name}: 텍스트에 키 원문"


def test_scrub_redacts_key_in_nested_values() -> None:
    out = S._scrub({"a": [f"https://x/y?serviceKey={_KEY}&z=1"], "b": {"c": f"ServiceKey={_KEY}"}})

    assert _KEY not in json.dumps(out, ensure_ascii=False)
    assert "z=1" in out["a"][0], "키 뒤의 다른 파라미터는 살아 있어야 한다"


class _NormalizeBoom(_ApiBoom):
    """도구가 try 로 감싸지 않은 지점에서 터진다 → FastMCP 까지 올라간다."""

    def fetch_from_api_multi_month(self, *a, **k):
        return pd.DataFrame([{"시군구": "서초구", "법정동": "우면동"}])

    def normalize_columns(self, *a, **k):
        raise ValueError(f"parse failed for https://apis.data.go.kr/x?serviceKey={_KEY}")


def test_uncaught_exception_is_masked_for_client(monkeypatch: pytest.MonkeyPatch) -> None:
    from fastmcp import Client

    monkeypatch.setattr(S, "get_pipeline", lambda: _NormalizeBoom())

    async def call() -> str:
        async with Client(S.mcp) as c:
            try:
                return str(await c.call_tool("deal_card_search", {"address": "서초구 우면동 72-8", "months": 3}))
            except Exception as e:  # noqa: BLE001
                return f"{type(e).__name__}: {e}"

    assert _KEY not in asyncio.run(call())
