"""deal_history 구조화 출력(structuredContent) 테스트.

test_deal_card_search.py 와 같은 방식 — 개조 전 렌더러를 _legacy_deal_history 로 보존해
신구 텍스트가 바이트 동일한지 대조하고, transactions[] 가 시계열 비교에 쓸 수 있게
기계 판독 가능한지 본다.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from deal_locator import server as S


# ── 개조 전 원본 렌더러 (수정 금지 — 골든 기준) ────────────────────────

def _legacy_deal_history(res: dict[str, Any]) -> str:
    p = res["parsed"]
    head = f"{p['gu']} {p['dong']} {p['bunji']}"
    txs = res["transactions"]
    if not txs:
        return (f"{S.NOT_FOUND} {head} — 최근 {res['period_months']}개월 내 매칭 실거래 없음"
                + (" (표제부는 존재)" if res.get("building") else " (표제부도 없음)")
                + f". {S.NO_GUESS}")

    lines = [f"■ 실거래 이력 — {head} · 최근 {res['period_months']}개월 {len(txs)}건 (최신순)"]
    for t in txs:
        lines.append(f"  · {S._fmt_tx_line(t)}")
        area = S._fmt_areas(t)
        if area or t.get("floor"):
            sub = [area] if area else []
            if t.get("floor"):
                sub.append(f"층 {t['floor']}")
            lines.append(f"    {' · '.join(sub)}")
    lines.extend("  " + n for n in S._context_notes(res))
    worst = min(txs, key=lambda x: x["confidence_score"])
    if S._caveat(worst):
        lines.append(f"  ⚠ {S._caveat(worst)}")
    lines.append(S.SOURCE_LINE)
    return "\n".join(lines)


# ── 픽스처 ──────────────────────────────────────────────────────────

def _tx(**over: Any) -> dict[str, Any]:
    t = {
        "price": "859억", "price_manwon": 8_590_000, "deal_date": "2026-06-09",
        "seller": "법인", "buyer": "법인", "floor": "", "building_type": "일반",
        "usage": "제2종근린생활", "zone": "준공업",
        "gross_sqm": 18_500.83, "land_sqm": 2_589.6,
        "land_pyeong": 783.4, "gross_pyeong": 5_596.5,
        "price_per_land_pyeong_manwon": 10_965,
        "build_year": 2010, "jibun_masked": True, "raw_jibun": "서울특별시 구로구 구로동 1***",
        "confidence": "정확매칭", "confidence_score": 0.97, "match_stage": "stage1",
        "match_basis": {"confidence": "정확매칭", "confidence_score": 0.97,
                        "stage": "stage1", "reasons": ["건축년도·연면적·대지면적 정확 일치"],
                        "anchor": {}, "row": {}, "caveat": ""},
    }
    t.update(over)
    return t


def _res(**over: Any) -> dict[str, Any]:
    r = {
        "query": "구로구 구로동 1128-1",
        "parsed": {"gu": "구로구", "dong": "구로동", "bunji": "1128-1"},
        "status": "거래있음",
        "building": {"address": "서울특별시 구로구 구로동 1128-1",
                     "road_address": "서울특별시 구로구 디지털로32길 72 (구로동)",
                     "land": 2_589.6, "gross": 18_500.83,
                     "land_pyeong": 783.4, "gross_pyeong": 5_596.5,
                     "build_year": 2010, "building_count": 1,
                     "anchor_bunji": "1128-1", "anchor_via": "직접"},
        "match_context": {"anchor_bunji": "1128-1", "anchor_via": "직접",
                          "lot_set": ["1128-1"], "multi_lot": False},
        "transactions": [_tx()], "latest": _tx(),
        "cancelled_count": 18, "period_months": 24, "message": "",
    }
    r.update(over)
    return r


# 재거래 이력(신뢰도가 건별로 다른 케이스) — 최저 신뢰도의 caveat 이 붙어야 한다
_추정 = _tx(price="700억", price_manwon=7_000_000, deal_date="2025-02-11",
            confidence="추정매칭", confidence_score=0.60, match_stage="stage3",
            floor="3", land_sqm=None, land_pyeong=None,
            match_basis={"reasons": ["오차범위 내"], "caveat": "동일 스펙 인접 건물 가능성"})

CASES: dict[str, dict[str, Any]] = {
    "이력_1건": _res(),
    "이력_다건_혼합신뢰도": _res(transactions=[_tx(), _추정]),
    "이력_부속지번_다필지_해제": _res(
        transactions=[_tx(), _추정],
        match_context={"anchor_bunji": "100-4", "anchor_via": "부속지번",
                       "lot_set": ["100-4", "103-2"], "multi_lot": True},
        cancelled_count=3,
    ),
    "이력_면적결측": _res(transactions=[
        _tx(land_sqm=None, gross_sqm=None, land_pyeong=None, gross_pyeong=None,
            floor="", usage="", seller="", buyer="", jibun_masked=False)]),
    "거래없음_표제부있음": _res(status="거래없음", transactions=[], latest=None),
    "표제부없음": _res(status="표제부없음", transactions=[], latest=None, building=None),
}


@pytest.fixture(autouse=True)
def _no_network(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DEAL_LOCATOR_SERVICE_KEY", "test-key")
    S._cache.clear()


def _patch_lookup(monkeypatch: pytest.MonkeyPatch, res: dict[str, Any]) -> None:
    monkeypatch.setattr(S, "_cached_lookup", lambda address, months: res)


# ── 1) 텍스트 회귀 ──────────────────────────────────────────────────

@pytest.mark.parametrize("name", list(CASES))
def test_text_output_is_byte_identical_to_legacy(name: str, monkeypatch: pytest.MonkeyPatch) -> None:
    res = CASES[name]
    _patch_lookup(monkeypatch, res)
    new = S._render_deal_history(S._deal_history_payload("구로구 구로동 1128-1", 24))
    assert new == _legacy_deal_history(res), f"[{name}] 텍스트 출력이 개조 전과 달라짐"


def test_error_texts_unchanged(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DEAL_LOCATOR_SERVICE_KEY", raising=False)
    monkeypatch.delenv("DATA_GO_KR_API_KEY", raising=False)
    d = S._deal_history_payload("구로구 구로동 1128-1", 24)
    assert d["status"] == "CONFIG_ERROR"
    assert S._render_deal_history(d) == f"{S.CONFIG_ERROR} {S.KEY_MISSING_MSG}"

    monkeypatch.setenv("DEAL_LOCATOR_SERVICE_KEY", "test-key")
    _patch_lookup(monkeypatch, {"status": "파싱실패", "message": "지번 해석 실패"})
    d = S._deal_history_payload("헛소리", 24)
    assert d["status"] == "PARSE_ERROR"
    assert S._render_deal_history(d) == f"{S.PARSE_ERROR} 지번 해석 실패"

    _patch_lookup(monkeypatch, {"status": "API_오류", "message": "타임아웃"})
    d = S._deal_history_payload("구로구 구로동 1128-1", 24)
    assert d["status"] == "EXTERNAL_API_ERROR"
    assert S._render_deal_history(d) == f"{S.API_ERROR} 타임아웃"


# ── 2) 구조화 출력 계약 ─────────────────────────────────────────────

def test_transactions_are_machine_readable(monkeypatch: pytest.MonkeyPatch) -> None:
    """시계열 비교를 텍스트 파싱 없이 — 배열 순서·수치 타입·건별 신뢰도."""
    _patch_lookup(monkeypatch, CASES["이력_다건_혼합신뢰도"])
    d = S._deal_history_payload("구로구 구로동 1128-1", 24)

    assert d["status"] == "OK"
    assert d["transaction_count"] == 2 == len(d["transactions"])
    first, second = d["transactions"]
    assert (first["price_manwon"], second["price_manwon"]) == (8_590_000, 7_000_000)
    assert (first["deal_date"], second["deal_date"]) == ("2026-06-09", "2025-02-11")
    # 건별 신뢰도가 다르다 — 이력 전체를 한 신뢰도로 뭉뚱그리면 안 된다
    assert first["confidence_score"] == pytest.approx(0.97)
    assert second["confidence_score"] == pytest.approx(0.60)
    assert d["lowest_confidence_score"] == pytest.approx(0.60)
    assert second["caveat"] == "동일 스펙 인접 건물 가능성"
    assert d["cancelled_count"] == 18
    assert d["building"]["road_address"].endswith("디지털로32길 72 (구로동)")


def test_notes_and_period_are_carried(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_lookup(monkeypatch, CASES["이력_부속지번_다필지_해제"])
    d = S._deal_history_payload("구로구 구로동 103-2", 24)
    assert d["period_months"] == 24
    assert any("부속지번" in n for n in d["notes"])
    assert any("해제신고 거래 3건 제외" in n for n in d["notes"])


def test_not_found_carries_no_transactions(monkeypatch: pytest.MonkeyPatch) -> None:
    """환각 차단 규약: 거래가 없으면 transactions 는 반드시 빈 배열."""
    for name in ("거래없음_표제부있음", "표제부없음"):
        _patch_lookup(monkeypatch, CASES[name])
        d = S._deal_history_payload("구로구 구로동 1128-1", 24)
        assert d["status"] == "NOT_FOUND", name
        assert d["transactions"] == [], name
        assert d["transaction_count"] == 0, name
        assert d["lowest_confidence_score"] is None, name
        assert d["message"], name
    # 표제부 유무가 message 로 구분된다
    _patch_lookup(monkeypatch, CASES["거래없음_표제부있음"])
    assert "표제부는 존재" in S._deal_history_payload("x", 24)["message"]
    _patch_lookup(monkeypatch, CASES["표제부없음"])
    assert "표제부도 없음" in S._deal_history_payload("x", 24)["message"]


def test_months_is_clamped(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DEAL_LOCATOR_SERVICE_KEY", raising=False)
    assert S._deal_history_payload("x", 999)["period_months"] == 60
    assert S._deal_history_payload("x", 0)["period_months"] == 1


def test_payload_matches_output_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    jsonschema = pytest.importorskip("jsonschema")
    for name, res in CASES.items():
        _patch_lookup(monkeypatch, res)
        d = S._deal_history_payload("구로구 구로동 1128-1", 24)
        jsonschema.validate(d, S.DEAL_HISTORY_OUTPUT_SCHEMA)


def test_tx_schema_is_shared_with_deal_card_search() -> None:
    """거래 항목 필드는 한 곳에서만 정의된다 (deal_card_search.latest == deal_history 항목)."""
    item = S.DEAL_HISTORY_OUTPUT_SCHEMA["properties"]["transactions"]["items"]
    assert item["properties"] is S.DEAL_CARD_SEARCH_OUTPUT_SCHEMA["properties"]["latest"]["properties"]
    assert item["type"] == "object"  # 배열 항목은 null 이 아니다


# ── 3) MCP 프로토콜 레벨 ────────────────────────────────────────────

def _call(name: str, args: dict[str, Any]):
    from fastmcp import Client

    async def run():
        async with Client(S.mcp) as c:
            return await c.call_tool(name, args, raise_on_error=False)

    return asyncio.run(run())


def test_tool_advertises_schema_and_annotations() -> None:
    from fastmcp import Client

    async def run():
        async with Client(S.mcp) as c:
            return await c.list_tools()

    t = {x.name: x for x in asyncio.run(run())}["deal_history"]
    assert t.outputSchema is not None
    assert "transactions" in t.outputSchema["properties"]
    assert t.annotations.readOnlyHint is True
    assert t.annotations.openWorldHint is True


def test_call_tool_returns_both_text_and_structured(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_lookup(monkeypatch, CASES["이력_다건_혼합신뢰도"])
    r = _call("deal_history", {"address": "구로구 구로동 1128-1"})
    assert r.content[0].text.startswith("■ 실거래 이력 — 구로구 구로동 1128-1")
    assert r.structured_content["transaction_count"] == 2
    assert r.content[0].text == S._render_deal_history(r.structured_content)
    assert r.is_error is False


def test_not_found_is_not_a_tool_error(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_lookup(monkeypatch, CASES["표제부없음"])
    r = _call("deal_history", {"address": "구로구 구로동 9999-99"})
    assert r.is_error is False
    assert r.structured_content["status"] == "NOT_FOUND"
    assert S.NOT_FOUND in r.content[0].text
