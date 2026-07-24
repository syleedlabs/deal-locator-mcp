"""match_explain 구조화 출력(structuredContent) 테스트.

개조 전 렌더러를 _legacy_match_explain 으로 보존해 신구 텍스트를 바이트 대조하고,
앵커 vs 거래행 대조(comparison)가 감사 자동화에 쓸 수 있는지 본다.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from deal_locator import server as S


# ── 개조 전 원본 렌더러 (수정 금지 — 골든 기준) ────────────────────────

def _legacy_match_explain(res: dict[str, Any]) -> str:
    p = res["parsed"]
    head = f"{p['gu']} {p['dong']} {p['bunji']}"
    t = res.get("latest")
    if not t:
        return (f"{S.NOT_FOUND} {head} — 설명할 매칭 거래가 없음 "
                f"(기간 {res['period_months']}개월). {S.NO_GUESS}")

    basis = t["match_basis"]
    a, r = basis.get("anchor") or {}, basis.get("row") or {}
    lines = [
        f"■ 매칭 근거 — {head} · {t['price']} ({t['deal_date']})",
        f"  판정: {basis['confidence']} (score {basis['confidence_score']:.2f}, {basis['stage']})",
        f"  사유: {' / '.join(basis['reasons']) or '-'}",
    ]
    if a:
        lines.append(f"  [표제부 앵커] {a.get('address','')} · "
                     f"연면적 {a.get('gross_sqm')}㎡ · 대지 {a.get('land_sqm')}㎡ · 준공 {a.get('build_year')}")
    lines.append(f"  [거래행] 지번 '{r.get('raw_jibun','')}' · "
                 f"연면적 {r.get('gross_sqm')}㎡ · 대지 {r.get('land_sqm')}㎡ · 건축년도 {r.get('build_year')}")
    mc = res.get("match_context") or {}
    lines.append(f"  필지세트: {', '.join(mc.get('lot_set', [])) or '-'}"
                 + (" (다필지)" if basis.get("multi_lot") else " (단일)"))
    lines.append(f"  앵커 경로: {mc.get('anchor_via','-')}"
                 + (f" → 대표지번 {mc['anchor_bunji']}" if mc.get("anchor_via") == "부속지번" else ""))
    if basis.get("caveat"):
        lines.append(f"  ⚠ {basis['caveat']}")
    lines.append("  ※ 부속지번 대장은 등재 누락 가능 — 매칭 보강용이며 단독 확정 근거가 아님.")
    lines.append(S.SOURCE_LINE)
    return "\n".join(lines)


# ── 픽스처 ──────────────────────────────────────────────────────────

def _basis(**over: Any) -> dict[str, Any]:
    b = {
        "stage": "stage1", "confidence": "정확매칭", "confidence_score": 0.97,
        "anchor": {"address": "서울특별시 구로구 구로동 1128-1",
                   "gross_sqm": 18_500.83, "land_sqm": 2_589.6, "build_year": 2010},
        "row": {"gross_sqm": 18_500.83, "land_sqm": 2_589.6, "build_year": 2010,
                "raw_jibun": "서울특별시 구로구 구로동 1***"},
        "reasons": ["건축년도·연면적·대지면적 정확 일치"],
        "multi_lot": False, "lot_set": ["1128-1"], "caveat": "",
    }
    b.update(over)
    return b


def _tx(**over: Any) -> dict[str, Any]:
    t = {"price": "859억", "price_manwon": 8_590_000, "deal_date": "2026-06-09",
         "confidence": "정확매칭", "confidence_score": 0.97, "match_stage": "stage1",
         "match_basis": _basis()}
    t.update(over)
    return t


def _res(**over: Any) -> dict[str, Any]:
    r = {
        "query": "구로구 구로동 1128-1",
        "parsed": {"gu": "구로구", "dong": "구로동", "bunji": "1128-1"},
        "status": "거래있음",
        "match_context": {"anchor_bunji": "1128-1", "anchor_via": "직접",
                          "lot_set": ["1128-1"], "multi_lot": False},
        "building": {"address": "서울특별시 구로구 구로동 1128-1"},
        "transactions": [_tx()], "latest": _tx(),
        "cancelled_count": 0, "period_months": 12, "message": "",
    }
    r.update(over)
    return r


CASES: dict[str, dict[str, Any]] = {
    "정확매칭_단일필지": _res(),
    "추정매칭_오차_caveat": _res(latest=_tx(
        confidence="추정매칭", confidence_score=0.60,
        match_basis=_basis(
            stage="stage3", confidence="추정매칭", confidence_score=0.60,
            reasons=["오차범위 ±3% 내 연면적 일치"],
            row={"gross_sqm": 18_000.0, "land_sqm": 2_500.0, "build_year": 2009,
                 "raw_jibun": "서울특별시 구로구 구로동 1***"},
            caveat="추정매칭 — 동일 스펙 인접 건물 가능성. 공부(등기/대장) 대조로 확정하세요."))),
    "부속지번_다필지": _res(
        latest=_tx(match_basis=_basis(multi_lot=True, lot_set=["100-4", "103-2"])),
        match_context={"anchor_bunji": "100-4", "anchor_via": "부속지번",
                       "lot_set": ["100-4", "103-2"], "multi_lot": True},
    ),
    # 지번이 노출된 거래 — 스펙 대조 없이 매칭돼 앵커가 비어 있다
    "지번노출_앵커없음": _res(latest=_tx(match_basis=_basis(
        stage="exact_jibun", confidence="확정", confidence_score=1.00,
        anchor={}, reasons=["노출 지번이 질의 지번과 일치"],
        row={"gross_sqm": None, "land_sqm": None, "build_year": None,
             "raw_jibun": "서울특별시 구로구 구로동 1128-1"}))),
    "인접본번_후보": _res(
        latest=_tx(match_basis=_basis(
            stage="neighbor_exact", confidence="인접후보", confidence_score=0.50,
            reasons=["질의 지번(612-99)엔 건물 표제부 없음", "동일 본번 인접 612-82의 정확일치"],
            caveat="인접후보 — 질의 지번엔 건물 표제부가 없고, 동일 본번 인접 지번의 정확일치 거래입니다.")),
        match_context={"anchor_bunji": "612-82", "anchor_via": "인접본번",
                       "lot_set": ["612-82"], "multi_lot": False},
    ),
    "설명할_거래없음": _res(status="거래없음", transactions=[], latest=None),
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
    new = S._render_match_explain(S._match_explain_payload("구로구 구로동 1128-1", 12))
    assert new == _legacy_match_explain(res), f"[{name}] 텍스트 출력이 개조 전과 달라짐"


def test_error_texts_unchanged(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DEAL_LOCATOR_SERVICE_KEY", raising=False)
    monkeypatch.delenv("DATA_GO_KR_API_KEY", raising=False)
    d = S._match_explain_payload("구로구 구로동 1128-1", 12)
    assert d["status"] == "CONFIG_ERROR"
    assert S._render_match_explain(d) == f"{S.CONFIG_ERROR} {S.KEY_MISSING_MSG}"

    monkeypatch.setenv("DEAL_LOCATOR_SERVICE_KEY", "test-key")
    _patch_lookup(monkeypatch, {"status": "API_오류", "message": "타임아웃"})
    d = S._match_explain_payload("구로구 구로동 1128-1", 12)
    assert d["status"] == "EXTERNAL_API_ERROR"
    assert S._render_match_explain(d) == f"{S.API_ERROR} 타임아웃"


# ── 2) 구조화 출력 계약 ─────────────────────────────────────────────

def test_comparison_exposes_anchor_vs_row(monkeypatch: pytest.MonkeyPatch) -> None:
    """감사 자동화: 무엇이 얼마나 달라서 이 신뢰도가 나왔는지 기계가 재검증한다."""
    _patch_lookup(monkeypatch, CASES["추정매칭_오차_caveat"])
    d = S._match_explain_payload("구로구 구로동 1128-1", 12)

    assert d["status"] == "OK"
    assert d["verdict"]["stage"] == "stage3"
    assert d["verdict"]["confidence_score"] == pytest.approx(0.60)
    assert d["verdict"]["caveat"]

    c = d["comparison"]
    assert c["gross_sqm"]["anchor"] == pytest.approx(18_500.83)
    assert c["gross_sqm"]["row"] == pytest.approx(18_000.0)
    assert c["gross_sqm"]["delta"] == pytest.approx(-500.83)
    assert c["gross_sqm"]["equal"] is False
    assert c["build_year"]["delta"] == -1
    assert c["land_sqm"]["equal"] is False


def test_comparison_marks_exact_match(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_lookup(monkeypatch, CASES["정확매칭_단일필지"])
    c = S._match_explain_payload("구로구 구로동 1128-1", 12)["comparison"]
    assert all(c[f]["equal"] is True and c[f]["delta"] == 0
               for f in ("gross_sqm", "land_sqm", "build_year"))


def test_no_anchor_means_no_comparison(monkeypatch: pytest.MonkeyPatch) -> None:
    """지번 노출 매칭엔 표제부 대조가 없다 — 없는 대조를 지어내지 않는다."""
    _patch_lookup(monkeypatch, CASES["지번노출_앵커없음"])
    d = S._match_explain_payload("구로구 구로동 1128-1", 12)
    assert d["anchor"] is None
    assert d["comparison"] is None
    assert d["row"]["raw_jibun"] == "서울특별시 구로구 구로동 1128-1"
    assert d["verdict"]["confidence_score"] == pytest.approx(1.00)


def test_partial_values_yield_null_delta(monkeypatch: pytest.MonkeyPatch) -> None:
    """한쪽이 결측이면 delta·equal 은 null — 0 으로 뭉개지 않는다."""
    res = _res(latest=_tx(match_basis=_basis(
        row={"gross_sqm": None, "land_sqm": 2_589.6, "build_year": 2010,
             "raw_jibun": "1***"})))
    _patch_lookup(monkeypatch, res)
    c = S._match_explain_payload("구로구 구로동 1128-1", 12)["comparison"]
    assert c["gross_sqm"]["row"] is None
    assert c["gross_sqm"]["delta"] is None
    assert c["gross_sqm"]["equal"] is None
    assert c["land_sqm"]["equal"] is True


def test_anchor_path_and_lots(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_lookup(monkeypatch, CASES["부속지번_다필지"])
    d = S._match_explain_payload("구로구 구로동 103-2", 12)
    assert d["anchor_via"] == "부속지번"
    assert d["anchor_bunji"] == "100-4"
    assert d["lot_set"] == ["100-4", "103-2"]
    assert d["multi_lot"] is True


def test_not_found_carries_no_verdict(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_lookup(monkeypatch, CASES["설명할_거래없음"])
    d = S._match_explain_payload("구로구 구로동 1128-1", 12)
    assert d["status"] == "NOT_FOUND"
    assert (d["verdict"], d["anchor"], d["row"], d["comparison"], d["deal"]) == \
        (None, None, None, None, None)
    assert d["message"]


def test_payload_matches_output_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    jsonschema = pytest.importorskip("jsonschema")
    for name, res in CASES.items():
        _patch_lookup(monkeypatch, res)
        d = S._match_explain_payload("구로구 구로동 1128-1", 12)
        jsonschema.validate(d, S.MATCH_EXPLAIN_OUTPUT_SCHEMA)


# ── 3) MCP 프로토콜 레벨 ────────────────────────────────────────────

def test_tool_advertises_schema_and_annotations() -> None:
    from fastmcp import Client

    async def run():
        async with Client(S.mcp) as c:
            return await c.list_tools()

    t = {x.name: x for x in asyncio.run(run())}["match_explain"]
    assert t.outputSchema is not None
    assert "comparison" in t.outputSchema["properties"]
    assert t.annotations.readOnlyHint is True
    assert t.annotations.openWorldHint is True


def test_call_tool_returns_both_text_and_structured(monkeypatch: pytest.MonkeyPatch) -> None:
    from fastmcp import Client

    _patch_lookup(monkeypatch, CASES["정확매칭_단일필지"])

    async def run():
        async with Client(S.mcp) as c:
            return await c.call_tool("match_explain", {"address": "구로구 구로동 1128-1"})

    r = asyncio.run(run())
    assert r.content[0].text.startswith("■ 매칭 근거 — 구로구 구로동 1128-1")
    assert r.structured_content["verdict"]["stage"] == "stage1"
    assert r.content[0].text == S._render_match_explain(r.structured_content)
    assert r.is_error is False
