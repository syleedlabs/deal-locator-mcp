"""deal_card_search 구조화 출력(structuredContent) 테스트.

두 가지를 지킨다.
1) 회귀 방지 — 개조 전 렌더러(_legacy_deal_card_search, 아래 보존)와 신 렌더러의
   텍스트가 바이트 단위로 동일해야 한다. LLM/사람이 읽는 계약은 불변.
2) 신규 계약 — structuredContent 가 output_schema 를 만족하고,
   status 가 환각 차단 규약(OK 일 때만 수치 존재)을 지켜야 한다.

네트워크·API 키 없이 돈다 (_cached_lookup 을 페이크로 대체).
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from deal_locator import server as S


# ── 개조 전 원본 렌더러 (수정 금지 — 골든 기준) ────────────────────────

def _legacy_deal_card_search(res: dict[str, Any]) -> str:
    p = res["parsed"]
    head = f"{p['gu']} {p['dong']} {p['bunji']}"
    b = res.get("building")

    if res["status"] == "표제부없음":
        return (f"{S.NOT_FOUND} {head} — 건축물대장 표제부와 매칭 실거래가 모두 없음 "
                f"(번지 오타/신축/멸실 가능). {S.NO_GUESS}")

    lines = [f"■ 실거래 종합카드 — {head}"]
    t = res.get("latest")
    if t:
        lines.append(f"  최신 실거래: {S._fmt_tx_line(t)}")
        if t.get("price_per_land_pyeong_manwon"):
            lines.append(f"  평단가(토지): {t['price_per_land_pyeong_manwon']:,}만원/평")
        area = S._fmt_areas(t)
        if area:
            lines.append(f"  거래 명세: {area}" + (f" · 준공 {t['build_year']}" if t.get("build_year") else ""))
        if t.get("zone"):
            lines.append(f"  용도지역: {t['zone']}")
        basis = t.get("match_basis", {})
        if basis.get("reasons"):
            lines.append(f"  매칭 근거: {' / '.join(basis['reasons'])}")
        if len(res["transactions"]) > 1:
            lines.append(f"  기간 내 매칭 거래 총 {len(res['transactions'])}건 (deal_history 로 전체 확인)")
    else:
        lines.append(f"  {S.NOT_FOUND} 최근 {res['period_months']}개월 내 매칭 실거래 없음")

    if b:
        lines.append(f"  [건축물대장] {b['address']}"
                     + (f" · {b['road_address']}" if b.get("road_address") else ""))
        spec = []
        if b.get("land"):
            spec.append(f"대지 {b['land']:,.1f}㎡({b['land_pyeong']:,}평)")
        if b.get("gross"):
            spec.append(f"연면적 {b['gross']:,.1f}㎡({b['gross_pyeong']:,}평)")
        if b.get("build_year"):
            spec.append(f"준공 {b['build_year']}")
        if spec:
            lines.append("  " + " · ".join(spec))

    lines.extend("  " + n for n in S._context_notes(res))
    if t and S._caveat(t):
        lines.append(f"  ⚠ {S._caveat(t)}")
    lines.append(S.SOURCE_LINE)
    return "\n".join(lines)


# ── 픽스처 ──────────────────────────────────────────────────────────

def _tx(**over: Any) -> dict[str, Any]:
    t = {
        "price": "859억", "price_manwon": 8_590_000, "deal_date": "2025-03-14",
        "seller": "법인", "buyer": "법인", "floor": "", "building_type": "일반",
        "usage": "업무시설", "zone": "일반상업지역",
        "gross_sqm": 25_910.4, "land_sqm": 2_589.0,
        "land_pyeong": 783.0, "gross_pyeong": 7_838.0,
        "price_per_land_pyeong_manwon": 10_965,
        "build_year": 2005, "jibun_masked": True, "raw_jibun": "1***",
        "confidence": "정확매칭", "confidence_score": 0.97,
        "match_stage": "exact",
        "match_basis": {"confidence": "정확매칭", "confidence_score": 0.97,
                        "stage": "exact", "reasons": ["연면적 일치", "대지 일치", "건축년도 일치"],
                        "anchor": {}, "row": {}, "caveat": ""},
    }
    t.update(over)
    return t


def _res(**over: Any) -> dict[str, Any]:
    r = {
        "query": "구로구 구로동 1128-1",
        "parsed": {"gu": "구로구", "dong": "구로동", "bunji": "1128-1"},
        "status": "거래있음",
        "building": {"address": "서울 구로구 구로동 1128-1", "road_address": "디지털로 300",
                     "land": 2_589.0, "gross": 25_910.4,
                     "land_pyeong": 783.0, "gross_pyeong": 7_838.0,
                     "build_year": 2005, "building_count": 1,
                     "anchor_bunji": "1128-1", "anchor_via": "지번"},
        "match_context": {"anchor_bunji": "1128-1", "anchor_via": "지번",
                          "lot_set": ["1128-1"], "multi_lot": False},
        "transactions": [_tx()], "latest": _tx(),
        "cancelled_count": 0, "period_months": 12, "message": "",
    }
    r.update(over)
    return r


CASES: dict[str, dict[str, Any]] = {
    "거래있음_기본": _res(),
    "거래있음_다건_해제_부속지번_caveat": _res(
        transactions=[_tx(), _tx(price="700억")],
        latest=_tx(confidence="추정매칭", confidence_score=0.72,
                   match_basis={"reasons": ["연면적 일치"], "caveat": "동일 스펙 인접 건물 가능성"}),
        match_context={"anchor_bunji": "100-4", "anchor_via": "부속지번",
                       "lot_set": ["100-4", "103-2"], "multi_lot": True},
        cancelled_count=2,
    ),
    "거래있음_최소필드": _res(
        latest=_tx(zone="", land_sqm=None, gross_sqm=None, land_pyeong=None,
                   gross_pyeong=None, build_year=None,
                   price_per_land_pyeong_manwon=None, seller="", buyer="",
                   usage="", jibun_masked=False,
                   match_basis={"reasons": [], "caveat": ""}),
        transactions=[_tx()],
        building=None,
    ),
    "거래없음_표제부있음": _res(status="거래없음", transactions=[], latest=None),
    "표제부없음": _res(status="표제부없음", transactions=[], latest=None, building=None),
}


@pytest.fixture(autouse=True)
def _no_network(monkeypatch: pytest.MonkeyPatch) -> None:
    """API 키/네트워크 없이 돌도록 가드와 조회를 페이크로."""
    monkeypatch.setenv("DEAL_LOCATOR_SERVICE_KEY", "test-key")
    S._cache.clear()


def _patch_lookup(monkeypatch: pytest.MonkeyPatch, res: dict[str, Any]) -> None:
    monkeypatch.setattr(S, "_cached_lookup", lambda address, months: res)


# ── 1) 텍스트 회귀 ──────────────────────────────────────────────────

@pytest.mark.parametrize("name", list(CASES))
def test_text_output_is_byte_identical_to_legacy(name: str, monkeypatch: pytest.MonkeyPatch) -> None:
    res = CASES[name]
    _patch_lookup(monkeypatch, res)
    new = S._render_deal_card_search(S._deal_card_search_payload("구로구 구로동 1128-1", 12))
    assert new == _legacy_deal_card_search(res), f"[{name}] 텍스트 출력이 개조 전과 달라짐"


def test_error_texts_unchanged(monkeypatch: pytest.MonkeyPatch) -> None:
    # 키 미설정 → CONFIG_ERROR
    monkeypatch.delenv("DEAL_LOCATOR_SERVICE_KEY", raising=False)
    monkeypatch.delenv("DATA_GO_KR_API_KEY", raising=False)
    d = S._deal_card_search_payload("구로구 구로동 1128-1", 12)
    assert d["status"] == "CONFIG_ERROR"
    assert S._render_deal_card_search(d) == f"{S.CONFIG_ERROR} {S.KEY_MISSING_MSG}"

    # 파싱 실패 → PARSE_ERROR
    monkeypatch.setenv("DEAL_LOCATOR_SERVICE_KEY", "test-key")
    _patch_lookup(monkeypatch, {"status": "파싱실패", "message": "지번 해석 실패"})
    d = S._deal_card_search_payload("헛소리", 12)
    assert d["status"] == "PARSE_ERROR"
    assert S._render_deal_card_search(d) == f"{S.PARSE_ERROR} 지번 해석 실패"

    # API 오류 → EXTERNAL_API_ERROR
    _patch_lookup(monkeypatch, {"status": "API_오류", "message": "타임아웃"})
    d = S._deal_card_search_payload("구로구 구로동 1128-1", 12)
    assert d["status"] == "EXTERNAL_API_ERROR"
    assert S._render_deal_card_search(d) == f"{S.API_ERROR} 타임아웃"


# ── 2) 구조화 출력 계약 ─────────────────────────────────────────────

def test_structured_payload_carries_measured_values(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_lookup(monkeypatch, CASES["거래있음_기본"])
    d = S._deal_card_search_payload("구로구 구로동 1128-1", 12)

    assert d["status"] == "OK"
    assert d["lookup_status"] == "거래있음"
    assert d["address"] == {"gu": "구로구", "dong": "구로동", "bunji": "1128-1"}
    # 계산용 원시 수치가 문자열이 아니라 숫자로 나온다 (텍스트 파싱 불필요)
    assert d["latest"]["price_manwon"] == 8_590_000
    assert d["latest"]["price_per_land_pyeong_manwon"] == 10_965
    assert d["latest"]["confidence_score"] == pytest.approx(0.97)
    # 마스킹 역매칭 사실과 근거가 기계 판독 가능
    assert d["latest"]["jibun_masked"] is True
    assert d["latest"]["match_reasons"] == ["연면적 일치", "대지 일치", "건축년도 일치"]
    assert d["building"]["gross_sqm"] == pytest.approx(25_910.4)
    assert d["source"] == S.SOURCE_LINE


def test_not_found_carries_no_numbers(monkeypatch: pytest.MonkeyPatch) -> None:
    """환각 차단 규약: 거래가 없으면 latest 는 반드시 null."""
    for name in ("거래없음_표제부있음", "표제부없음"):
        _patch_lookup(monkeypatch, CASES[name])
        d = S._deal_card_search_payload("구로구 구로동 1128-1", 12)
        assert d["status"] == "NOT_FOUND", name
        assert d["latest"] is None, name
        assert d["message"], name  # 사유가 비어있지 않다


def test_months_is_clamped(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DEAL_LOCATOR_SERVICE_KEY", raising=False)
    assert S._deal_card_search_payload("x", 999)["period_months"] == 60
    assert S._deal_card_search_payload("x", 0)["period_months"] == 1


def test_payload_matches_output_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    """모든 케이스의 payload 가 선언한 output_schema 를 만족하는가."""
    jsonschema = pytest.importorskip("jsonschema")
    for name, res in CASES.items():
        _patch_lookup(monkeypatch, res)
        d = S._deal_card_search_payload("구로구 구로동 1128-1", 12)
        jsonschema.validate(d, S.DEAL_CARD_SEARCH_OUTPUT_SCHEMA)  # 위반 시 예외


# ── 3) MCP 프로토콜 레벨 (in-process 클라이언트) ─────────────────────

def test_tool_advertises_schema_and_annotations() -> None:
    from fastmcp import Client

    async def run():
        async with Client(S.mcp) as c:
            return await c.list_tools()

    tools = {t.name: t for t in asyncio.run(run())}
    dc = tools["deal_card_search"]
    assert dc.outputSchema is not None, "outputSchema 가 tools/list 에 노출되지 않음"
    assert dc.outputSchema["properties"]["status"]["enum"][0] == "OK"
    ann = dc.annotations
    assert ann is not None and ann.readOnlyHint is True
    # 파일시스템 서버와 달리 외부 공공 API 를 호출한다
    assert ann.openWorldHint is True


def test_call_tool_returns_both_text_and_structured(monkeypatch: pytest.MonkeyPatch) -> None:
    from fastmcp import Client

    _patch_lookup(monkeypatch, CASES["거래있음_기본"])

    async def run():
        async with Client(S.mcp) as c:
            return await c.call_tool("deal_card_search", {"address": "구로구 구로동 1128-1"})

    r = asyncio.run(run())
    assert r.content[0].text.startswith("■ 실거래 종합카드 — 구로구 구로동 1128-1")
    assert r.structured_content["status"] == "OK"
    assert r.structured_content["latest"]["price_manwon"] == 8_590_000
    # 텍스트와 구조화가 같은 payload 에서 나왔는지
    assert r.content[0].text == S._render_deal_card_search(r.structured_content)
    assert r.is_error is False


def _call(name: str, args: dict[str, Any]):
    from fastmcp import Client

    async def run():
        async with Client(S.mcp) as c:
            # 에러도 결과로 받아서 검사한다(기본값은 예외를 던짐)
            return await c.call_tool(name, args, raise_on_error=False)

    return asyncio.run(run())


def test_not_found_is_not_a_tool_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """데이터 부재는 '실패'가 아니다 — isError 로 만들면 재시도·환각을 유발한다."""
    for name in ("거래없음_표제부있음", "표제부없음"):
        _patch_lookup(monkeypatch, CASES[name])
        r = _call("deal_card_search", {"address": "구로구 구로동 1128-1"})
        assert r.is_error is False, name
        assert r.structured_content["status"] == "NOT_FOUND", name
        assert S.NOT_FOUND in r.content[0].text, name


def test_config_and_api_failures_are_tool_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DEAL_LOCATOR_SERVICE_KEY", raising=False)
    monkeypatch.delenv("DATA_GO_KR_API_KEY", raising=False)
    r = _call("deal_card_search", {"address": "구로구 구로동 1128-1"})
    assert r.is_error is True
    assert S.CONFIG_ERROR in r.content[0].text

    monkeypatch.setenv("DEAL_LOCATOR_SERVICE_KEY", "test-key")
    _patch_lookup(monkeypatch, {"status": "API_오류", "message": "타임아웃"})
    r = _call("deal_card_search", {"address": "구로구 구로동 1128-1"})
    assert r.is_error is True
    assert S.API_ERROR in r.content[0].text
