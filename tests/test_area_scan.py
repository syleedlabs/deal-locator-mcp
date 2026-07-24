"""area_scan 구조화 출력(structuredContent) 테스트.

test_deal_card_search.py 와 같은 방식 — 개조 전 렌더러를 _legacy_area_scan 으로 보존해
신구 텍스트가 바이트 동일한지 대조하고, coverage 모수가 기계 판독 가능한지 본다.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pandas as pd
import pytest

from deal_locator import server as S
from deal_locator.core import lookup as L


# ── 개조 전 원본 렌더러 (수정 금지 — 골든 기준) ────────────────────────

def _legacy_area_scan(res: dict[str, Any], lo: int | None, hi: int | None,
                      months: int, road_contains: str) -> str:
    st = res["status"]
    if st in ("파싱실패", "구_미상"):
        return f"{S.PARSE_ERROR} {res['message']}"
    if st == "API_오류":
        return f"{S.API_ERROR} {res['message']}"
    if st in ("거래없음", "조건_불일치"):
        return f"{S.NOT_FOUND} {res['message']} {S.NO_GUESS}"

    gu, dong = res["gu"], res["dong"]
    deals = res["deals"]
    lines = [
        f"■ 지역 스캔 — {gu} {dong} · 최근 {months}개월 · 연면적 평당 {S._fmt_band(lo, hi)}",
    ]
    cov = [f"동내 매매 {res['total_in_dong']}건",
           f"해제 {res['cancelled_count']} 제외",
           f"평단가 산출불가 {res['ppp_uncomputable']}",
           f"조건 밖 {res['price_excluded']}"]
    if road_contains.strip():
        cov.append(f"도로명 미상 {res['road_unknown']}")
        cov.append(f"도로명 불일치 {res['road_filtered_out']}")
    lines.append("  [커버리지] " + " · ".join(cov)
                 + f" → 부합 {len(deals)}건"
                 + (f" (+{res['truncated']}건 생략)" if res["truncated"] else ""))
    if road_contains.strip():
        lines.append(f"  ※ 도로명 필터는 표제부 역매칭 성공 거래에만 적용 — "
                     f"미복원 거래 {res['road_unknown']}건은 도로명 특정 불가로 제외됨.")
        rb = res.get("road_unknown_reasons") or {}
        if rb:
            parts = " · ".join(f"{k} {v}" for k, v in
                               sorted(rb.items(), key=lambda kv: -kv[1]))
            lines.append(f"    └ 미상 {res['road_unknown']}건 사유: {parts}")

    for d in deals:
        lines.extend(S._fmt_scan_deal(d))

    if any(d.get("jibun_masked") and not d.get("resolved_address") for d in deals):
        lines.append("  ⚠ '마스킹 미복원' 표기 거래는 지번·주소를 특정하지 못한 것 "
                     "(평단가·거래금액은 실측). 지번 확정은 deal_card_search 로 개별 조회 요망.")
    lines.append(S.NO_GUESS)
    lines.append(S.SOURCE_LINE)
    return "\n".join(lines)


# ── 픽스처 ──────────────────────────────────────────────────────────

def _deal(**over: Any) -> dict[str, Any]:
    d = {
        "address": "성동구 성수동2가 321-90", "resolved_address": True,
        "road": "서울특별시 성동구 연무장길 45", "jibun_masked": True,
        "demask_stage": "stage1", "price": "120억", "price_manwon": 1_200_000,
        "deal_date": "2026-04-11", "ppp_gross_manwon": 48_000,
        "ppp_land_manwon": 92_000, "gross_sqm": 826.4, "gross_pyeong": 250.0,
        "land_sqm": 431.4, "land_pyeong": 130.5, "build_year": 2018,
        "floor": "", "usage": "제2종근린생활", "zone": "준공업",
        "seller": "개인", "buyer": "법인",
    }
    d.update(over)
    return d


def _res(**over: Any) -> dict[str, Any]:
    r = {
        "query": "성수동2가", "parsed": {}, "status": "거래있음",
        "gu": "성동구", "dong": "성수동2가",
        "deals": [_deal()], "period_months": 12,
        "total_in_dong": 59, "cancelled_count": 3, "ppp_uncomputable": 2,
        "price_excluded": 40, "road_unknown": 5, "road_filtered_out": 89,
        "road_unknown_reasons": {}, "truncated": 0,
        "band": (None, None), "road_contains": "", "message": "",
    }
    r.update(over)
    return r


# (res, lo, hi, months, road_contains)
CASES: dict[str, tuple[dict[str, Any], Any, Any, int, str]] = {
    "기본_밴드없음": (_res(), None, None, 12, ""),
    "밴드양쪽_도로명필터": (
        _res(road_unknown_reasons={"비마스킹_도로명미조회": 46, "스펙_오차밖": 5},
             deals=[_deal(), _deal(price="88억", price_manwon=880_000)]),
        45_000, 55_000, 12, "연무장길",
    ),
    "생략발생_마스킹미복원": (
        _res(truncated=17,
             deals=[_deal(resolved_address=False, demask_stage="",
                          address="성수동2가 3**(마스킹·주소미상)", road="")]),
        None, 55_000, 24, "",
    ),
    "하한만": (_res(deals=[_deal(ppp_land_manwon=None, build_year=None, usage="")]),
                40_000, None, 6, ""),
    "조건_불일치": (_res(status="조건_불일치", deals=[],
                        message="성동구 성수동2가 — 평단가 조건은 통과했으나 도로명 '연무장길' 부합 0건 (도로명 미상 5, 불일치 89)."),
                   None, None, 12, "연무장길"),
    "거래없음": (_res(status="거래없음", deals=[],
                     message="성동구 성수동2가 — 최근 12개월 상업업무용 매매 없음"),
                None, None, 12, ""),
    "파싱실패": (_res(status="파싱실패", deals=[], gu="", dong="",
                     message="동을 해석하지 못함: '헛소리'. 예) '성수동1가'"),
                None, None, 12, ""),
    "API오류": (_res(status="API_오류", deals=[],
                    message="실거래 API 조회 실패: TimeoutError()"),
               None, None, 12, ""),
}


@pytest.fixture(autouse=True)
def _key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DEAL_LOCATOR_SERVICE_KEY", "test-key")
    S._cache.clear()  # 스캔 결과가 캐시되므로 케이스 간 격리 필수


def _patch_scan(monkeypatch: pytest.MonkeyPatch, res: dict[str, Any]) -> None:
    monkeypatch.setattr(S, "get_pipeline", lambda: object())
    monkeypatch.setattr(S, "scan_area", lambda *a, **k: res)


def _eok(manwon: int | None) -> float:
    return (manwon / 10000) if manwon else 0.0


# ── 1) 텍스트 회귀 ──────────────────────────────────────────────────

@pytest.mark.parametrize("name", list(CASES))
def test_text_output_is_byte_identical_to_legacy(name: str, monkeypatch: pytest.MonkeyPatch) -> None:
    res, lo, hi, months, road = CASES[name]
    _patch_scan(monkeypatch, res)
    d = S._area_scan_payload("성수동2가", _eok(lo), _eok(hi), months, road, 30)
    assert S._render_area_scan(d) == _legacy_area_scan(res, lo, hi, months, road), \
        f"[{name}] 텍스트 출력이 개조 전과 달라짐"


def test_config_error_text_unchanged(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DEAL_LOCATOR_SERVICE_KEY", raising=False)
    monkeypatch.delenv("DATA_GO_KR_API_KEY", raising=False)
    d = S._area_scan_payload("성수동2가", 0.0, 0.0, 12, "", 30)
    assert d["status"] == "CONFIG_ERROR"
    assert S._render_area_scan(d) == f"{S.CONFIG_ERROR} {S.KEY_MISSING_MSG}"


# ── 2) 구조화 출력 계약 ─────────────────────────────────────────────

def test_coverage_is_machine_readable(monkeypatch: pytest.MonkeyPatch) -> None:
    res, lo, hi, months, road = CASES["밴드양쪽_도로명필터"]
    _patch_scan(monkeypatch, res)
    d = S._area_scan_payload("성수동2가", _eok(lo), _eok(hi), months, road, 30)

    c = d["coverage"]
    assert d["status"] == "OK"
    assert d["area"] == {"gu": "성동구", "dong": "성수동2가"}
    # 모수 분해 — "부합 2건"만 보고 대표성을 오판하지 않도록
    assert (c["total_in_dong"], c["matched"]) == (59, 2)
    assert (c["cancelled_count"], c["ppp_uncomputable"], c["price_excluded"]) == (3, 2, 40)
    assert (c["road_unknown"], c["road_filtered_out"]) == (5, 89)
    # 미상 사유는 건수 내림차순 배열
    assert c["road_unknown_reasons"] == [
        {"reason": "비마스킹_도로명미조회", "count": 46},
        {"reason": "스펙_오차밖", "count": 5},
    ]
    assert d["deals"][0]["price_manwon"] == 1_200_000
    assert d["deals"][0]["ppp_gross_manwon"] == 48_000


def test_filter_reports_applied_values(monkeypatch: pytest.MonkeyPatch) -> None:
    """억/평 입력이 만원 단위로 환산되고, 뒤집힌 밴드는 스왑된 값이 보고된다."""
    _patch_scan(monkeypatch, _res())
    d = S._area_scan_payload("성수동2가", 5.5, 4.5, 12, "  연무장길  ", 30)
    f = d["filter"]
    assert f["min_manwon_per_gross_pyeong"] == 45_000
    assert f["max_manwon_per_gross_pyeong"] == 55_000
    assert f["road_contains"] == "연무장길"  # 공백 제거된 실제 적용값


def test_clamps(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_scan(monkeypatch, _res())
    assert S._area_scan_payload("x", 0, 0, 999, "", 9999)["period_months"] == 60
    assert S._area_scan_payload("x", 0, 0, 999, "", 9999)["filter"]["limit"] == 200
    assert S._area_scan_payload("x", 0, 0, 0, "", 0)["filter"]["limit"] == 1


def test_not_found_carries_no_deals(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("조건_불일치", "거래없음"):
        res, lo, hi, months, road = CASES[name]
        _patch_scan(monkeypatch, res)
        d = S._area_scan_payload("성수동2가", _eok(lo), _eok(hi), months, road, 30)
        assert d["status"] == "NOT_FOUND", name
        assert d["deals"] == [], name
        assert d["message"], name


def test_payload_matches_output_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    jsonschema = pytest.importorskip("jsonschema")
    for name, (res, lo, hi, months, road) in CASES.items():
        _patch_scan(monkeypatch, res)
        d = S._area_scan_payload("성수동2가", _eok(lo), _eok(hi), months, road, 30)
        jsonschema.validate(d, S.AREA_SCAN_OUTPUT_SCHEMA)


# ── 3) MCP 프로토콜 레벨 ────────────────────────────────────────────

def test_tool_advertises_schema_and_annotations() -> None:
    from fastmcp import Client

    async def run():
        async with Client(S.mcp) as c:
            return await c.list_tools()

    dc = {t.name: t for t in asyncio.run(run())}["area_scan"]
    assert dc.outputSchema is not None
    assert "coverage" in dc.outputSchema["properties"]
    assert dc.annotations.readOnlyHint is True
    assert dc.annotations.openWorldHint is True


def test_call_tool_returns_both_text_and_structured(monkeypatch: pytest.MonkeyPatch) -> None:
    from fastmcp import Client

    _patch_scan(monkeypatch, _res())

    async def run():
        async with Client(S.mcp) as c:
            return await c.call_tool("area_scan", {"area": "성수동2가"})

    r = asyncio.run(run())
    assert r.content[0].text.startswith("■ 지역 스캔 — 성동구 성수동2가")
    assert r.structured_content["coverage"]["total_in_dong"] == 59
    assert r.content[0].text == S._render_area_scan(r.structured_content)


# ── 4) 동 시세 평균 aggregate ───────────────────────────────────────

def test_ppp_stats_helper() -> None:
    """평균·중앙·범위·사분위를 정확히 산출하고, 빈 표본은 None."""
    g = L._ppp_agg([45_000, 50_000, 55_000, 60_000, 70_000])
    assert g == {"n": 5, "mean_manwon": 56_000, "median_manwon": 55_000,
                 "min_manwon": 45_000, "max_manwon": 70_000,
                 "p25_manwon": 50_000, "p75_manwon": 60_000}
    assert L._ppp_agg([]) is None
    s = L._ppp_stats([48_000], [])
    assert s["gross"]["n"] == 1 and s["gross"]["mean_manwon"] == 48_000
    assert s["land"] is None  # 대지 표본 없음


# ── 가짜 파이프라인: 정규화·역매칭을 통과시켜 raw 로직만 검증 ──────
class _FakePipeline:
    def __init__(self, df: pd.DataFrame) -> None:
        self._df = df

    def fetch_from_api_multi_month(self, months: int = 12, gus: Any = None) -> pd.DataFrame:
        return self._df

    def normalize_columns(self, raw: pd.DataFrame, source: str = "api") -> pd.DataFrame:
        return raw

    def bulk_match(self, sub: pd.DataFrame) -> pd.DataFrame:
        return sub  # 역매칭 결과 컬럼 없음 → 통계는 이와 무관해야 정상


def _row(price_manwon: int, gross_sqm: float, land_sqm: float,
         cancelled: str = "", ym: str = "202605") -> dict[str, Any]:
    return {
        "시군구": "서울특별시 성동구 성수동2가",
        "해제사유발생일": cancelled,
        "거래금액(만원)": str(price_manwon),
        "전용/연면적(㎡)": gross_sqm, "대지면적(㎡)": land_sqm,
        "지번": "321-90", "건축년도": 2018, "층": "", "건축물주용도": "제2종근린생활",
        "용도지역": "준공업", "매도": "개인", "매수": "법인",
        "계약년월": ym, "계약일": 8,
    }


# 연면적 10평(=33.05785㎡), 대지 20평(=66.1157㎡) 고정 → 평단가 = 금액/평
_PY10, _PY20 = 33.05785, 66.1157


def test_stats_uses_raw_before_band_filter(monkeypatch: pytest.MonkeyPatch) -> None:
    """핵심: 동 시세 통계는 가격대 필터로 표본이 줄지 않는다(국토부 원본 전수).

    거래 5건(연면적 평단가 4.5~7.0억) 중 밴드 5.2~5.8억은 1건만 통과하지만,
    통계 표본 n 은 5건 전체여야 한다 — 매칭·필터가 평균을 편향시키면 안 됨.
    """
    df = pd.DataFrame([
        _row(450_000, _PY10, _PY20),  # 4.5억/평
        _row(500_000, _PY10, _PY20),  # 5.0
        _row(550_000, _PY10, _PY20),  # 5.5  ← 밴드 통과
        _row(600_000, _PY10, _PY20),  # 6.0
        _row(700_000, _PY10, _PY20),  # 7.0
        _row(300_000, _PY10, _PY20, cancelled="20260601"),  # 해제 → 제외
        _row(400_000, None, _PY20),                          # 연면적 결측 → 산출불가
    ])
    res = L.scan_area(_FakePipeline(df), "성동구 성수동2가",
                      ppp_min_manwon=52_000, ppp_max_manwon=58_000)

    assert res["status"] == "거래있음"
    assert res["total_in_dong"] == 7
    assert res["cancelled_count"] == 1
    assert res["ppp_uncomputable"] == 1
    assert len(res["deals"]) == 1          # 밴드 통과분
    assert res["price_excluded"] == 4

    g = res["stats"]["gross"]
    assert g["n"] == 5                     # ← 밴드와 무관한 전수 표본
    assert g["mean_manwon"] == 56_000
    assert g["median_manwon"] == 55_000
    assert (g["min_manwon"], g["max_manwon"]) == (45_000, 70_000)
    land = res["stats"]["land"]
    assert land["n"] == 5                  # 대지 평단가도 전수
    assert land["mean_manwon"] == 28_000


def test_stats_survives_zero_band_match(monkeypatch: pytest.MonkeyPatch) -> None:
    """밴드에 0건 부합(조건_불일치)이어도 동 시세는 산출·반환된다."""
    df = pd.DataFrame([
        _row(450_000, _PY10, _PY20),
        _row(500_000, _PY10, _PY20),
    ])
    res = L.scan_area(_FakePipeline(df), "성동구 성수동2가",
                      ppp_min_manwon=90_000, ppp_max_manwon=99_000)
    assert res["status"] == "조건_불일치"
    assert res["deals"] == []
    assert res["stats"]["gross"]["n"] == 2  # 통계는 남는다


def test_stats_renders_in_text(monkeypatch: pytest.MonkeyPatch) -> None:
    """payload 에 stats 가 있으면 '[동 시세]' 라인이 텍스트에 나온다."""
    stats = {"gross": {"n": 54, "mean_manwon": 46_000, "median_manwon": 44_000,
                       "min_manwon": 31_000, "max_manwon": 72_000,
                       "p25_manwon": 40_000, "p75_manwon": 52_000},
             "land": {"n": 51, "mean_manwon": 88_000, "median_manwon": 85_000,
                      "min_manwon": 60_000, "max_manwon": 120_000,
                      "p25_manwon": 78_000, "p75_manwon": 96_000}}
    _patch_scan(monkeypatch, _res(stats=stats))
    d = S._area_scan_payload("성수동2가", 0.0, 0.0, 12, "", 30)
    txt = S._render_area_scan(d)
    assert ("  [동 시세] 연면적 평당 평균 46,000만원/평 · 중앙 44,000 · "
            "흔한구간 40,000~52,000 · 전체 31,000~72,000 "
            "(표본 54건 · 가격대·도로명 필터 무관 · 국토부 원본)") in txt
    assert ("    └ 대지 평당 평균 88,000만원/평 · 중앙 85,000 · "
            "흔한구간 78,000~96,000 (표본 51건)") in txt


def test_stats_absent_renders_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    """stats 없거나 표본 0이면 시세 라인은 추가되지 않는다(레거시 텍스트 보존)."""
    _patch_scan(monkeypatch, _res())  # stats 키 없음
    d = S._area_scan_payload("성수동2가", 0.0, 0.0, 12, "", 30)
    assert "[동 시세]" not in S._render_area_scan(d)
    _patch_scan(monkeypatch, _res(stats={"gross": None, "land": None}))
    d2 = S._area_scan_payload("성수동2가", 0.0, 0.0, 12, "", 30)
    assert "[동 시세]" not in S._render_area_scan(d2)


def test_stats_schema_validates(monkeypatch: pytest.MonkeyPatch) -> None:
    jsonschema = pytest.importorskip("jsonschema")
    stats = {"gross": {"n": 3, "mean_manwon": 50_000, "median_manwon": 50_000,
                       "min_manwon": 45_000, "max_manwon": 55_000,
                       "p25_manwon": 47_500, "p75_manwon": 52_500},
             "land": None}
    _patch_scan(monkeypatch, _res(stats=stats))
    d = S._area_scan_payload("성수동2가", 0.0, 0.0, 12, "", 30)
    jsonschema.validate(d, S.AREA_SCAN_OUTPUT_SCHEMA)
