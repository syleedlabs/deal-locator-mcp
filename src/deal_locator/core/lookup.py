"""deal_locator.core.lookup — 지번 실거래 조회 오케스트레이터
==============================================================================
MCP 서버(deal-locator-mcp)의 코어. 파이프라인 조각을 엮어 한 지번의 실거래를 특정한다:

  파싱(matching.parse_address) → 표제부 앵커(load_pyoje_data)
  → 부속지번 재앵커/필지세트(load_attached_index)
  → 상업업무용 실거래 조회(fetch_from_api_multi_month → normalize_columns)
  → 단계 매칭(matching.match_stage) + 근거(build_match_basis)

조회 범위는 서울 상업업무용 매매 한정(v1 — 오피스텔·단독다가구·토지 제외).
"""

from __future__ import annotations

import logging
import re
import statistics
from datetime import datetime
from typing import Any, Optional

import pandas as pd

from .matching import (
    CONFIDENCE,
    build_match_basis,
    jibun_equal,
    masked_prefix_ok,
    masked_prefix_ok_any,
    match_stage,
    norm_bunji,
    parse_address,
)
from .constants import SEOUL_GU_CODES
from .pipeline import RealEstateDataPipeline, filter_ilban

logger = logging.getLogger("deal_locator.lookup")

SQM_PER_PYEONG = 3.305785

# 법정동명 → 시군구명 역캐시 (PublicDataReader code_bdong, 서울 한정)
_dong_to_gu: Optional[dict[str, str]] = None


def resolve_gu(dong: str) -> str:
    """법정동명 → 시군구명 역추적 (서울, 1회 로드 캐시)."""
    global _dong_to_gu
    if _dong_to_gu is None:
        _dong_to_gu = {}
        try:
            import PublicDataReader as pdr

            df = pdr.code_bdong()
            seoul = df[
                (df["시도명"] == "서울특별시")
                & (df["말소일자"].isna() | (df["말소일자"] == ""))
            ]
            for _, r in seoul.iterrows():
                name = str(r["읍면동명"]).strip()
                if name and name not in _dong_to_gu:
                    _dong_to_gu[name] = str(r["시군구명"]).strip()
        except Exception as e:  # noqa: BLE001
            logger.warning(f"법정동코드 로드 실패: {e}")
    return _dong_to_gu.get(dong, "")


def fmt_price(manwon: int) -> str:
    """만원 정수 → '859억' / '12억 3,000만원' / '9,500만원'."""
    if manwon <= 0:
        return ""
    eok, rem = divmod(int(manwon), 10000)
    if eok and rem:
        return f"{eok:,}억 {rem:,}만원"
    if eok:
        return f"{eok:,}억"
    return f"{rem:,}만원"


def to_pyeong(sqm: Optional[float]) -> Optional[float]:
    return round(sqm / SQM_PER_PYEONG, 1) if sqm else None


def _parse_manwon(v: Any) -> int:
    """'8,590,000' → 8590000 (만원)."""
    s = str(v or "").replace(",", "").strip()
    try:
        return int(float(s)) if s else 0
    except ValueError:
        return 0


def _s(v: Any) -> str:
    """NA-safe 문자열 변환 (pd.NA/NaN → '')."""
    if v is None or (not isinstance(v, str) and pd.isna(v)):
        return ""
    return str(v).strip()


def _last_token_bunji(jibun_full: str) -> str:
    """'서울특별시 구로구 구로동 1128-1' → '1128-1' (마지막 토큰)."""
    toks = str(jibun_full).strip().split()
    return norm_bunji(toks[-1]) if toks else ""


def find_buildings(
    pipeline: RealEstateDataPipeline, gu: str, dong: str, bunji: str,
) -> list[dict[str, Any]]:
    """표제부에서 해당 지번의 건축물 행을 찾는다 (여러 동이면 모두)."""
    if not (gu and dong and bunji):
        return []
    df = pipeline.load_pyoje_data(gu, dong)
    if df.empty or "대지위치" not in df.columns:
        return []
    addr = df["대지위치"].astype(str).str.replace(r"\s+", " ", regex=True)
    pat = re.compile(rf"{re.escape(dong)}\s+{re.escape(bunji)}(?:번지)?(?:\D|$)")
    hits = df[addr.str.contains(pat, na=False)]
    out: list[dict[str, Any]] = []
    for _, r in hits.iterrows():
        gross, land, year = r.get("연면적_float"), r.get("대지면적_float"), r.get("건축년도_매칭")
        out.append({
            "address": re.sub(r"번지$", "", str(r.get("대지위치", "")).strip()),
            "road_address": _s(r.get("도로명대지위치")),
            "gross": float(gross) if pd.notna(gross) else None,
            "land": float(land) if pd.notna(land) else None,
            "build_year": int(year) if pd.notna(year) else None,
        })
    return out


def _bonbun(bunji: str) -> str:
    """지번의 본번만 추출: '612-82' → '612', '612' → '612'."""
    return norm_bunji(bunji).split("-")[0]


def find_bunji_neighbors(
    pipeline: RealEstateDataPipeline, gu: str, dong: str, bunji: str,
) -> list[dict[str, Any]]:
    """질의 지번에 건물이 없을 때, 동일 본번(예: 612-*) 표제부 건물을 후보로 수집.

    유령 지번(표제부·부속지번 대장 모두 없음) 조회 시, 같은 본번의 인접 부번
    건물을 후보 앵커로 제공해 '정확일치(연면적+대지+건축년도)' 거래를 회복한다.
    질의 지번 자신은 제외(그건 find_buildings 가 이미 다룸).
    """
    bon = _bonbun(bunji)
    if not (gu and dong and bon):
        return []
    df = pipeline.load_pyoje_data(gu, dong)
    if df.empty or "대지위치" not in df.columns:
        return []
    addr = df["대지위치"].astype(str).str.replace(r"\s+", " ", regex=True)
    # '방화동 612-<부번>' — 본번만(부번 없는 612) 및 다른 본번은 제외
    pat = re.compile(rf"{re.escape(dong)}\s+{re.escape(bon)}-\d+(?:번지)?(?:\D|$)")
    hits = df[addr.str.contains(pat, na=False)]
    self_norm = norm_bunji(bunji)
    out: list[dict[str, Any]] = []
    for _, r in hits.iterrows():
        addr_s = re.sub(r"번지$", "", str(r.get("대지위치", "")).strip())
        b_bunji = _last_token_bunji(addr_s)
        if b_bunji == self_norm:  # 질의 지번 자신은 인접 후보에서 제외
            continue
        gross, land, year = r.get("연면적_float"), r.get("대지면적_float"), r.get("건축년도_매칭")
        out.append({
            "address": addr_s,
            "road_address": _s(r.get("도로명대지위치")),
            "gross": float(gross) if pd.notna(gross) else None,
            "land": float(land) if pd.notna(land) else None,
            "build_year": int(year) if pd.notna(year) else None,
            "bunji": b_bunji,
        })
    return out


def lookup_deal(
    pipeline: RealEstateDataPipeline, query: str, months: int = 12,
) -> dict[str, Any]:
    """지번의 실거래를 조회·매칭해 구조화 결과를 반환한다.

    Returns 주요 키: status(거래있음|거래없음|표제부없음|파싱실패|구_미상),
      parsed, building, match_context, transactions(최신순), latest,
      cancelled_count, period_months, message
    """
    result: dict[str, Any] = {
        "query": query,
        "generated_at": datetime.now().isoformat(),
        "parsed": {}, "status": "", "building": None,
        "match_context": {}, "transactions": [], "latest": None,
        "cancelled_count": 0, "jiphap_excluded": 0,
        "period_months": months, "message": "",
    }

    parsed = parse_address(query, gu_resolver=resolve_gu)
    result["parsed"] = parsed
    gu, dong, bunji = parsed["gu"], parsed["dong"], parsed["bunji"]

    if not (dong and bunji):
        result["status"] = "파싱실패"
        result["message"] = f"지번을 해석하지 못함: '{query}'. 예) '구로구 구로동 1128-1'"
        return result
    if not gu:
        result["status"] = "구_미상"
        result["message"] = f"'{dong}'의 구를 찾지 못함. '강남구 {dong} {bunji}'처럼 구를 함께 입력."
        return result

    # 부속지번 인덱스 (실패해도 계속)
    atch = None
    try:
        atch = pipeline.load_attached_index(gu, dong)
    except Exception as e:  # noqa: BLE001
        logger.warning(f"부속지번 인덱스 로드 실패({gu} {dong}): {e}")

    # 표제부 앵커 + 부속→대표 재앵커
    buildings = find_buildings(pipeline, gu, dong, bunji)
    anchor_bunji, anchor_via = bunji, "직접"
    if not buildings and atch is not None:
        for main in atch.resolve_main(bunji):
            found = find_buildings(pipeline, gu, dong, main)
            if found:
                buildings, anchor_bunji, anchor_via = found, main, "부속지번"
                break

    # 유령 지번(표제부·부속지번 대장 모두 없음) → 동일 본번 인접 후보 폴백.
    # 근거: 국토부 마스킹('6**')은 본번만 남기므로, 같은 본번의 인접 부번 건물과
    # 연면적+대지+건축년도가 '정확히' 일치하면 그 거래를 후보로 회복한다(미확정).
    neighbor_mode = False
    neighbor_pool: list[dict[str, Any]] = []
    if not buildings:
        neighbor_pool = find_bunji_neighbors(pipeline, gu, dong, bunji)
        if neighbor_pool:
            neighbor_mode = True
            anchor_via = "인접본번"

    lots = (atch.lot_set(anchor_bunji) | atch.lot_set(bunji)) if atch else {norm_bunji(bunji)}
    multi_lot = len(lots) > 1
    result["match_context"] = {
        "anchor_bunji": anchor_bunji, "anchor_via": anchor_via,
        "lot_set": sorted(lots), "multi_lot": multi_lot,
        "neighbor": neighbor_mode,
    }

    if buildings:
        rep = max(buildings, key=lambda b: b.get("gross") or 0)
        result["building"] = {
            **rep,
            "land_pyeong": to_pyeong(rep["land"]),
            "gross_pyeong": to_pyeong(rep["gross"]),
            "building_count": len(buildings),
            "anchor_bunji": anchor_bunji, "anchor_via": anchor_via,
        }

    # 실거래 조회 (상업업무용 매매, 해당 구만)
    try:
        raw = pipeline.fetch_from_api_multi_month(months=months, gus=[gu])
    except Exception as e:  # noqa: BLE001
        result["status"] = "API_오류"
        result["message"] = f"실거래 API 조회 실패: {e!r}"
        return result
    if raw is None or raw.empty:
        result["status"] = "거래없음" if buildings else "표제부없음"
        result["message"] = f"{gu} 상업업무용 실거래 데이터 없음 (기간 {months}개월)"
        return result

    dfn = pipeline.normalize_columns(raw, source="api")
    sig = dfn.get("시군구")
    if sig is None:
        result["status"] = "API_오류"
        result["message"] = "실거래 정규화 실패(시군구 컬럼 없음)"
        return result
    df = dfn[sig.astype(str).str.endswith(f" {dong}")].copy()
    # 취급 범위 = 통건물뿐. 집합(구분상가)은 표제부 역매칭 대상이 아니다.
    df, result["jiphap_excluded"] = filter_ilban(df)

    # 매칭
    transactions: list[dict[str, Any]] = []
    cancelled = 0
    for _, row in df.iterrows():
        # 해제신고 제외 — PDR 신버전은 미해제를 NaN 으로 반환(str 'nan' 오인 금지)
        cd = row.get("해제사유발생일", "")
        cd = "" if pd.isna(cd) else str(cd).strip()
        if cd and cd not in ("-", "nan", "None"):
            cancelled += 1
            continue
        if neighbor_mode:
            tx = _match_row_neighbor(row, bunji, neighbor_pool,
                                     pipeline._match_tolerance)
        else:
            tx = _match_row(row, bunji, buildings, lots, multi_lot,
                            pipeline._match_tolerance)
        if tx:
            transactions.append(tx)
    transactions.sort(key=lambda t: t["_sort_key"], reverse=True)
    for t in transactions:
        t.pop("_sort_key", None)
    result["transactions"] = transactions
    result["cancelled_count"] = cancelled

    # 인접 후보로 회복된 경우, 표제부 카드는 실제 매칭된 인접 건물로 채운다.
    if neighbor_mode and transactions and result["building"] is None:
        a = transactions[0].get("match_basis", {}).get("anchor", {}) or {}
        nb = transactions[0].get("neighbor_bunji", "")
        result["building"] = {
            "address": a.get("address", ""), "road_address": "",
            "gross": a.get("gross_sqm"), "land": a.get("land_sqm"),
            "build_year": a.get("build_year"),
            "land_pyeong": to_pyeong(a.get("land_sqm")),
            "gross_pyeong": to_pyeong(a.get("gross_sqm")),
            "building_count": 1,
            "anchor_bunji": nb, "anchor_via": "인접본번",
        }
        result["match_context"]["anchor_bunji"] = nb

    atch_note = ""
    if anchor_via == "부속지번":
        atch_note = f" ※ '{bunji}'는 대표지번 {anchor_bunji} 대지의 부속지번으로 인식."
    elif neighbor_mode and transactions:
        nb = transactions[0].get("neighbor_bunji", "")
        atch_note = (f" ※ '{bunji}'엔 건물 표제부 없음 — 동일 본번 인접 "
                     f"{dong} {nb}의 정확일치 거래(지번 재확인 요망).")
    elif multi_lot:
        atch_note = f" ※ 다필지 대지(필지 {len(lots)}개) — 대지면적 비교 완화."

    if transactions:
        result["latest"] = transactions[0]
        result["status"] = "거래있음"
        t0 = transactions[0]
        result["message"] = (
            f"{gu} {dong} {bunji} — 최근 {months}개월 매칭 거래 {len(transactions)}건. "
            f"최신 {t0['price']} ({t0['deal_date']}, {t0['confidence']})." + atch_note
        )
    elif buildings:
        result["status"] = "거래없음"
        result["message"] = (
            f"{gu} {dong} {bunji} — 최근 {months}개월 매칭 실거래 없음. "
            f"표제부: 준공 {result['building']['build_year']}." + atch_note
        )
    else:
        result["status"] = "표제부없음"
        result["message"] = (
            f"{gu} {dong} {bunji} — 표제부·매칭 실거래 모두 없음 "
            f"(번지 오타/신축/멸실 가능)." + atch_note
        )
    return result


def _deal_date_key(row: pd.Series) -> tuple[str, str]:
    """계약년월(+계약일) → ('YYYY-MM[-DD]', 정렬키 'YYYYMMDD')."""
    ym = str(row.get("계약년월", "") or "")
    day = pd.to_numeric(row.get("계약일"), errors="coerce")
    if len(ym) != 6:
        return "", ""
    deal_date, sort_key = f"{ym[:4]}-{ym[4:6]}", ym
    if pd.notna(day):
        deal_date += f"-{int(day):02d}"
        sort_key += f"{int(day):02d}"
    else:
        sort_key += "00"
    return deal_date, sort_key


def _strip_sido(addr: str) -> str:
    return re.sub(r"^서울특별시\s*", "", str(addr or "")).strip()


def _percentile(xs_sorted: list[int], q: float) -> float:
    """선형보간 백분위수. xs_sorted 는 오름차순 정렬된 비어있지 않은 리스트."""
    if len(xs_sorted) == 1:
        return float(xs_sorted[0])
    pos = q * (len(xs_sorted) - 1)
    lo = int(pos)
    if lo + 1 >= len(xs_sorted):
        return float(xs_sorted[-1])
    frac = pos - lo
    return xs_sorted[lo] * (1 - frac) + xs_sorted[lo + 1] * frac


def _ppp_agg(xs: list[int]) -> Optional[dict[str, Any]]:
    """평단가 표본(만원/평) → 요약 통계. 표본이 0이면 None."""
    if not xs:
        return None
    s = sorted(xs)
    return {
        "n": len(s),
        "mean_manwon": round(statistics.fmean(s)),
        "median_manwon": round(statistics.median(s)),
        "min_manwon": s[0],
        "max_manwon": s[-1],
        "p25_manwon": round(_percentile(s, 0.25)),
        "p75_manwon": round(_percentile(s, 0.75)),
    }


def _quarter_key(ym: Any) -> str:
    """계약년월('202606') → 분기 라벨('2026 Q2'). 해석 불가면 빈 문자열."""
    s = _s(ym)
    if len(s) < 6 or not s[:6].isdigit():
        return ""
    year, month = int(s[:4]), int(s[4:6])
    if not 1 <= month <= 12:
        return ""
    return f"{year} Q{(month - 1) // 3 + 1}"


def _ppp_quarterly(
    buckets: dict[str, tuple[list[int], list[int]]],
) -> list[dict[str, Any]]:
    """분기별 평단가 요약을 오래된 순으로 정렬해 반환.

    월 단위로 끊으면 표본이 무너진다 — 대치동(서울에서 거래가 활발한 축)조차
    통건물이 월 1~7건이고 22개월 중 10개월이 2건 이하였다. 분기로 묶어야 추이를
    읽을 최소 표본이 나온다(그래도 분기당 7~11건 수준이라 등락 폭은 크다 —
    `n` 을 반드시 함께 노출해 한두 건짜리 분기를 사용자가 걸러낼 수 있게 한다).
    """
    out: list[dict[str, Any]] = []
    for q in sorted(buckets):
        g, l = buckets[q]
        out.append({"quarter": q, "gross": _ppp_agg(g), "land": _ppp_agg(l)})
    return out


def _ppp_stats(
    gross: list[int],
    land: list[int],
    quarters: Optional[dict[str, tuple[list[int], list[int]]]] = None,
) -> dict[str, Any]:
    """동 전체(사용자 가격대·도로명 필터 이전) 평단가 통계 — 국토부 원본 기준.

    gross=연면적 평단가, land=대지 평단가 표본(만원/평). 표제부 역매칭과 무관하게
    산출한다 — 금액·면적은 마스킹 없이 원본 실거래에 그대로 있으므로, 매칭 성공
    여부로 표본을 줄이지 않아야 평균이 편향되지 않는다.

    quarters 를 주면 같은 표본을 분기로 쪼갠 `quarterly` 추이를 함께 담는다.
    """
    return {
        "gross": _ppp_agg(gross),
        "land": _ppp_agg(land),
        "quarterly": _ppp_quarterly(quarters or {}),
    }


def resolve_gu_query(query: str) -> str:
    """구 단위 질의면 정규화된 구 이름을, 아니면 빈 문자열을 반환.

    '강남구'·'강남'·'서울 강남구' 모두 '강남구'. 동이 섞이면('강남구 대치동')
    구 단위가 아니므로 '' — 동 단위 경로가 처리한다.
    """
    q = _s(query).replace("서울특별시", "").replace("서울시", "").replace("서울", "")
    q = q.replace(" ", "")
    if not q:
        return ""
    if q in SEOUL_GU_CODES:
        return q
    if f"{q}구" in SEOUL_GU_CODES:   # '강남' → '강남구'
        return f"{q}구"
    return ""


def scan_gu(
    pipeline: RealEstateDataPipeline,
    query: str,
    months: int = 12,
) -> dict[str, Any]:
    """구(區) 단위 통건물 시세 — 구 전체 통계 + 분기 추이 + 동별 순위.

    표제부 역매칭을 **하지 않는다.** 금액·면적은 마스킹 없이 국토부 원본에 다 있어
    통계에는 역매칭이 필요 없고, 구 하나에 동이 20개가 넘어 동마다 표제부를 부르면
    첫 조회가 수 분 걸린다. 그래서 구 모드는 통계 전용이고 거래 목록을 주지 않는다
    — 개별 지번이 필요하면 동 단위(scan_area)로 좁혀 들어가야 한다.

    동 단위와 같은 이유로 집합(구분상가)은 제외하고 통건물만 집계한다.

    Returns 주요 키: status(거래있음|거래없음|파싱실패|API_오류), gu, scope='구',
      stats(gross·land·quarterly), by_dong(대지 중앙 내림차순), total_in_gu,
      jiphap_excluded, cancelled_count, ppp_uncomputable, message
    """
    result: dict[str, Any] = {
        "query": query, "generated_at": datetime.now().isoformat(),
        "scope": "구", "status": "", "gu": "", "dong": "",
        "period_months": months, "total_in_gu": 0, "jiphap_excluded": 0,
        "cancelled_count": 0, "ppp_uncomputable": 0,
        "stats": {}, "by_dong": [], "message": "",
    }

    gu = resolve_gu_query(query)
    result["gu"] = gu
    if not gu:
        result["status"] = "파싱실패"
        result["message"] = f"구를 해석하지 못함: '{query}'. 예) '강남구'"
        return result

    try:
        raw = pipeline.fetch_from_api_multi_month(months=months, gus=[gu])
    except Exception as e:  # noqa: BLE001
        result["status"] = "API_오류"
        result["message"] = f"실거래 API 조회 실패: {e!r}"
        return result
    if raw is None or raw.empty:
        result["status"] = "거래없음"
        result["message"] = f"{gu} 상업업무용 실거래 데이터 없음 (기간 {months}개월)"
        return result

    dfn = pipeline.normalize_columns(raw, source="api")
    sig = dfn.get("시군구")
    if sig is None:
        result["status"] = "API_오류"
        result["message"] = "실거래 정규화 실패(시군구 컬럼 없음)"
        return result

    df, result["jiphap_excluded"] = filter_ilban(dfn.copy())
    result["total_in_gu"] = len(df)
    if df.empty:
        result["status"] = "거래없음"
        result["message"] = (
            f"{gu} — 최근 {months}개월 통건물 상업업무용 매매 없음 "
            f"(구분상가 {result['jiphap_excluded']}건은 취급 범위 밖)"
        )
        return result

    gross_all: list[int] = []
    land_all: list[int] = []
    q_buckets: dict[str, tuple[list[int], list[int]]] = {}
    # 동 이름 → (연면적 표본, 대지 표본)
    dong_buckets: dict[str, tuple[list[int], list[int]]] = {}

    for _, row in df.iterrows():
        cd = _s(row.get("해제사유발생일"))
        if cd and cd not in ("-",):
            result["cancelled_count"] += 1
            continue
        manwon = _parse_manwon(row.get("거래금액(만원)"))
        gross_sqm = pd.to_numeric(row.get("전용/연면적(㎡)"), errors="coerce")
        gross_py = to_pyeong(float(gross_sqm)) if pd.notna(gross_sqm) else None
        ppp_gross = round(manwon / gross_py) if (manwon and gross_py) else None
        if ppp_gross is None:
            result["ppp_uncomputable"] += 1
            continue
        land_sqm = pd.to_numeric(row.get("대지면적(㎡)"), errors="coerce")
        land_py = to_pyeong(float(land_sqm)) if pd.notna(land_sqm) else None
        ppp_land = round(manwon / land_py) if (manwon and land_py) else None

        gross_all.append(ppp_gross)
        if ppp_land is not None:
            land_all.append(ppp_land)

        qk = _quarter_key(row.get("계약년월"))
        if qk:
            qg, ql = q_buckets.setdefault(qk, ([], []))
            qg.append(ppp_gross)
            if ppp_land is not None:
                ql.append(ppp_land)

        dong = _s(row.get("시군구")).split()[-1] if _s(row.get("시군구")) else ""
        if dong:
            dg, dl = dong_buckets.setdefault(dong, ([], []))
            dg.append(ppp_gross)
            if ppp_land is not None:
                dl.append(ppp_land)

    result["stats"] = _ppp_stats(gross_all, land_all, q_buckets)

    rows = []
    for dong, (dg, dl) in dong_buckets.items():
        rows.append({"dong": dong, "gross": _ppp_agg(dg), "land": _ppp_agg(dl)})
    # 대지 중앙값 내림차순. 대지 표본이 없는 동은 뒤로 밀되 목록에는 남긴다
    # (거래가 있었다는 사실 자체가 정보다).
    rows.sort(key=lambda r: (r["land"] or {}).get("median_manwon", -1), reverse=True)
    result["by_dong"] = rows

    if not gross_all:
        result["status"] = "거래없음"
        result["message"] = f"{gu} — 평단가 산출 가능한 통건물 매매 없음"
        return result

    result["status"] = "거래있음"
    result["message"] = (
        f"{gu} — 최근 {months}개월 통건물 {len(gross_all)}건 · {len(rows)}개 동"
    )
    return result


def scan_area(
    pipeline: RealEstateDataPipeline,
    query: str,
    months: int = 12,
    ppp_min_manwon: Optional[int] = None,
    ppp_max_manwon: Optional[int] = None,
    road_contains: str = "",
    limit: int = 30,
) -> dict[str, Any]:
    """동 단위 상업업무용 매매를 '연면적 평단가'로 필터·정렬해 후보 리스트를 반환.

    지번을 몰라도 '동 + 가격대(연면적 평당, 만원/평)'로 거래를 훑는다.
    가격대 필터는 연면적 평단가 기준(거래금액 ÷ 연면적평).
    road_contains 가 있으면 표제부 역매칭으로 도로명이 특정된 거래에 한해
    부분일치 필터(도로명 미상 거래는 제외 — 커버리지 한계를 카운트로 고지).

    Returns 주요 키: status(거래있음|조건_불일치|거래없음|파싱실패|구_미상|API_오류),
      gu, dong, deals(최신순), period_months, total_in_dong, cancelled_count,
      ppp_uncomputable, price_excluded, road_unknown, road_filtered_out,
      truncated, message
    """
    result: dict[str, Any] = {
        "query": query, "generated_at": datetime.now().isoformat(),
        "parsed": {}, "status": "", "gu": "", "dong": "",
        "deals": [], "period_months": months, "total_in_dong": 0,
        "jiphap_excluded": 0,
        "cancelled_count": 0, "ppp_uncomputable": 0, "price_excluded": 0,
        "road_unknown": 0, "road_filtered_out": 0, "road_unknown_reasons": {},
        "truncated": 0, "stats": {},
        "band": (ppp_min_manwon, ppp_max_manwon), "road_contains": road_contains,
        "message": "",
    }

    parsed = parse_address(query, gu_resolver=resolve_gu)
    result["parsed"] = parsed
    gu, dong = parsed["gu"], parsed["dong"]
    result["gu"], result["dong"] = gu, dong
    if not dong:
        result["status"] = "파싱실패"
        result["message"] = f"동을 해석하지 못함: '{query}'. 예) '성수동1가'"
        return result
    if not gu:
        result["status"] = "구_미상"
        result["message"] = f"'{dong}'의 구를 찾지 못함. '성동구 {dong}'처럼 구를 함께 입력."
        return result

    try:
        raw = pipeline.fetch_from_api_multi_month(months=months, gus=[gu])
    except Exception as e:  # noqa: BLE001
        result["status"] = "API_오류"
        result["message"] = f"실거래 API 조회 실패: {e!r}"
        return result
    if raw is None or raw.empty:
        result["status"] = "거래없음"
        result["message"] = f"{gu} 상업업무용 실거래 데이터 없음 (기간 {months}개월)"
        return result

    dfn = pipeline.normalize_columns(raw, source="api")
    sig = dfn.get("시군구")
    if sig is None:
        result["status"] = "API_오류"
        result["message"] = "실거래 정규화 실패(시군구 컬럼 없음)"
        return result
    df = dfn[sig.astype(str).str.endswith(f" {dong}")].copy()
    # 취급 범위 = 통건물뿐. 집합(구분상가)은 여기서 걷어내되 건수는 고지한다.
    df, result["jiphap_excluded"] = filter_ilban(df)
    result["total_in_dong"] = len(df)
    if df.empty:
        result["status"] = "거래없음"
        result["message"] = f"{gu} {dong} — 최근 {months}개월 상업업무용 매매 없음"
        return result

    # 해제신고 제외 + 연면적 평단가 산출·필터 → 생존 인덱스별 메타 보관
    keep: dict[Any, dict[str, Any]] = {}
    ppp_gross_all: list[int] = []   # 동 전체 연면적 평단가 — 국토부 원본(밴드·도로명 무관)
    ppp_land_all: list[int] = []    # 동 전체 대지 평단가 — 국토부 원본
    # 분기 라벨 → (연면적 표본, 대지 표본). 위 전체 표본과 같은 원천을 쪼갠 것.
    q_buckets: dict[str, tuple[list[int], list[int]]] = {}
    for idx, row in df.iterrows():
        cd = row.get("해제사유발생일", "")
        cd = "" if pd.isna(cd) else str(cd).strip()
        if cd and cd not in ("-", "nan", "None"):
            result["cancelled_count"] += 1
            continue
        manwon = _parse_manwon(row.get("거래금액(만원)"))
        gross_sqm = pd.to_numeric(row.get("전용/연면적(㎡)"), errors="coerce")
        gross_py = to_pyeong(float(gross_sqm)) if pd.notna(gross_sqm) else None
        ppp_gross = round(manwon / gross_py) if (manwon and gross_py) else None
        if ppp_gross is None:
            result["ppp_uncomputable"] += 1
            continue
        # 동 시세 표본 — 사용자 가격대/도로명 필터 이전, 국토부 원본 기준으로 적재.
        # (역매칭 성공 여부와 무관 — 매칭 실패분을 빼면 평균이 편향되기 때문)
        ppp_gross_all.append(ppp_gross)
        land_sqm_raw = pd.to_numeric(row.get("대지면적(㎡)"), errors="coerce")
        land_py_raw = to_pyeong(float(land_sqm_raw)) if pd.notna(land_sqm_raw) else None
        ppp_land = round(manwon / land_py_raw) if (manwon and land_py_raw) else None
        if ppp_land is not None:
            ppp_land_all.append(ppp_land)
        qk = _quarter_key(row.get("계약년월"))
        if qk:
            qg, ql = q_buckets.setdefault(qk, ([], []))
            qg.append(ppp_gross)
            if ppp_land is not None:
                ql.append(ppp_land)
        if ppp_min_manwon is not None and ppp_gross < ppp_min_manwon:
            result["price_excluded"] += 1
            continue
        if ppp_max_manwon is not None and ppp_gross > ppp_max_manwon:
            result["price_excluded"] += 1
            continue
        keep[idx] = {"manwon": manwon, "gross_sqm": float(gross_sqm),
                     "gross_py": gross_py, "ppp_gross": ppp_gross}

    # 동 시세 통계는 밴드 0매칭이어도 유효하므로 조건_불일치 조기반환 전에 확정한다.
    result["stats"] = _ppp_stats(ppp_gross_all, ppp_land_all, q_buckets)

    if not keep:
        result["status"] = "조건_불일치"
        result["message"] = (
            f"{gu} {dong} — 기간 내 매매 {result['total_in_dong']}건 중 "
            f"연면적 평단가 조건 부합 0건 (평단가 산출불가 {result['ppp_uncomputable']}, "
            f"조건 밖 {result['price_excluded']})."
        )
        return result

    # 생존 거래만 표제부 역매칭(도로명·대지위치 확보) — 실패해도 계속
    sub = df.loc[list(keep.keys())].copy()
    try:
        sub = pipeline.bulk_match(sub)
    except Exception as e:  # noqa: BLE001
        logger.warning(f"scan_area 표제부 역매칭 실패({gu} {dong}): {e}")

    road_q = road_contains.strip()
    deals: list[dict[str, Any]] = []
    for idx, row in sub.iterrows():
        meta = keep[idx]
        jibun_full = str(row.get("지번", "") or "")
        masked = "*" in jibun_full
        daeji = _strip_sido(row.get("대지위치_표제부", ""))
        road = _s(row.get("도로명대지위치_표제부"))
        stage = _s(row.get("매칭단계"))

        if daeji:
            address, resolved = daeji, True
        elif not masked and jibun_full.strip():
            address, resolved = _strip_sido(jibun_full), True
        else:
            address = f"{dong} {_last_token_bunji(jibun_full)}(마스킹·주소미상)"
            resolved = False

        if road_q:
            if not road:
                result["road_unknown"] += 1
                # 계측: 도로명 미상 사유 분해. bulk_match 가 남긴 사유코드를 우선
                # 사용(마스킹 역매칭 실패 / 비마스킹 지번매칭 실패 모두 기록됨).
                rk = _s(row.get("역매칭실패사유"))
                if not rk:
                    rk = "비마스킹_도로명미조회" if not masked else "미분류"
                result["road_unknown_reasons"][rk] = (
                    result["road_unknown_reasons"].get(rk, 0) + 1
                )
                continue
            if road_q not in road:
                result["road_filtered_out"] += 1
                continue

        land_sqm = pd.to_numeric(row.get("대지면적(㎡)"), errors="coerce")
        land_py = to_pyeong(float(land_sqm)) if pd.notna(land_sqm) else None
        row_year = pd.to_numeric(row.get("건축년도"), errors="coerce")
        deal_date, sort_key = _deal_date_key(row)
        deals.append({
            "address": address, "resolved_address": resolved, "road": road,
            "jibun_masked": masked, "demask_stage": stage,
            "price": fmt_price(meta["manwon"]), "price_manwon": meta["manwon"],
            "deal_date": deal_date,
            "ppp_gross_manwon": meta["ppp_gross"],
            "ppp_land_manwon": (round(meta["manwon"] / land_py)
                                if land_py else None),
            "gross_sqm": meta["gross_sqm"], "gross_pyeong": meta["gross_py"],
            "land_sqm": float(land_sqm) if pd.notna(land_sqm) else None,
            "land_pyeong": land_py,
            "build_year": int(row_year) if pd.notna(row_year) else None,
            "floor": _s(row.get("층")).removesuffix(".0"),
            "usage": _s(row.get("건축물주용도")), "zone": _s(row.get("용도지역")),
            "seller": _s(row.get("매도")), "buyer": _s(row.get("매수")),
            "_sort_key": sort_key,
        })

    deals.sort(key=lambda d: d["_sort_key"], reverse=True)
    for d in deals:
        d.pop("_sort_key", None)
    if len(deals) > limit:
        result["truncated"] = len(deals) - limit
        deals = deals[:limit]
    result["deals"] = deals

    if not deals:
        result["status"] = "조건_불일치"
        rb = result["road_unknown_reasons"]
        rb_txt = ""
        if rb:
            parts = ", ".join(f"{k} {v}" for k, v in
                              sorted(rb.items(), key=lambda kv: -kv[1]))
            rb_txt = f" [미상 사유: {parts}]"
        result["message"] = (
            f"{gu} {dong} — 평단가 조건은 통과했으나 도로명 '{road_q}' 부합 0건 "
            f"(도로명 미상 {result['road_unknown']}, 불일치 {result['road_filtered_out']})."
            + rb_txt
        )
        return result

    result["status"] = "거래있음"
    result["message"] = (
        f"{gu} {dong} — 최근 {months}개월 조건 부합 {len(deals)}건"
        + (f" (+{result['truncated']}건 생략)" if result["truncated"] else "")
    )
    return result


def _match_row(
    row: pd.Series, bunji: str, buildings: list[dict[str, Any]],
    lots: set, multi_lot: bool, tol: float,
) -> Optional[dict[str, Any]]:
    """정규화 거래 행 1건 매칭 → tx dict (미매칭 None)."""
    jibun_full = str(row.get("지번", "") or "")
    row_gross = pd.to_numeric(row.get("전용/연면적(㎡)"), errors="coerce")
    row_land = pd.to_numeric(row.get("대지면적(㎡)"), errors="coerce")
    row_year = pd.to_numeric(row.get("건축년도"), errors="coerce")

    stage_key: Optional[str] = None
    reasons: list[str] = []
    anchor_used: dict[str, Any] = {}

    if "*" not in jibun_full and jibun_full.strip():
        tail = _last_token_bunji(jibun_full)
        if jibun_equal(tail, bunji):
            stage_key, reasons = "exact_jibun", ["노출 지번이 질의 지번과 일치"]
        elif any(jibun_equal(tail, lot) for lot in lots):
            stage_key = "exact_jibun_attached"
            reasons = [f"노출 지번({tail})이 같은 대지의 부속지번과 일치(부속지번 대장)"]
        else:
            return None
    else:
        if not buildings:
            return None
        if not masked_prefix_ok_any(jibun_full, lots):
            return None
        best: Optional[str] = None
        for b in buildings:
            s, rs = match_stage(
                row_gross, row_land, row_year,
                b.get("gross"), b.get("land"), b.get("build_year"),
                tol, multi_lot=multi_lot,
            )
            if s and (best is None or CONFIDENCE[s][1] > CONFIDENCE[best][1]):
                best, reasons = s, rs
                anchor_used = {
                    "address": b.get("address", ""),
                    "gross_sqm": b.get("gross"), "land_sqm": b.get("land"),
                    "build_year": b.get("build_year"),
                }
        stage_key = best

    if not stage_key:
        return None

    return _build_tx(row, jibun_full, stage_key, reasons, anchor_used,
                     lots, multi_lot)


def _build_tx(
    row: pd.Series, jibun_full: str, stage_key: str, reasons: list[str],
    anchor_used: dict[str, Any], lots: set, multi_lot: bool,
    *, extra: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """매칭 성립 행 → 표준 tx dict (금액·일자·면적·근거 조립). 공용."""
    row_gross = pd.to_numeric(row.get("전용/연면적(㎡)"), errors="coerce")
    row_land = pd.to_numeric(row.get("대지면적(㎡)"), errors="coerce")
    row_year = pd.to_numeric(row.get("건축년도"), errors="coerce")

    label, score = CONFIDENCE[stage_key]
    manwon = _parse_manwon(row.get("거래금액(만원)"))

    ym = str(row.get("계약년월", "") or "")
    day = pd.to_numeric(row.get("계약일"), errors="coerce")
    deal_date, sort_key = "", ""
    if len(ym) == 6:
        deal_date = f"{ym[:4]}-{ym[4:6]}"
        sort_key = ym
        if pd.notna(day):
            deal_date += f"-{int(day):02d}"
            sort_key += f"{int(day):02d}"
        else:
            sort_key += "00"

    land_sqm = float(row_land) if pd.notna(row_land) else None
    gross_sqm = float(row_gross) if pd.notna(row_gross) else None
    land_py = to_pyeong(land_sqm)
    ppp = round(manwon / land_py) if (manwon and land_py) else None

    basis = build_match_basis(
        stage_key, anchor=anchor_used,
        row={"gross_sqm": gross_sqm, "land_sqm": land_sqm,
             "build_year": int(row_year) if pd.notna(row_year) else None,
             "raw_jibun": jibun_full},
        reasons=reasons, multi_lot=multi_lot, lot_set=lots,
    )
    if stage_key == "neighbor_exact":
        basis["caveat"] = (
            "인접후보 — 질의 지번엔 건물 표제부가 없고, 동일 본번 인접 지번의 "
            "연면적·대지·건축년도 정확일치 거래입니다. 지번을 재확인하세요"
            + (f" (표제부: {anchor_used.get('address','')})" if anchor_used.get("address") else "")
            + "."
        )

    tx = {
        "price": fmt_price(manwon),
        "price_manwon": manwon,
        "deal_date": deal_date,
        "seller": _s(row.get("매도")),
        "buyer": _s(row.get("매수")),
        "floor": _s(row.get("층")).removesuffix(".0"),
        "building_type": _s(row.get("유형")),
        "usage": _s(row.get("건축물주용도")),
        "zone": _s(row.get("용도지역")),
        "gross_sqm": gross_sqm, "land_sqm": land_sqm,
        "land_pyeong": land_py, "gross_pyeong": to_pyeong(gross_sqm),
        "price_per_land_pyeong_manwon": ppp,
        "build_year": int(row_year) if pd.notna(row_year) else None,
        "jibun_masked": "*" in jibun_full,
        "raw_jibun": jibun_full,
        "confidence": label, "confidence_score": score, "match_stage": stage_key,
        "match_basis": basis,
        "_sort_key": sort_key,
    }
    if extra:
        tx.update(extra)
    return tx


def _match_row_neighbor(
    row: pd.Series, bunji: str, neighbor_pool: list[dict[str, Any]], tol: float,
) -> Optional[dict[str, Any]]:
    """유령 지번용 매처: 동일 본번 인접 건물과 '정확일치(stage1)'만 후보로 채택.

    연면적+대지+건축년도가 모두 정확히 일치할 때만 성립(오차 매칭 배제)하여
    오탐을 억제한다. 성립 시 신뢰도 '인접후보(0.50)'로 강등하고 caveat 를 붙인다.
    """
    jibun_full = str(row.get("지번", "") or "")
    row_gross = pd.to_numeric(row.get("전용/연면적(㎡)"), errors="coerce")
    row_land = pd.to_numeric(row.get("대지면적(㎡)"), errors="coerce")
    row_year = pd.to_numeric(row.get("건축년도"), errors="coerce")

    best_b: Optional[dict[str, Any]] = None
    for b in neighbor_pool:
        s, _ = match_stage(
            row_gross, row_land, row_year,
            b.get("gross"), b.get("land"), b.get("build_year"),
            tol, multi_lot=False,
        )
        if s == "stage1":  # 연면적+대지+건축년도 정확 일치만
            best_b = b
            break
    if best_b is None:
        return None
    # 마스킹 접두(본번)가 후보 지번과 호환일 때만 (노출 지번이면 그대로 검사)
    if not masked_prefix_ok(jibun_full, best_b["bunji"]):
        return None

    nb = best_b["bunji"]
    anchor_used = {
        "address": best_b.get("address", ""),
        "gross_sqm": best_b.get("gross"), "land_sqm": best_b.get("land"),
        "build_year": best_b.get("build_year"),
    }
    reasons = [
        f"질의 지번({bunji})엔 건물 표제부 없음",
        f"동일 본번 인접 {nb}의 연면적·대지·건축년도 정확일치",
    ]
    return _build_tx(row, jibun_full, "neighbor_exact", reasons, anchor_used,
                     {nb}, False, extra={"neighbor_bunji": nb})
