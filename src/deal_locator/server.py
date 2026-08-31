"""deal-locator-mcp — 실거래 필지 매칭 MCP 서버 (stdio)
====================================================
국토부 상업업무용 실거래가는 일반건물 지번을 '1***' 식으로 마스킹한다.
이 서버는 건축물대장 표제부(건축년도·연면적·대지면적) + 부속지번 대장으로
역매칭해 "이 필지가 실제로 얼마에 팔렸나"를 특정한다.

v1 범위: 서울 · 상업업무용 매매 한정 (오피스텔·단독다가구·토지 제외).
모든 수치는 공공데이터포털(data.go.kr) 공식 API 실측값이며 출처를 명시한다.
결과가 없으면 [NOT_FOUND] — LLM은 절대 수치를 추측·보간하지 말 것.

환경변수:
  DEAL_LOCATOR_SERVICE_KEY (우선) 또는 DATA_GO_KR_API_KEY — data.go.kr 디코딩 인증키
  DEAL_LOCATOR_CACHE_DIR — 캐시 경로 (기본 ~/.cache/deal-locator-mcp)
"""

from __future__ import annotations

import os
import re
import sys
import time
from contextlib import contextmanager, redirect_stdout
from pathlib import Path
from typing import Any, Optional

from fastmcp import FastMCP
from fastmcp.tools.tool import ToolResult

from deal_locator.core import PipelineConfig, RealEstateDataPipeline, SEOUL_GU_CODES
from deal_locator.core.lookup import (
    lookup_deal,
    resolve_gu,
    resolve_gu_query,
    scan_area,
    scan_gu,
)
from deal_locator.core.matching import parse_address

# ── 머신 파싱 프리픽스 (환각 차단 규약) ──
NOT_FOUND = "[NOT_FOUND]"
PARSE_ERROR = "[PARSE_ERROR]"
CONFIG_ERROR = "[CONFIG_ERROR]"
API_ERROR = "[EXTERNAL_API_ERROR]"

# 상태코드 → 텍스트 프리픽스. structuredContent 의 status 값이 정본이고
# 텍스트 프리픽스는 그 투영이다(둘이 어긋나지 않도록 한 곳에서 매핑).
_ERR_PREFIX = {
    "PARSE_ERROR": PARSE_ERROR,
    "CONFIG_ERROR": CONFIG_ERROR,
    "EXTERNAL_API_ERROR": API_ERROR,
}
KEY_MISSING_MSG = (
    "data.go.kr 인증키 미설정 — DEAL_LOCATOR_SERVICE_KEY "
    "또는 DATA_GO_KR_API_KEY 환경변수를 설정하세요."
)

# 외부 공공 API를 호출하는 읽기 전용 도구 — 파일시스템 서버와 달리 openWorld=True.
READ_ONLY_EXTERNAL = {
    "readOnlyHint": True,
    "idempotentHint": True,
    "destructiveHint": False,
    "openWorldHint": True,
}

# MCP isError=true 로 표시할 상태.
# 설정 누락·잘못된 입력·외부 API 장애는 '도구 실패'다.
# NOT_FOUND 는 조회가 정상 수행된 결과(데이터 부재)이므로 절대 포함하지 않는다 —
# 이걸 에러로 만들면 "없음"이 "실패"로 둔갑해 재시도·환각을 유발한다.
_IS_ERROR_STATUSES = frozenset({"CONFIG_ERROR", "PARSE_ERROR", "EXTERNAL_API_ERROR"})

SOURCE_LINE = "출처: 국토교통부 실거래가 공개시스템 · 건축HUB (공공데이터포털 data.go.kr)"
NO_GUESS = "LLM은 위 실측값만 사용할 것 — 없는 수치를 추측·보간하지 말 것."

mcp = FastMCP(
    name="deal-locator",
    instructions=(
        "국토부 실거래가 × 건축물대장 역매칭 도구. 서울 상업업무용 매매 한정(v1). "
        "**취급 범위는 통건물(유형='일반')뿐이다 — 집합(구분상가) 거래는 모든 응답에서 "
        "제외된다.** 따라서 여기서 나오는 시세·평단가는 전부 통건물 기준이며, "
        "구분상가 한 칸 시세로 인용해서는 안 된다(제외 건수는 coverage.jiphap_excluded). "
        "마스킹된 실거래 지번을 표제부·부속지번으로 특정한다. "
        "주소는 '구 동 번지'(예: '구로구 구로동 1128-1') 형태가 가장 정확하다. "
        "응답의 매칭 신뢰도(확정/정확매칭/추정매칭)를 반드시 함께 전달하고, "
        "추정매칭은 동일 스펙 인접 건물일 가능성을 사용자에게 고지할 것. "
        f"{NOT_FOUND} 응답이면 데이터가 없는 것 — 수치를 지어내지 말 것. "
        "모든 도구가 structuredContent 를 반환한다 — 계산·인용 시 "
        "텍스트가 아니라 구조화 필드를 읽을 것: status='OK' 일 때만 실측값이 있고, "
        "area_scan 은 coverage(모수 분해)를 함께 확인해 표본 대표성을 판단할 것. "
        "쓰기 도구는 2종뿐이다 — deal_card_create(카드 PNG)와 deals_export"
        "(연월 지정 실거래 CSV 다운로드, 파일 생성 외 부작용 없음). "
        "deal_card_create 의 PHOTO_MISSING·LOW_CONFIDENCE 는 실패가 아니라 사람이 "
        "결정할 상태다 — 재시도하지 말고 사용자에게 그대로 전하고, 추정매칭 발행은 "
        "match_explain 으로 근거를 확인시킨 뒤 allow_estimated=true 로 진행할 것. "
        "이 도구의 결과는 참고자료이지 중개대상물 확인·설명서가 아니다 — "
        "사용자가 고객에게 제시할 목적이면 원문(실거래가 공개시스템·건축물대장)을 "
        "직접 확인하도록 안내할 것."
    ),
)

# ── 파이프라인 싱글턴 + 조회 캐시 ──
_pipeline: Optional[RealEstateDataPipeline] = None
_cache: dict[tuple[Any, ...], tuple[float, dict[str, Any]]] = {}
_CACHE_TTL = 900  # 15분
_CACHE_MAX = 128  # 항목 상한 — TTL만 있으면 조회한 주소 수만큼 무한 증가한다


@contextmanager
def _quiet_stdout():
    """라이브러리의 stray print 를 stderr 로 돌린다 — **stdio 전송로 보호**.

    이 서버는 stdout 이 JSON-RPC 통신선이다. 그런데 PublicDataReader 는 법정동 코드를
    처음 적재할 때 '출처: 행정기관(행정동) …' 한 줄을 stdout 으로 찍는다. import 시점이
    아니라 **툴 호출 도중**이라, 그대로 두면 응답 프레임 사이에 비-JSON 한 줄이 끼어든다.
    관대한 클라이언트는 파싱 에러로 넘기지만 엄격한 쪽은 연결을 끊는다.

    sys.stdout 전역 교체는 쓸 수 없다 — MCP stdio 전송이 실행 시점에 sys.stdout.buffer 를
    직접 잡으므로 전송로까지 같이 죽는다. 그래서 코어 호출 구간만 감싼다.
    """
    with redirect_stdout(sys.stderr):
        yield


def get_pipeline() -> RealEstateDataPipeline:
    global _pipeline
    if _pipeline is None:
        key = os.environ.get("DEAL_LOCATOR_SERVICE_KEY") or os.environ.get("DATA_GO_KR_API_KEY") or ""
        cache_dir = os.environ.get(
            "DEAL_LOCATOR_CACHE_DIR", str(Path.home() / ".cache" / "deal-locator-mcp")
        )
        p = RealEstateDataPipeline()
        with _quiet_stdout():
            p.initialize(PipelineConfig(api_key=key, cache_dir=cache_dir,
                                        max_api_calls_per_fetch=120))
        _pipeline = p
    return _pipeline


def _cache_get(key: tuple[Any, ...]) -> Optional[dict[str, Any]]:
    hit = _cache.get(key)
    if not hit:
        return None
    if time.time() - hit[0] >= _CACHE_TTL:
        _cache.pop(key, None)
        return None
    return hit[1]


def _cache_put(key: tuple[Any, ...], value: dict[str, Any]) -> None:
    now = time.time()
    for k, (ts, _) in list(_cache.items()):  # 만료분 청소
        if now - ts >= _CACHE_TTL:
            _cache.pop(k, None)
    while len(_cache) >= _CACHE_MAX:  # 상한 초과 시 가장 오래된 것부터 축출
        _cache.pop(min(_cache, key=lambda k: _cache[k][0]), None)
    _cache[key] = (now, value)


def _norm_key(s: str) -> str:
    return " ".join(str(s).split())


_CTRL = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")


def _scrub(v: Any) -> Any:
    """외부 API 문자열에서 제어문자·개행을 제거한다(재귀).

    이 값들은 도구 텍스트에 실려 그대로 LLM 에게 전달된다. 개행이 섞이면
    "새 줄에 쓰인 지시"처럼 보일 수 있어, 신뢰 불가 데이터가 지시로 해석되는
    통로가 된다. 정상 데이터에는 영향이 없다(무해한 무동작).
    """
    if isinstance(v, str):
        return _CTRL.sub("", v.replace("\r", " ").replace("\n", " ")).strip()
    if isinstance(v, dict):
        return {k: _scrub(x) for k, x in v.items()}
    if isinstance(v, list):
        return [_scrub(x) for x in v]
    return v


def _cached_lookup(address: str, months: int) -> dict[str, Any]:
    months = max(1, min(int(months), 60))
    key = ("deal", _norm_key(address), months)
    hit = _cache_get(key)
    if hit is not None:
        return hit
    with _quiet_stdout():
        res = _scrub(lookup_deal(get_pipeline(), address, months=months))
    if res.get("status") != "API_오류":  # 일시 오류는 캐시하지 않음(재시도 허용)
        _cache_put(key, res)
    return res


def _cached_scan(area: str, months: int, lo: Optional[int], hi: Optional[int],
                 road_q: str, limit: int) -> dict[str, Any]:
    """동 단위 스캔은 가장 비싼 호출(구 전체 fetch + 표제부 역매칭) — 캐시 필수."""
    key = ("scan", _norm_key(area), months, lo, hi, road_q, limit)
    hit = _cache_get(key)
    if hit is not None:
        return hit
    with _quiet_stdout():
        res = _scrub(scan_area(get_pipeline(), area, months=months,
                        ppp_min_manwon=lo, ppp_max_manwon=hi,
                        road_contains=road_q, limit=limit))
    if res.get("status") != "API_오류":
        _cache_put(key, res)
    return res


def _cached_scan_gu(area: str, months: int) -> dict[str, Any]:
    """구 단위 스캔 — 역매칭이 없어 동 스캔보다 싸지만 fetch 는 동일하게 비싸다."""
    key = ("scan_gu", _norm_key(area), months)
    hit = _cache_get(key)
    if hit is not None:
        return hit
    with _quiet_stdout():
        res = _scrub(scan_gu(get_pipeline(), area, months=months))
    if res.get("status") != "API_오류":
        _cache_put(key, res)
    return res


def _key_ok() -> bool:
    return bool(os.environ.get("DEAL_LOCATOR_SERVICE_KEY") or os.environ.get("DATA_GO_KR_API_KEY"))


# ── 포맷터 ──

def _fmt_areas(t: dict[str, Any]) -> str:
    parts = []
    if t.get("land_sqm"):
        s = f"토지 {t['land_sqm']:,.1f}㎡"
        if t.get("land_pyeong"):
            s += f"({t['land_pyeong']:,}평)"
        parts.append(s)
    if t.get("gross_sqm"):
        s = f"연면적 {t['gross_sqm']:,.1f}㎡"
        if t.get("gross_pyeong"):
            s += f"({t['gross_pyeong']:,}평)"
        parts.append(s)
    return " · ".join(parts)


def _fmt_tx_line(t: dict[str, Any]) -> str:
    """거래 1건 요약 라인."""
    bits = [f"{t['price']} ({t['deal_date']})", f"{t['confidence']}({t['confidence_score']:.2f})"]
    if t.get("seller") or t.get("buyer"):
        bits.append(f"{t.get('seller') or '?'} → {t.get('buyer') or '?'}")
    if t.get("usage"):
        bits.append(t["usage"])
    if t.get("jibun_masked"):
        bits.append("지번마스킹→역매칭")
    return " · ".join(bits)


def _current_month_note(res: dict[str, Any]) -> str:
    """당월 표본 고지.

    조회 창은 당월부터 센다(v1.5.0). 당월은 계약일로부터 30일인 신고기한이
    아직 안 지나 거래가 계속 들어온다 — 지금 0건이어도 '거래가 없었다'는 뜻이
    아니다. 거래가 없을 때(오해가 가장 큰 경우)와 최신 거래가 당월일 때 붙인다.
    """
    from datetime import datetime as _dt

    this_month = _dt.now().strftime("%Y-%m")
    txs = res.get("transactions") or []
    latest = txs[0] if txs else None
    latest_ym = str((latest or {}).get("deal_date", ""))[:7]
    if not txs or latest_ym == this_month:
        return (f"※ {this_month} 은 신고 진행 중(계약일로부터 30일) — "
                f"당월 거래는 이후 더 늘어날 수 있다")
    return ""


def _context_notes(res: dict[str, Any]) -> list[str]:
    out = []
    mc = res.get("match_context") or {}
    if mc.get("anchor_via") == "부속지번":
        out.append(f"※ 질의 지번은 대표지번 {mc['anchor_bunji']} 대지의 부속지번(대장 기준)")
    elif mc.get("anchor_via") == "인접본번":
        out.append(
            f"※ 질의 지번엔 건물 표제부 없음 — 동일 본번 인접 {mc.get('anchor_bunji','')}의 "
            f"정확일치 거래(인접후보, 미확정 · 지번 재확인 요망)"
        )
    elif mc.get("multi_lot"):
        out.append(f"※ 다필지 대지(필지 {len(mc['lot_set'])}개: {', '.join(mc['lot_set'])}) — 대지면적 비교 완화 적용")
    if res.get("cancelled_count"):
        out.append(f"※ 해제신고 거래 {res['cancelled_count']}건 제외")
    note = _current_month_note(res)
    if note:
        out.append(note)
    return out


def _caveat(t: dict[str, Any]) -> str:
    return t.get("match_basis", {}).get("caveat", "")


def _guard_data(address: str, months: int) -> tuple[Optional[dict[str, str]], Optional[dict[str, Any]]]:
    """공통 가드(구조화): 오류 → ({status, message}, None) / 정상 → (None, result)."""
    if not _key_ok():
        return {"status": "CONFIG_ERROR", "message": KEY_MISSING_MSG}, None
    res = _cached_lookup(address, months)
    if res["status"] in ("파싱실패", "구_미상"):
        return {"status": "PARSE_ERROR", "message": res["message"]}, None
    if res["status"] == "API_오류":
        return {"status": "EXTERNAL_API_ERROR", "message": res["message"]}, None
    return None, res


# (_guard 텍스트 래퍼는 5개 도구가 모두 구조화된 뒤 삭제됨 — 에러 정본은 _guard_data)


# ── 도구 ──
# 5개 도구 모두 같은 3단 구조를 따른다:
#   _xxx_payload()  — 구조화 정본(structuredContent). 상태·수치의 유일한 출처.
#   _render_xxx()   — payload → 텍스트. payload 밖의 값을 읽지 않는다.
#   @mcp.tool       — ToolResult(content=렌더, structured_content=payload, is_error=…)
# 텍스트는 바이트 단위로 고정된 계약이라 tests/ 의 _legacy_* 골든과 대조된다.

# ── resolve_address: 구조화 출력 ──

RESOLVE_ADDRESS_OUTPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "status": {
            "type": "string",
            "enum": ["OK", "NOT_FOUND", "PARSE_ERROR", "CONFIG_ERROR", "EXTERNAL_API_ERROR"],
            "description": "OK 일 때만 해석 결과가 유효하다.",
        },
        "message": {"type": "string", "description": "status != 'OK' 인 사유"},
        "query": {"type": "string"},
        "address": {
            "type": ["object", "null"],
            "properties": {"gu": {"type": "string"}, "dong": {"type": "string"},
                           "bunji": {"type": "string"}},
            "required": ["gu", "dong", "bunji"],
        },
        "sigungu_code": {"type": "string", "description": "시군구 5자리. 미상이면 빈 문자열"},
        "bdong_code": {"type": "string", "description": "법정동 5자리. 조회 실패면 빈 문자열"},
        "lot_set": {"type": "array", "items": {"type": "string"},
                    "description": "질의 지번이 속한 대지의 필지세트(대표+부속). lot_structure='조회실패'면 빈 배열"},
        "lot_structure": {
            "type": "string",
            "enum": ["단일", "다필지", "부속지번", "조회실패"],
            "description": "필지 구성. '조회실패'는 '단일'이 아니라 미확인 — 단독 근거로 쓰지 말 것",
        },
        "main_bunji": {"type": ["string", "null"],
                       "description": "lot_structure='부속지번' 일 때 대표지번"},
        "source": {"type": "string"},
    },
    "required": ["status", "query", "lot_structure", "lot_set", "source"],
}


def _resolve_address_payload(query: str) -> dict[str, Any]:
    """resolve_address 의 구조화 정본. 텍스트는 이 payload 에서만 파생된다."""
    d: dict[str, Any] = {
        "status": "", "message": "", "query": query, "address": None,
        "sigungu_code": "", "bdong_code": "",
        "lot_set": [], "lot_structure": "조회실패", "main_bunji": None,
        "source": SOURCE_LINE,
    }

    parsed = parse_address(query, gu_resolver=resolve_gu)
    gu, dong, bunji = parsed["gu"], parsed["dong"], parsed["bunji"]
    if not (dong and bunji):
        d.update(status="PARSE_ERROR",
                 message=f"지번 해석 실패: '{query}' — '구 동 번지' 형태로 입력하세요.")
        return d
    if not gu:
        d.update(status="PARSE_ERROR",
                 message=f"'{dong}'의 구를 찾지 못함 — 구를 함께 입력하세요.")
        return d

    d["status"] = "OK"
    d["address"] = {"gu": gu, "dong": dong, "bunji": bunji}
    d["sigungu_code"] = SEOUL_GU_CODES.get(gu, "")

    # 부속지번 보강은 실패해도 해석 결과(구/동/번지)를 무효화하지 않는다.
    # 실패는 '단일 필지'와 구분해 lot_structure='조회실패'로 남긴다.
    try:
        p = get_pipeline()
        d["bdong_code"] = p.get_bdong_code(gu, dong) or ""
        idx = p.load_attached_index(gu, dong)
        d["lot_set"] = sorted(idx.lot_set(bunji))
        mains = idx.resolve_main(bunji)
        if mains:
            d["lot_structure"], d["main_bunji"] = "부속지번", mains[0]
        elif len(d["lot_set"]) > 1:
            d["lot_structure"] = "다필지"
        else:
            d["lot_structure"] = "단일"
    except Exception:  # noqa: BLE001 — 보강 정보 실패는 해석 결과에 영향 없음
        d["lot_set"] = []
        d["lot_structure"] = "조회실패"
    return d


def _render_resolve_address(d: dict[str, Any]) -> str:
    """payload → 사람/LLM 가독 텍스트. 개조 전 출력과 바이트 동일."""
    if d["status"] in _ERR_PREFIX:
        return f"{_ERR_PREFIX[d['status']]} {d['message']}"

    a = d["address"] or {}
    lines = [f"■ 주소 해석 — {d['query']}",
             f"  구/동/번지: {a.get('gu', '')} / {a.get('dong', '')} / {a.get('bunji', '')}",
             f"  시군구코드: {d['sigungu_code'] or '?'}"]
    if d["bdong_code"]:
        lines.append(f"  법정동코드: {d['bdong_code']}")

    lots, struct = ", ".join(d["lot_set"]), d["lot_structure"]
    if struct == "부속지번":
        lines.append(f"  필지 구성: '{a.get('bunji', '')}'는 대표지번 {d['main_bunji']}의 부속지번 — 세트 {lots}")
    elif struct == "다필지":
        lines.append(f"  필지 구성: 다필지 대지 — 세트 {lots}")
    elif struct == "단일":
        lines.append("  필지 구성: 단일 필지(부속지번 대장 기준)")
    else:
        lines.append("  필지 구성: (부속지번 조회 실패 — 키/네트워크 확인)")
    lines.append(SOURCE_LINE)
    return "\n".join(lines)


@mcp.tool(title="주소 해석", output_schema=RESOLVE_ADDRESS_OUTPUT_SCHEMA,
          annotations=READ_ONLY_EXTERNAL)
def resolve_address(query: str) -> ToolResult:
    """주소 문자열을 구/동/번지 + 법정 코드로 해석하고 필지 구성(부속지번)을 확인한다.

    예: '구로구 구로동 1128-1', '서울 성수동2가 321-90' (동만 줘도 구를 역추적).
    다른 도구를 쓰기 전 주소가 모호할 때 먼저 호출.
    lot_structure='조회실패'는 '단일 필지'가 아니라 미확인이다 — 구분해 읽을 것.
    """
    data = _resolve_address_payload(query)
    return ToolResult(content=_render_resolve_address(data), structured_content=data,
                      is_error=data["status"] in _IS_ERROR_STATUSES)


# ── deal_card_search: 구조화 출력 ──
# LLM이 텍스트를 오독해도 수치가 흔들리지 않도록, 카드의 모든 실측값을
# structuredContent 로 함께 내보낸다. status 가 환각 차단 규약의 기계 정본.

_NUM = {"type": ["number", "null"]}
_INT = {"type": ["integer", "null"]}

_TX_PROPS = {
        "price": {"type": "string", "description": "표시용 금액 (예: '859억')"},
        "price_manwon": {"type": "integer", "description": "거래금액(만원) — 계산에는 이 값을 쓸 것"},
        "deal_date": {"type": "string"},
        "confidence": {"type": "string", "description": "확정/정확매칭/추정매칭/인접후보"},
        "confidence_score": {"type": "number"},
        "match_stage": {"type": "string"},
        "match_reasons": {"type": "array", "items": {"type": "string"}},
        "caveat": {"type": "string", "description": "비었으면 특기사항 없음"},
        "jibun_masked": {"type": "boolean", "description": "true면 마스킹 지번을 역매칭으로 특정한 건"},
        "raw_jibun": {"type": "string"},
        "seller": {"type": "string"}, "buyer": {"type": "string"},
        "floor": {"type": "string"}, "usage": {"type": "string"}, "zone": {"type": "string"},
        "land_sqm": _NUM, "gross_sqm": _NUM,
        "land_pyeong": _NUM, "gross_pyeong": _NUM,
        "build_year": _INT,
        "price_per_land_pyeong_manwon": _INT,
}
_TX_REQUIRED = ["price", "price_manwon", "deal_date", "confidence", "confidence_score",
                "jibun_masked", "match_reasons"]

# 같은 거래 스키마의 두 쓰임 — deal_card_search.latest(단건·nullable)와
# deal_history.transactions[](배열 항목·non-null). 필드는 한 곳에서만 정의한다.
_TX_SCHEMA = {
    "type": ["object", "null"],
    "description": "최신 매칭 거래 1건. status != 'OK' 이면 null.",
    "properties": _TX_PROPS,
    "required": _TX_REQUIRED,
}

_TX_ITEM_SCHEMA = {
    "type": "object",
    "description": "매칭 거래 1건 (deal_card_search.latest 와 동일 필드).",
    "properties": _TX_PROPS,
    "required": _TX_REQUIRED,
}

_BUILDING_SCHEMA = {
    "type": ["object", "null"],
    "description": "매칭 앵커가 된 건축물대장 표제부.",
    "properties": {
        "address": {"type": "string"}, "road_address": {"type": "string"},
        "land_sqm": _NUM, "gross_sqm": _NUM,
        "land_pyeong": _NUM, "gross_pyeong": _NUM,
        "build_year": _INT, "building_count": _INT,
        "anchor_bunji": {"type": "string"},
        "anchor_via": {"type": "string", "description": "지번/부속지번/인접본번"},
    },
    "required": ["address"],
}

DEAL_CARD_SEARCH_OUTPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "status": {
            "type": "string",
            "enum": ["OK", "NOT_FOUND", "PARSE_ERROR", "CONFIG_ERROR", "EXTERNAL_API_ERROR"],
            "description": "OK 일 때만 latest 에 실측값이 있다. 그 외에는 수치를 추측·보간하지 말 것.",
        },
        "message": {"type": "string", "description": "status != 'OK' 인 사유"},
        "lookup_status": {"type": "string", "description": "도메인 원본 상태(거래있음/거래없음/표제부없음 …)"},
        "query": {"type": "string"},
        "address": {
            "type": ["object", "null"],
            "properties": {"gu": {"type": "string"}, "dong": {"type": "string"},
                           "bunji": {"type": "string"}},
            "required": ["gu", "dong", "bunji"],
        },
        "period_months": {"type": "integer"},
        "latest": _TX_SCHEMA,
        "building": _BUILDING_SCHEMA,
        "transaction_count": {"type": "integer", "description": "기간 내 매칭 거래 수(deal_history 로 전체 확인)"},
        "cancelled_count": {"type": "integer", "description": "제외된 해제신고 건수"},
        "notes": {"type": "array", "items": {"type": "string"}},
        "source": {"type": "string"},
    },
    "required": ["status", "query", "period_months", "transaction_count", "source"],
}


def _tx_payload(t: dict[str, Any]) -> dict[str, Any]:
    """거래 dict → 구조화 출력. 키는 텍스트 포맷터(_fmt_tx_line/_fmt_areas)와 호환 유지."""
    basis = t.get("match_basis") or {}
    return {
        "price": t.get("price", ""),
        "price_manwon": t.get("price_manwon"),
        "deal_date": t.get("deal_date", ""),
        "confidence": t.get("confidence", ""),
        "confidence_score": t.get("confidence_score"),
        "match_stage": t.get("match_stage", ""),
        "match_reasons": list(basis.get("reasons") or []),
        "caveat": basis.get("caveat", "") or "",
        "jibun_masked": bool(t.get("jibun_masked")),
        "raw_jibun": t.get("raw_jibun", ""),
        "seller": t.get("seller", ""), "buyer": t.get("buyer", ""),
        "floor": t.get("floor", ""), "usage": t.get("usage", ""), "zone": t.get("zone", ""),
        "land_sqm": t.get("land_sqm"), "gross_sqm": t.get("gross_sqm"),
        "land_pyeong": t.get("land_pyeong"), "gross_pyeong": t.get("gross_pyeong"),
        "build_year": t.get("build_year"),
        "price_per_land_pyeong_manwon": t.get("price_per_land_pyeong_manwon"),
    }


def _building_payload(b: dict[str, Any]) -> dict[str, Any]:
    return {
        "address": b.get("address", ""),
        "road_address": b.get("road_address", "") or "",
        "land_sqm": b.get("land"), "gross_sqm": b.get("gross"),
        "land_pyeong": b.get("land_pyeong"), "gross_pyeong": b.get("gross_pyeong"),
        "build_year": b.get("build_year"), "building_count": b.get("building_count"),
        "anchor_bunji": b.get("anchor_bunji", "") or "",
        "anchor_via": b.get("anchor_via", "") or "",
    }


def _deal_card_search_payload(address: str, months: int) -> dict[str, Any]:
    """deal_card_search 의 구조화 정본. 텍스트는 이 payload 에서만 파생된다."""
    d: dict[str, Any] = {
        "status": "", "message": "", "lookup_status": "",
        "query": address, "address": None,
        "period_months": max(1, min(int(months), 60)),
        "latest": None, "building": None,
        "transaction_count": 0, "cancelled_count": 0,
        "notes": [], "source": SOURCE_LINE,
    }

    err, res = _guard_data(address, months)
    if err:
        d.update(status=err["status"], message=err["message"])
        return d

    p = res["parsed"]
    head = f"{p['gu']} {p['dong']} {p['bunji']}"
    d["address"] = {"gu": p["gu"], "dong": p["dong"], "bunji": p["bunji"]}
    d["lookup_status"] = res["status"]
    d["period_months"] = res["period_months"]
    d["cancelled_count"] = res.get("cancelled_count", 0)
    d["transaction_count"] = len(res.get("transactions") or [])

    if res["status"] == "표제부없음":
        d["status"] = "NOT_FOUND"
        d["message"] = (f"{head} — 건축물대장 표제부와 매칭 실거래가 모두 없음 "
                        f"(번지 오타/신축/멸실 가능).")
        return d

    if res.get("building"):
        d["building"] = _building_payload(res["building"])

    t = res.get("latest")
    if t:
        d["status"] = "OK"
        d["latest"] = _tx_payload(t)
    else:
        d["status"] = "NOT_FOUND"
        d["message"] = f"{head} — 최근 {res['period_months']}개월 내 매칭 실거래 없음"

    d["notes"] = _context_notes(res)
    return d


def _render_deal_card_search(d: dict[str, Any]) -> str:
    """payload → 사람/LLM 가독 텍스트. 개조 전 출력과 바이트 동일."""
    if d["status"] in _ERR_PREFIX:
        return f"{_ERR_PREFIX[d['status']]} {d['message']}"
    if d["lookup_status"] == "표제부없음":
        return f"{NOT_FOUND} {d['message']} {NO_GUESS}"

    a = d["address"] or {}
    head = f"{a.get('gu', '')} {a.get('dong', '')} {a.get('bunji', '')}"
    lines = [f"■ 실거래 종합카드 — {head}"]

    t = d["latest"]
    if t:
        lines.append(f"  최신 실거래: {_fmt_tx_line(t)}")
        if t.get("price_per_land_pyeong_manwon"):
            lines.append(f"  평단가(토지): {t['price_per_land_pyeong_manwon']:,}만원/평")
        area = _fmt_areas(t)
        if area:
            lines.append(f"  거래 명세: {area}" + (f" · 준공 {t['build_year']}" if t.get("build_year") else ""))
        if t.get("zone"):
            lines.append(f"  용도지역: {t['zone']}")
        if t.get("match_reasons"):
            lines.append(f"  매칭 근거: {' / '.join(t['match_reasons'])}")
        if d["transaction_count"] > 1:
            lines.append(f"  기간 내 매칭 거래 총 {d['transaction_count']}건 (deal_history 로 전체 확인)")
    else:
        lines.append(f"  {NOT_FOUND} 최근 {d['period_months']}개월 내 매칭 실거래 없음")

    b = d["building"]
    if b:
        lines.append(f"  [건축물대장] {b['address']}"
                     + (f" · {b['road_address']}" if b.get("road_address") else ""))
        spec = []
        if b.get("land_sqm"):
            spec.append(f"대지 {b['land_sqm']:,.1f}㎡({b['land_pyeong']:,}평)")
        if b.get("gross_sqm"):
            spec.append(f"연면적 {b['gross_sqm']:,.1f}㎡({b['gross_pyeong']:,}평)")
        if b.get("build_year"):
            spec.append(f"준공 {b['build_year']}")
        if spec:
            lines.append("  " + " · ".join(spec))

    lines.extend("  " + n for n in d["notes"])
    if t and t.get("caveat"):
        lines.append(f"  ⚠ {t['caveat']}")
    lines.append(SOURCE_LINE)
    return "\n".join(lines)


@mcp.tool(title="실거래 종합카드", output_schema=DEAL_CARD_SEARCH_OUTPUT_SCHEMA,
          annotations=READ_ONLY_EXTERNAL)
def deal_card_search(address: str, months: int = 12) -> ToolResult:
    """지번의 실거래 종합카드 1콜 — 최신 매매가·평단가·매도/매수·매칭 신뢰도 + 표제부.

    서울 상업업무용 매매 한정. months(1~60)는 조회 기간(개월).
    첫 조회는 표제부·실거래 API를 불러 수십 초~수 분 걸릴 수 있다(이후 15분 캐시).
    수치를 계산에 쓸 때는 텍스트가 아니라 structuredContent 를 읽을 것
    (status='OK' 일 때만 latest 에 실측값이 있다).
    """
    data = _deal_card_search_payload(address, months)
    return ToolResult(content=_render_deal_card_search(data), structured_content=data,
                      is_error=data["status"] in _IS_ERROR_STATUSES)


# ── deal_history: 구조화 출력 ──
# 시계열 비교(재거래·가격 추이)를 텍스트 파싱 없이 하게 만드는 게 목적.
# 거래 항목 스키마는 deal_card_search 와 공유(_TX_ITEM_SCHEMA).

DEAL_HISTORY_OUTPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "status": {
            "type": "string",
            "enum": ["OK", "NOT_FOUND", "PARSE_ERROR", "CONFIG_ERROR", "EXTERNAL_API_ERROR"],
            "description": "OK 일 때만 transactions 에 실측값이 있다.",
        },
        "message": {"type": "string", "description": "status != 'OK' 인 사유"},
        "lookup_status": {"type": "string", "description": "도메인 원본 상태(거래있음/거래없음/표제부없음 …)"},
        "query": {"type": "string"},
        "address": {
            "type": ["object", "null"],
            "properties": {"gu": {"type": "string"}, "dong": {"type": "string"},
                           "bunji": {"type": "string"}},
            "required": ["gu", "dong", "bunji"],
        },
        "period_months": {"type": "integer"},
        "transactions": {
            "type": "array", "items": _TX_ITEM_SCHEMA,
            "description": "최신순(deal_date 내림차순). 건별 신뢰도가 다를 수 있으니 각 건의 confidence 를 볼 것.",
        },
        "transaction_count": {"type": "integer"},
        "building": _BUILDING_SCHEMA,
        "lowest_confidence_score": {
            "type": ["number", "null"],
            "description": "이력 중 최저 신뢰도. 이력 전체를 한 덩어리로 인용할 땐 이 값 기준으로 고지할 것",
        },
        "cancelled_count": {"type": "integer", "description": "제외된 해제신고 건수"},
        "notes": {"type": "array", "items": {"type": "string"}},
        "source": {"type": "string"},
    },
    "required": ["status", "query", "period_months", "transactions",
                 "transaction_count", "source"],
}


def _deal_history_payload(address: str, months: int) -> dict[str, Any]:
    """deal_history 의 구조화 정본. 텍스트는 이 payload 에서만 파생된다."""
    d: dict[str, Any] = {
        "status": "", "message": "", "lookup_status": "",
        "query": address, "address": None,
        "period_months": max(1, min(int(months), 60)),
        "transactions": [], "transaction_count": 0, "building": None,
        "lowest_confidence_score": None, "cancelled_count": 0,
        "notes": [], "source": SOURCE_LINE,
    }

    err, res = _guard_data(address, months)
    if err:
        d.update(status=err["status"], message=err["message"])
        return d

    p = res["parsed"]
    head = f"{p['gu']} {p['dong']} {p['bunji']}"
    d["address"] = {"gu": p["gu"], "dong": p["dong"], "bunji": p["bunji"]}
    d["lookup_status"] = res["status"]
    d["period_months"] = res["period_months"]
    d["cancelled_count"] = res.get("cancelled_count", 0)
    if res.get("building"):
        d["building"] = _building_payload(res["building"])

    txs = res["transactions"]
    if not txs:
        d["status"] = "NOT_FOUND"
        d["message"] = (f"{head} — 최근 {res['period_months']}개월 내 매칭 실거래 없음"
                        + (" (표제부는 존재)" if res.get("building") else " (표제부도 없음)")
                        + ".")
        return d

    d["status"] = "OK"
    d["transactions"] = [_tx_payload(t) for t in txs]
    d["transaction_count"] = len(txs)
    d["lowest_confidence_score"] = min(
        (t["confidence_score"] for t in d["transactions"]
         if t["confidence_score"] is not None), default=None)
    d["notes"] = _context_notes(res)
    return d


def _render_deal_history(d: dict[str, Any]) -> str:
    """payload → 사람/LLM 가독 텍스트. 개조 전 출력과 바이트 동일."""
    if d["status"] in _ERR_PREFIX:
        return f"{_ERR_PREFIX[d['status']]} {d['message']}"
    if d["status"] == "NOT_FOUND":
        return f"{NOT_FOUND} {d['message']} {NO_GUESS}"

    a = d["address"] or {}
    head = f"{a.get('gu', '')} {a.get('dong', '')} {a.get('bunji', '')}"
    txs = d["transactions"]
    lines = [f"■ 실거래 이력 — {head} · 최근 {d['period_months']}개월 {d['transaction_count']}건 (최신순)"]
    for t in txs:
        lines.append(f"  · {_fmt_tx_line(t)}")
        area = _fmt_areas(t)
        if area or t.get("floor"):
            sub = [area] if area else []
            if t.get("floor"):
                sub.append(f"층 {t['floor']}")
            lines.append(f"    {' · '.join(sub)}")
    lines.extend("  " + n for n in d["notes"])
    worst = min(txs, key=lambda x: x["confidence_score"])
    if worst.get("caveat"):
        lines.append(f"  ⚠ {worst['caveat']}")
    lines.append(SOURCE_LINE)
    return "\n".join(lines)


@mcp.tool(title="실거래 이력", output_schema=DEAL_HISTORY_OUTPUT_SCHEMA,
          annotations=READ_ONLY_EXTERNAL)
def deal_history(address: str, months: int = 24) -> ToolResult:
    """지번의 매칭 실거래 이력 전체(최신순) — 각 건에 매칭 신뢰도 표기.

    서울 상업업무용 매매 한정. months(1~60) 기본 24개월.
    재거래·가격 추이를 계산할 때는 텍스트가 아니라 structuredContent 의
    transactions[] 를 읽을 것 (건별 신뢰도가 다를 수 있다).
    """
    data = _deal_history_payload(address, months)
    return ToolResult(content=_render_deal_history(data), structured_content=data,
                      is_error=data["status"] in _IS_ERROR_STATUSES)


# ── match_explain: 구조화 출력 ──
# 감사 자동화용 — 앵커(표제부) vs 거래행 대조를 객체로 내보내 "무엇이 얼마나
# 달라서 이 신뢰도가 나왔는지"를 기계가 재검증할 수 있게 한다.

_DIFF_SCHEMA = {
    "type": "object",
    "properties": {
        "anchor": _NUM, "row": _NUM,
        "delta": {**_NUM, "description": "거래행 − 앵커. 한쪽이라도 결측이면 null"},
        "equal": {"type": ["boolean", "null"], "description": "오차 허용 없는 완전 일치 여부"},
    },
}

MATCH_EXPLAIN_OUTPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "status": {
            "type": "string",
            "enum": ["OK", "NOT_FOUND", "PARSE_ERROR", "CONFIG_ERROR", "EXTERNAL_API_ERROR"],
            "description": "OK 일 때만 verdict/anchor/row 에 실측값이 있다.",
        },
        "message": {"type": "string", "description": "status != 'OK' 인 사유"},
        "lookup_status": {"type": "string"},
        "query": {"type": "string"},
        "address": {
            "type": ["object", "null"],
            "properties": {"gu": {"type": "string"}, "dong": {"type": "string"},
                           "bunji": {"type": "string"}},
            "required": ["gu", "dong", "bunji"],
        },
        "period_months": {"type": "integer"},
        "deal": {
            "type": ["object", "null"],
            "description": "설명 대상 거래(최신 1건).",
            "properties": {"price": {"type": "string"}, "price_manwon": _INT,
                           "deal_date": {"type": "string"}},
        },
        "verdict": {
            "type": ["object", "null"],
            "description": "매칭 판정. confidence_score < 0.90 은 추정 — 공부 대조 전 확정 인용 금지.",
            "properties": {
                "confidence": {"type": "string"},
                "confidence_score": {"type": "number"},
                "stage": {"type": "string"},
                "reasons": {"type": "array", "items": {"type": "string"}},
                "caveat": {"type": "string", "description": "비었으면 특기사항 없음"},
            },
        },
        "anchor": {
            "type": ["object", "null"],
            "description": "표제부 앵커(비교 기준). null 이면 지번이 노출된 거래라 스펙 대조 없이 매칭된 것.",
            "properties": {"address": {"type": "string"}, "gross_sqm": _NUM,
                           "land_sqm": _NUM, "build_year": _INT},
        },
        "row": {
            "type": ["object", "null"],
            "description": "실거래 원본 행(비교 대상).",
            "properties": {"raw_jibun": {"type": "string"}, "gross_sqm": _NUM,
                           "land_sqm": _NUM, "build_year": _INT},
        },
        "comparison": {
            "type": ["object", "null"],
            "description": "앵커 vs 거래행 대조. anchor 가 없으면 null.",
            "properties": {"gross_sqm": _DIFF_SCHEMA, "land_sqm": _DIFF_SCHEMA,
                           "build_year": _DIFF_SCHEMA},
        },
        "lot_set": {"type": "array", "items": {"type": "string"}},
        "multi_lot": {"type": "boolean", "description": "true면 대지면적 비교를 완화해 매칭한 것"},
        "anchor_via": {"type": "string", "description": "직접/부속지번/인접본번"},
        "anchor_bunji": {"type": "string"},
        "disclaimer": {"type": "string"},
        "source": {"type": "string"},
    },
    "required": ["status", "query", "period_months", "source"],
}

ATCH_DISCLAIMER = "※ 부속지번 대장은 등재 누락 가능 — 매칭 보강용이며 단독 확정 근거가 아님."


def _diff(anchor: Any, row: Any) -> dict[str, Any]:
    """앵커/거래행 수치 1쌍 → 대조 객체. 한쪽이라도 결측이면 delta/equal 은 null."""
    both = isinstance(anchor, (int, float)) and isinstance(row, (int, float))
    return {
        "anchor": anchor if isinstance(anchor, (int, float)) else None,
        "row": row if isinstance(row, (int, float)) else None,
        "delta": round(row - anchor, 4) if both else None,
        "equal": (row == anchor) if both else None,
    }


def _match_explain_payload(address: str, months: int) -> dict[str, Any]:
    """match_explain 의 구조화 정본. 텍스트는 이 payload 에서만 파생된다."""
    d: dict[str, Any] = {
        "status": "", "message": "", "lookup_status": "",
        "query": address, "address": None,
        "period_months": max(1, min(int(months), 60)),
        "deal": None, "verdict": None, "anchor": None, "row": None,
        "comparison": None, "lot_set": [], "multi_lot": False,
        "anchor_via": "", "anchor_bunji": "",
        "disclaimer": ATCH_DISCLAIMER, "source": SOURCE_LINE,
    }

    err, res = _guard_data(address, months)
    if err:
        d.update(status=err["status"], message=err["message"])
        return d

    p = res["parsed"]
    head = f"{p['gu']} {p['dong']} {p['bunji']}"
    d["address"] = {"gu": p["gu"], "dong": p["dong"], "bunji": p["bunji"]}
    d["lookup_status"] = res["status"]
    d["period_months"] = res["period_months"]

    t = res.get("latest")
    if not t:
        d["status"] = "NOT_FOUND"
        d["message"] = f"{head} — 설명할 매칭 거래가 없음 (기간 {res['period_months']}개월)."
        return d

    basis = t["match_basis"]
    a, r = basis.get("anchor") or {}, basis.get("row") or {}
    mc = res.get("match_context") or {}

    d["status"] = "OK"
    d["deal"] = {"price": t.get("price", ""), "price_manwon": t.get("price_manwon"),
                 "deal_date": t.get("deal_date", "")}
    d["verdict"] = {
        "confidence": basis["confidence"],
        "confidence_score": basis["confidence_score"],
        "stage": basis["stage"],
        "reasons": list(basis["reasons"]),
        "caveat": basis.get("caveat", "") or "",
    }
    if a:
        d["anchor"] = {"address": a.get("address", ""), "gross_sqm": a.get("gross_sqm"),
                       "land_sqm": a.get("land_sqm"), "build_year": a.get("build_year")}
    d["row"] = {"raw_jibun": r.get("raw_jibun", ""), "gross_sqm": r.get("gross_sqm"),
                "land_sqm": r.get("land_sqm"), "build_year": r.get("build_year")}
    if d["anchor"]:
        d["comparison"] = {
            f: _diff(d["anchor"][f], d["row"][f])
            for f in ("gross_sqm", "land_sqm", "build_year")
        }
    # lot_set 은 조회 컨텍스트, multi_lot 은 매칭 근거 기준 (원 렌더러와 동일 출처).
    d["lot_set"] = list(mc.get("lot_set", []))
    d["multi_lot"] = bool(basis.get("multi_lot"))
    d["anchor_via"] = mc.get("anchor_via", "")
    d["anchor_bunji"] = mc.get("anchor_bunji", "") or ""
    return d


def _render_match_explain(d: dict[str, Any]) -> str:
    """payload → 사람/LLM 가독 텍스트. 개조 전 출력과 바이트 동일."""
    if d["status"] in _ERR_PREFIX:
        return f"{_ERR_PREFIX[d['status']]} {d['message']}"
    if d["status"] == "NOT_FOUND":
        return f"{NOT_FOUND} {d['message']} {NO_GUESS}"

    ad = d["address"] or {}
    head = f"{ad.get('gu', '')} {ad.get('dong', '')} {ad.get('bunji', '')}"
    deal, v = d["deal"] or {}, d["verdict"] or {}
    lines = [
        f"■ 매칭 근거 — {head} · {deal.get('price', '')} ({deal.get('deal_date', '')})",
        f"  판정: {v['confidence']} (score {v['confidence_score']:.2f}, {v['stage']})",
        f"  사유: {' / '.join(v['reasons']) or '-'}",
    ]
    a = d["anchor"]
    if a:
        lines.append(f"  [표제부 앵커] {a.get('address','')} · "
                     f"연면적 {a.get('gross_sqm')}㎡ · 대지 {a.get('land_sqm')}㎡ · 준공 {a.get('build_year')}")
    r = d["row"] or {}
    lines.append(f"  [거래행] 지번 '{r.get('raw_jibun','')}' · "
                 f"연면적 {r.get('gross_sqm')}㎡ · 대지 {r.get('land_sqm')}㎡ · 건축년도 {r.get('build_year')}")
    lines.append(f"  필지세트: {', '.join(d['lot_set']) or '-'}"
                 + (" (다필지)" if d["multi_lot"] else " (단일)"))
    lines.append(f"  앵커 경로: {d['anchor_via'] or '-'}"
                 + (f" → 대표지번 {d['anchor_bunji']}" if d["anchor_via"] == "부속지번" else ""))
    if v.get("caveat"):
        lines.append(f"  ⚠ {v['caveat']}")
    lines.append(f"  {d['disclaimer']}")
    lines.append(SOURCE_LINE)
    return "\n".join(lines)


@mcp.tool(title="매칭 근거", output_schema=MATCH_EXPLAIN_OUTPUT_SCHEMA,
          annotations=READ_ONLY_EXTERNAL)
def match_explain(address: str, months: int = 12) -> ToolResult:
    """최신 매칭의 근거를 투명하게 — 표제부 앵커값 vs 거래행값, 발화 단계, 필지세트.

    추정매칭 검증·감사용. 보고서 인용 전 반드시 확인 권장.
    structuredContent 의 comparison 이 앵커 대 거래행 대조(delta·equal)를 담는다.
    """
    data = _match_explain_payload(address, months)
    return ToolResult(content=_render_match_explain(data), structured_content=data,
                      is_error=data["status"] in _IS_ERROR_STATUSES)


def _fmt_ppp(manwon: Optional[int]) -> str:
    """평단가(만원/평) → '50,000만원/평(5억)' — 억 병기(1억 이상)."""
    if not manwon:
        return "-"
    s = f"{manwon:,}만원/평"
    if manwon >= 10000:
        s += f"({round(manwon / 10000, 2):g}억)"
    return s


def _fmt_stats_lines(stats: dict[str, Any]) -> list[str]:
    """동 시세 통계 → 텍스트 라인(만원/평). 표본이 없으면 빈 리스트.

    연면적 평단가는 흔히 sub-1억/평이라 억 표기는 해상도를 잃는다 — per-deal 라인과
    같은 만원/평으로 통일한다. '흔한구간'은 사분위(25~75%)로 이상치를 배제한 대표대역.
    """
    g = (stats or {}).get("gross")
    if not (g and g.get("n")):
        return []
    out = [
        f"  [동 시세] 연면적 평당 평균 {g['mean_manwon']:,}만원/평 · "
        f"중앙 {g['median_manwon']:,} · "
        f"흔한구간 {g['p25_manwon']:,}~{g['p75_manwon']:,} · "
        f"전체 {g['min_manwon']:,}~{g['max_manwon']:,} "
        f"(표본 {g['n']}건 · 가격대·도로명 필터 무관 · 국토부 원본)"
    ]
    la = (stats or {}).get("land")
    if la and la.get("n"):
        out.append(
            f"    └ 대지 평당 평균 {la['mean_manwon']:,}만원/평 · "
            f"중앙 {la['median_manwon']:,} · "
            f"흔한구간 {la['p25_manwon']:,}~{la['p75_manwon']:,} (표본 {la['n']}건)"
        )
    out.extend(_fmt_quarterly_lines((stats or {}).get("quarterly") or []))
    return out


def _fmt_quarterly_lines(quarterly: list[dict[str, Any]]) -> list[str]:
    """분기별 추이 → 텍스트 라인. 표본이 없으면 빈 리스트.

    각 분기는 대지 평단가를 우선 쓰고(통건물 시세는 실무상 대지 기준), 대지 표본이
    없는 분기는 연면적으로 대체한다. 표본이 3건 미만이면 수치 대신 '표본부족'으로
    적어 한두 건이 추세처럼 읽히는 것을 막는다.
    """
    if not quarterly:
        return []
    cells: list[str] = []
    for q in quarterly:
        agg = q.get("land") or q.get("gross")
        basis = "대지" if q.get("land") else "연면적"
        if not (agg and agg.get("n")):
            continue
        if agg["n"] < 3:
            cells.append(f"{q['quarter']} 표본부족(n={agg['n']})")
        else:
            cells.append(f"{q['quarter']} {agg['median_manwon']:,}({basis}·n={agg['n']})")
    if not cells:
        return []
    return ["    └ 분기 추이(중앙, 만원/평) — " + " · ".join(cells)]


def _fmt_band(lo: Optional[int], hi: Optional[int]) -> str:
    def eok(m: Optional[int]) -> str:
        return f"{m / 10000:g}억" if m else "-"
    if lo is None and hi is None:
        return "전체(제한 없음)"
    if lo is not None and hi is not None:
        return f"{eok(lo)}~{eok(hi)}/평"
    return (f"{eok(lo)}/평 이상" if lo is not None else f"{eok(hi)}/평 이하")


def _fmt_scan_deal(d: dict[str, Any]) -> list[str]:
    """스캔 거래 1건 → 2줄(요약 + 명세)."""
    head = f"  · {d['price']} ({d['deal_date']}) · 연면적 {_fmt_ppp(d['ppp_gross_manwon'])}"
    addr = d["address"] + (f" · {d['road']}" if d.get("road") else "")
    head += f" · {addr}"
    sub: list[str] = []
    if d.get("gross_pyeong"):
        sub.append(f"연면적 {d['gross_pyeong']:,}평")
    if d.get("land_pyeong"):
        sub.append(f"대지 {d['land_pyeong']:,}평")
    if d.get("ppp_land_manwon"):
        sub.append(f"대지평당 {d['ppp_land_manwon']:,}만원/평")
    if d.get("build_year"):
        sub.append(f"준공 {d['build_year']}")
    if d.get("usage"):
        sub.append(d["usage"])
    # 매칭 투명성: 마스킹 복원 여부/단계
    if d.get("jibun_masked"):
        sub.append(d["demask_stage"] or "마스킹 미복원(주소 미상)")
    lines = [head]
    if sub:
        lines.append("    " + " · ".join(sub))
    return lines


# ── area_scan: 구조화 출력 ──
# 커버리지 수치(동내 매매/해제/산출불가/조건밖/도로명 미상·불일치)가 텍스트 한 줄에
# 뭉쳐 있으면 "부합 N건"만 읽히고 모수가 사라진다. 표본 대표성을 판단하려면
# 이 수치들이 기계 판독 가능해야 한다.

_SCAN_DEAL_SCHEMA = {
    "type": "object",
    "properties": {
        "address": {"type": "string", "description": "역매칭으로 특정된 대지위치. 미복원이면 '(마스킹·주소미상)' 표기"},
        "resolved_address": {"type": "boolean", "description": "false면 지번을 특정하지 못한 건 — 금액·평단가는 실측이나 주소는 미상"},
        "road": {"type": "string"},
        "jibun_masked": {"type": "boolean"},
        "demask_stage": {"type": "string", "description": "마스킹 복원 단계. 빈 값이면 미복원"},
        "price": {"type": "string"},
        "price_manwon": {"type": "integer"},
        "deal_date": {"type": "string"},
        "ppp_gross_manwon": _INT,
        "ppp_land_manwon": _INT,
        "gross_sqm": _NUM, "gross_pyeong": _NUM,
        "land_sqm": _NUM, "land_pyeong": _NUM,
        "build_year": _INT,
        "floor": {"type": "string"}, "usage": {"type": "string"}, "zone": {"type": "string"},
        "seller": {"type": "string"}, "buyer": {"type": "string"},
    },
    "required": ["address", "resolved_address", "price", "price_manwon", "deal_date",
                 "jibun_masked", "ppp_gross_manwon"],
}

_COVERAGE_SCHEMA = {
    "type": "object",
    "description": "모수 분해 — matched 만 보지 말고 무엇이 왜 빠졌는지 함께 읽을 것.",
    "properties": {
        "total_in_dong": {"type": "integer", "description": "기간 내 해당 동의 통건물 상업업무용 매매 (집합 제외 후)"},
        "jiphap_excluded": {"type": "integer", "description": "집합(구분상가)으로 제외 — 이 도구는 통건물만 취급"},
        "cancelled_count": {"type": "integer", "description": "해제신고로 제외"},
        "ppp_uncomputable": {"type": "integer", "description": "연면적 결측 등으로 평단가 산출 불가"},
        "price_excluded": {"type": "integer", "description": "평단가 조건 밖"},
        "road_unknown": {"type": "integer", "description": "도로명 미상으로 제외 (road_contains 지정 시에만)"},
        "road_filtered_out": {"type": "integer", "description": "도로명 불일치로 제외"},
        "road_unknown_reasons": {
            "type": "array",
            "description": "도로명 미상 사유 분해(건수 내림차순)",
            "items": {
                "type": "object",
                "properties": {"reason": {"type": "string"}, "count": {"type": "integer"}},
                "required": ["reason", "count"],
            },
        },
        "matched": {"type": "integer", "description": "최종 부합 건수(= deals 길이)"},
        "truncated": {"type": "integer", "description": "limit 초과로 생략된 건수"},
    },
    "required": ["total_in_dong", "matched", "truncated"],
}

_PPP_AGG_SCHEMA = {
    "type": ["object", "null"],
    "description": "평단가 요약(만원/평). 표본이 없으면 null.",
    "properties": {
        "n": {"type": "integer", "description": "표본 수(평단가 산출 가능 거래)"},
        "mean_manwon": _INT, "median_manwon": _INT,
        "min_manwon": _INT, "max_manwon": _INT,
        "p25_manwon": _INT, "p75_manwon": _INT,
    },
}
_STATS_SCHEMA = {
    "type": "object",
    "description": (
        "동 전체 평단가 시세 — 사용자 가격대·도로명 필터와 무관하게, 표제부 역매칭 "
        "이전의 국토부 원본 실거래(해제·산출불가 제외)로 산출. deals(필터 통과분)와 "
        "달리 표본이 줄지 않아 평균이 편향되지 않는다."
    ),
    "properties": {
        "gross": _PPP_AGG_SCHEMA,
        "land": _PPP_AGG_SCHEMA,
        "quarterly": {
            "type": "array",
            "description": (
                "분기별 추이(오래된 순). 같은 표본을 3개월로 쪼갠 것. 월 단위는 "
                "통건물 거래가 월 1~7건이라 무너져 분기로 묶었다 — 그래도 분기당 "
                "표본이 적으니 각 항목의 n 을 함께 읽고, n 이 3 미만인 분기는 "
                "추세 근거로 쓰지 말 것."
            ),
            "items": {
                "type": "object",
                "properties": {
                    "quarter": {"type": "string", "description": "예: '2026 Q2'"},
                    "gross": _PPP_AGG_SCHEMA,
                    "land": _PPP_AGG_SCHEMA,
                },
                "required": ["quarter"],
            },
        },
    },
}

AREA_SCAN_OUTPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "status": {
            "type": "string",
            "enum": ["OK", "NOT_FOUND", "PARSE_ERROR", "CONFIG_ERROR", "EXTERNAL_API_ERROR"],
            "description": "OK 일 때만 deals 에 실측값이 있다. 그 외에는 수치를 추측·보간하지 말 것.",
        },
        "message": {"type": "string"},
        "lookup_status": {"type": "string", "description": "도메인 원본 상태(거래있음/조건_불일치/거래없음 …)"},
        "query": {"type": "string"},
        "scope": {
            "type": "string",
            "enum": ["구", "동"],
            "description": (
                "'구' 면 구 전체 통계 모드 — deals 는 항상 비어 있고(역매칭 미실행) "
                "by_dong 에 동별 순위가 담긴다. 개별 지번이 필요하면 동으로 다시 조회할 것."
            ),
        },
        "area": {
            "type": ["object", "null"],
            "properties": {"gu": {"type": "string"}, "dong": {"type": "string"}},
            "required": ["gu", "dong"],
        },
        "by_dong": {
            "type": "array",
            "description": "구 모드 전용 — 동별 평단가(대지 중앙값 내림차순). 동 모드에서는 빈 배열.",
            "items": {
                "type": "object",
                "properties": {
                    "dong": {"type": "string"},
                    "gross": _PPP_AGG_SCHEMA,
                    "land": _PPP_AGG_SCHEMA,
                },
                "required": ["dong"],
            },
        },
        "period_months": {"type": "integer"},
        "filter": {
            "type": "object",
            "description": "실제 적용된 필터(클램프·스왑 후 값).",
            "properties": {
                "min_manwon_per_gross_pyeong": _INT,
                "max_manwon_per_gross_pyeong": _INT,
                "road_contains": {"type": "string"},
                "limit": {"type": "integer"},
            },
        },
        "coverage": _COVERAGE_SCHEMA,
        "stats": _STATS_SCHEMA,
        "deals": {"type": "array", "items": _SCAN_DEAL_SCHEMA,
                  "description": "최신순. 평단가 기준은 연면적(거래금액 ÷ 연면적평)."},
        "source": {"type": "string"},
    },
    "required": ["status", "query", "period_months", "deals", "source"],
}

_SCAN_STATUS = {
    "파싱실패": "PARSE_ERROR", "구_미상": "PARSE_ERROR",
    "API_오류": "EXTERNAL_API_ERROR",
    "거래없음": "NOT_FOUND", "조건_불일치": "NOT_FOUND",
    "거래있음": "OK",
}


def _fill_gu_payload(d: dict[str, Any], area: str, months: int) -> dict[str, Any]:
    """구 모드 payload 채우기 — 거래 목록 없이 통계·분기추이·동별 순위만.

    동 모드와 같은 스키마를 쓰되 deals 는 항상 비고(역매칭을 돌리지 않는다),
    coverage 는 구 기준 모수로 채운다. scope='구' 로 소비자가 구분한다.
    """
    res = _cached_scan_gu(area, months)
    d["scope"] = "구"
    d["lookup_status"] = res["status"]
    d["status"] = _SCAN_STATUS.get(res["status"], "EXTERNAL_API_ERROR")
    d["message"] = res.get("message", "")
    if res.get("gu"):
        d["area"] = {"gu": res["gu"], "dong": ""}
    if d["status"] in _ERR_PREFIX:
        return d

    d["coverage"].update(
        total_in_dong=res.get("total_in_gu", 0),
        jiphap_excluded=res.get("jiphap_excluded", 0),
        cancelled_count=res.get("cancelled_count", 0),
        ppp_uncomputable=res.get("ppp_uncomputable", 0),
        matched=len(res.get("by_dong") or []),
    )
    d["stats"] = res.get("stats") or {}
    d["by_dong"] = res.get("by_dong") or []
    return d


def _area_scan_payload(
    area: str, min_eok_per_pyeong: float, max_eok_per_pyeong: float,
    months: int, road_contains: str, limit: int,
) -> dict[str, Any]:
    """area_scan 의 구조화 정본. 텍스트는 이 payload 에서만 파생된다."""
    months = max(1, min(int(months), 60))
    limit = max(1, min(int(limit), 200))
    lo = round(min_eok_per_pyeong * 10000) if min_eok_per_pyeong and min_eok_per_pyeong > 0 else None
    hi = round(max_eok_per_pyeong * 10000) if max_eok_per_pyeong and max_eok_per_pyeong > 0 else None
    if lo is not None and hi is not None and lo > hi:
        lo, hi = hi, lo
    road_q = road_contains.strip()

    d: dict[str, Any] = {
        "status": "", "message": "", "lookup_status": "",
        "query": area, "scope": "동", "area": None, "period_months": months,
        "filter": {"min_manwon_per_gross_pyeong": lo,
                   "max_manwon_per_gross_pyeong": hi,
                   "road_contains": road_q, "limit": limit},
        "coverage": {"total_in_dong": 0, "jiphap_excluded": 0, "cancelled_count": 0,
                     "ppp_uncomputable": 0,
                     "price_excluded": 0, "road_unknown": 0, "road_filtered_out": 0,
                     "road_unknown_reasons": [], "matched": 0, "truncated": 0},
        "stats": {}, "by_dong": [], "deals": [], "source": SOURCE_LINE,
    }

    if not _key_ok():
        d.update(status="CONFIG_ERROR", message=KEY_MISSING_MSG)
        return d

    # '강남구'처럼 구만 들어오면 구 모드 — 역매칭 없이 통계·동별 순위만 낸다.
    if resolve_gu_query(area):
        return _fill_gu_payload(d, area, months)

    res = _cached_scan(area, months, lo, hi, road_q, limit)

    d["lookup_status"] = res["status"]
    d["status"] = _SCAN_STATUS.get(res["status"], "EXTERNAL_API_ERROR")
    d["message"] = res.get("message", "")
    if res.get("gu") or res.get("dong"):
        d["area"] = {"gu": res.get("gu", ""), "dong": res.get("dong", "")}

    if d["status"] in _ERR_PREFIX:
        return d

    deals = res.get("deals") or []
    reasons = res.get("road_unknown_reasons") or {}
    d["coverage"].update(
        total_in_dong=res.get("total_in_dong", 0),
        jiphap_excluded=res.get("jiphap_excluded", 0),
        cancelled_count=res.get("cancelled_count", 0),
        ppp_uncomputable=res.get("ppp_uncomputable", 0),
        price_excluded=res.get("price_excluded", 0),
        road_unknown=res.get("road_unknown", 0),
        road_filtered_out=res.get("road_filtered_out", 0),
        road_unknown_reasons=[{"reason": k, "count": v} for k, v in
                              sorted(reasons.items(), key=lambda kv: -kv[1])],
        matched=len(deals),
        truncated=res.get("truncated", 0),
    )
    d["stats"] = res.get("stats") or {}
    d["deals"] = deals
    return d


def _render_gu_scan(d: dict[str, Any]) -> str:
    """구 모드 텍스트 — 구 전체 시세 · 분기 추이 · 동별 순위. 거래 목록은 없다."""
    a, c = d["area"] or {}, d["coverage"]
    lines = [
        f"■ 구 시세 — {a.get('gu', '')} · 최근 {d['period_months']}개월 · 통건물"
    ]
    cov = [f"구내 통건물 매매 {c['total_in_dong']}건"]
    if c.get("jiphap_excluded"):
        cov.append(f"집합(구분상가) {c['jiphap_excluded']} 제외")
    cov += [f"해제 {c['cancelled_count']} 제외",
            f"평단가 산출불가 {c['ppp_uncomputable']}"]
    lines.append("  [커버리지] " + " · ".join(cov) + f" → {c['matched']}개 동")
    lines.extend(_fmt_stats_lines(d.get("stats") or {}))

    rows = d.get("by_dong") or []
    if rows:
        lines.append("  [동별 대지 평단가 — 중앙, 만원/평]")
        for r in rows:
            la = r.get("land")
            if la and la.get("n"):
                lines.append(
                    f"    {r['dong']} {la['median_manwon']:,} (n={la['n']})"
                    + ("  ※표본적음" if la["n"] < 5 else "")
                )
            else:
                lines.append(f"    {r['dong']} 대지 표본 없음")
    lines.append("  ※ 구 모드는 통계 전용 — 개별 거래·지번은 동으로 다시 조회할 것"
                 " (예: '대치동').")
    lines.append(d["source"])
    return "\n".join(lines)


def _render_area_not_found(d: dict[str, Any]) -> str:
    """조건 0건이어도 동 시세는 살려서 보여준다.

    가격대 밴드가 실제 시세대와 안 맞으면 0건이 나는데, 그때 사용자에게 가장
    필요한 정보가 '그럼 실제 시세는 얼마냐'다. stats 는 밴드·도로명 필터와 무관한
    전수라 이 상황에서도 유효하다 — 조기 종료로 감추면 왜 0건인지 설명할 길이 없다.
    거래 자체가 없으면(stats 없음) 기존과 같이 한 줄로 끝난다.
    """
    head = f"{NOT_FOUND} {d['message']} {NO_GUESS}"
    stats_lines = _fmt_stats_lines(d.get("stats") or {})
    if not stats_lines:
        return head

    lines = [head]
    g = (d.get("stats") or {}).get("gross") or {}
    f = d.get("filter") or {}
    lo, hi = f.get("min_manwon_per_gross_pyeong"), f.get("max_manwon_per_gross_pyeong")
    med = g.get("median_manwon")
    if med and (lo or hi):
        # 배수는 항상 '큰 값 ÷ 작은 값'으로 적는다 — 0.1배 같은 표기는 읽기 어렵다.
        gap = ""
        if lo and med and lo > med * 2:
            gap = f" — 하한 {lo:,}만원/평이 실제 중앙값의 {lo / med:.1f}배"
        elif hi and med and hi * 2 < med:
            gap = f" — 상한 {hi:,}만원/평보다 실제 중앙값이 {med / hi:.1f}배 높음"
        lines.append(
            "  ※ 아래 동 시세는 가격대 조건과 무관한 전수라 이 경우에도 유효하다"
            f"{gap}."
        )
    lines.extend(stats_lines)
    return "\n".join(lines)


def _render_area_scan(d: dict[str, Any]) -> str:
    """payload → 사람/LLM 가독 텍스트. 개조 전 출력과 바이트 동일."""
    if d["status"] in _ERR_PREFIX:
        return f"{_ERR_PREFIX[d['status']]} {d['message']}"
    if d["status"] == "NOT_FOUND":
        return _render_area_not_found(d)
    if d.get("scope") == "구":
        return _render_gu_scan(d)

    a, f, c = d["area"] or {}, d["filter"], d["coverage"]
    road_q = f["road_contains"]
    lines = [
        f"■ 지역 스캔 — {a.get('gu', '')} {a.get('dong', '')} · 최근 {d['period_months']}개월 "
        f"· 연면적 평당 {_fmt_band(f['min_manwon_per_gross_pyeong'], f['max_manwon_per_gross_pyeong'])}",
    ]
    cov = [f"동내 통건물 매매 {c['total_in_dong']}건"
           if c.get("jiphap_excluded") else f"동내 매매 {c['total_in_dong']}건"]
    if c.get("jiphap_excluded"):
        cov.append(f"집합(구분상가) {c['jiphap_excluded']} 제외")
    cov += [f"해제 {c['cancelled_count']} 제외",
            f"평단가 산출불가 {c['ppp_uncomputable']}",
            f"조건 밖 {c['price_excluded']}"]
    if road_q:
        cov.append(f"도로명 미상 {c['road_unknown']}")
        cov.append(f"도로명 불일치 {c['road_filtered_out']}")
    lines.append("  [커버리지] " + " · ".join(cov)
                 + f" → 부합 {c['matched']}건"
                 + (f" (+{c['truncated']}건 생략)" if c["truncated"] else ""))
    if road_q:
        lines.append(f"  ※ 도로명 필터는 표제부 역매칭 성공 거래에만 적용 — "
                     f"미복원 거래 {c['road_unknown']}건은 도로명 특정 불가로 제외됨.")
        rb = c["road_unknown_reasons"]
        if rb:
            parts = " · ".join(f"{r['reason']} {r['count']}" for r in rb)
            lines.append(f"    └ 미상 {c['road_unknown']}건 사유: {parts}")

    lines.extend(_fmt_stats_lines(d.get("stats") or {}))

    for deal in d["deals"]:
        lines.extend(_fmt_scan_deal(deal))

    if any(x.get("jibun_masked") and not x.get("resolved_address") for x in d["deals"]):
        lines.append("  ⚠ '마스킹 미복원' 표기 거래는 지번·주소를 특정하지 못한 것 "
                     "(평단가·거래금액은 실측). 지번 확정은 deal_card_search 로 개별 조회 요망.")
    lines.append(NO_GUESS)
    lines.append(SOURCE_LINE)
    return "\n".join(lines)


@mcp.tool(title="지역 스캔", output_schema=AREA_SCAN_OUTPUT_SCHEMA,
          annotations=READ_ONLY_EXTERNAL)
def area_scan(
    area: str,
    min_eok_per_pyeong: float = 0.0,
    max_eok_per_pyeong: float = 0.0,
    months: int = 12,
    road_contains: str = "",
    limit: int = 30,
) -> ToolResult:
    """구·동 단위 상업업무용(통건물) 시세 — 동이면 후보 거래까지, 구면 통계·동별 순위.

    area 에 구만 주면('강남구') 구 모드, 동을 주면('대치동') 동 모드다.
    지번을 몰라도 '동 + 가격대'로 조회한다.
    예: area='성수동1가', min_eok_per_pyeong=4.5, max_eok_per_pyeong=5.5
        → 연면적 평당 4.5~5.5억 거래를 최신순으로.
    min/max 가 0 이면 해당 방향 제한 없음. 평당가 기준 = 연면적(거래금액÷연면적평).
    road_contains(예: '연무장길')를 주면 표제부 역매칭으로 도로명이 특정된 거래만
    부분일치 필터 — 도로명 미상(마스킹 미복원) 거래는 제외되며 그 건수를 함께 고지한다.
    서울 상업업무용 매매 한정(v1). 첫 조회는 표제부 API 로딩으로 수십 초 걸릴 수 있다.
    표본 대표성을 판단할 때는 structuredContent 의 coverage(모수 분해)를 함께 읽을 것.

    취급 범위는 통건물(유형='일반')뿐 — 집합(구분상가)은 제외한다(coverage.jiphap_excluded
    에 건수). 그러므로 stats 는 전부 '통건물 시세'이며, 상가 한 칸 시세가 아니다.

    area 에 구만 주면('강남구'·'강남') **구 모드**로 동작한다 — 구 전체 시세와 분기
    추이, 그리고 by_dong(동별 대지 평단가 내림차순)을 낸다. 구 모드는 표제부 역매칭을
    돌리지 않아 deals 가 항상 비어 있다(scope='구' 로 구분). 개별 지번·거래가 필요하면
    동으로 다시 부를 것.

    동 평균 평단가는 stats(연면적·대지)에 담긴다 — 가격대·도로명 필터와 무관하게,
    표제부 역매칭 이전의 국토부 원본 실거래(통건물, 해제·산출불가 제외)로 산출하므로
    전수에 가깝다. '이 동 평단가 시세'를 물으면 deals 가 아니라 stats 를 근거로 답할 것
    (deals 는 필터 통과분이라 평균 내면 편향된다). 평균이 필요할 뿐이면 min/max 를
    주지 말고(밴드 없이) 호출하면 된다.
    """
    data = _area_scan_payload(area, min_eok_per_pyeong, max_eok_per_pyeong,
                              months, road_contains, limit)
    return ToolResult(content=_render_area_scan(data), structured_content=data,
                      is_error=data["status"] in _IS_ERROR_STATUSES)


# ── deal_card_create: 데이터카드 PNG 1장 ────────────────────────────────
# 조회 결과를 그대로 이미지로 옮긴다. 창작 문구가 없으므로 카드의 모든 글자가
# 실측값이거나 고정 라벨이며, LLM 이 지어낼 자리가 없다.
#
# 신뢰도는 **차단 + 표기** 둘 다로 다룬다.
#   차단 — 카드 PNG 는 대화를 떠나 고객 손에 가는, 이 도구 유일의 '맥락 없이
#     유통되는 산출물'이다. '추정매칭'은 정의상 동일 스펙 옆 건물일 수 있는
#     상태라, 옆 건물 실거래가가 이 건물 값으로 박힌 이미지가 돌 수 있다.
#     그래서 사진 게이트와 같은 관용구로 기본 차단하고, 맥락을 아는 사람이
#     allow_estimated=true 로 의식적으로 통과시키게 한다.
#   표기 — 통과한 카드에는 신뢰도 배지를 계속 박는다. 이미지가 손을 떠난 뒤에는
#     대화로 고지할 방법이 없으므로 근거가 늘 따라다녀야 한다.
# 배지만으로는 '받는 사람이 배지를 읽는다'에 기대게 된다 — 그래서 보내는 사람의
# 결정을 한 번 요구하는 게이트를 앞에 둔다(2026-08-22 이슈 반영).
_CARD_CONFIDENCE_MIN = 0.90  # matching.CONFIDENCE: 정확매칭 0.90/0.97 통과, 추정 0.60·인접 0.50 차단

DEAL_CARD_CREATE_OUTPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "status": {
            "type": "string",
            "enum": ["OK", "NOT_FOUND", "PHOTO_MISSING", "LOW_CONFIDENCE",
                     "RENDER_ERROR", "PARSE_ERROR", "CONFIG_ERROR",
                     "EXTERNAL_API_ERROR"],
            "description": (
                "OK 일 때만 out_png 에 카드가 생성됐다. "
                "PHOTO_MISSING·LOW_CONFIDENCE 는 실패가 아니라 '사람이 결정할 게 "
                "남은' 상태다 — 전자는 사진 경로를 주거나 allow_no_photo=true, "
                "후자는 근거(match_explain) 확인 후 allow_estimated=true 로 "
                "재호출하면 진행된다. 재시도로 뚫리지 않으니 사용자에게 그대로 전할 것."
            ),
        },
        "message": {"type": "string"},
        "query": {"type": "string"},
        "address": {"type": "object"},
        "out_png": {"type": "string", "description": "생성된 카드 PNG 경로(4:5, 2160×2700)"},
        "photo": {"type": "string", "description": "카드에 쓴 건물 사진(미지정 시 빈 문자열)"},
        "eyebrow": {"type": "string"},
        "price": {"type": "string"},
        "rows": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {"label": {"type": "string"}, "value": {"type": "string"}},
                "required": ["label", "value"],
            },
        },
        "confidence": {"type": "string"},
        "confidence_score": {"type": ["number", "null"]},
        "notes": {"type": "array", "items": {"type": "string"}},
        "source": {"type": "string"},
    },
    "required": ["status", "query", "out_png", "rows", "source"],
}


_UNSAFE_NAME = re.compile(r"[\\/\x00-\x1f:*?\"<>|]")


def _safe_stem(name: str) -> str:
    """지번 문자열 → 안전한 파일명. 경로 구분자·상위참조·제어문자를 제거한다.

    지금은 parse_address 가 이런 문자를 이미 걸러내지만, 그건 **부수효과**다.
    파서가 완화되는 순간 임의 경로 쓰기로 조용히 승격되므로 여기서 못을 박는다.
    """
    s = _UNSAFE_NAME.sub("", name).replace("..", "").strip(" .")
    return " ".join(s.split()) or "card"


def _card_out_dir() -> Path:
    """카드 저장 폴더. `DEAL_LOCATOR_CARD_DIR` 로 재정의 가능.

    기본은 홈 아래 고정 경로다 — 서버의 실행 cwd 는 MCP 클라이언트가 정하므로
    상대경로로 두면 카드가 어디에 떨어졌는지 사용자가 찾지 못한다.
    """
    from datetime import date
    env = os.environ.get("DEAL_LOCATOR_CARD_DIR", "").strip()
    root = Path(env).expanduser() if env else (Path.home() / "deal-locator-cards")
    return root / date.today().isoformat()


def _card_price(manwon: Optional[int]) -> str:
    """만원 → 카드 매매가 문자열. **반올림**한다(절사 금지).

    절사는 오차가 항상 한 방향(과소표기)이고 최대 9,999만원까지 벌어진다 —
    19억 9,900만원이 '19억'으로 나가면 약 1억을 깎아 표기하는 셈이다.
    반올림은 오차가 절반(≤5,000만원)이고 양방향이라 계통 편향이 없다.
    """
    if not manwon or manwon <= 0:
        return ""
    eok = manwon / 10000
    if eok >= 10:
        r = round(eok, 1)
        return f"{int(r)}억" if r == int(r) else f"{r}억"
    if eok >= 1:
        return f"{f'{eok:.2f}'.rstrip('0').rstrip('.')}억"
    return f"{manwon:,}만원"


def _pyeong(v: Optional[float]) -> str:
    """평 표기 — 정수 반올림 + 천단위 콤마('1,493평')."""
    return f"{int(round(v)):,}평" if v else ""


def _resolve_card_path(head: str) -> Path:
    """카드 저장 경로. **반드시 카드 폴더 하위**여야 한다(경로 탈출 방지)."""
    root = _card_out_dir().resolve()
    out = (root / f"{_safe_stem(head)}.png").resolve()
    if root not in out.parents:
        raise ValueError(f"카드 저장 경로가 지정 폴더를 벗어난다: {out}")
    return out


def _card_rows(t: dict[str, Any], b: Optional[dict[str, Any]], head: str) -> list[dict[str, str]]:
    """조회 payload → 카드 표 행. 값이 없는 행은 **생략**한다.

    '-' 나 '미상'을 박느니 행을 빼는 게 낫다 — 빈칸은 사용자가 원장을 확인하게
    만들지만, 그럴듯한 자리표시자는 확인 없이 인용된다.
    """
    b = b or {}
    rows: list[dict[str, str]] = [{"label": "지번", "value": head}]

    land = t.get("land_pyeong") or b.get("land_pyeong")
    if land:
        v = _pyeong(land)
        ppp = t.get("price_per_land_pyeong_manwon")
        if ppp:
            v += f" [평단가 {ppp:,}만원]"
        rows.append({"label": "토지면적", "value": v})

    gross = t.get("gross_pyeong") or b.get("gross_pyeong")
    if gross:
        rows.append({"label": "연면적", "value": _pyeong(gross)})
    if t.get("zone"):
        rows.append({"label": "용도지역", "value": t["zone"]})
    if t.get("build_year") or b.get("build_year"):
        rows.append({"label": "준공연도", "value": str(t.get("build_year") or b.get("build_year"))})
    if t.get("deal_date"):
        rows.append({"label": "거래일", "value": t["deal_date"]})
    return rows


def _deal_card_create_payload(address: str, months: int, photo: str,
                              eyebrow: str, allow_no_photo: bool,
                              allow_estimated: bool = False) -> dict[str, Any]:
    d: dict[str, Any] = {
        "status": "NOT_FOUND", "message": "", "query": address, "address": {},
        "out_png": "", "photo": "", "eyebrow": "", "price": "", "rows": [],
        "confidence": "", "confidence_score": None, "notes": [], "source": SOURCE_LINE,
    }
    card = _deal_card_search_payload(address, months)
    d["address"] = card["address"]
    d["notes"] = list(card["notes"])
    if card["status"] != "OK":
        d.update(status=card["status"], message=card["message"])
        return d

    t = card["latest"]
    b = card.get("building") or {}
    a = card["address"] or {}
    head = f"{a.get('gu','')} {a.get('dong','')} {a.get('bunji','')}".strip()
    d["confidence"] = t.get("confidence", "")
    d["confidence_score"] = t.get("confidence_score")

    # ── 신뢰도 게이트 ────────────────────────────────────────────────
    # 사진 게이트보다 **먼저** 건다: 사진을 구해 온 뒤에야 "사실 지번이 미확정"을
    # 듣게 되면 헛수고가 된다. 더 치명적인 쪽(지번 미확정)을 먼저 알린다.
    # 신뢰도 미상(score=None)도 **막는다**(fail-closed). 배지는 라벨이 비면 렌더에서
    # 생략되므로(render/card.html `{% if conf_text %}`), 미상을 통과시키면 게이트도
    # 배지도 없는 카드가 나가 이 설계의 두 겹이 한꺼번에 무너진다.
    score = t.get("confidence_score")
    if not allow_estimated and (score is None or score < _CARD_CONFIDENCE_MIN):
        caveat = t.get("caveat") or ""
        shown = f"{score:.2f}" if score is not None else "미상"
        label = t.get("confidence") or "미상"
        d.update(status="LOW_CONFIDENCE",
                 message=(f"{head} — 매칭 신뢰도가 낮아('{label}' {shown}) "
                          f"카드를 만들지 않았다.\n"
                          + (f"{caveat}\n" if caveat else "")
                          + "카드 PNG 는 대화를 떠나 고객 손에 가므로, 지번이 확정되지 "
                            "않은 건은 옆 건물 실거래가가 이 건물 값으로 박힌 이미지가 "
                            "돌 수 있다.\n"
                            "match_explain 으로 매칭 근거를 확인한 뒤, 그래도 발행하려면 "
                            "allow_estimated=true 로 재호출할 것 "
                            "(카드에는 신뢰도 배지가 함께 찍힌다)."))
        return d

    # ── 사진 게이트 ──────────────────────────────────────────────────
    # 사진 없는 카드는 결국 다시 만들게 된다. 렌더(수 초 + 브라우저 기동)를
    # 태우기 전에 멈추고 사람에게 물어보는 편이 싸다. 실패가 아니라 결정 대기다.
    photo_path = ""
    if photo:
        p = Path(photo).expanduser()
        if p.is_file():
            # 확장자가 아니라 내용으로 판별한다. 이미지가 아니면 렌더러가 조용히
            # 무시해 '사진이 들어간 줄 알았는데 안 들어간' 카드가 나간다.
            from deal_locator.render import image_mime
            if not image_mime(p):
                if not allow_no_photo:
                    d.update(status="PHOTO_MISSING",
                             message=(f"{head} — 이미지 파일이 아니라 렌더를 멈췄다: {p}\n"
                                      f"PNG·JPEG·GIF·WebP 만 쓸 수 있다(확장자가 아니라 "
                                      f"파일 내용으로 판별한다). 다른 파일을 지정할 것."))
                    return d
            else:
                photo_path = str(p)
        elif not allow_no_photo:
            d.update(status="PHOTO_MISSING",
                     message=(f"{head} — 지정한 사진을 찾지 못해 렌더를 멈췄다: {p}\n"
                              f"경로를 확인해 다시 호출하거나, 사진 없이 만들려면 "
                              f"allow_no_photo=true 로 호출할 것."))
            return d
    if not photo_path and not allow_no_photo:
        d.update(status="PHOTO_MISSING",
                 message=(f"{head} — 건물 사진이 없어 렌더를 멈췄다. "
                          f"카드 상단 전체가 건물 사진이라 사진이 없으면 빈 회색으로 나간다.\n"
                          f"photo 인자에 사진 파일 경로를 주고 다시 호출할 것 "
                          f"(예: photo='~/Desktop/{head}.jpg'). "
                          f"사진 없이 그대로 만들려면 allow_no_photo=true."))
        return d

    ym = (t.get("deal_date", "") or "")[:7].replace("-", ".")
    d.update(
        eyebrow=eyebrow or (f"{a.get('gu','')} 실거래" + (f" · {ym}" if ym else "")),
        price=_card_price(t.get("price_manwon")) or t.get("price", ""),
        rows=_card_rows(t, b, head),
        confidence=t.get("confidence", ""),
        confidence_score=t.get("confidence_score"),
        photo=photo_path,
    )

    try:
        from deal_locator.render import render_card
        out = render_card({
            "out_png": str(_resolve_card_path(head)),
            "photo": photo_path,
            "eyebrow": d["eyebrow"],
            "price": d["price"],
            "rows": d["rows"],
            "confidence": d["confidence"],
            "confidence_score": d["confidence_score"],
            "source": SOURCE_LINE,
        })
    except ImportError as e:
        d.update(status="CONFIG_ERROR",
                 message=(f"렌더 의존 모듈 없음({e}). "
                          "`pip install jinja2 playwright` 후 `playwright install chromium` 을 실행하세요."))
        return d
    except Exception as e:  # noqa: BLE001
        msg = f"{type(e).__name__}: {e}"
        if "executable doesn't exist" in msg.lower() or "playwright install" in msg.lower():
            msg += " — 최초 1회 `playwright install chromium` 이 필요합니다."
        d.update(status="RENDER_ERROR", message=f"카드 렌더 실패 — {msg}")
        return d

    d["status"] = "OK"
    d["out_png"] = out
    if not photo_path:
        d["notes"].append("※ 건물 사진 없이 생성됨(allow_no_photo) — 사진을 주면 카드가 크게 달라진다")
    if t.get("caveat"):
        d["notes"].append(f"※ {t['caveat']}")
    return d


def _render_deal_card_create(d: dict[str, Any]) -> str:
    if d["status"] in _ERR_PREFIX:
        return f"{_ERR_PREFIX[d['status']]} {d['message']}"
    if d["status"] == "NOT_FOUND":
        return f"{NOT_FOUND} {d['message']} {NO_GUESS}"
    if d["status"] == "PHOTO_MISSING":
        return f"[PHOTO_MISSING] {d['message']}"
    if d["status"] == "LOW_CONFIDENCE":
        return f"[LOW_CONFIDENCE] {d['message']}"
    if d["status"] == "RENDER_ERROR":
        return f"[RENDER_ERROR] {d['message']}"

    a = d["address"] or {}
    head = f"{a.get('gu','')} {a.get('dong','')} {a.get('bunji','')}".strip()
    lines = [f"■ 데이터카드 생성 — {head}",
             f"  카드: {d['out_png']}",
             f"  키커: {d['eyebrow']} · 매매가: {d['price']}"]
    lines.extend(f"  {r['label']}: {r['value']}" for r in d["rows"])
    if d["confidence"]:
        lines.append(f"  매칭 신뢰도: {d['confidence']}({d['confidence_score']}) — 카드에도 표기됨")
    if d["photo"]:
        lines.append(f"  사진: {d['photo']}")
    lines.extend("  " + n for n in d["notes"])
    lines.append(SOURCE_LINE)
    return "\n".join(lines)


@mcp.tool(title="데이터카드 만들기", output_schema=DEAL_CARD_CREATE_OUTPUT_SCHEMA,
          annotations={**READ_ONLY_EXTERNAL, "readOnlyHint": False, "idempotentHint": False})
def deal_card_create(
    address: str,
    months: int = 12,
    photo: str = "",
    eyebrow: str = "",
    allow_no_photo: bool = False,
    allow_estimated: bool = False,
) -> ToolResult:
    """지번 하나로 실거래 데이터카드 PNG 1장(4:5)을 만든다 — 조회부터 이미지까지.

    카드에 들어가는 값은 전부 실거래·건축물대장 실측값이다(매매가·토지/연면적·
    평단가·용도지역·준공연도·거래일·매도매수). 홍보 문구는 넣지 않는다.

    **추정매칭 이하(신뢰도 0.90 미만)는 기본적으로 만들지 않고 LOW_CONFIDENCE 로
    멈춘다** — 카드는 대화를 떠나 고객 손에 가는데, 지번이 확정되지 않은 건은 옆
    건물 실거래가가 이 건물 값으로 박힌 이미지가 될 수 있기 때문이다. 이때는
    재시도하지 말고 match_explain 으로 근거를 확인시킨 뒤, 사용자가 발행하기로
    정하면 allow_estimated=true 로 재호출한다.
    **통과한 카드에는 매칭 신뢰도가 배지로 찍힌다.** 이미지가 손을 떠난 뒤에는
    대화로 고지할 수 없기 때문이며, '추정매칭' 이하는 색으로 구분된다.

    **사진이 없으면 렌더하지 않고 PHOTO_MISSING 으로 멈춘다** — 카드는 건물 사진
    위에 수치를 얹는 형태라, 사진이 없으면 회색 판이 나가고 결국 다시 만들게 된다.
    이때는 재시도하지 말고 사용자에게 사진 경로를 물어볼 것. 사진 없이 진행하기로
    사용자가 정하면 allow_no_photo=true 로 재호출한다.
    eyebrow 로 매매가 위 골드 한 줄을 덮어쓸 수 있다. 기본값은
    '<구> 실거래 · YYYY.MM'. **'최고가/최저가' 같은 단정 표현은 쓰지 말 것** —
    이 도구의 조회 범위(서울 상업업무용·기간 한정) 안에서의 순위일 뿐이라
    카드에 박히면 근거 없는 단정이 된다.
    저장 위치는 ~/deal-locator-cards/<날짜>/ (DEAL_LOCATOR_CARD_DIR 로 변경 가능).

    최초 1회 `playwright install chromium` 이 필요하다.
    """
    data = _deal_card_create_payload(address, months, photo, eyebrow,
                                     allow_no_photo, allow_estimated)
    return ToolResult(content=_render_deal_card_create(data), structured_content=data,
                      is_error=data["status"] in _IS_ERROR_STATUSES
                      or data["status"] == "RENDER_ERROR")


# ── deals_export: 연월 지정 실거래 CSV 내보내기 ──

DEALS_EXPORT_OUTPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "status": {
            "type": "string",
            "enum": ["OK", "NOT_FOUND", "PARSE_ERROR", "CONFIG_ERROR",
                     "EXTERNAL_API_ERROR"],
            "description": "OK 일 때만 path 에 CSV 가 생성됐다.",
        },
        "message": {"type": "string"},
        "path": {"type": "string",
                 "description": "복원본 CSV 절대경로(utf-8-sig). match=false 면 원본 CSV"},
        "filename": {"type": "string"},
        "path_unmatched": {"type": "string",
                           "description": "미복원본 CSV 경로 — 미복원 0건이거나 "
                                          "match=false 면 빈 문자열"},
        "filename_unmatched": {"type": "string"},
        "rows_total": {"type": "integer",
                       "description": "통건물 전체 행 수(해제 포함, 두 파일 합)"},
        "rows_matched": {"type": ["integer", "null"],
                         "description": "복원본 행 수 — match=false 면 null"},
        "rows_unmatched": {"type": ["integer", "null"],
                           "description": "미복원본 행 수 — match=false 면 null"},
        "confidence_breakdown": {
            "type": "object",
            "description": "복원본의 매칭신뢰도별 건수(정확매칭/추정매칭/지번공개)"},
        "months": {"type": "array", "items": {"type": "string"},
                   "description": "내보낸 연월(YYYYMM) 목록"},
        "gu_scope": {"type": "string", "description": "'서울전역' 또는 구 이름"},
        "per_gu": {"type": "object", "description": "구별 전체 행 수"},
        "jiphap_excluded": {"type": "integer",
                            "description": "취급 범위 밖이라 제외한 집합(구분상가) 건수"},
        "cancelled_count": {"type": "integer",
                            "description": "해제신고 건수 — 행은 남아 있고 컬럼으로 구분"},
        "masked_count": {"type": "integer", "description": "지번 마스킹 거래 건수"},
        "match": {"type": "boolean"},
        "source": {"type": "string"},
    },
    "required": ["status", "path", "rows_total", "months", "gu_scope", "source"],
}

# 매칭단계 문자열 → 파이프라인 표준 신뢰도 등급.
# CONFIDENCE(matching.py) 정본과 같은 어휘: 1·2단계 = 정확매칭,
# 3단계·추정 = 추정매칭. 비마스킹 행은 지번이 애초에 공개라 '지번공개'.
def _stage_confidence(stage: str, masked: bool) -> str:
    s = str(stage).strip()
    if not masked:
        return "지번공개"
    if s.startswith(("1단계", "2단계", "지번매칭")):
        return "정확매칭"
    if s.startswith(("3단계", "추정매칭")):
        return "추정매칭"
    return ""

_YM_RE = re.compile(r"^\d{6}$")
_EXPORT_MAX_MONTHS = 24


def _export_out_dir() -> Path:
    """CSV 저장 폴더. `DEAL_LOCATOR_EXPORT_DIR` 로 재정의 가능.

    카드(_card_out_dir)와 같은 이유로 홈 아래 고정 경로가 기본이다 —
    서버 cwd 는 MCP 클라이언트가 정하므로 상대경로는 사용자가 못 찾는다.
    카드와 달리 **날짜 하위폴더를 만들지 않는다** — 이 CSV 는 파이프라인이
    집어가는 원천 데이터라 경로가 실행일과 무관하게 예측 가능해야 하고,
    같은 연월 재실행은 같은 파일을 덮어쓰는 게(멱등) 적재에 안전하다.
    """
    env = os.environ.get("DEAL_LOCATOR_EXPORT_DIR", "").strip()
    return Path(env).expanduser() if env else (Path.home() / "deal-locator-exports")


def _ym_parse(ym: str) -> Optional[tuple[int, int]]:
    """'YYYYMM' → (년, 월). 형식·범위(2006~, 1~12월) 밖이면 None."""
    if not _YM_RE.match(ym or ""):
        return None
    y, m = int(ym[:4]), int(ym[4:])
    if not (2006 <= y <= 2100 and 1 <= m <= 12):
        return None
    return y, m


def _deals_export_payload(year_month: str, year_month_to: str,
                          gu: str, match: bool) -> dict[str, Any]:
    d: dict[str, Any] = {
        "status": "NOT_FOUND", "message": "", "path": "", "filename": "",
        "path_unmatched": "", "filename_unmatched": "",
        "rows_total": 0, "rows_matched": None, "rows_unmatched": None,
        "confidence_breakdown": {},
        "months": [], "gu_scope": gu or "서울전역", "per_gu": {},
        "jiphap_excluded": 0, "cancelled_count": 0, "masked_count": 0,
        "match": bool(match), "source": SOURCE_LINE,
    }
    if not _key_ok():
        d.update(status="CONFIG_ERROR", message=KEY_MISSING_MSG)
        return d

    ym1 = _ym_parse(year_month)
    if ym1 is None:
        d.update(status="PARSE_ERROR",
                 message=f"연월은 'YYYYMM'(2006년 이후) 형식이어야 한다: '{year_month}'")
        return d
    ym2 = ym1
    if year_month_to:
        ym2 = _ym_parse(year_month_to)
        if ym2 is None:
            d.update(status="PARSE_ERROR",
                     message=f"연월은 'YYYYMM' 형식이어야 한다: '{year_month_to}'")
            return d
    span = (ym2[0] - ym1[0]) * 12 + (ym2[1] - ym1[1]) + 1
    if span < 1:
        d.update(status="PARSE_ERROR",
                 message=f"범위가 역순이다: {year_month} → {year_month_to}")
        return d
    if span > _EXPORT_MAX_MONTHS:
        d.update(status="PARSE_ERROR",
                 message=(f"범위가 너무 넓다({span}개월) — 한 번에 최대 "
                          f"{_EXPORT_MAX_MONTHS}개월까지. 나눠서 호출할 것."))
        return d
    if gu and gu not in SEOUL_GU_CODES:
        d.update(status="PARSE_ERROR",
                 message=f"'{gu}' 는 지원 범위 밖 — 서울 25개 구만 지원한다(v1).")
        return d

    months: list[str] = []
    y, m = ym1
    for _ in range(span):
        months.append(f"{y}{m:02d}")
        m += 1
        if m > 12:
            y, m = y + 1, 1
    d["months"] = months
    gus = [gu] if gu else list(SEOUL_GU_CODES)

    import pandas as pd
    from deal_locator.core.pipeline import filter_ilban

    p = get_pipeline()
    frames = []
    with _quiet_stdout():
        for g in gus:
            for ym in months:
                df = p.fetch_month_cached(ym, g)
                if df is not None and not df.empty:
                    frames.append(df)
    if not frames:
        d["message"] = (f"{d['gu_scope']} {months[0]}~{months[-1]} 상업업무용 "
                        f"실거래가 없다(또는 아직 공개 전).")
        return d

    raw = pd.concat(frames, ignore_index=True)
    with _quiet_stdout():
        norm = p.normalize_columns(raw, source="api")
    norm, jiphap = filter_ilban(norm)
    norm = norm.reset_index(drop=True)
    d["jiphap_excluded"] = int(jiphap)
    if norm.empty:
        d["message"] = (f"{d['gu_scope']} {months[0]}~{months[-1]} — 통건물(일반) "
                        f"거래가 없다. 집합(구분상가) {jiphap}건은 취급 범위 밖이라 "
                        f"제외했다(v1).")
        return d

    masked = norm["지번"].astype(str).str.contains(r"\*", regex=True, na=False)
    d["masked_count"] = int(masked.sum())
    cancel = norm["해제사유발생일"].fillna("").astype(str).str.strip()
    d["cancelled_count"] = int(((cancel != "") & (cancel != "-")).sum())
    d["rows_total"] = int(len(norm))

    gu_series = norm["시군구"].astype(str).str.extract(r"(\S+구)", expand=False)
    d["per_gu"] = {g: int(n) for g, n in gu_series.value_counts().items()}

    # 파일명 기간 표기: 단월='YYYYMM', 한 해 1~12월 전체='YYYY'(연 파티션 정렬),
    # 그 외 범위='YYYYMM-YYYYMM'. 연 단위 백필이 …_2016_ 로 깔끔히 떨어진다.
    if len(months) == 1:
        span_txt = months[0]
    elif (len(months) == 12 and months[0].endswith("01")
          and months[-1].endswith("12") and months[0][:4] == months[-1][:4]):
        span_txt = months[0][:4]
    else:
        span_txt = f"{months[0]}-{months[-1]}"
    base = f"실거래_통건물_{_safe_stem(d['gu_scope'])}_{span_txt}"
    out_dir = _export_out_dir()
    out_dir.mkdir(parents=True, exist_ok=True)

    if not match:
        # 원본 모드: 국토부 원본 그대로 1파일 (지번 마스킹 유지)
        fname = f"{base}_원본.csv"
        out_path = out_dir / fname
        norm.to_csv(out_path, index=False, encoding="utf-8-sig")
        d.update(status="OK", path=str(out_path), filename=fname)
        return d

    # 역매칭 모드(기본): 동별 표제부 전체를 받는다 — 콜드 캐시면 수십 분.
    with _quiet_stdout():
        norm = p.bulk_match(norm)

    stage = norm["매칭단계"].astype(str).str.strip()
    # 파이프라인 표준 파생 컬럼 — 원본(마스킹 지번)은 그대로 두고 옆에 붙인다.
    norm["매칭신뢰도"] = [
        _stage_confidence(s, m) for s, m in zip(stage, masked)
    ]
    addr = norm["대지위치_표제부"].fillna("").astype(str).str.strip()
    norm["복원지번"] = addr.str.extract(r"(\d+(?:-\d+)?)(?:번지)?$", expand=False).fillna("")
    # 비마스킹 행은 원지번이 곧 확정 지번
    own = norm["지번"].astype(str).str.extract(r"(\d+(?:-\d+)?)\s*$", expand=False).fillna("")
    norm.loc[~masked & (norm["복원지번"] == ""), "복원지번"] = own[~masked]

    # 미복원 = 마스킹인데 매칭단계가 비어 있는 행. 나머지는 전부 복원본.
    unmatched_mask = masked & (stage == "")
    df_matched = norm[~unmatched_mask]
    df_unmatched = norm[unmatched_mask].drop(columns=["매칭신뢰도", "복원지번"],
                                             errors="ignore")

    fname = f"{base}_복원.csv"
    out_path = out_dir / fname
    df_matched.to_csv(out_path, index=False, encoding="utf-8-sig")
    d.update(status="OK", path=str(out_path), filename=fname,
             rows_matched=int(len(df_matched)),
             rows_unmatched=int(len(df_unmatched)))
    d["confidence_breakdown"] = {
        k: int(v) for k, v in
        df_matched["매칭신뢰도"].value_counts().items() if k
    }
    if len(df_unmatched) > 0:
        fname_u = f"{base}_미복원.csv"
        out_path_u = out_dir / fname_u
        df_unmatched.to_csv(out_path_u, index=False, encoding="utf-8-sig")
        d.update(path_unmatched=str(out_path_u), filename_unmatched=fname_u)
    return d


def _render_deals_export(d: dict[str, Any]) -> str:
    if d["status"] in _ERR_PREFIX:
        return f"{_ERR_PREFIX[d['status']]} {d['message']}"
    if d["status"] == "NOT_FOUND":
        return f"{NOT_FOUND} {d['message']} {NO_GUESS}"

    span = d["months"][0] if len(d["months"]) == 1 else f"{d['months'][0]}~{d['months'][-1]}"
    lines = [f"■ 실거래 CSV 내보내기 — {d['gu_scope']} {span} (통건물)"]
    if d["match"]:
        cb = d["confidence_breakdown"]
        cb_txt = " · ".join(f"{k} {v:,}건" for k, v in
                            sorted(cb.items(), key=lambda kv: -kv[1]))
        lines.append(f"  복원본: {d['path']} — {d['rows_matched']:,}건"
                     + (f" ({cb_txt})" if cb_txt else ""))
        if d["path_unmatched"]:
            lines.append(f"  미복원본: {d['path_unmatched']} — {d['rows_unmatched']:,}건"
                         " (금액·면적은 실측, 지번만 미확정)")
        else:
            lines.append("  미복원 0건 — 전량 복원됨")
    else:
        lines.append(f"  원본: {d['path']} — {d['rows_total']:,}건 (지번 마스킹 유지)")
    lines.append(f"  전체 {d['rows_total']:,}건 · 해제신고 {d['cancelled_count']}건 포함"
                 "(해제사유발생일 컬럼으로 구분)")
    if d["jiphap_excluded"]:
        lines.append(f"  ※ 집합(구분상가) {d['jiphap_excluded']:,}건은 취급 범위 밖이라 제외(v1)")
    top = sorted(d["per_gu"].items(), key=lambda kv: -kv[1])[:5]
    if len(d["per_gu"]) > 1 and top:
        lines.append("  구별 상위: " + " · ".join(f"{g} {n}건" for g, n in top))
    lines.append(SOURCE_LINE)
    return "\n".join(lines)


@mcp.tool(title="실거래 CSV 내보내기", output_schema=DEALS_EXPORT_OUTPUT_SCHEMA,
          annotations={**READ_ONLY_EXTERNAL, "readOnlyHint": False})
def deals_export(
    year_month: str,
    year_month_to: str = "",
    gu: str = "",
    match: bool = True,
) -> ToolResult:
    """연월(YYYYMM)을 지정해 서울 전역(또는 한 구)의 상업업무용 통건물 실거래를
    **마스킹 지번을 역매칭으로 복원한 CSV**로 내려받는다. DB·분석 파이프라인의
    원천 데이터 용도다.

    year_month 하나면 그 달, year_month_to 까지 주면 범위(최대 24개월)다.
    gu 를 주면 그 구만(예: '강남구'), 비우면 서울 25개 구 전체를 내보낸다.

    기본(match=true)은 **복원본·미복원본 2파일**로 나온다:
      복원본(…_복원.csv) — 지번이 특정된 거래. 원본 마스킹 지번은 그대로 두고
        복원지번·대지위치_표제부·도로명대지위치_표제부·매칭단계·매칭신뢰도
        (정확매칭/추정매칭) 컬럼을 추가. 추정매칭 행은 동일 스펙 인접 건물일
        가능성이 있으니 하류에서 매칭신뢰도로 필터할 것.
      미복원본(…_미복원.csv) — 지번 특정 실패 거래(역매칭실패사유 포함).
        금액·면적은 실측이므로 버리지 말 것. 0건이면 파일을 만들지 않는다.
    **콜드 캐시면 동별 표제부 수신에 수 분~수십 분** 걸린다(캐시 후 수 분).
    match=false 면 역매칭 없이 국토부 원본 1파일(…_원본.csv, 수십 초).

    집합(구분상가)은 취급 범위 밖이라 제외되며 건수만 보고한다(v1).
    해제신고 거래는 행으로 남긴다 — '해제사유발생일' 컬럼으로 구분할 것.

    저장 위치는 ~/deal-locator-exports/ 고정(DEAL_LOCATOR_EXPORT_DIR 로 변경
    가능, 날짜 하위폴더 없음 — 파이프라인이 집어가기 좋게 경로가 실행일과
    무관하다). 인코딩은 utf-8-sig(엑셀 호환). 같은 인자로 다시 부르면 같은
    파일을 덮어쓴다(멱등).
    """
    data = _deals_export_payload(year_month, year_month_to, gu, match)
    return ToolResult(content=_render_deals_export(data), structured_content=data,
                      is_error=data["status"] in _IS_ERROR_STATUSES)


def _env_candidates() -> list[Path]:
    """.env 탐색 경로 — 우선순위 순, 중복 제거.

    1) DEAL_LOCATOR_ENV_FILE — 명시 지정(파일 경로). cwd 와 무관하게 항상 통한다.
    2) cwd → 상위 1단계 — 프로젝트 폴더에서 띄운 경우(.mcp.json 관행).
    3) 홈의 사용자 설정 파일 — 플러그인/Desktop 설치 경로. cwd 가 예측 불가라
       2)가 통하지 않는다.

    **범위를 좁게 유지한다.** 예전엔 cwd 상위 3단계와 설치 위치 상위 4단계까지
    훑었는데, 그러면 사용자가 남의 리포 안에서 서버를 띄웠을 때 그 리포의 .env 를
    삼킨다. 설치 위치(site-packages) 상위 탐색은 특히 위험해 제거했다.
    3)은 그 문제가 없다 — 조상 디렉터리를 훑는 게 아니라 사용자 소유의 고정 경로
    하나를 보는 것이라, 남의 파일을 삼킬 수 없다. 우선순위를 맨 뒤에 둬서 기존
    동작(프로젝트 .env 우선)은 그대로다.
    """
    out: list[Path] = []
    explicit = os.environ.get("DEAL_LOCATOR_ENV_FILE", "").strip()
    if explicit:
        out.append(Path(explicit).expanduser())

    dirs = [Path.cwd(), *list(Path.cwd().parents)[:1]]
    out.extend(d / ".env" for d in dirs)

    home = Path.home()
    out.append(home / ".deal-locator.env")
    out.append(home / ".config" / "deal-locator" / ".env")

    seen: set[Path] = set()
    uniq: list[Path] = []
    for p in out:
        if p not in seen:
            seen.add(p)
            uniq.append(p)
    return uniq


# .env 에서 받아들일 키. **이 목록 밖은 무시한다** — .env 를 통째로 환경변수에
# 올리면 HTTPS_PROXY 같은 값이 섞여 들어와 API 요청이 공격자 서버를 경유하고
# 인증키가 새어나갈 수 있다(감사 지적).
_ENV_ALLOWLIST = frozenset({
    "DEAL_LOCATOR_SERVICE_KEY",
    "DATA_GO_KR_API_KEY",
    "DEAL_LOCATOR_CACHE_DIR",
    "DEAL_LOCATOR_CARD_DIR",
    "DEAL_LOCATOR_EXPORT_DIR",
})


def _load_dotenv_fallback() -> None:
    """환경변수에 키가 없으면 .env 에서 보충한다 (탐색 경로는 _env_candidates).

    로컬 stdio 실행 편의용 — 등록 파일(.mcp.json)에 키를 남기지 않기 위함.
    이미 설정된 환경변수는 덮어쓰지 않으며(setdefault), 키를 찾는 즉시 멈춘다.
    _ENV_ALLOWLIST 에 있는 키만 받아들인다 — .env 의 나머지 줄은 읽지도 않는다.
    키가 없는 .env 를 만나도 탐색을 계속한다(예전엔 첫 파일에서 포기했다).
    """
    if _key_ok():
        return
    for p in _env_candidates():
        if not p.is_file():
            continue
        try:
            for raw in p.read_text(encoding="utf-8-sig").splitlines():
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, _, v = line.partition("=")
                k = k.strip()
                if k not in _ENV_ALLOWLIST:
                    continue
                os.environ.setdefault(k, v.strip().strip('"').strip("'"))
        except Exception:  # noqa: BLE001
            continue
        if _key_ok():
            return


def main() -> None:
    _load_dotenv_fallback()
    mcp.run()


if __name__ == "__main__":
    main()
