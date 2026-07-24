"""deal_locator.core.matching — 실거래 ↔ 표제부 매칭 코어 엔진
=====================================================
외부 의존 없는 순수 로직. 조회 도구들이 공용으로 사용한다.

핵심 과제 — 마스킹:
  국토부 상업업무용 실거래가는 일반(단독)건물의 지번을 '3**' 식으로 마스킹한다.
  건축물대장 표제부의 물리 속성(건축년도+연면적+대지면적)으로 역매칭해 필지를 특정한다.

다필지(합필) 대지:
  건물 하나가 여러 지번 위에 있으면 실거래 대지면적이 표제부 platArea와 어긋날 수 있다.
  이때 대지면적 불일치를 치명으로 보지 않고 강등+사유 기록으로 처리한다(multi_lot=True).
  필지세트(대표+부속지번)는 attached_jibun.AttachedJibunIndex 가 공급한다.
"""

from __future__ import annotations

import math
import re
from typing import Any, Callable, Iterable, Optional

# 매칭 신뢰도 라벨 (단계별)
CONFIDENCE: dict[str, tuple[str, float]] = {
    "exact_jibun": ("확정", 1.00),          # 집합/구분 거래 — 지번 그대로 일치
    "exact_jibun_attached": ("확정", 0.99),  # 지번이 같은 대지의 부속지번과 일치
    "stage1": ("정확매칭", 0.97),            # 건축년도 + 연면적 + 대지면적 일치
    "stage2": ("정확매칭", 0.90),            # 건축년도 + 연면적 일치
    "stage3": ("추정매칭", 0.60),            # 오차범위 ±tolerance
    "land_area": ("추정매칭", 0.55),         # 토지 거래 — 대지면적 일치
    "neighbor_exact": ("인접후보", 0.50),    # 질의 지번엔 건물 없음 — 동일 본번 인접 지번의 정확일치
}


def _isna(v: Any) -> bool:
    """None/NaN 안전 판별 (pandas 비의존)."""
    if v is None:
        return True
    try:
        return math.isnan(float(v))
    except (TypeError, ValueError):
        return False


# ======================================================================
# 지번 유틸
# ======================================================================

def norm_bunji(bunji: str) -> str:
    """지번 정규화: '321-090'→'321-90', '321-0'→'321', '321번지'→'321'."""
    s = re.sub(r"번지$", "", str(bunji).strip())
    m = re.match(r"^(\d+)(?:-(\d+))?$", s)
    if not m:
        return s
    bun = str(int(m.group(1)))
    ji = int(m.group(2)) if m.group(2) else 0
    return f"{bun}-{ji}" if ji else bun


def jibun_equal(a: str, b: str) -> bool:
    """지번 동등 비교 ('321-90' == '321-090', '321-0' == '321')."""
    return norm_bunji(a) == norm_bunji(b)


def masked_prefix_ok(masked: str, bunji: str) -> bool:
    """마스킹 지번('3**')이 타깃 본번('321')과 접두 호환인지.

    국토부 마스킹은 본번 첫 자리만 남기고 '*' 처리하는 경향이 있다.
    잘못된 접두면 빠르게 제외(false positive 감소)하되,
    포맷이 불확실하면 통과시켜 속성 매칭에 맡긴다.
    """
    if not masked or "*" not in masked:
        return True
    # 시리즈 실거래 지번은 '서울특별시 구로구 구로동 1***'처럼 주소 전체가
    # 붙어 오기도 한다 → 마지막 토큰(마스킹 번지)만 취해 검사.
    masked = masked.strip().split()[-1]
    head = re.match(r"\d+", masked)
    bun_head = re.match(r"\d+", str(bunji))
    if not head or not bun_head:
        return True
    h = head.group(0)
    return str(bunji).startswith(h[: len(h)]) or bun_head.group(0).startswith(h)


def masked_prefix_ok_any(masked: str, lots: Iterable[str]) -> bool:
    """필지세트(대표+부속지번) 중 하나라도 마스킹 접두와 호환이면 후보 유지.

    다필지 대지 거래가 부속지번 쪽 번지로 신고된 경우를 놓치지 않기 위함.
    빈 세트면 판단 불가 → 통과(속성 매칭에 위임).
    """
    lots = list(lots)
    if not lots:
        return True
    return any(masked_prefix_ok(masked, lot) for lot in lots)


# ======================================================================
# 주소 파싱
# ======================================================================

def parse_address(
    query: str,
    gu_resolver: Optional[Callable[[str], str]] = None,
) -> dict[str, str]:
    """'성수동2가 321-90' 또는 '성동구 성수동2가 321-90' → 구/동/번지.

    Args:
        query: 지번 주소 문자열
        gu_resolver: 동명 → 구명 역추적 콜러블(선택). 없으면 구 미상 시 gu="".

    Returns:
        {"gu": "성동구", "dong": "성수동2가", "bunji": "321-90"}
    """
    q = query.strip()
    q = re.sub(r"^서울(특별시|시)?\s*", "", q)
    q = q.replace(",", " ")
    q = re.sub(r"\s+", " ", q).strip()

    # 번지: 끝쪽의 'NNN-NN' 또는 'NNN' (마지막 토큰 우선)
    bunji = ""
    bunji_m = re.search(r"(\d+(?:-\d+)?)\s*(?:번지)?\s*$", q)
    q_wo_bunji = q
    if bunji_m:
        bunji = bunji_m.group(1)
        q_wo_bunji = q[: bunji_m.start()].strip()

    # 구: 'XX구' 토큰 → 추출 후 문자열에서 제거 (동 오인식 방지)
    gu = ""
    gu_m = re.search(r"([가-힣]+구)\b", q_wo_bunji)
    if gu_m:
        gu = gu_m.group(1)
        q_wo_bunji = (q_wo_bunji[: gu_m.start()] + q_wo_bunji[gu_m.end():]).strip()

    # 동 추출: '...동N가'(성수동2가) → '...동'(역삼동) → '...N가'(을지로3가)
    dong = ""
    for pat in (r"[가-힣]+동\d*가", r"[가-힣]+동", r"[가-힣]+\d+가"):
        dong_m = re.search(pat, q_wo_bunji)
        if dong_m:
            dong = dong_m.group(0)
            break

    # 구 미입력 시 동으로 역추적
    if not gu and dong and gu_resolver is not None:
        try:
            gu = gu_resolver(dong) or ""
        except Exception:  # noqa: BLE001 — 역추적 실패는 gu 미상으로 처리
            gu = ""

    return {"gu": gu, "dong": dong, "bunji": bunji}


# ======================================================================
# 단계 매칭
# ======================================================================

def match_stage(
    row_gross: Any, row_land: Any, row_year: Any,
    t_gross: Optional[float], t_land: Optional[float], t_year: Optional[int],
    tol: float,
    multi_lot: bool = False,
) -> tuple[Optional[str], list[str]]:
    """거래 행의 (연면적,대지면적,건축년도)을 표제부 앵커와 단계 매칭.

    Args:
        row_*: 거래 행 값 (None/NaN 허용)
        t_*: 표제부 앵커 값
        tol: 오차 허용 비율 (예: 0.10)
        multi_lot: 다필지(합필) 대지 여부 — True 면 대지면적 불일치를
            치명으로 보지 않는다(합필 시 거래 대지면적이 필지 합계/개별값으로
            흔들리므로 연면적+건축년도 중심으로 판단, 사유 기록).

    Returns:
        (stage_key | None, reasons)  — reasons 는 match_basis 기록용 사유 목록
    """
    reasons: list[str] = []

    if _isna(row_year) or t_year is None or int(float(row_year)) != int(t_year):
        return None, ["건축년도 불일치 또는 결측"]
    if _isna(row_gross) or t_gross is None:
        return None, ["연면적 결측"]

    rg, tg = round(float(row_gross), 2), round(float(t_gross), 2)
    has_land = (t_land is not None) and not _isna(row_land)

    # Stage 1: 건축년도 + 연면적 + 대지면적 정확
    if has_land and rg == tg and round(float(row_land), 2) == round(float(t_land), 2):
        return "stage1", ["건축년도·연면적·대지면적 정확 일치"]

    # Stage 2: 건축년도 + 연면적 정확 (다필지면 대지면적 불일치 사유 기록)
    if rg == tg:
        if has_land and multi_lot:
            reasons.append("합필지(다필지 대지) — 대지면적 비교 생략, 연면적+건축년도로 판정")
        elif has_land:
            reasons.append("대지면적 불일치 → stage2 강등")
        else:
            reasons.append("대지면적 결측 → 연면적+건축년도로 판정")
        return "stage2", reasons

    # Stage 3: 오차범위 ±tol (연면적 필수, 대지면적은 있으면 함께 검사)
    gross_ok = tg * (1 - tol) <= rg <= tg * (1 + tol)
    land_ok = True
    if has_land and not multi_lot:
        rl, tl = float(row_land), float(t_land)
        land_ok = tl * (1 - tol) <= rl <= tl * (1 + tol)
    if gross_ok and land_ok:
        reasons.append(f"오차범위 ±{tol:.0%} 내 근사")
        if multi_lot and has_land:
            reasons.append("합필지 — 대지면적 검사 생략")
        return "stage3", reasons

    return None, ["연면적/대지면적 오차범위 밖"]


def build_match_basis(
    stage_key: str,
    anchor: dict[str, Any],
    row: dict[str, Any],
    reasons: list[str],
    multi_lot: bool = False,
    lot_set: Optional[Iterable[str]] = None,
) -> dict[str, Any]:
    """매칭 근거 payload — 응답·검수·MCP match_explain 의 공용 데이터.

    anchor/row 는 {"gross": ㎡, "land": ㎡, "year": int, ...} 형태.
    """
    label, score = CONFIDENCE[stage_key]
    return {
        "stage": stage_key,
        "confidence": label,
        "confidence_score": score,
        "anchor": anchor,
        "row": row,
        "reasons": list(reasons),
        "multi_lot": multi_lot,
        "lot_set": sorted(lot_set) if lot_set else [],
        "caveat": (
            "추정매칭 — 동일 스펙 인접 건물 가능성. 공부(등기/대장) 대조로 확정하세요."
            if score < 0.90 else ""
        ),
    }
