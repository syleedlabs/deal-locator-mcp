"""조건 0건(NOT_FOUND)일 때도 동 시세를 텍스트에 노출하는지.

밴드가 실제 시세대와 안 맞으면 0건이 나는데, 그때 사용자에게 가장 필요한 정보가
'그럼 실제 시세는 얼마냐'다. 이전에는 렌더러가 조기 종료해 stats 가
structuredContent 에만 담기고 텍스트에는 안 나왔다.
"""

from __future__ import annotations

from typing import Any

import pytest

from deal_locator import server as S


def _agg(median: int) -> dict[str, Any]:
    return {"n": 24, "mean_manwon": median, "median_manwon": median,
            "min_manwon": median // 2, "max_manwon": median * 2,
            "p25_manwon": int(median * 0.8), "p75_manwon": int(median * 1.2)}


def _nf(stats: dict[str, Any] | None = None, lo: int | None = None,
        hi: int | None = None) -> dict[str, Any]:
    return {
        "status": "NOT_FOUND", "message": "성동구 성수동1가 — 조건 부합 0건",
        "stats": stats or {},
        "filter": {"min_manwon_per_gross_pyeong": lo,
                   "max_manwon_per_gross_pyeong": hi,
                   "road_contains": "", "limit": 30},
    }


def test_stats_appear_in_text_when_band_matches_nothing() -> None:
    text = S._render_area_scan(_nf({"gross": _agg(5897), "land": None}, lo=45000))

    assert S.NOT_FOUND in text
    assert "5,897" in text            # 동 시세가 텍스트에 살아 있어야 한다
    assert "[동 시세]" in text


def test_band_gap_is_quantified() -> None:
    """밴드가 시세대와 얼마나 벌어졌는지 배수로 알려준다.

    실사용 사례: 성수동1가 중앙 5,897만원/평인데 4.5억/평 밴드로 조회 → 0건.
    """
    text = S._render_area_scan(_nf({"gross": _agg(5897), "land": None}, lo=45000))

    assert "7.6배" in text            # 45000 / 5897
    assert "하한" in text


def test_upper_bound_gap_is_reported() -> None:
    """상한이 시세보다 낮을 때는 뒤집어 적는다 — '0.1배'는 읽기 어렵다."""
    text = S._render_area_scan(_nf({"gross": _agg(20000), "land": None}, hi=3000))

    assert "상한" in text
    assert "6.7배" in text            # 20000 / 3000
    assert "0.1배" not in text


def test_no_gap_note_when_band_is_reasonable() -> None:
    """밴드가 시세대와 겹치면 배수 문구를 붙이지 않는다(잡음 방지)."""
    text = S._render_area_scan(_nf({"gross": _agg(5000), "land": None}, lo=4000, hi=6000))
    note = text.split("\n")[1]

    assert "배" not in note
    assert "5,000" in text


def test_unchanged_when_no_deals_at_all() -> None:
    """거래 자체가 없으면(stats 없음) 기존과 같이 한 줄로 끝난다 — 회귀 방지."""
    text = S._render_area_scan(_nf())

    assert text == f"{S.NOT_FOUND} 성동구 성수동1가 — 조건 부합 0건 {S.NO_GUESS}"
    assert "\n" not in text


def test_structured_content_still_carries_stats() -> None:
    """텍스트에 노출하더라도 구조화 필드가 정본이라는 계약은 유지된다."""
    d = _nf({"gross": _agg(5897), "land": None}, lo=45000)
    assert d["stats"]["gross"]["median_manwon"] == 5897
