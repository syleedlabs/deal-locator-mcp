"""분기(3개월) 추이 집계 테스트.

사용자 요청(2026-07-24): 24개월 통짜 값은 범위가 넓다 → 주기적으로 끊어서 보고 싶다.
월 단위는 표본이 무너져(대치동 통건물 22개월 중 10개월이 2건 이하) 분기로 묶었다.
"""

from __future__ import annotations

from deal_locator.core.lookup import _ppp_stats, _quarter_key
from deal_locator import server as S


# ── 분기 라벨 ──────────────────────────────────────────────────────────

def test_quarter_key_maps_month_to_quarter() -> None:
    assert _quarter_key("202601") == "2026 Q1"
    assert _quarter_key("202603") == "2026 Q1"
    assert _quarter_key("202604") == "2026 Q2"
    assert _quarter_key("202612") == "2026 Q4"


def test_quarter_key_rejects_garbage() -> None:
    """해석 불가는 빈 문자열 — 조용히 엉뚱한 분기로 넣지 않는다."""
    for bad in ("", "2026", "20261", "abcdef", None, float("nan"), "202613", "202600"):
        assert _quarter_key(bad) == ""


# ── 집계 ───────────────────────────────────────────────────────────────

def test_quarterly_is_ordered_oldest_first() -> None:
    buckets = {
        "2026 Q1": ([100], [200]),
        "2025 Q3": ([300], [400]),
        "2025 Q4": ([500], [600]),
    }
    q = _ppp_stats([100, 300, 500], [200, 400, 600], buckets)["quarterly"]

    assert [x["quarter"] for x in q] == ["2025 Q3", "2025 Q4", "2026 Q1"]


def test_quarterly_totals_match_overall_sample() -> None:
    """분기 표본의 합 = 전체 표본. 쪼갠 것이지 다시 센 게 아니다."""
    buckets = {"2026 Q1": ([10, 20], [30]), "2026 Q2": ([30], [40, 50])}
    stats = _ppp_stats([10, 20, 30], [30, 40, 50], buckets)

    assert sum(x["gross"]["n"] for x in stats["quarterly"]) == stats["gross"]["n"]
    assert sum(x["land"]["n"] for x in stats["quarterly"]) == stats["land"]["n"]


def test_quarter_without_land_sample_yields_null_land() -> None:
    """대지면적이 없는 분기는 land=None — 0 으로 채우지 않는다."""
    q = _ppp_stats([10], [], {"2026 Q1": ([10], [])})["quarterly"]

    assert q[0]["gross"]["n"] == 1
    assert q[0]["land"] is None


def test_no_buckets_yields_empty_list() -> None:
    stats = _ppp_stats([10], [20])
    assert stats["quarterly"] == []


# ── 텍스트 렌더 ────────────────────────────────────────────────────────

def _agg(n: int, median: int) -> dict:
    return {"n": n, "mean_manwon": median, "median_manwon": median,
            "min_manwon": median, "max_manwon": median,
            "p25_manwon": median, "p75_manwon": median}


def test_render_marks_thin_quarters_as_insufficient() -> None:
    """n<3 인 분기는 수치를 내지 않는다 — 한두 건이 추세처럼 읽히면 안 된다."""
    lines = S._fmt_quarterly_lines([
        {"quarter": "2026 Q1", "gross": _agg(1, 9999), "land": _agg(1, 9999)},
        {"quarter": "2026 Q2", "gross": _agg(8, 5000), "land": _agg(8, 5000)},
    ])
    text = "\n".join(lines)

    assert "2026 Q1 표본부족(n=1)" in text
    assert "9,999" not in text          # 표본부족 분기의 수치는 새면 안 됨
    assert "2026 Q2 5,000(대지·n=8)" in text


def test_render_falls_back_to_gross_when_no_land() -> None:
    lines = S._fmt_quarterly_lines([
        {"quarter": "2026 Q2", "gross": _agg(5, 3000), "land": None},
    ])

    assert "3,000(연면적·n=5)" in "\n".join(lines)


def test_render_empty_when_nothing_usable() -> None:
    assert S._fmt_quarterly_lines([]) == []
    assert S._fmt_quarterly_lines([{"quarter": "2026 Q1", "gross": None, "land": None}]) == []
