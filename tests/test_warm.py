"""캐시 프리워밍 CLI 테스트 — 순수 로직만(네트워크 없음).

fetch_month_cached 를 가짜로 갈아끼워, 워머가 (구×월) 조합을 빠짐없이 훑고
실패를 삼키지 않는지 본다. 실제 API·디스크는 test_deals_cache.py 소관.
"""

from __future__ import annotations

from datetime import datetime

import pandas as pd
import pytest

from deal_locator import warm as W


# ── 월 키 ──────────────────────────────────────────────────────────────

def test_month_keys_counts_back_from_previous_month() -> None:
    """이번 달이 아니라 직전 달부터 센다(이번 달은 신고 전이라 비어 있다)."""
    keys = W.month_keys(3, datetime(2026, 3, 15))
    assert keys == ["202602", "202601", "202512"]


def test_month_keys_crosses_year_boundary() -> None:
    keys = W.month_keys(2, datetime(2026, 1, 10))
    assert keys == ["202512", "202511"]


def test_month_keys_length_matches_months() -> None:
    assert len(W.month_keys(12, datetime(2026, 7, 1))) == 12


# ── 조합 커버리지 ──────────────────────────────────────────────────────

class _FakePipeline:
    def __init__(self, fail_on: set[tuple[str, str]] | None = None) -> None:
        self.calls: list[tuple[str, str]] = []
        self.fail_on = fail_on or set()

    def fetch_month_cached(self, year_month: str, gu: str) -> pd.DataFrame:
        self.calls.append((gu, year_month))
        if (gu, year_month) in self.fail_on:
            raise RuntimeError("API 429")
        return pd.DataFrame({"지번": ["1-1", "2-2"]})


def test_every_gu_month_pair_is_fetched() -> None:
    p = _FakePipeline()
    now = datetime(2026, 7, 1)
    stats = W.warm_cache(p, ["강남구", "성동구"], months=3, workers=2, now=now)

    assert stats.total == 6            # 2구 × 3개월
    assert stats.ok == 6
    assert stats.failed == 0
    assert stats.rows == 12            # 6건 × 2행
    # 조합이 빠짐없이 정확히 한 번씩
    expected = {(g, m) for g in ("강남구", "성동구")
                for m in W.month_keys(3, now)}
    assert set(p.calls) == expected
    assert len(p.calls) == 6


def test_failures_are_counted_not_swallowed() -> None:
    """실패를 세어서 돌려준다 — 조용히 '성공'으로 넘기면 안 된다."""
    now = datetime(2026, 7, 1)
    fail = {("강남구", "202606")}
    p = _FakePipeline(fail_on=fail)
    stats = W.warm_cache(p, ["강남구"], months=3, workers=1, now=now)

    assert stats.failed == 1
    assert stats.ok == 2
    assert stats.total == 3
    assert any("강남구 202606" in e for e in stats.errors)


def test_progress_callback_fires_per_job() -> None:
    p = _FakePipeline()
    seen: list[str] = []
    W.warm_cache(p, ["강남구"], months=4, workers=1,
                 now=datetime(2026, 7, 1),
                 on_progress=lambda st, gu, ym, err: seen.append(ym))

    assert len(seen) == 4


def test_rerun_is_safe_because_cache_layer_dedupes() -> None:
    """워머 자체는 매번 fetch_month_cached 를 부른다 — 신선분 스킵은 캐시 계층 몫.

    즉 재실행해도 워머는 같은 조합을 다시 '요청'하고, 실제 API 재호출 여부는
    fetch_month_cached 의 TTL 이 정한다(test_deals_cache 소관). 여기선 워머가
    재실행에서 조합을 누락하지 않는 것만 본다.
    """
    p = _FakePipeline()
    now = datetime(2026, 7, 1)
    W.warm_cache(p, ["강남구"], months=2, workers=1, now=now)
    W.warm_cache(p, ["강남구"], months=2, workers=1, now=now)

    assert len(p.calls) == 4           # 2회 실행 × 2개월, 누락 없음
