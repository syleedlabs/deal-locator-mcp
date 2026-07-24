"""조회 캐시 — TTL·상한 축출·비캐시 정책 테스트.

TTL 만 있고 축출이 없으면 조회한 주소 수만큼 무한히 쌓인다(장기 세션 누수).
"""

from __future__ import annotations

from typing import Any

import pytest

from deal_locator import server as S


@pytest.fixture(autouse=True)
def _clean() -> Any:
    S._cache.clear()
    yield
    S._cache.clear()


def test_hit_and_miss(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = []

    def fake_lookup(pipeline, address, months):
        calls.append(address)
        return {"status": "거래있음", "message": ""}

    monkeypatch.setattr(S, "get_pipeline", lambda: object())
    monkeypatch.setattr(S, "lookup_deal", fake_lookup)

    S._cached_lookup("구로구 구로동 1128-1", 12)
    S._cached_lookup("구로구  구로동  1128-1", 12)  # 공백만 다름 → 같은 키
    assert len(calls) == 1
    S._cached_lookup("구로구 구로동 1128-1", 24)  # 기간 다름 → 미스
    assert len(calls) == 2


def test_api_error_is_not_cached(monkeypatch: pytest.MonkeyPatch) -> None:
    """일시 장애를 캐시하면 15분간 복구 불가가 된다."""
    calls = []

    def fake_lookup(pipeline, address, months):
        calls.append(address)
        return {"status": "API_오류", "message": "타임아웃"}

    monkeypatch.setattr(S, "get_pipeline", lambda: object())
    monkeypatch.setattr(S, "lookup_deal", fake_lookup)

    S._cached_lookup("구로구 구로동 1128-1", 12)
    S._cached_lookup("구로구 구로동 1128-1", 12)
    assert len(calls) == 2
    assert not S._cache


def test_expired_entry_is_evicted_on_read(monkeypatch: pytest.MonkeyPatch) -> None:
    S._cache[("deal", "x", 12)] = (0.0, {"status": "거래있음"})  # 아주 옛날 타임스탬프
    assert S._cache_get(("deal", "x", 12)) is None
    assert ("deal", "x", 12) not in S._cache


def test_size_is_capped() -> None:
    for i in range(S._CACHE_MAX + 20):
        S._cache_put(("deal", f"addr-{i}", 12), {"status": "거래있음"})
    assert len(S._cache) <= S._CACHE_MAX


def test_scan_results_are_cached(monkeypatch: pytest.MonkeyPatch) -> None:
    """동 단위 스캔이 가장 비싼 호출인데 개조 전에는 캐시가 없었다."""
    calls = []

    def fake_scan(pipeline, area, **kw):
        calls.append((area, kw.get("months"), kw.get("road_contains")))
        return {"status": "거래없음", "message": "없음", "deals": [],
                "gu": "성동구", "dong": "성수동2가"}

    monkeypatch.setenv("DEAL_LOCATOR_SERVICE_KEY", "test-key")
    monkeypatch.setattr(S, "get_pipeline", lambda: object())
    monkeypatch.setattr(S, "scan_area", fake_scan)

    S._area_scan_payload("성수동2가", 0.0, 0.0, 12, "", 30)
    S._area_scan_payload("성수동2가", 0.0, 0.0, 12, "", 30)
    assert len(calls) == 1
    S._area_scan_payload("성수동2가", 0.0, 0.0, 12, "연무장길", 30)  # 필터 다름 → 미스
    assert len(calls) == 2
