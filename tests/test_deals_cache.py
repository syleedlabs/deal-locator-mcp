"""월·구 단위 실거래 디스크 캐시 테스트.

월별 API 호출이 3~4초라 12개월이면 45초가 걸렸다 — 중개사 첫 조회가 1분 가까이
걸리던 원인. 월 결과는 (최근 월을 빼면) 사실상 불변이라 디스크에 둔다.
핵심은 TTL 정책이다: 최근 월은 신고가 계속 들어오므로 짧게, 지난 월은 길게.
"""

from __future__ import annotations

import os
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from deal_locator.core import PipelineConfig, RealEstateDataPipeline


def _pipeline(tmp_path: Path) -> RealEstateDataPipeline:
    p = RealEstateDataPipeline()
    p.initialize(PipelineConfig(api_key="dummy", cache_dir=str(tmp_path)))
    return p


def _ym(months_ago: int) -> str:
    now = datetime.now()
    m = now.month - months_ago
    y = now.year
    while m <= 0:
        m += 12
        y -= 1
    return f"{y}{m:02d}"


# ── TTL 정책 ───────────────────────────────────────────────────────────

def test_recent_month_cache_expires_quickly(tmp_path: Path) -> None:
    """최근 월은 신고가 계속 들어오므로 오래된 캐시를 신선하다고 보면 안 된다."""
    p = _pipeline(tmp_path)
    path = p._deals_disk_path("강남구", _ym(0))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("x")

    assert p._deals_cache_fresh(path, _ym(0)) is True

    old = time.time() - (p._FRESH_TTL_HOURS + 1) * 3600
    os.utime(path, (old, old))
    assert p._deals_cache_fresh(path, _ym(0)) is False


def test_old_month_cache_survives_long(tmp_path: Path) -> None:
    """지난 월은 해제신고 정정 정도만 생기므로 오래 써도 된다."""
    p = _pipeline(tmp_path)
    ym = _ym(6)
    path = p._deals_disk_path("강남구", ym)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("x")

    aged = time.time() - 10 * 86400          # 10일 — 최근 월이면 만료였을 나이
    os.utime(path, (aged, aged))
    assert p._deals_cache_fresh(path, ym) is True

    expired = time.time() - (p._STALE_TTL_DAYS + 1) * 86400
    os.utime(path, (expired, expired))
    assert p._deals_cache_fresh(path, ym) is False


def test_malformed_year_month_is_not_fresh(tmp_path: Path) -> None:
    """년월을 못 읽으면 캐시를 신뢰하지 않는다 — 조용히 엉뚱한 TTL 을 적용하지 않는다."""
    p = _pipeline(tmp_path)
    path = p._deals_disk_path("강남구", "abcd")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("x")

    assert p._deals_cache_fresh(path, "abcd") is False


def test_missing_file_is_not_fresh(tmp_path: Path) -> None:
    p = _pipeline(tmp_path)
    assert p._deals_cache_fresh(tmp_path / "없는파일.csv", _ym(3)) is False


# ── 캐시 경유 조회 ─────────────────────────────────────────────────────

def test_cache_hit_skips_api(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    p = _pipeline(tmp_path)
    ym = _ym(3)
    calls: list[str] = []

    def _fake_api(year_month: str = "", gus: list | None = None) -> pd.DataFrame:
        calls.append(year_month)
        return pd.DataFrame({"지번": ["1-1"], "거래금액": ["10000"]})

    monkeypatch.setattr(p, "fetch_from_api", _fake_api)

    first = p.fetch_month_cached(ym, "강남구")
    second = p.fetch_month_cached(ym, "강남구")

    assert len(calls) == 1                    # 두 번째는 디스크에서
    assert len(first) == len(second) == 1
    assert second["지번"].tolist() == ["1-1"]


def test_cache_is_per_gu_and_month(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """구가 다르면 캐시를 공유하면 안 된다."""
    p = _pipeline(tmp_path)
    monkeypatch.setattr(p, "fetch_from_api",
                        lambda year_month="", gus=None: pd.DataFrame({"구": [gus[0]]}))

    a = p.fetch_month_cached(_ym(3), "강남구")
    b = p.fetch_month_cached(_ym(3), "성동구")

    assert a["구"].tolist() == ["강남구"]
    assert b["구"].tolist() == ["성동구"]


def test_unwritable_cache_dir_does_not_break_fetch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """캐시는 최적화지 정확성의 근거가 아니다 — 쓰기 실패해도 결과는 나와야 한다."""
    p = _pipeline(tmp_path)
    monkeypatch.setattr(p, "fetch_from_api",
                        lambda year_month="", gus=None: pd.DataFrame({"a": [1]}))
    monkeypatch.setattr(
        pd.DataFrame, "to_csv",
        lambda *a, **k: (_ for _ in ()).throw(OSError("read-only")),
    )

    df = p.fetch_month_cached(_ym(3), "강남구")

    assert len(df) == 1


def test_corrupt_cache_falls_back_to_api(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """깨진 캐시 파일이 조회를 막지 않는다."""
    p = _pipeline(tmp_path)
    ym = _ym(3)
    path = p._deals_disk_path("강남구", ym)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"\x00\x01\x02 not a csv")

    monkeypatch.setattr(pd, "read_csv",
                        lambda *a, **k: (_ for _ in ()).throw(ValueError("corrupt")))
    monkeypatch.setattr(p, "fetch_from_api",
                        lambda year_month="", gus=None: pd.DataFrame({"a": [1]}))

    assert len(p.fetch_month_cached(ym, "강남구")) == 1


def test_no_cache_dir_still_fetches(monkeypatch: pytest.MonkeyPatch) -> None:
    """cache_dir 미설정이면 캐시 없이 그냥 API 를 탄다."""
    p = RealEstateDataPipeline()
    p.initialize(PipelineConfig(api_key="dummy", cache_dir=""))
    monkeypatch.setattr(p, "fetch_from_api",
                        lambda year_month="", gus=None: pd.DataFrame({"a": [1]}))

    assert p._deals_disk_path("강남구", _ym(1)) is None
    assert len(p.fetch_month_cached(_ym(1), "강남구")) == 1
