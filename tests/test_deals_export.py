"""deals_export — 연월 지정 서울 전역 실거래 CSV 내보내기 계약 테스트.

네트워크·API 키 없이 돈다:
  · get_pipeline() 을 실제 파이프라인 인스턴스 + fetch_month_cached 페이크로 대체
    (normalize_columns·filter_ilban 은 실물을 그대로 태워 스키마 회귀를 잡는다)
  · match=True 경로는 bulk_match 스파이로 검사 (표제부 API 차단)
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from deal_locator import server as S
from deal_locator.core.constants import SEOUL_GU_CODES
from deal_locator.core.pipeline import RealEstateDataPipeline


def _raw_rows(gu: str, ym: str) -> pd.DataFrame:
    """국토부 API 원본 형태의 월·구 거래 3건: 통건물 마스킹 / 집합 / 통건물 해제."""
    return pd.DataFrame([
        {"시군구": gu, "법정동": "역삼동", "지번": "6**", "건물유형": "일반",
         "거래금액": "500000", "계약년도": ym[:4], "계약월": str(int(ym[4:])),
         "계약일": "14", "건물면적": "1000.5", "대지면적": "300.2",
         "건축년도": "1995", "해제사유발생일": None, "shareDealingType": None},
        {"시군구": gu, "법정동": "역삼동", "지번": "736-14", "건물유형": "집합",
         "거래금액": "80000", "계약년도": ym[:4], "계약월": str(int(ym[4:])),
         "계약일": "20", "건물면적": "84.2", "대지면적": "12.1",
         "건축년도": "2005", "해제사유발생일": None, "shareDealingType": None},
        {"시군구": gu, "법정동": "역삼동", "지번": "7**", "건물유형": "일반",
         "거래금액": "310000", "계약년도": ym[:4], "계약월": str(int(ym[4:])),
         "계약일": "02", "건물면적": "800.0", "대지면적": "250.0",
         "건축년도": "1988", "해제사유발생일": "26.01.30", "shareDealingType": "지분"},
    ])


@pytest.fixture
def fake_pipe(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """실제 파이프라인 인스턴스에 fetch 페이크를 얹고 호출 기록을 남긴다."""
    box: dict[str, Any] = {"fetch_calls": [], "bulk_calls": 0}
    p = RealEstateDataPipeline()

    def _fake_fetch(ym: str, gu: str) -> pd.DataFrame:
        box["fetch_calls"].append((ym, gu))
        return _raw_rows(gu, ym)

    def _fake_bulk(df: pd.DataFrame) -> pd.DataFrame:
        box["bulk_calls"] += 1
        out = df.copy()
        out["매칭단계"] = "1단계: 정확매칭"
        out["대지위치_표제부"] = "서울특별시 강남구 역삼동 601"
        return out

    monkeypatch.setattr(p, "fetch_month_cached", _fake_fetch)
    monkeypatch.setattr(p, "bulk_match", _fake_bulk)
    monkeypatch.setattr(S, "get_pipeline", lambda: p)
    box["pipe"] = p
    return box


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    monkeypatch.setenv("DEAL_LOCATOR_SERVICE_KEY", "test-key")
    monkeypatch.setenv("DEAL_LOCATOR_EXPORT_DIR", str(tmp_path))
    return tmp_path


def _call(**kw: Any) -> dict[str, Any]:
    kw.setdefault("year_month", "202607")
    kw.setdefault("year_month_to", "")
    kw.setdefault("gu", "")
    kw.setdefault("match", False)
    return S._deals_export_payload(**kw)


# ── 입력 검증 ─────────────────────────────────────────────────────────

@pytest.mark.parametrize("bad", ["2026-07", "20267", "abc123", "202613", "199901"])
def test_연월_형식이_아니면_PARSE_ERROR(fake_pipe, bad) -> None:
    d = _call(year_month=bad)
    assert d["status"] == "PARSE_ERROR"
    assert fake_pipe["fetch_calls"] == [], "검증 실패인데 API 를 호출했다"


def test_범위_역순이면_PARSE_ERROR(fake_pipe) -> None:
    d = _call(year_month="202607", year_month_to="202601")
    assert d["status"] == "PARSE_ERROR"


def test_범위_24개월_초과는_PARSE_ERROR(fake_pipe) -> None:
    d = _call(year_month="202301", year_month_to="202607")
    assert d["status"] == "PARSE_ERROR"
    assert "24" in d["message"]


def test_모르는_구는_PARSE_ERROR(fake_pipe) -> None:
    d = _call(gu="부산진구")
    assert d["status"] == "PARSE_ERROR"


def test_키없으면_CONFIG_ERROR(monkeypatch, fake_pipe) -> None:
    monkeypatch.delenv("DEAL_LOCATOR_SERVICE_KEY", raising=False)
    monkeypatch.delenv("DATA_GO_KR_API_KEY", raising=False)
    d = _call()
    assert d["status"] == "CONFIG_ERROR"


# ── 범위·스코프 ────────────────────────────────────────────────────────

def test_전역구는_25개_구를_모두_조회한다(fake_pipe) -> None:
    _call()
    gus = {g for _, g in fake_pipe["fetch_calls"]}
    assert gus == set(SEOUL_GU_CODES)


def test_단일_구_지정시_그_구만_조회한다(fake_pipe) -> None:
    d = _call(gu="강남구")
    assert {g for _, g in fake_pipe["fetch_calls"]} == {"강남구"}
    assert d["gu_scope"] == "강남구"


def test_범위지정시_모든_월을_조회한다(fake_pipe) -> None:
    d = _call(year_month="202511", year_month_to="202602", gu="강남구")
    yms = [ym for ym, _ in fake_pipe["fetch_calls"]]
    assert yms == ["202511", "202512", "202601", "202602"]
    assert d["months"] == ["202511", "202512", "202601", "202602"]


# ── 필터·집계 계약 ────────────────────────────────────────────────────

def test_통건물만_남기고_집합은_제외_건수로_보고한다(fake_pipe) -> None:
    d = _call(gu="강남구")
    assert d["status"] == "OK"
    assert d["rows"] == 2                 # 일반 2건 (집합 1건 제외)
    assert d["jiphap_excluded"] == 1


def test_해제거래는_행으로_남기고_건수만_보고한다(fake_pipe, _env) -> None:
    d = _call(gu="강남구")
    assert d["cancelled_count"] == 1
    with open(d["path"], encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == d["rows"]
    assert any((r.get("해제사유발생일") or "").strip() for r in rows), \
        "해제 거래가 CSV 에서 사라졌다 — 데이터 다운로드는 행을 지우면 안 된다"


def test_CSV_행수와_payload_rows_가_일치한다(fake_pipe, _env) -> None:
    d = _call()
    with open(d["path"], encoding="utf-8-sig") as f:
        assert len(list(csv.DictReader(f))) == d["rows"] == 50  # 25구 × 일반 2건


def test_파일명에_스코프와_연월이_들어간다(fake_pipe) -> None:
    d = _call(gu="강남구", year_month="202511", year_month_to="202601")
    assert "강남구" in d["filename"] and "202511-202601" in d["filename"]
    d2 = _call()
    assert "서울전역" in d2["filename"] and "202607" in d2["filename"]


def test_마스킹_건수를_보고한다(fake_pipe) -> None:
    d = _call(gu="강남구")
    assert d["masked_count"] == 2         # 6**, 7**


# ── 역매칭 옵션 ───────────────────────────────────────────────────────

def test_기본은_역매칭을_하지_않는다(fake_pipe) -> None:
    d = _call(gu="강남구")
    assert fake_pipe["bulk_calls"] == 0
    assert d["match"] is False and d["matched_count"] is None


def test_match_true_면_역매칭하고_복원_건수를_보고한다(fake_pipe, _env) -> None:
    d = _call(gu="강남구", match=True)
    assert fake_pipe["bulk_calls"] == 1
    assert d["match"] is True and d["matched_count"] == 2
    with open(d["path"], encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    assert all(r["매칭단계"] for r in rows), "역매칭 컬럼이 CSV 에 없다"


# ── 데이터 부재 ───────────────────────────────────────────────────────

def test_거래없음은_NOT_FOUND_이고_파일을_쓰지_않는다(fake_pipe, monkeypatch, _env) -> None:
    monkeypatch.setattr(fake_pipe["pipe"], "fetch_month_cached",
                        lambda ym, gu: pd.DataFrame())
    d = _call(gu="강남구")
    assert d["status"] == "NOT_FOUND"
    assert d["path"] == ""
    assert list(Path(_env).rglob("*.csv")) == []


def test_NOT_FOUND_는_에러가_아니다() -> None:
    assert "NOT_FOUND" not in S._IS_ERROR_STATUSES
