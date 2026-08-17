"""deals_export — 연월 지정 서울 전역 실거래 CSV 내보내기 계약 테스트.

계약(사용자 인터뷰로 확정, 2026-08):
  · 기본 동작 = 역매칭(match=True) → 복원본 + 미복원본 2파일
  · 원본 마스킹 지번 유지 + 복원지번·매칭신뢰도 등 파생 컬럼 추가
  · 해제신고 행은 각 파일에 남기고 '해제사유발생일' 컬럼으로 구분
  · 고정 폴더(날짜 하위폴더 없음) + 연월 파일명 → 재실행 멱등 덮어쓰기
  · match=False = 원본 1파일 탈출구

네트워크·API 키 없이 돈다: 실제 파이프라인 인스턴스에 fetch/bulk 페이크.
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
    """국토부 API 원본 형태 4건: 통건물 3(정확복원/추정복원/미복원·해제) + 집합 1."""
    def row(jibun, kind, area, land, year, cancel=None, share=None):
        return {"시군구": gu, "법정동": "역삼동", "지번": jibun, "건물유형": kind,
                "거래금액": "500000", "계약년도": ym[:4], "계약월": str(int(ym[4:])),
                "계약일": "14", "건물면적": area, "대지면적": land,
                "건축년도": year, "해제사유발생일": cancel, "shareDealingType": share}
    return pd.DataFrame([
        row("6**", "일반", "1000.5", "300.2", "1995"),
        row("8**", "일반", "820.0", "210.0", "2001"),
        row("7**", "일반", "41.12", "11.84", "1988", cancel="26.01.30", share="지분"),
        row("736-14", "집합", "84.2", "12.1", "2005"),
    ])


@pytest.fixture
def fake_pipe(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """실제 파이프라인 인스턴스에 fetch/bulk 페이크를 얹고 호출을 기록한다."""
    box: dict[str, Any] = {"fetch_calls": [], "bulk_calls": 0}
    p = RealEstateDataPipeline()

    def _fake_fetch(ym: str, gu: str) -> pd.DataFrame:
        box["fetch_calls"].append((ym, gu))
        return _raw_rows(gu, ym)

    def _fake_bulk(df: pd.DataFrame) -> pd.DataFrame:
        # 6** → 정확매칭, 8** → 추정매칭, 7** → 미복원 (실엔진 축소 모형)
        box["bulk_calls"] += 1
        out = df.copy()
        for col in ("매칭단계", "대지위치_표제부", "역매칭실패사유"):
            if col not in out.columns:
                out[col] = ""
        for idx, r in out.iterrows():
            jib = str(r["지번"])
            if "6**" in jib:
                out.at[idx, "매칭단계"] = "1단계: 정확매칭"
                out.at[idx, "대지위치_표제부"] = "서울특별시 강남구 역삼동 601-5번지"
            elif "8**" in jib:
                out.at[idx, "매칭단계"] = "추정매칭: 비율일치 (유일후보)"
                out.at[idx, "대지위치_표제부"] = "서울특별시 강남구 역삼동 823"
            elif "7**" in jib:
                out.at[idx, "역매칭실패사유"] = "지분_비율불일치"
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
    kw.setdefault("match", True)
    return S._deals_export_payload(**kw)


def _read(path: str) -> list[dict[str, str]]:
    with open(path, encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


# ── 입력 검증 ─────────────────────────────────────────────────────────

@pytest.mark.parametrize("bad", ["2026-07", "20267", "abc123", "202613", "199901"])
def test_연월_형식이_아니면_PARSE_ERROR(fake_pipe, bad) -> None:
    d = _call(year_month=bad)
    assert d["status"] == "PARSE_ERROR"
    assert fake_pipe["fetch_calls"] == [], "검증 실패인데 API 를 호출했다"


def test_범위_역순이면_PARSE_ERROR(fake_pipe) -> None:
    assert _call(year_month="202607", year_month_to="202601")["status"] == "PARSE_ERROR"


def test_범위_24개월_초과는_PARSE_ERROR(fake_pipe) -> None:
    d = _call(year_month="202301", year_month_to="202607")
    assert d["status"] == "PARSE_ERROR" and "24" in d["message"]


def test_모르는_구는_PARSE_ERROR(fake_pipe) -> None:
    assert _call(gu="부산진구")["status"] == "PARSE_ERROR"


def test_키없으면_CONFIG_ERROR(monkeypatch, fake_pipe) -> None:
    monkeypatch.delenv("DEAL_LOCATOR_SERVICE_KEY", raising=False)
    monkeypatch.delenv("DATA_GO_KR_API_KEY", raising=False)
    assert _call()["status"] == "CONFIG_ERROR"


# ── 범위·스코프 ────────────────────────────────────────────────────────

def test_전역구는_25개_구를_모두_조회한다(fake_pipe) -> None:
    _call()
    assert {g for _, g in fake_pipe["fetch_calls"]} == set(SEOUL_GU_CODES)


def test_단일_구_지정시_그_구만_조회한다(fake_pipe) -> None:
    d = _call(gu="강남구")
    assert {g for _, g in fake_pipe["fetch_calls"]} == {"강남구"}
    assert d["gu_scope"] == "강남구"


def test_범위지정시_모든_월을_조회한다(fake_pipe) -> None:
    d = _call(year_month="202511", year_month_to="202602", gu="강남구")
    assert d["months"] == ["202511", "202512", "202601", "202602"]


# ── 기본 = 역매칭 · 2파일 분리 ────────────────────────────────────────

def test_기본은_역매칭이다(fake_pipe) -> None:
    _call(gu="강남구")
    assert fake_pipe["bulk_calls"] == 1


def test_복원본과_미복원본_2파일로_나뉜다(fake_pipe) -> None:
    d = _call(gu="강남구")
    assert d["status"] == "OK"
    assert d["rows_total"] == 3 and d["rows_matched"] == 2 and d["rows_unmatched"] == 1
    assert "복원" in d["filename"] and "미복원" in d["filename_unmatched"]
    assert len(_read(d["path"])) == 2
    assert len(_read(d["path_unmatched"])) == 1


def test_복원본은_원본_지번을_유지하고_파생컬럼을_추가한다(fake_pipe) -> None:
    d = _call(gu="강남구")
    rows = {r["지번"].split()[-1]: r for r in _read(d["path"])}
    exact = rows["6**"]
    assert exact["지번"].endswith("6**"), "원본 마스킹 지번이 사라졌다"
    assert exact["복원지번"] == "601-5"
    assert exact["매칭신뢰도"] == "정확매칭"
    assert rows["8**"]["매칭신뢰도"] == "추정매칭"
    assert rows["8**"]["복원지번"] == "823"


def test_신뢰도_분해를_보고한다(fake_pipe) -> None:
    d = _call(gu="강남구")
    assert d["confidence_breakdown"] == {"정확매칭": 1, "추정매칭": 1}


def test_미복원본에는_실패사유가_있고_파생컬럼은_없다(fake_pipe) -> None:
    d = _call(gu="강남구")
    (row,) = _read(d["path_unmatched"])
    assert row["역매칭실패사유"] == "지분_비율불일치"
    assert "매칭신뢰도" not in row and "복원지번" not in row


def test_미복원_0건이면_미복원_파일을_만들지_않는다(fake_pipe, monkeypatch, _env) -> None:
    def _all_match(df):
        out = df.copy()
        out["매칭단계"] = "1단계: 정확매칭"
        out["대지위치_표제부"] = "서울특별시 강남구 역삼동 601"
        return out
    monkeypatch.setattr(fake_pipe["pipe"], "bulk_match", _all_match)
    d = _call(gu="강남구")
    assert d["rows_unmatched"] == 0 and d["path_unmatched"] == ""
    assert not any("미복원" in p.name for p in Path(_env).glob("*.csv"))


def test_해제거래는_행으로_남고_건수를_보고한다(fake_pipe) -> None:
    d = _call(gu="강남구")
    assert d["cancelled_count"] == 1
    all_rows = _read(d["path"]) + _read(d["path_unmatched"])
    assert any((r.get("해제사유발생일") or "").strip() for r in all_rows), \
        "해제 거래가 CSV 에서 사라졌다 — 데이터 다운로드는 행을 지우면 안 된다"


# ── 원본 모드 탈출구 ──────────────────────────────────────────────────

def test_match_false_면_원본_1파일이고_역매칭하지_않는다(fake_pipe) -> None:
    d = _call(gu="강남구", match=False)
    assert fake_pipe["bulk_calls"] == 0
    assert d["rows_matched"] is None and d["path_unmatched"] == ""
    assert "원본" in d["filename"]
    rows = _read(d["path"])
    assert len(rows) == d["rows_total"] == 3
    assert "매칭신뢰도" not in rows[0]


# ── 필터·경로·집계 ────────────────────────────────────────────────────

def test_통건물만_남기고_집합은_제외_건수로_보고한다(fake_pipe) -> None:
    d = _call(gu="강남구")
    assert d["rows_total"] == 3 and d["jiphap_excluded"] == 1


def test_전역_행수는_구별_합과_같다(fake_pipe) -> None:
    d = _call()
    assert d["rows_total"] == 75 == sum(d["per_gu"].values())  # 25구 × 통건물 3건


def test_고정폴더에_저장한다_날짜_하위폴더_없음(fake_pipe, _env) -> None:
    d = _call(gu="강남구")
    assert Path(d["path"]).parent == Path(_env), "날짜 하위폴더가 생겼다 — 파이프라인 경로 예측 불가"


def test_파일명에_스코프와_연월이_들어간다(fake_pipe) -> None:
    d = _call(gu="강남구", year_month="202511", year_month_to="202601")
    assert "강남구" in d["filename"] and "202511-202601" in d["filename"]
    d2 = _call()
    assert "서울전역" in d2["filename"] and "202607" in d2["filename"]


def test_재실행은_같은_경로를_덮어쓴다(fake_pipe) -> None:
    d1 = _call(gu="강남구")
    d2 = _call(gu="강남구")
    assert d1["path"] == d2["path"] and d1["rows_total"] == d2["rows_total"]


# ── 데이터 부재 ───────────────────────────────────────────────────────

def test_거래없음은_NOT_FOUND_이고_파일을_쓰지_않는다(fake_pipe, monkeypatch, _env) -> None:
    monkeypatch.setattr(fake_pipe["pipe"], "fetch_month_cached",
                        lambda ym, gu: pd.DataFrame())
    d = _call(gu="강남구")
    assert d["status"] == "NOT_FOUND" and d["path"] == ""
    assert list(Path(_env).rglob("*.csv")) == []


def test_NOT_FOUND_는_에러가_아니다() -> None:
    assert "NOT_FOUND" not in S._IS_ERROR_STATUSES
