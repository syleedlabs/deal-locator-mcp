"""구(區) 단위 스캔 테스트.

사용자 요청(2026-07-24): "구랑 동 범위 커버가 되는 명령어면 중개사 입장에서
범용성이 높다." → 같은 도구가 '강남구'와 '대치동'을 모두 받는다.
구 모드는 표제부 역매칭을 돌리지 않으므로 deals 가 항상 비어 있어야 한다.
"""

from __future__ import annotations

from typing import Any

import pytest

from deal_locator import server as S
from deal_locator.core.lookup import resolve_gu_query


# ── 구 질의 판별 ───────────────────────────────────────────────────────

@pytest.mark.parametrize("q, expected", [
    ("강남구", "강남구"),
    ("강남", "강남구"),          # '구' 생략
    ("  송파구 ", "송파구"),      # 공백
    ("서울 마포구", "마포구"),     # 시 접두
    ("서울특별시 성동구", "성동구"),
])
def test_gu_queries_resolve(q: str, expected: str) -> None:
    assert resolve_gu_query(q) == expected


@pytest.mark.parametrize("q", [
    "대치동",           # 동은 구 질의가 아니다
    "강남구 대치동",     # 동이 섞이면 동 모드로 가야 한다
    "구로동",           # '구'로 시작하지만 동이다
    "",
    "없는구",
])
def test_non_gu_queries_return_empty(q: str) -> None:
    assert resolve_gu_query(q) == ""


def test_gu_mode_does_not_swallow_dong_query() -> None:
    """'구로동'은 구가 아니라 동 — 구 모드로 새면 안 된다.

    '구로구'와 한 글자 차이라 판별이 틀리기 쉬운 자리다.
    """
    assert resolve_gu_query("구로동") == ""
    assert resolve_gu_query("구로구") == "구로구"


# ── payload 분기 ───────────────────────────────────────────────────────

def _stub_gu(monkeypatch: pytest.MonkeyPatch, res: dict[str, Any]) -> None:
    monkeypatch.setattr(S, "_cached_scan_gu", lambda area, months: res)
    monkeypatch.setattr(S, "_key_ok", lambda: True)


def _ok_gu_result() -> dict[str, Any]:
    agg = {"n": 10, "mean_manwon": 5000, "median_manwon": 4800,
           "min_manwon": 1000, "max_manwon": 9000,
           "p25_manwon": 4000, "p75_manwon": 6000}
    return {
        "status": "거래있음", "gu": "강남구", "scope": "구",
        "total_in_gu": 120, "jiphap_excluded": 300, "cancelled_count": 8,
        "ppp_uncomputable": 0, "message": "강남구 — …",
        "stats": {"gross": agg, "land": agg, "quarterly": []},
        "by_dong": [{"dong": "신사동", "gross": agg, "land": agg}],
    }


def test_gu_payload_has_scope_and_no_deals(monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_gu(monkeypatch, _ok_gu_result())
    d = S._area_scan_payload("강남구", 0, 0, 12, "", 30)

    assert d["scope"] == "구"
    assert d["status"] == "OK"
    assert d["deals"] == []          # 구 모드는 역매칭을 돌리지 않는다
    assert d["area"] == {"gu": "강남구", "dong": ""}
    assert len(d["by_dong"]) == 1


def test_gu_payload_maps_coverage(monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_gu(monkeypatch, _ok_gu_result())
    c = S._area_scan_payload("강남구", 0, 0, 12, "", 30)["coverage"]

    assert c["total_in_dong"] == 120       # 구 기준 모수
    assert c["jiphap_excluded"] == 300
    assert c["cancelled_count"] == 8
    assert c["matched"] == 1               # 동 개수


def test_gu_payload_validates_against_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    jsonschema = pytest.importorskip("jsonschema")
    _stub_gu(monkeypatch, _ok_gu_result())
    d = S._area_scan_payload("강남구", 0, 0, 12, "", 30)

    jsonschema.validate(d, S.AREA_SCAN_OUTPUT_SCHEMA)


def test_gu_not_found_carries_no_stats(monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_gu(monkeypatch, {
        "status": "거래없음", "gu": "강남구", "scope": "구",
        "message": "강남구 — 최근 12개월 통건물 상업업무용 매매 없음",
        "stats": {}, "by_dong": [],
    })
    d = S._area_scan_payload("강남구", 0, 0, 12, "", 30)

    assert d["status"] == "NOT_FOUND"
    assert d["stats"] == {}
    assert d["by_dong"] == []


# ── 텍스트 렌더 ────────────────────────────────────────────────────────

def test_gu_render_marks_thin_dong(monkeypatch: pytest.MonkeyPatch) -> None:
    res = _ok_gu_result()
    thin = dict(res["by_dong"][0]["land"], n=1)
    res["by_dong"].append({"dong": "수서동", "gross": thin, "land": thin})
    _stub_gu(monkeypatch, res)

    text = S._render_area_scan(S._area_scan_payload("강남구", 0, 0, 12, "", 30))

    assert "■ 구 시세 — 강남구" in text
    assert "수서동 4,800 (n=1)  ※표본적음" in text
    assert "신사동 4,800 (n=10)" in text
    assert "※표본적음" not in text.split("신사동")[1].split("\n")[0]
    assert "동으로 다시 조회" in text


def test_gu_render_shows_jiphap_exclusion(monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_gu(monkeypatch, _ok_gu_result())
    text = S._render_area_scan(S._area_scan_payload("강남구", 0, 0, 12, "", 30))

    assert "구내 통건물 매매 120건" in text
    assert "집합(구분상가) 300 제외" in text
