"""NaN → 'nan' 문자열 누출 회귀 테스트.

표제부 행의 도로명대지위치가 비어 있으면 pandas 는 float('nan') 을 준다.
`str(v or "")` 는 nan 이 truthy 라 'nan' 이라는 문자열을 만들고, 그게
도로명 컬럼에 실려 area_scan 응답의 road 로 그대로 노출됐다
(실측: 광진구 군자동 64-9, road="nan"). 원인 지점은 _s() 로 통일한다.
"""

from __future__ import annotations

import pandas as pd

from deal_locator.core import lookup as L
from deal_locator.core import pipeline as P


def test_s_helpers_map_na_to_empty() -> None:
    for na in (None, float("nan"), pd.NA, pd.NaT):
        assert P._s(na) == ""
        assert L._s(na) == ""
    assert P._s("  서울특별시 광진구 군자로 137  ") == "서울특별시 광진구 군자로 137"


def test_extract_match_does_not_leak_nan_road() -> None:
    """도로명이 NaN 인 표제부 행 → road_address 는 'nan' 이 아니라 ''."""
    row = pd.Series({
        "대지위치": "서울특별시 광진구 군자동 64-9번지",
        "도로명대지위치": float("nan"),
    })
    out = P.RealEstateDataPipeline._extract_match(row, "1단계: 정확매칭")

    assert out["road_address"] == ""
    assert out["address"] == "서울특별시 광진구 군자동 64-9"
    assert "nan" not in out["road_address"]


def test_extract_match_does_not_leak_nan_address() -> None:
    """대지위치가 NaN 이어도 address 에 'nan' 이 새지 않는다."""
    row = pd.Series({"대지위치": float("nan"), "도로명대지위치": float("nan")})
    out = P.RealEstateDataPipeline._extract_match(row, "3단계: 오차범위+-10%")

    assert out == {"address": "", "road_address": "", "match_stage": "3단계: 오차범위+-10%"}


def test_nan_road_does_not_pass_road_contains_filter() -> None:
    """도로명 미상은 road_contains 필터에서 '미상'으로 세어야 한다.

    'nan' 이 새면 truthy 라 미상 카운트를 건너뛰고 불일치로 분류돼,
    coverage 의 road_unknown 모수가 틀어진다.
    """
    row = pd.Series({"도로명대지위치_표제부": float("nan")})
    assert L._s(row.get("도로명대지위치_표제부")) == ""
