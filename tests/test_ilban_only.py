"""취급 범위 = 통건물(유형='일반')만. 집합(구분상가) 제외 규칙 테스트.

제품 결정(2026-07-24): 이 MCP 는 구분상가를 취급하지 않는다. 섞이면 평단가 통계가
구분상가 쪽으로 끌려간다(실측: 대치동 24개월 329건 중 집합 252 · 일반 77 —
연면적 중앙값이 통건물 시세가 아니라 사실상 구분상가 시세가 됐다).
"""

from __future__ import annotations

import pandas as pd

from deal_locator.core.pipeline import filter_ilban


def _df(types: list[str]) -> pd.DataFrame:
    return pd.DataFrame({
        "유형": types,
        "거래금액(만원)": [10000] * len(types),
    })


def test_keeps_only_ilban() -> None:
    kept, dropped = filter_ilban(_df(["일반", "집합", "일반", "집합", "집합"]))

    assert len(kept) == 2
    assert dropped == 3
    assert set(kept["유형"]) == {"일반"}


def test_all_jiphap_yields_empty_not_error() -> None:
    """전부 구분상가인 동 — 빈 결과가 정상이다(예외 아님)."""
    kept, dropped = filter_ilban(_df(["집합", "집합"]))

    assert kept.empty
    assert dropped == 2


def test_nan_type_is_dropped() -> None:
    """유형이 NaN 이면 통건물로 확정할 수 없으니 제외한다."""
    kept, dropped = filter_ilban(_df(["일반", float("nan")]))

    assert len(kept) == 1
    assert dropped == 1


def test_whitespace_type_is_normalized() -> None:
    """앞뒤 공백이 붙어도 통건물로 인식한다(_s 가 strip)."""
    kept, dropped = filter_ilban(_df([" 일반 ", "일반"]))

    assert len(kept) == 2
    assert dropped == 0


def test_missing_column_passes_through() -> None:
    """'유형' 컬럼이 없는 입력은 판별 불가 — 전량 삭제하지 않고 통과시킨다.

    조용한 전량 삭제가 잘못된 필터보다 위험하다.
    """
    df = pd.DataFrame({"거래금액(만원)": [10000, 20000]})
    kept, dropped = filter_ilban(df)

    assert len(kept) == 2
    assert dropped == 0


def test_empty_input_is_safe() -> None:
    kept, dropped = filter_ilban(pd.DataFrame())

    assert kept.empty
    assert dropped == 0


def test_original_frame_not_mutated() -> None:
    """호출자가 넘긴 DF 는 그대로 남는다(복사본을 반환)."""
    src = _df(["일반", "집합"])
    kept, _ = filter_ilban(src)
    kept.loc[kept.index[0], "거래금액(만원)"] = 99999

    assert len(src) == 2
    assert src["거래금액(만원)"].tolist() == [10000, 10000]
