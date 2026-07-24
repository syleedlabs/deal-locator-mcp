"""resolve_address 구조화 출력(structuredContent) 테스트.

개조 전 렌더러를 _legacy_resolve_address 로 보존해 신구 텍스트를 바이트 대조하고,
필지 구성(lot_structure)이 '단일'과 '조회실패'를 구분해 내보내는지 본다.
개조 전 텍스트는 둘을 구분했지만(문구가 다름) 기계 판독은 불가능했다.
"""

from __future__ import annotations

import asyncio
from typing import Any, Optional

import pytest

from deal_locator import server as S
from deal_locator.core import SEOUL_GU_CODES


# ── 개조 전 원본 렌더러 (수정 금지 — 골든 기준) ────────────────────────

def _legacy_resolve_address(query: str) -> str:
    parsed = S.parse_address(query, gu_resolver=S.resolve_gu)
    gu, dong, bunji = parsed["gu"], parsed["dong"], parsed["bunji"]
    if not (dong and bunji):
        return f"{S.PARSE_ERROR} 지번 해석 실패: '{query}' — '구 동 번지' 형태로 입력하세요."
    if not gu:
        return f"{S.PARSE_ERROR} '{dong}'의 구를 찾지 못함 — 구를 함께 입력하세요."

    lines = [f"■ 주소 해석 — {query}",
             f"  구/동/번지: {gu} / {dong} / {bunji}",
             f"  시군구코드: {SEOUL_GU_CODES.get(gu, '?')}"]
    try:
        p = S.get_pipeline()
        bdong = p._get_bdong_code(gu, dong)
        if bdong:
            lines.append(f"  법정동코드: {bdong}")
        idx = p.load_attached_index(gu, dong)
        lots = sorted(idx.lot_set(bunji))
        mains = idx.resolve_main(bunji)
        if mains:
            lines.append(f"  필지 구성: '{bunji}'는 대표지번 {mains[0]}의 부속지번 — 세트 {', '.join(lots)}")
        elif len(lots) > 1:
            lines.append(f"  필지 구성: 다필지 대지 — 세트 {', '.join(lots)}")
        else:
            lines.append("  필지 구성: 단일 필지(부속지번 대장 기준)")
    except Exception:  # noqa: BLE001
        lines.append("  필지 구성: (부속지번 조회 실패 — 키/네트워크 확인)")
    lines.append(S.SOURCE_LINE)
    return "\n".join(lines)


# ── 페이크 파이프라인 ───────────────────────────────────────────────

class _FakeIndex:
    def __init__(self, lots: tuple[str, ...], mains: tuple[str, ...]) -> None:
        self._lots, self._mains = lots, mains

    def lot_set(self, bunji: str) -> set[str]:
        return set(self._lots) or {bunji}

    def resolve_main(self, bunji: str) -> list[str]:
        return list(self._mains)


class _FakePipeline:
    """실 파이프라인의 두 진입점만 흉내낸다 (신·구 렌더러 공용)."""

    def __init__(self, bdong: str = "10300", lots: tuple[str, ...] = ("1128-1",),
                 mains: tuple[str, ...] = (), fail: Optional[str] = None) -> None:
        self._bdong, self._lots, self._mains, self._fail = bdong, lots, mains, fail

    def get_bdong_code(self, gu: str, dong: str) -> str:      # 개조 후 경로(public)
        return self._get_bdong_code(gu, dong)

    def _get_bdong_code(self, gu: str, dong: str) -> str:     # 개조 전 경로(private)
        if self._fail == "bdong":
            raise RuntimeError("법정동코드 조회 실패")
        return self._bdong

    def load_attached_index(self, gu: str, dong: str) -> _FakeIndex:
        if self._fail == "index":
            raise RuntimeError("부속지번 API 실패")
        return _FakeIndex(self._lots, self._mains)


# (query, 파이프라인)
CASES: dict[str, tuple[str, _FakePipeline]] = {
    "단일필지": ("구로구 구로동 1128-1", _FakePipeline()),
    "부속지번": ("구로구 구로동 103-2",
                _FakePipeline(lots=("100-4", "103-2"), mains=("100-4",))),
    "다필지": ("구로구 구로동 100-4", _FakePipeline(lots=("100-4", "103-2"))),
    "법정동코드_미상": ("구로구 구로동 1128-1", _FakePipeline(bdong="")),
    "부속지번_조회실패": ("구로구 구로동 1128-1", _FakePipeline(fail="index")),
    "법정동코드_조회실패": ("구로구 구로동 1128-1", _FakePipeline(fail="bdong")),
    "서울_접두_생략형": ("서울 성동구 성수동2가 321-90",
                     _FakePipeline(bdong="10500", lots=("321-90",))),
}


@pytest.fixture(autouse=True)
def _isolate(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DEAL_LOCATOR_SERVICE_KEY", "test-key")
    S._cache.clear()


def _patch_pipeline(monkeypatch: pytest.MonkeyPatch, p: _FakePipeline) -> None:
    monkeypatch.setattr(S, "get_pipeline", lambda: p)


# ── 1) 텍스트 회귀 ──────────────────────────────────────────────────

@pytest.mark.parametrize("name", list(CASES))
def test_text_output_is_byte_identical_to_legacy(name: str, monkeypatch: pytest.MonkeyPatch) -> None:
    query, pipe = CASES[name]
    _patch_pipeline(monkeypatch, pipe)
    new = S._render_resolve_address(S._resolve_address_payload(query))
    assert new == _legacy_resolve_address(query), f"[{name}] 텍스트 출력이 개조 전과 달라짐"


def test_parse_error_texts_unchanged(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_pipeline(monkeypatch, _FakePipeline())

    d = S._resolve_address_payload("헛소리")
    assert d["status"] == "PARSE_ERROR"
    assert S._render_resolve_address(d) == _legacy_resolve_address("헛소리")
    assert "'구 동 번지' 형태" in d["message"]

    # 동만 주고 구 역추적 실패 → 구 미상
    monkeypatch.setattr(S, "resolve_gu", lambda dong: "")
    d = S._resolve_address_payload("성수동2가 321-90")
    assert d["status"] == "PARSE_ERROR"
    assert S._render_resolve_address(d) == _legacy_resolve_address("성수동2가 321-90")
    assert "구를 찾지 못함" in d["message"]


# ── 2) 구조화 출력 계약 ─────────────────────────────────────────────

def test_structured_payload_carries_codes_and_lots(monkeypatch: pytest.MonkeyPatch) -> None:
    query, pipe = CASES["부속지번"]
    _patch_pipeline(monkeypatch, pipe)
    d = S._resolve_address_payload(query)

    assert d["status"] == "OK"
    assert d["address"] == {"gu": "구로구", "dong": "구로동", "bunji": "103-2"}
    assert d["sigungu_code"] == SEOUL_GU_CODES["구로구"]
    assert d["bdong_code"] == "10300"
    assert d["lot_structure"] == "부속지번"
    assert d["main_bunji"] == "100-4"
    assert d["lot_set"] == ["100-4", "103-2"]


@pytest.mark.parametrize("name,expected", [
    ("단일필지", "단일"),
    ("다필지", "다필지"),
    ("부속지번", "부속지번"),
    ("부속지번_조회실패", "조회실패"),
    ("법정동코드_조회실패", "조회실패"),
])
def test_lot_structure_is_machine_readable(name: str, expected: str,
                                           monkeypatch: pytest.MonkeyPatch) -> None:
    query, pipe = CASES[name]
    _patch_pipeline(monkeypatch, pipe)
    assert S._resolve_address_payload(query)["lot_structure"] == expected


def test_lookup_failure_is_not_reported_as_single_lot(monkeypatch: pytest.MonkeyPatch) -> None:
    """'조회실패'를 '단일 필지'로 읽으면 다필지 대지를 단일로 오판한다."""
    query, pipe = CASES["부속지번_조회실패"]
    _patch_pipeline(monkeypatch, pipe)
    d = S._resolve_address_payload(query)
    assert d["status"] == "OK"          # 구/동/번지 해석 자체는 유효
    assert d["lot_structure"] == "조회실패"
    assert d["lot_set"] == []           # 빈 배열 — 단일 필지 세트로 오인 금지
    assert d["main_bunji"] is None


def test_missing_bdong_code_is_empty_not_placeholder(monkeypatch: pytest.MonkeyPatch) -> None:
    query, pipe = CASES["법정동코드_미상"]
    _patch_pipeline(monkeypatch, pipe)
    d = S._resolve_address_payload(query)
    assert d["bdong_code"] == ""        # 텍스트의 '?' 는 표시용, 구조화엔 새지 않는다
    assert "법정동코드" not in S._render_resolve_address(d)


def test_payload_matches_output_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    jsonschema = pytest.importorskip("jsonschema")
    for name, (query, pipe) in CASES.items():
        _patch_pipeline(monkeypatch, pipe)
        jsonschema.validate(S._resolve_address_payload(query),
                            S.RESOLVE_ADDRESS_OUTPUT_SCHEMA)
    jsonschema.validate(S._resolve_address_payload("헛소리"),
                        S.RESOLVE_ADDRESS_OUTPUT_SCHEMA)


# ── 3) MCP 프로토콜 레벨 ────────────────────────────────────────────

def _call(args: dict[str, Any]):
    from fastmcp import Client

    async def run():
        async with Client(S.mcp) as c:
            return await c.call_tool("resolve_address", args, raise_on_error=False)

    return asyncio.run(run())


def test_tool_advertises_schema_and_annotations() -> None:
    from fastmcp import Client

    async def run():
        async with Client(S.mcp) as c:
            return await c.list_tools()

    t = {x.name: x for x in asyncio.run(run())}["resolve_address"]
    assert t.outputSchema is not None
    assert "lot_structure" in t.outputSchema["properties"]
    assert t.annotations.readOnlyHint is True
    assert t.annotations.openWorldHint is True


def test_call_tool_returns_both_text_and_structured(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_pipeline(monkeypatch, CASES["단일필지"][1])
    r = _call({"query": "구로구 구로동 1128-1"})
    assert r.content[0].text.startswith("■ 주소 해석 — 구로구 구로동 1128-1")
    assert r.structured_content["lot_structure"] == "단일"
    assert r.content[0].text == S._render_resolve_address(r.structured_content)
    assert r.is_error is False


def test_parse_error_is_a_tool_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """입력 오류는 도구 실패다 (데이터 부재인 NOT_FOUND 와 달리)."""
    _patch_pipeline(monkeypatch, _FakePipeline())
    r = _call({"query": "헛소리"})
    assert r.is_error is True
    assert S.PARSE_ERROR in r.content[0].text
