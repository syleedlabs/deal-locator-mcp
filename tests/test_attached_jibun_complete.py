"""부속지번 대장을 다 받지 못한 것을 '단일 필지'로 오판하지 않는지 검증.

배경(2026-10-24 제보): 강남구 삼성동 157-27(3필지)이 resolve_address 에서 '단일'로 나왔다.
삼성동 대장은 627행(7페이지)인데 캐시에는 첫 페이지 분량(76쌍)만 있었다. 부분 수신을
'전부'로 저장한 캐시가 7일마다 같은 실패를 재사용했고, 인덱스에 없으면 '단일'로 읽혔다.

여기서 고정하는 계약:
  1) 일부 페이지만 받으면 complete=False 다(HTTP 실패·중간 빈 페이지·예외 모두).
  2) 불완전한 인덱스는 디스크에 저장하지 않는다. 메타데이터 없는 옛 캐시는 미검증으로 보고 다시 받는다.
  3) API 가 전멸하면 빈 인덱스라도 complete=False — 미등재를 '단일'로 말하지 않는다.
  4) 불완전하면 질의 지번을 직접 묻는다. 부속지번이 나오면 '다필지', 0건·실패면 '조회불완전'.
     직접 조회는 대표지번에서만 부속지번을 돌려주므로 0건이 '단일'의 근거가 되지 않는다.
  5) '단일'은 인덱스가 완전할 때만.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from deal_locator.core import attached_jibun as A
from deal_locator.core import pipeline as P
from deal_locator.core.attached_jibun import AttachedFetch, AttachedJibunIndex


class _Resp:
    def __init__(self, status: int, text: str) -> None:
        self.status_code, self.text = status, text


def _page(rows: list[tuple[str, str, str, str]], total: int) -> _Resp:
    items = [{"bun": b, "ji": j, "atchBun": ab, "atchJi": aj} for b, j, ab, aj in rows]
    return _Resp(200, json.dumps({"response": {"header": {"resultCode": "00"},
                                               "body": {"items": {"item": items},
                                                        "totalCount": str(total)}}}))


def _rows(n: int, start: int = 1) -> list[tuple[str, str, str, str]]:
    return [(f"{i:04d}", "0001", f"{i:04d}", "0002") for i in range(start, start + n)]


def _feed(monkeypatch: pytest.MonkeyPatch, seq: list[Any]) -> list[dict[str, Any]]:
    calls: list[dict[str, Any]] = []

    def fake_get(url: str, params: Any = None, timeout: Any = None) -> _Resp:
        calls.append(dict(params or {}))
        item = seq.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    import requests
    monkeypatch.setattr(requests, "get", fake_get)
    monkeypatch.setattr("time.sleep", lambda *_: None)
    return calls


# ── 1) 수신 완전성 ──────────────────────────────────────────────────

def test_all_pages_received_is_complete(monkeypatch: pytest.MonkeyPatch) -> None:
    _feed(monkeypatch, [_page(_rows(100), 150), _page(_rows(50, 101), 150)])
    f = A.fetch_attached_jibun("k", "11680", "10500")
    assert f is not None and f.complete and f.rows == 150 and len(f.pairs) == 150


def test_http_failure_on_page_2_is_partial(monkeypatch: pytest.MonkeyPatch) -> None:
    _feed(monkeypatch, [_page(_rows(100), 627)] + [_Resp(500, "")] * 3)
    f = A.fetch_attached_jibun("k", "11680", "10500")
    assert f is not None and f.complete is False
    assert (f.rows, f.total_count) == (100, 627)


def test_empty_page_before_total_is_partial(monkeypatch: pytest.MonkeyPatch) -> None:
    _feed(monkeypatch, [_page(_rows(100), 627), _page([], 627)])
    f = A.fetch_attached_jibun("k", "11680", "10500")
    assert f is not None and f.complete is False


def test_exception_after_page_1_is_partial_not_none(monkeypatch: pytest.MonkeyPatch) -> None:
    import requests
    _feed(monkeypatch, [_page(_rows(100), 627), requests.ConnectionError("끊김")])
    f = A.fetch_attached_jibun("k", "11680", "10500")
    assert f is not None and f.complete is False


def test_failure_on_page_1_is_none(monkeypatch: pytest.MonkeyPatch) -> None:
    _feed(monkeypatch, [_Resp(500, "")] * 3)
    assert A.fetch_attached_jibun("k", "11680", "10500") is None


def test_dong_without_attached_lots_is_complete(monkeypatch: pytest.MonkeyPatch) -> None:
    _feed(monkeypatch, [_page([], 0)])
    f = A.fetch_attached_jibun("k", "11680", "10500")
    assert f is not None and f.complete and f.pairs == []


# ── 2) 캐시 직렬화 ──────────────────────────────────────────────────

def test_incomplete_index_refuses_to_save(tmp_path: Path) -> None:
    idx = AttachedJibunIndex([{"main": "1-1", "attached": "1-2"}], complete=False)
    with pytest.raises(ValueError):
        idx.save_json(tmp_path / "x.json")
    assert not (tmp_path / "x.json").exists()


def test_complete_index_round_trips_metadata(tmp_path: Path) -> None:
    idx = AttachedJibunIndex([{"main": "157-27", "attached": "157-29"}],
                             complete=True, total_count=627, fetched_at="2026-10-24T23:00:00")
    idx.save_json(tmp_path / "x.json")
    back = AttachedJibunIndex.load_json(tmp_path / "x.json")
    assert back.complete is True and back.total_count == 627 and back.lot_set("157-27") == {"157-27", "157-29"}


def test_legacy_cache_without_metadata_is_unverified(tmp_path: Path) -> None:
    (tmp_path / "x.json").write_text(json.dumps({"pairs": []}), encoding="utf-8")
    assert AttachedJibunIndex.load_json(tmp_path / "x.json").complete is None


# ── 3) 파이프라인: 저장·재수신·전멸 ─────────────────────────────────

def _pipe(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> P.RealEstateDataPipeline:
    p = P.RealEstateDataPipeline()
    p._cache_dir = tmp_path
    monkeypatch.setattr(p, "_get_bdong_code", lambda g, d: "10500")
    monkeypatch.setattr(p, "_get_api_key", lambda: "k")
    return p


def _disk(tmp_path: Path) -> Path:
    return tmp_path / "attached_jibun" / "강남구_삼성동.json"


def test_partial_fetch_is_not_written_to_disk(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    p = _pipe(tmp_path, monkeypatch)
    monkeypatch.setattr(P, "fetch_attached_jibun",
                        lambda *a, **k: AttachedFetch([{"main": "1-1", "attached": "1-2"}], 627, 100, False))
    idx = p.load_attached_index("강남구", "삼성동")
    assert idx.complete is False
    assert not _disk(tmp_path).exists()


def test_legacy_cache_is_refetched_and_upgraded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _disk(tmp_path).parent.mkdir(parents=True)
    _disk(tmp_path).write_text(json.dumps({"pairs": [{"main": "1-1", "attached": "1-2"}]}), encoding="utf-8")
    p = _pipe(tmp_path, monkeypatch)
    calls = [0]

    def fake(*a: Any, **k: Any) -> AttachedFetch:
        calls[0] += 1
        return AttachedFetch([{"main": "157-27", "attached": "157-29"},
                              {"main": "157-27", "attached": "157-31"}], 627, 627, True)

    monkeypatch.setattr(P, "fetch_attached_jibun", fake)
    idx = p.load_attached_index("강남구", "삼성동")
    assert calls[0] == 1 and idx.complete is True
    saved = json.loads(_disk(tmp_path).read_text(encoding="utf-8"))
    assert saved["complete"] is True and saved["total_count"] == 627


def test_fresh_complete_cache_is_reused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    AttachedJibunIndex([], complete=True, total_count=3).save_json(_disk(tmp_path))
    p = _pipe(tmp_path, monkeypatch)
    monkeypatch.setattr(P, "fetch_attached_jibun", lambda *a, **k: pytest.fail("API 를 부르면 안 된다"))
    assert p.load_attached_index("강남구", "삼성동").complete is True


def test_partial_fetch_does_not_replace_complete_cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """만료된 완전 캐시가 있으면 이번 부분 수신보다 그쪽이 낫다."""
    AttachedJibunIndex([{"main": "157-27", "attached": "157-29"}], complete=True).save_json(_disk(tmp_path))
    import os
    old = _disk(tmp_path).stat().st_mtime - 30 * 86400
    os.utime(_disk(tmp_path), (old, old))
    p = _pipe(tmp_path, monkeypatch)
    monkeypatch.setattr(P, "fetch_attached_jibun",
                        lambda *a, **k: AttachedFetch([], 627, 100, False))
    idx = p.load_attached_index("강남구", "삼성동")
    assert idx.complete is True and idx.lot_set("157-27") == {"157-27", "157-29"}


def test_total_api_failure_is_incomplete_not_single(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    p = _pipe(tmp_path, monkeypatch)
    monkeypatch.setattr(P, "fetch_attached_jibun", lambda *a, **k: None)
    monkeypatch.setattr(P, "fetch_lot_direct", lambda *a, **k: None)
    assert p.load_attached_index("강남구", "삼성동").complete is False
    assert p.lot_info("강남구", "삼성동", "157-27")["lot_structure"] == "조회불완전"


def test_partial_index_is_retried_after_interval(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    p = _pipe(tmp_path, monkeypatch)
    calls = [0]

    def fake(*a: Any, **k: Any) -> AttachedFetch:
        calls[0] += 1
        return AttachedFetch([], 627, 100, False)

    monkeypatch.setattr(P, "fetch_attached_jibun", fake)
    p.load_attached_index("강남구", "삼성동")
    p.load_attached_index("강남구", "삼성동")
    assert calls[0] == 1                                   # 간격 안에서는 재사용
    p._attached_partial_at["강남구_삼성동"] -= p._PARTIAL_RETRY_SEC + 1
    p.load_attached_index("강남구", "삼성동")
    assert calls[0] == 2                                   # 간격이 지나면 다시 받는다


# ── 4) 판정: lot_info ───────────────────────────────────────────────

def _with_index(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fetch: AttachedFetch,
                direct: Any) -> P.RealEstateDataPipeline:
    p = _pipe(tmp_path, monkeypatch)
    monkeypatch.setattr(P, "fetch_attached_jibun", lambda *a, **k: fetch)
    monkeypatch.setattr(P, "fetch_lot_direct", lambda *a, **k: direct)
    return p


def test_samseong_157_27_case_becomes_multi_lot(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """제보 사례 그대로: 첫 페이지에 157-27 이 없고 2페이지가 실패한 상태."""
    p = _with_index(tmp_path, monkeypatch, AttachedFetch([{"main": "1-1", "attached": "1-2"}], 627, 100, False),
                    {"157-27", "157-29", "157-31"})
    d = p.lot_info("강남구", "삼성동", "157-27")
    assert d["lot_structure"] == "다필지" and d["lot_source"] == "직접조회"
    assert d["lot_set"] == ["157-27", "157-29", "157-31"] and d["index_complete"] is False


def test_incomplete_and_direct_empty_is_not_single(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    p = _with_index(tmp_path, monkeypatch, AttachedFetch([], 627, 100, False), set())
    d = p.lot_info("강남구", "삼성동", "157-29")
    assert d["lot_structure"] == "조회불완전" and d["lot_set"] == []


def test_complete_index_without_entry_is_single(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    p = _with_index(tmp_path, monkeypatch, AttachedFetch([], 3, 3, True),
                    lambda: pytest.fail("완전하면 직접 조회하지 않는다"))
    d = p.lot_info("강남구", "삼성동", "200-1")
    assert d["lot_structure"] == "단일" and d["index_complete"] is True


def test_positive_evidence_from_partial_index_is_kept(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """불완전해도 인덱스에 있는 다필지·부속지번은 그대로 믿는다(있다는 증거는 유효)."""
    p = _with_index(tmp_path, monkeypatch,
                    AttachedFetch([{"main": "157-27", "attached": "157-29"}], 627, 100, False), None)
    assert p.lot_info("강남구", "삼성동", "157-29")["lot_structure"] == "부속지번"
    assert p.lot_info("강남구", "삼성동", "157-27")["lot_structure"] == "다필지"


# ── 5) 직접 조회 ────────────────────────────────────────────────────

def test_direct_query_pads_bun_ji_and_collects_lots(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = _feed(monkeypatch, [_page([("0157", "0027", "0157", "0029"), ("0157", "0027", "0157", "0031")], 2)])
    assert A.fetch_lot_direct("k", "11680", "10500", "157-27") == {"157-27", "157-29", "157-31"}
    assert (calls[0]["bun"], calls[0]["ji"], calls[0]["platGbCd"]) == ("0157", "0027", "0")


def test_direct_query_failure_is_none(monkeypatch: pytest.MonkeyPatch) -> None:
    _feed(monkeypatch, [_Resp(500, "")])
    assert A.fetch_lot_direct("k", "11680", "10500", "157-27") is None


def test_direct_query_for_attached_lot_is_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    _feed(monkeypatch, [_page([], 0)])
    assert A.fetch_lot_direct("k", "11680", "10500", "157-29") == set()
