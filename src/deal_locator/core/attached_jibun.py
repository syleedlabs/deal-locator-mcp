"""deal_locator.core.attached_jibun — 건축물대장 부속지번 역인덱스
==========================================================
국토교통부 건축HUB `getBrAtchJibunInfo` 로 동(법정동) 단위 부속지번 대장을 받아
필지세트를 만든다. 다필지(합필) 대지 매칭 정밀화의 데이터 기반.

용도 3가지 (address_lookup / MCP 공용):
  1. 앵커 폴백  — 표제부는 대표지번으로만 기재. 부속지번으로 질의가 오면
                  resolve_main() 으로 대표지번을 찾아 재앵커.
  2. 필지세트   — lot_set() 으로 대표+부속 전체를 얻어 노출지번/마스킹
                  프리픽스 검사를 세트 대상으로 확장.
  3. 다필지 플래그 — is_multi_lot() 이 True 면 대지면적 비교를 완화(사유 기록).

주의: 부속지번 대장은 등재 누락이 있을 수 있다. 매칭을 살리는 보강으로만 쓰고
단독 확정 근거로 쓰지 않는다(신뢰도 체계는 matching.CONFIDENCE 를 따름).

완전성(2026-10-24 제보: 삼성동 157-27 이 '단일'로 나왔으나 실제 3필지):
  동 대장을 끝까지 못 받은 인덱스(부분 수신·절단·API 실패)는 complete=False 다.
  그 인덱스에 '없다'는 것은 '단일'이 아니라 '못 받았다'일 수 있으므로,
  '단일'은 complete=True 일 때만 말할 수 있다. 불완전하면 fetch_lot_direct 로
  질의 지번 1건을 직접 묻는다 — 단, 직접 조회는 대표지번에서만 부속지번을 돌려주므로
  0건이어도 '단일'을 확정하지 못한다(다른 대지의 부속지번일 수 있다).
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from .constants import redact_secrets

logger = logging.getLogger("deal_locator.attached_jibun")

# https 고정 — 평문 HTTP(80)는 일부 네트워크에서 중간 장비에 끊겨 "200 + 빈 본문"
# 이나 ConnectTimeout 으로 돌아온다(2026-09 실측: http 7/10·평균 11.7초 vs
# https 10/10·평균 0.38초). 표제부와 같은 호스트라 같은 규칙을 적용한다.
ATCH_JIBUN_URL = "https://apis.data.go.kr/1613000/BldRgstHubService/getBrAtchJibunInfo"


def _norm_bun_ji(bun: Any, ji: Any) -> str:
    """API 번/지('0321','0090') → 정규 지번('321-90'). 지 0/결측 → '321'."""
    try:
        b = int(str(bun).strip() or 0)
    except (TypeError, ValueError):
        return ""
    try:
        j = int(str(ji).strip() or 0)
    except (TypeError, ValueError):
        j = 0
    if b <= 0:
        return ""
    return f"{b}-{j}" if j else str(b)


@dataclass
class AttachedFetch:
    """동 단위 부속지번 대장 수신 결과. complete=False 면 일부만 받은 것이다."""
    pairs: list[dict[str, str]] = field(default_factory=list)
    total_count: int = 0      # API 가 밝힌 전체 행 수
    rows: int = 0             # 실제로 받은 행 수(대표==부속 행 포함)
    complete: bool = False


def fetch_attached_jibun(
    api_key: str,
    sigungu_code: str,
    bdong_code: str,
    timeout: int = 10,
    max_pages: int = 100,
) -> Optional[AttachedFetch]:
    """동 단위 부속지번 대장 조회 → AttachedFetch(pairs=[{"main", "attached"}, ...]).

    표제부 조회(_fetch_pyoje_api)와 동일한 페이지네이션 패턴.
    한 행도 못 받은 실패는 None. 일부라도 받았으면 complete=False 로 돌려준다
    (부분 결과를 '전부'로 오인해 캐시에 굳히던 결함 — 2026-10-24).
    """
    if not (api_key and sigungu_code and bdong_code):
        return None

    try:
        import requests
        import xmltodict
        from urllib.parse import unquote
    except ImportError as e:  # noqa: BLE001
        logger.warning(f"부속지번 조회 의존성 없음: {e}")
        return None

    decoded_key = unquote(api_key)
    pairs: list[dict[str, str]] = []
    page = 1
    total_count = 0
    rows = 0

    def _partial() -> Optional[AttachedFetch]:
        # 앞 페이지에서 모은 행이 있으면 '불완전' 표시와 함께 돌려준다(전멸 방지).
        return AttachedFetch(pairs, total_count, rows, False) if rows else None

    try:
        while page <= max_pages:
            params = {
                "serviceKey": decoded_key,
                "sigunguCd": sigungu_code,
                "bjdongCd": bdong_code,
                "numOfRows": "100",
                "pageNo": str(page),
                # 건축HUB 기본 응답이 JSON 으로 바뀜(2026-08) — XML 명시 필수
                "_type": "xml",
            }
            # HTTP 5xx·429 는 data.go.kr 일시 장애 — 지수 백오프로 재시도한다.
            # 여기서 즉시 None 을 반환하면 그 동의 부속지번 인덱스가 통째로 비어
            # 다필지(합필) 대지면적 완화가 전면 비활성된다.
            import time as _time
            resp = None
            for attempt in range(3):
                resp = requests.get(ATCH_JIBUN_URL, params=params, timeout=timeout)
                if resp.status_code in (429, 500, 502, 503, 504) and attempt < 2:
                    _time.sleep(2 ** attempt)
                    continue
                break
            if resp.status_code != 200:
                logger.warning(f"부속지번 API HTTP {resp.status_code} (재시도 후)")
                return _partial()

            try:
                # _type=xml 을 무시하고 JSON 이 오는 경우까지 겸용 파싱.
                # 실측 JSON 은 top-level 이 {header, body} 라 response 로
                # 감싸지만, response 래퍼가 이미 있는 형태도 수용한다.
                if resp.text.lstrip().startswith("{"):
                    payload = json.loads(resp.text)
                    data = payload if "response" in payload else {"response": payload}
                else:
                    data = xmltodict.parse(
                        resp.text, disable_entities=True, process_namespaces=False
                    )
            except Exception:  # noqa: BLE001
                logger.warning("부속지번 API 응답 파싱 실패")
                return _partial()

            header = data.get("response", {}).get("header", {})
            if header.get("resultCode") != "00":
                logger.warning(f"부속지번 API 에러: {header.get('resultMsg')}")
                return _partial()

            body = data.get("response", {}).get("body", {})
            items = body.get("items") or {}
            item_list = items.get("item", []) if isinstance(items, dict) else []
            if isinstance(item_list, dict):
                item_list = [item_list]
            total_count = int(body.get("totalCount", 0) or 0)
            if not item_list:
                break        # 마지막 페이지 전에 비면 아래 완전성 판정에서 걸린다
            rows += len(item_list)

            for it in item_list:
                main = _norm_bun_ji(it.get("bun"), it.get("ji"))
                attached = _norm_bun_ji(it.get("atchBun"), it.get("atchJi"))
                if main and attached and main != attached:
                    pairs.append({"main": main, "attached": attached})

            if page * 100 >= total_count:
                break
            page += 1
    except Exception as e:  # noqa: BLE001
        logger.warning(f"부속지번 API 조회 실패: {redact_secrets(e)}")
        return _partial()

    complete = rows >= total_count
    if not complete:
        logger.warning(f"부속지번 불완전 수신: 전체 {total_count}행 중 {rows}행")
    return AttachedFetch(pairs, total_count, rows, complete)


def fetch_lot_direct(
    api_key: str,
    sigungu_code: str,
    bdong_code: str,
    bunji: str,
    timeout: int = 10,
) -> Optional[set[str]]:
    """질의 지번 1건을 대장에 직접 묻는다 → 그 지번이 대표인 대지의 필지세트.

    반환: 부속지번이 있으면 {대표, 부속...}, 없으면 빈 set, 실패면 None.
    실측(2026-10-24): bun/ji 지정은 '대표지번'으로 물을 때만 부속지번을 돌려주고,
    부속지번으로 물으면 0건이다(atchBun/atchJi 필터는 무시되고 동 전체가 온다).
    그래서 빈 set 은 '단일'이 아니다 — 다른 대지의 부속지번일 수 있다.
    """
    if not (api_key and sigungu_code and bdong_code and bunji):
        return None
    try:
        import requests
        from urllib.parse import unquote
    except ImportError:  # noqa: BLE001
        return None
    bon, _, bu = str(bunji).partition("-")
    if not bon.isdigit() or (bu and not bu.isdigit()):
        return None
    params = {
        "serviceKey": unquote(api_key), "sigunguCd": sigungu_code, "bjdongCd": bdong_code,
        "platGbCd": "0", "bun": bon.zfill(4), "ji": (bu or "0").zfill(4),
        "numOfRows": "100", "pageNo": "1", "_type": "json",
    }
    try:
        resp = requests.get(ATCH_JIBUN_URL, params=params, timeout=timeout)
        if resp.status_code != 200:
            return None
        payload = json.loads(resp.text)
        data = payload.get("response", payload)
        if (data.get("header") or {}).get("resultCode") != "00":
            return None
        items = (data.get("body") or {}).get("items") or {}
        item_list = items.get("item", []) if isinstance(items, dict) else []
        if isinstance(item_list, dict):
            item_list = [item_list]
    except Exception as e:  # noqa: BLE001
        logger.warning(f"부속지번 직접 조회 실패: {redact_secrets(e)}")
        return None
    lots: set[str] = set()
    for it in item_list:
        main = _norm_bun_ji(it.get("bun"), it.get("ji"))
        attached = _norm_bun_ji(it.get("atchBun"), it.get("atchJi"))
        if main and attached and main != attached:
            lots.update((main, attached))
    return lots


class AttachedJibunIndex:
    """부속지번 ↔ 대표지번 인덱스 (동 단위)."""

    def __init__(self, pairs: Optional[list[dict[str, str]]] = None,
                 complete: Optional[bool] = True, total_count: int = 0,
                 fetched_at: str = "") -> None:
        # 부속지번 → 대표지번들 (드물게 복수 대장에 걸릴 수 있어 set)
        self._attached_to_main: dict[str, set[str]] = {}
        # 대표지번 → 필지세트(대표 포함)
        self._main_to_lots: dict[str, set[str]] = {}
        for p in pairs or []:
            self.add(p["main"], p["attached"])
        # 동 대장을 끝까지 받았는가. None = 메타데이터 없는 옛 캐시(미검증).
        # '단일'(미등재)은 complete is True 일 때만 확정이다.
        self.complete: Optional[bool] = complete
        self.total_count = total_count
        self.fetched_at = fetched_at

    @classmethod
    def from_fetch(cls, f: "AttachedFetch") -> "AttachedJibunIndex":
        return cls(f.pairs, complete=f.complete, total_count=f.total_count,
                   fetched_at=datetime.now().isoformat(timespec="seconds"))

    def add(self, main: str, attached: str) -> None:
        self._attached_to_main.setdefault(attached, set()).add(main)
        lots = self._main_to_lots.setdefault(main, {main})
        lots.add(attached)

    def __len__(self) -> int:
        return len(self._main_to_lots)

    def resolve_main(self, bunji: str) -> list[str]:
        """지번이 어떤 대지의 부속지번이면 그 대표지번들을 반환 (아니면 빈 리스트)."""
        from .matching import norm_bunji
        return sorted(self._attached_to_main.get(norm_bunji(bunji), set()))

    def lot_set(self, bunji: str) -> set[str]:
        """지번이 속한 대지의 필지세트(대표+부속). 미등재면 {지번} 단독."""
        from .matching import norm_bunji
        b = norm_bunji(bunji)
        if b in self._main_to_lots:
            return set(self._main_to_lots[b])
        for main in self._attached_to_main.get(b, set()):
            return set(self._main_to_lots.get(main, {main, b}))
        return {b}

    def is_multi_lot(self, bunji: str) -> bool:
        """해당 지번의 대지가 다필지(합필)인지."""
        return len(self.lot_set(bunji)) > 1

    # --- JSON 캐시 직렬화 ---

    def to_pairs(self) -> list[dict[str, str]]:
        out: list[dict[str, str]] = []
        for main, lots in sorted(self._main_to_lots.items()):
            for lot in sorted(lots):
                if lot != main:
                    out.append({"main": main, "attached": lot})
        return out

    def save_json(self, path: Path) -> None:
        """완전한 인덱스만 저장한다 — 불완전본이 디스크에 굳으면 한 번의 실패가 영구 오류가 된다."""
        if self.complete is not True:
            raise ValueError("불완전한 부속지번 인덱스는 캐시에 저장하지 않는다")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps({"pairs": self.to_pairs(), "complete": True,
                        "total_count": self.total_count, "fetched_at": self.fetched_at},
                       ensure_ascii=False),
            encoding="utf-8",
        )

    @classmethod
    def load_json(cls, path: Path) -> "AttachedJibunIndex":
        """메타데이터가 없는 옛 캐시는 complete=None(미검증)으로 읽는다."""
        data = json.loads(path.read_text(encoding="utf-8"))
        complete = data.get("complete")
        return cls(data.get("pairs", []),
                   complete=True if complete is True else (None if complete is None else False),
                   total_count=int(data.get("total_count", 0) or 0),
                   fetched_at=str(data.get("fetched_at", "")))
