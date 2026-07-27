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
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger("deal_locator.attached_jibun")

ATCH_JIBUN_URL = "http://apis.data.go.kr/1613000/BldRgstHubService/getBrAtchJibunInfo"


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


def fetch_attached_jibun(
    api_key: str,
    sigungu_code: str,
    bdong_code: str,
    timeout: int = 10,
    max_pages: int = 100,
) -> Optional[list[dict[str, str]]]:
    """동 단위 부속지번 대장 조회 → [{"main": 대표지번, "attached": 부속지번}, ...].

    표제부 조회(_fetch_pyoje_api)와 동일한 페이지네이션 패턴.
    API 실패 시 None (빈 리스트는 '부속지번 없음'과 구분).
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

    try:
        while page <= max_pages:
            params = {
                "serviceKey": decoded_key,
                "sigunguCd": sigungu_code,
                "bjdongCd": bdong_code,
                "numOfRows": "100",
                "pageNo": str(page),
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
                # 앞 페이지에서 이미 모은 쌍이 있으면 부분이라도 반환(전멸 방지)
                return pairs if pairs else None

            try:
                data = xmltodict.parse(
                    resp.text, disable_entities=True, process_namespaces=False
                )
            except Exception:  # noqa: BLE001
                logger.warning("부속지번 API XML 파싱 실패")
                return None

            header = data.get("response", {}).get("header", {})
            if header.get("resultCode") != "00":
                logger.warning(f"부속지번 API 에러: {header.get('resultMsg')}")
                return None

            body = data.get("response", {}).get("body", {})
            items = body.get("items") or {}
            item_list = items.get("item", []) if isinstance(items, dict) else []
            if isinstance(item_list, dict):
                item_list = [item_list]
            if not item_list:
                break

            for it in item_list:
                main = _norm_bun_ji(it.get("bun"), it.get("ji"))
                attached = _norm_bun_ji(it.get("atchBun"), it.get("atchJi"))
                if main and attached and main != attached:
                    pairs.append({"main": main, "attached": attached})

            total_count = int(body.get("totalCount", 0) or 0)
            if page * 100 >= total_count:
                break
            page += 1
    except Exception as e:  # noqa: BLE001
        logger.warning(f"부속지번 API 조회 실패: {e}")
        return None

    if total_count > max_pages * 100:
        logger.warning(
            f"부속지번 절단: 전체 {total_count}건 중 {max_pages * 100}건만 수신"
        )
    return pairs


class AttachedJibunIndex:
    """부속지번 ↔ 대표지번 인덱스 (동 단위)."""

    def __init__(self, pairs: Optional[list[dict[str, str]]] = None) -> None:
        # 부속지번 → 대표지번들 (드물게 복수 대장에 걸릴 수 있어 set)
        self._attached_to_main: dict[str, set[str]] = {}
        # 대표지번 → 필지세트(대표 포함)
        self._main_to_lots: dict[str, set[str]] = {}
        for p in pairs or []:
            self.add(p["main"], p["attached"])

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
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps({"pairs": self.to_pairs()}, ensure_ascii=False),
            encoding="utf-8",
        )

    @classmethod
    def load_json(cls, path: Path) -> "AttachedJibunIndex":
        data = json.loads(path.read_text(encoding="utf-8"))
        return cls(data.get("pairs", []))
