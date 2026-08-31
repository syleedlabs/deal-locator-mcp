"""실거래 월 캐시 프리워밍 — 첫 조회 지연을 미리 걷어낸다.

구·동 조회는 국토부 API 를 (구, 월) 단위로 부른다. 12개월이면 한 구에 12회라
캐시가 빈 구를 처음 열면 20~35초가 걸린다. 중개사의 첫인상이 여기서 결정되므로,
쓰기 전에 미리 채워두는 경로를 따로 둔다.

배치 엔드포인트가 없어 (구, 월)마다 1회씩이 최소다 — 25구 12개월이면 300회.
그래서 이건 MCP 도구가 아니라 **사람이 한 번 돌리는 CLI** 다. 도구로 만들면
대화 중에 몇 분씩 멈추게 된다.

재실행해도 안전하다: 이미 신선한 캐시는 `fetch_month_cached` 가 건너뛰므로
중단된 지점부터 이어진다.

사용:
    deal-locator-warm                 # 25구 · 12개월
    deal-locator-warm --gus 강남구 성동구
    deal-locator-warm --months 24 --workers 8
"""

from __future__ import annotations

import argparse
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from deal_locator.core import PipelineConfig, RealEstateDataPipeline, SEOUL_GU_CODES

logger = logging.getLogger("deal_locator.warm")


def month_keys(months: int, now: datetime) -> list[str]:
    """최근 N개월의 'YYYYMM' 목록 (fetch_from_api_multi_month 와 같은 규칙).

    **이번 달부터** 센다. 당월은 신고(계약일로부터 30일)가 진행 중이라 표본이
    얇지만, 빼면 며칠 전 체결된 거래가 조회에서 통째로 사라져 '거래없음'
    오답이 된다(실측: 1,200억 거래 5일 뒤 NOT_FOUND). 얇은 표본은 고지로 다룬다.
    """
    out: list[str] = []
    for i in range(months):
        m = now.month - i
        y = now.year
        while m <= 0:
            m += 12
            y -= 1
        out.append(f"{y}{m:02d}")
    return out


@dataclass
class WarmStats:
    """워밍 결과. 실패를 삼키지 않고 세어서 돌려준다."""

    total: int = 0
    ok: int = 0
    failed: int = 0
    rows: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def done(self) -> int:
        return self.ok + self.failed


def warm_cache(
    pipeline: RealEstateDataPipeline,
    gus: list[str],
    months: int,
    workers: int = 4,
    now: datetime | None = None,
    on_progress=None,
) -> WarmStats:
    """(구 × 월) 조합을 훑어 월 캐시를 채운다.

    workers 는 외부 API 를 동시에 때리는 수라 기본값을 낮게(4) 잡는다 — 공공데이터
    포털은 호출 한도가 있고, 빨리 채우려다 차단당하면 워밍 자체가 무의미해진다.
    """
    yms = month_keys(months, now or datetime.now())
    jobs = [(gu, ym) for gu in gus for ym in yms]
    stats = WarmStats(total=len(jobs))

    def _one(job: tuple[str, str]) -> tuple[str, str, int, str]:
        gu, ym = job
        try:
            df = pipeline.fetch_month_cached(ym, gu)
            return gu, ym, len(df), ""
        except Exception as e:  # noqa: BLE001
            return gu, ym, 0, repr(e)

    with ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
        futures = {ex.submit(_one, j): j for j in jobs}
        for fut in as_completed(futures):
            gu, ym, n, err = fut.result()
            if err:
                stats.failed += 1
                stats.errors.append(f"{gu} {ym}: {err}")
            else:
                stats.ok += 1
                stats.rows += n
            if on_progress:
                on_progress(stats, gu, ym, err)
    return stats


def main() -> int:
    from deal_locator.server import _load_dotenv_fallback  # 지연 import (순환 방지)

    ap = argparse.ArgumentParser(
        prog="deal-locator-warm",
        description="실거래 월 캐시를 미리 채워 첫 조회 지연을 없앤다.",
    )
    ap.add_argument("--gus", nargs="*", default=None,
                    help="대상 구(기본: 서울 25구 전체). 예: --gus 강남구 성동구")
    ap.add_argument("--months", type=int, default=12, help="조회 개월 수 (기본 12)")
    ap.add_argument("--workers", type=int, default=4,
                    help="동시 호출 수 (기본 4). 올리면 빠르지만 API 한도에 걸릴 수 있다.")
    args = ap.parse_args()

    logging.basicConfig(level=logging.WARNING, stream=sys.stderr)
    _load_dotenv_fallback()

    import os
    key = os.environ.get("DEAL_LOCATOR_SERVICE_KEY") or os.environ.get("DATA_GO_KR_API_KEY")
    if not key:
        print("인증키가 없습니다. ~/.deal-locator.env 에 "
              "DEAL_LOCATOR_SERVICE_KEY=... 를 넣어주세요.", file=sys.stderr)
        return 2

    gus = args.gus or list(SEOUL_GU_CODES)
    unknown = [g for g in gus if g not in SEOUL_GU_CODES]
    if unknown:
        print(f"알 수 없는 구: {', '.join(unknown)}", file=sys.stderr)
        return 2

    cache_dir = os.environ.get(
        "DEAL_LOCATOR_CACHE_DIR", str(Path.home() / ".cache" / "deal-locator-mcp")
    )
    p = RealEstateDataPipeline()
    p.initialize(PipelineConfig(api_key=key, cache_dir=cache_dir,
                                max_api_calls_per_fetch=120))

    total = len(gus) * args.months
    print(f"프리워밍 시작 — {len(gus)}개 구 × {args.months}개월 = {total}건 "
          f"(동시 {args.workers})", file=sys.stderr)
    print(f"캐시 위치: {cache_dir}/deals", file=sys.stderr)

    def progress(st: WarmStats, gu: str, ym: str, err: str) -> None:
        mark = "!" if err else "."
        end = "\n" if st.done % 50 == 0 or st.done == st.total else ""
        print(mark, end=end, file=sys.stderr, flush=True)

    started = datetime.now()
    stats = warm_cache(p, gus, args.months, args.workers, on_progress=progress)
    elapsed = (datetime.now() - started).total_seconds()

    print(f"\n완료 — 성공 {stats.ok}/{stats.total} · 실패 {stats.failed} · "
          f"거래 {stats.rows:,}건 · {elapsed:.0f}초", file=sys.stderr)
    if stats.errors:
        print("실패 목록(재실행하면 이어서 시도합니다):", file=sys.stderr)
        for e in stats.errors[:10]:
            print(f"  {e}", file=sys.stderr)
        if len(stats.errors) > 10:
            print(f"  … 외 {len(stats.errors) - 10}건", file=sys.stderr)
    return 1 if stats.failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
