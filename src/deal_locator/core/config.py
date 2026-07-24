"""PipelineConfig - 소비자가 주입하는 설정 dataclass"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional


@dataclass
class PipelineConfig:
    """데이터 파이프라인 설정.

    공유 패키지는 .env를 직접 읽지 않습니다.
    소비자가 이 dataclass를 생성하여 RealEstateDataPipeline.initialize(config)에 전달합니다.
    """

    api_key: str = ""
    cache_dir: str = ""
    manual_dir: str = ""
    pyoje_dir: str = ""
    staleness_days: int = 7
    auto_fetch: bool = True
    match_tolerance: float = 0.10
    max_api_calls_per_fetch: int = 60
    legacy_csv_path: str = ""
