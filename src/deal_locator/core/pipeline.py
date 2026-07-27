"""
deal_locator.core - 실거래가 데이터 파이프라인
=====================================
실거래가 API -> 표제부 매칭 -> 캐시 CSV 자동 파이프라인

데이터 흐름:
  1순위: API (PublicDataReader -> 25개 구 실거래가 + 표제부 매칭)
  2순위: 수동 CSV (manual_dir 에 배치)
  3순위: 기존 캐시 or 레거시 CSV
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Optional

import pandas as pd

from .attached_jibun import AttachedJibunIndex, fetch_attached_jibun
from .config import PipelineConfig
from .constants import API_TO_CSV_COLUMNS, SEOUL_GU_CODES, redact_secrets
from .matching import jibun_equal, masked_prefix_ok_any, norm_bunji

logger = logging.getLogger("deal_locator.data_pipeline")

# sentinel: 설정되지 않은 Path를 구분하기 위한 상수
_UNSET_PATH = Path()

# PublicDataReader 1.1+ 는 영문 원시 컬럼(sggNm…)을, 구버전(1.0.x)은 한글 컬럼을
# 반환한다. 파이프라인 전체가 한글 컬럼을 전제하므로 신버전 응답을 번역한다.
_API_EN_TO_KO: dict[str, str] = {
    "sggNm": "시군구",
    "umdNm": "법정동",
    "jibun": "지번",
    "dealAmount": "거래금액",
    "dealYear": "계약년도",
    "dealMonth": "계약월",
    "dealDay": "계약일",
    "buildYear": "건축년도",
    "buildingAr": "건물면적",
    "plottageAr": "대지면적",
    "buildingType": "건물유형",
    "buildingUse": "건물주용도",
    "landUse": "용도지역",
    "floor": "층",
    "cdealDay": "해제사유발생일",
    "dealingGbn": "거래유형",
    "estateAgentSggNm": "중개사소재지",
    "slerGbn": "매도자",
    "buyerGbn": "매수자",
}


def _translate_api_columns(df: pd.DataFrame) -> pd.DataFrame:
    """PublicDataReader 신버전(영문 컬럼) 응답 → 한글 컬럼으로 번역 (구버전은 무변경).

    거래금액은 '8,590,000' 문자열로 올 수 있어 숫자로 정규화한다.
    """
    if df.empty or "시군구" in df.columns or "sggNm" not in df.columns:
        return df
    df = df.rename(columns={k: v for k, v in _API_EN_TO_KO.items() if k in df.columns})
    if "거래금액" in df.columns:
        df["거래금액"] = pd.to_numeric(
            df["거래금액"].astype(str).str.replace(",", "", regex=False),
            errors="coerce",
        )
    return df


def _s(v: object) -> str:
    """NA-safe 문자열 변환 (pd.NA/NaN → '').

    `str(v or "")` 는 float('nan') 이 truthy 라 'nan' 문자열을 만든다 —
    그게 컬럼에 실려 도로명으로 노출되므로 이 헬퍼를 쓸 것.
    """
    if v is None or (not isinstance(v, str) and pd.isna(v)):
        return ""
    return str(v).strip()


BUILDING_TYPE_COL = "유형"
BUILDING_TYPE_KEPT = "일반"   # 통건물 (꼬마빌딩·사옥 등 한 채 통거래)


def filter_ilban(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """집합(구분상가) 거래를 걷어내고 일반(통건물)만 남긴다.

    이 MCP 의 취급 범위는 **통건물뿐**이다(제품 결정). 국토부 상업업무용 실거래는
    '유형' 컬럼으로 둘을 가른다:
      - 집합 = 건물 한 칸(구분상가). 원본에 대지면적이 없고, 표제부 역매칭도
        의미가 없다(표제부는 건물 한 채 단위 대장이다).
      - 일반 = 통건물. 대지면적이 있고 역매칭 대상이 된다.
    도시 상권은 집합 건수가 통건물보다 훨씬 많아, 섞으면 평단가 통계가 구분상가
    쪽으로 끌려간다(실측: 대치동 24개월 329건 중 집합 252 · 일반 77).

    '유형' 컬럼이 없는 입력(수동 CSV 등)은 판별 불가이므로 **거르지 않고 통과**시킨다
    — 전량 삭제가 조용히 일어나는 편이 더 위험하다.

    Returns: (통건물만 남은 DF, 제외된 집합 건수)
    """
    if df.empty or BUILDING_TYPE_COL not in df.columns:
        return df, 0
    is_ilban = df[BUILDING_TYPE_COL].map(_s) == BUILDING_TYPE_KEPT
    return df[is_ilban].copy(), int((~is_ilban).sum())


def _is_valid_dir(p: Path) -> bool:
    """Path가 의미 있는 디렉토리 경로인지 확인 (빈 Path 제외)."""
    return p != _UNSET_PATH and str(p) != ""


class RealEstateDataPipeline:
    """실거래가 -> 표제부 매칭 -> 캐시 CSV 자동 파이프라인"""

    def __init__(self) -> None:
        self._config: Optional[PipelineConfig] = None
        self._config_factory: Optional[Callable[[], PipelineConfig]] = None
        self._cache_dir: Path = _UNSET_PATH
        self._manual_dir: Path = _UNSET_PATH
        self._pyoje_dir: Path = _UNSET_PATH
        self._staleness_days: int = 7
        self._auto_fetch: bool = True
        self._match_tolerance: float = 0.10
        self._max_api_calls_per_fetch: int = 60
        self._legacy_csv: Optional[Path] = None
        self._initialized = False
        # 표제부 데이터 메모리 캐시 (구_동 키, 최대 100 엔트리)
        self._pyoje_cache: dict[str, pd.DataFrame] = {}
        self._pyoje_cache_max: int = 100
        # 부속지번 인덱스 메모리 캐시 (구_동 키)
        self._attached_cache: dict[str, AttachedJibunIndex] = {}
        # 법정동코드 매핑 캐시
        self._bdong_codes: Optional[pd.DataFrame] = None

    def set_config_factory(self, factory: Callable[[], PipelineConfig]) -> None:
        """인스턴스별 config factory 등록 (지연 초기화용)."""
        self._config_factory = factory

    def initialize(self, config: Optional[PipelineConfig] = None) -> None:
        """파이프라인 초기화: 디렉토리 생성

        Args:
            config: PipelineConfig 인스턴스.
                    None이면 인스턴스에 등록된 config_factory를 사용.
        """
        if config is None:
            if self._config_factory is not None:
                config = self._config_factory()
            else:
                raise RuntimeError(
                    "PipelineConfig이 제공되지 않았고 config_factory도 등록되지 않았습니다. "
                    "initialize(config) 또는 set_config_factory()를 먼저 호출하세요."
                )

        # [H1] 재초기화 시 캐시 클리어
        self._pyoje_cache.clear()
        self._attached_cache.clear()
        self._bdong_codes = None

        self._config = config
        self._cache_dir = Path(config.cache_dir) if config.cache_dir else _UNSET_PATH
        self._manual_dir = Path(config.manual_dir) if config.manual_dir else _UNSET_PATH
        self._pyoje_dir = Path(config.pyoje_dir) if config.pyoje_dir else _UNSET_PATH
        self._staleness_days = config.staleness_days
        self._auto_fetch = config.auto_fetch
        self._match_tolerance = config.match_tolerance
        self._max_api_calls_per_fetch = config.max_api_calls_per_fetch
        self._legacy_csv = Path(config.legacy_csv_path) if config.legacy_csv_path else None

        if _is_valid_dir(self._cache_dir):
            self._cache_dir.mkdir(parents=True, exist_ok=True)
        if _is_valid_dir(self._manual_dir):
            self._manual_dir.mkdir(parents=True, exist_ok=True)

        self._initialized = True
        logger.info(
            f"데이터 파이프라인 초기화 완료 "
            f"(캐시: {self._cache_dir}, 유효기간: {self._staleness_days}일)"
        )

    @property
    def config(self) -> Optional[PipelineConfig]:
        return self._config

    # --- Public API ---

    def ensure_fresh_data(self) -> Path:
        """메인 진입점. 신선한 매칭 CSV 경로를 반환합니다.

        캐시 확인 -> API/수동 -> 매칭 -> 저장 순서로 진행.
        """
        if not self._initialized:
            self.initialize()

        # 1. 캐시가 신선하면 그대로 사용
        cached = self.get_cached_csv_path()
        if cached and not self.is_cache_stale():
            logger.info(f"캐시 CSV 사용 (신선): {cached.name}")
            return cached

        # 2. API 자동 조회
        if self._auto_fetch:
            try:
                df_trades = self.fetch_from_api()
                if not df_trades.empty:
                    df_normalized = self.normalize_columns(df_trades, source="api")
                    df_matched = self.bulk_match(df_normalized)
                    now = datetime.now()
                    ym = now.strftime("%Y%m")
                    result_path = self.save_cache(df_matched, ym)
                    self.cleanup_cache(keep_n=3)
                    logger.info(
                        f"API 파이프라인 완료: {len(df_matched)}건 -> {result_path.name}"
                    )
                    return result_path
            except Exception as e:
                logger.warning(f"API 파이프라인 실패: {redact_secrets(e)}")

        # 3. 수동 CSV
        df_manual = self.load_manual_csv()
        if df_manual is not None and not df_manual.empty:
            try:
                df_normalized = self.normalize_columns(df_manual, source="manual")
                df_matched = self.bulk_match(df_normalized)
                now = datetime.now()
                ym = now.strftime("%Y%m")
                result_path = self.save_cache(df_matched, ym)
                self.cleanup_cache(keep_n=3)
                logger.info(
                    f"수동 CSV 파이프라인 완료: {len(df_matched)}건 -> {result_path.name}"
                )
                return result_path
            except Exception as e:
                logger.warning(f"수동 CSV 파이프라인 실패: {e}")

        # 4. 기존 캐시 (오래되었어도 사용)
        if cached and cached.exists():
            logger.info(f"오래된 캐시 CSV 사용 (폴백): {cached.name}")
            return cached

        # 5. 레거시 CSV
        if self._legacy_csv and self._legacy_csv.exists():
            logger.info(f"레거시 CSV 사용 (최후 폴백): {self._legacy_csv.name}")
            return self._legacy_csv

        logger.error(
            "모든 데이터 소스 실패: API 조회, 수동 CSV, 캐시, 레거시 CSV 모두 사용 불가. "
            "DATA_GO_KR_API_KEY 환경변수와 네트워크 연결을 확인하세요."
        )
        raise FileNotFoundError(
            "사용 가능한 실거래가 데이터가 없습니다. "
            "API 키 설정 및 네트워크 연결을 확인하세요."
        )

    def get_cached_csv_path(self) -> Optional[Path]:
        """data/cache/ 에서 가장 최신 매칭 CSV를 반환합니다."""
        if not _is_valid_dir(self._cache_dir) or not self._cache_dir.exists():
            return None
        csvs = sorted(
            self._cache_dir.glob("matched_*.csv"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        return csvs[0] if csvs else None

    def is_cache_stale(self) -> bool:
        """캐시 CSV가 staleness_days를 초과했는지 확인합니다."""
        cached = self.get_cached_csv_path()
        if not cached:
            return True
        mtime = datetime.fromtimestamp(cached.stat().st_mtime)
        age = datetime.now() - mtime
        return age > timedelta(days=self._staleness_days)

    # --- Step 1: 실거래가 취득 ---

    def fetch_from_api(
        self, year_month: str = "", gus: Optional[list] = None
    ) -> pd.DataFrame:
        """PublicDataReader -> 서울 상업업무용 매매 조회.

        이번 달 데이터가 없으면 전달로 폴백합니다.
        API 호출은 최대 max_api_calls_per_fetch 회로 제한됩니다.

        gus: 대상 구 이름 리스트(예: ["강남구"]). None 이면 25구 전체(기존 동작).
             지정 시 해당 구만 조회해 API 호출을 줄인다(하위호환).
        """
        api_key = self._get_api_key()
        if not api_key:
            return pd.DataFrame()

        try:
            from PublicDataReader import TransactionPrice
            tp = TransactionPrice(api_key)
        except Exception as e:
            logger.error(f"TransactionPrice 초기화 실패: {redact_secrets(e)}")
            return pd.DataFrame()

        now = datetime.now()
        if not year_month:
            year_month = now.strftime("%Y%m")

        # 전달 계산
        prev_month = now.month - 1 if now.month > 1 else 12
        prev_year = now.year if now.month > 1 else now.year - 1
        ym_prev = f"{prev_year}{prev_month:02d}"

        # 대상 구 선택(None=전체). 알 수 없는 구명은 무시.
        if gus:
            gu_items = [(g, SEOUL_GU_CODES[g]) for g in gus if g in SEOUL_GU_CODES]
        else:
            gu_items = list(SEOUL_GU_CODES.items())

        all_dfs: list[pd.DataFrame] = []
        api_calls = 0

        for gu_name, gu_code in gu_items:
            if api_calls >= self._max_api_calls_per_fetch:
                logger.warning(
                    f"API 호출 제한 도달 ({self._max_api_calls_per_fetch}회) "
                    f"- 나머지 구 스킵"
                )
                break

            max_retries = 2
            for attempt in range(max_retries):
                try:
                    df = tp.get_data(
                        property_type="상업업무용",
                        trade_type="매매",
                        sigungu_code=gu_code,
                        year_month=year_month,
                    )
                    api_calls += 1

                    if df is None or df.empty:
                        # 이번 달 데이터 없으면 전달 시도
                        df = tp.get_data(
                            property_type="상업업무용",
                            trade_type="매매",
                            sigungu_code=gu_code,
                            year_month=ym_prev,
                        )
                        api_calls += 1

                    if df is not None and not df.empty:
                        # PDR 신버전 영문 컬럼 → 한글 번역 (구버전 무변경)
                        df = _translate_api_columns(df)
                        # 구 이름 보존 (API는 시군구에 구 이름만 반환)
                        df["_gu_name"] = gu_name
                        all_dfs.append(df)
                        logger.debug(f"API 조회: {gu_name} - {len(df)}건")
                    break  # 성공 시 재시도 루프 탈출

                except Exception as e:
                    if attempt < max_retries - 1:
                        import time
                        time.sleep(1)
                        logger.info(f"API 재시도 ({gu_name}): {attempt + 2}/{max_retries}")
                    else:
                        logger.warning(f"API 조회 실패 ({gu_name}): {redact_secrets(e)} ({max_retries}회 시도)")
                    continue

        logger.info(f"API 조회 완료: {len(all_dfs)}개 구, API 호출 {api_calls}회")

        if not all_dfs:
            return pd.DataFrame()

        result = pd.concat(all_dfs, ignore_index=True)
        _MAX_ROWS = 500_000
        if len(result) > _MAX_ROWS:
            logger.warning(
                f"API 결과 행 수({len(result)})가 상한({_MAX_ROWS})을 초과하여 잘림"
            )
            result = result.head(_MAX_ROWS)
        return result

    def fetch_from_api_multi_month(
        self, months: int = 12, max_total_api_calls: int = 0,
        gus: Optional[list] = None,
    ) -> pd.DataFrame:
        """최근 N개월 실거래가를 API로 조회합니다.

        Args:
            months: 조회 개월 수 (기본 12개월)
            max_total_api_calls: 전체 API 호출 한도 (0=무제한)
            gus: 대상 구 이름 리스트(None=25구 전체, 기존 동작). 지정 시 해당 구만.

        Returns:
            전체 기간 합산 DataFrame
        """
        now = datetime.now()
        all_dfs: list[pd.DataFrame] = []
        success_count = 0
        fail_count = 0
        total_api_calls = 0

        for i in range(months):
            # [H5] 전체 API 호출 한도 체크
            if max_total_api_calls > 0 and total_api_calls >= max_total_api_calls:
                logger.warning(
                    f"전체 API 호출 한도 도달 ({max_total_api_calls}회) "
                    f"- {i}/{months}개월째에서 중단"
                )
                break

            # i개월 전 계산 (dateutil 사용)
            try:
                from dateutil.relativedelta import relativedelta
                target_date = now - relativedelta(months=i + 1)
            except ImportError:
                # dateutil 없으면 순차 감산 방식 폴백
                m = now.month - (i + 1)
                y = now.year
                while m <= 0:
                    m += 12
                    y -= 1
                target_date = datetime(y, m, 1)
            ym = target_date.strftime("%Y%m")

            logger.info(f"API 조회: {ym} ({i+1}/{months})")
            try:
                # 구를 하나만 지정한 경우(구·동 조회의 실제 경로)에만 월 캐시를 탄다.
                # 25구 전체 조회는 캐시 키가 달라져 재사용이 안 되므로 그대로 둔다.
                if gus and len(gus) == 1:
                    df = self.fetch_month_cached(ym, gus[0])
                else:
                    df = self.fetch_from_api(year_month=ym, gus=gus)
                # per-month 호출 수 추정 (최대 50 = 25구 * 2)
                total_api_calls += min(len(SEOUL_GU_CODES) * 2,
                                       self._max_api_calls_per_fetch)
                if not df.empty:
                    all_dfs.append(df)
                    success_count += 1
                    logger.info(f"  -> {len(df)}건")
                else:
                    success_count += 1  # 0건도 정상 응답
                    logger.info(f"  -> 0건")
            except Exception as e:
                fail_count += 1
                logger.warning(f"  -> 실패: {redact_secrets(e)}")
                continue

        # 실패율 체크
        total_attempted = success_count + fail_count
        success_rate = success_count / total_attempted if total_attempted > 0 else 0
        if fail_count > 0:
            logger.warning(
                f"API 조회 결과: {success_count}/{total_attempted}개월 성공, "
                f"{fail_count}개월 실패 (성공률 {success_rate:.0%})"
            )
        if total_attempted > 0 and success_rate < 0.8:
            logger.error(
                f"API 성공률 {success_rate:.0%} < 80%: "
                f"데이터 신뢰도가 낮을 수 있습니다. "
                f"네트워크/API 상태를 확인하세요."
            )

        if not all_dfs:
            return pd.DataFrame()

        result = pd.concat(all_dfs, ignore_index=True)
        logger.info(
            f"전체 {months}개월 API 조회 완료: {len(result)}건 "
            f"(성공 {success_count}/{total_attempted}, API ~{total_api_calls}회)"
        )
        return result

    def load_manual_csv(self) -> Optional[pd.DataFrame]:
        """manual_dir 에서 수동 다운로드 CSV를 로드합니다.

        파일 패턴: *실거래가*.csv 또는 *상업*.csv
        """
        if not _is_valid_dir(self._manual_dir) or not self._manual_dir.exists():
            return None

        csv_files = list(self._manual_dir.glob("*.csv"))
        if not csv_files:
            return None

        # 가장 최근 파일 사용
        csv_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        csv_path = csv_files[0]

        for encoding in ["utf-8-sig", "euc-kr", "cp949"]:
            try:
                df = pd.read_csv(csv_path, encoding=encoding)
                logger.info(f"수동 CSV 로드: {csv_path.name} ({len(df)}건, {encoding})")
                return df
            except (UnicodeDecodeError, UnicodeError):
                continue

        logger.warning(f"수동 CSV 인코딩 실패: {csv_path.name}")
        return None

    # --- Step 2: 컬럼 정규화 ---
    # [H3/H4] public API로 승격 (기존 _접두사 메서드는 호환용 별칭 유지)

    def normalize_columns(self, df: pd.DataFrame, source: str = "api") -> pd.DataFrame:
        """API/수동 CSV -> 통일 포맷 (매칭 CSV 기준)으로 변환합니다."""
        result = pd.DataFrame()

        if source == "api":
            # 시군구: "강남구" -> "서울특별시 강남구 논현동"
            if "시군구" in df.columns and "법정동" in df.columns:
                result["시군구"] = "서울특별시 " + df["시군구"].astype(str) + " " + df["법정동"].astype(str)
            elif "시군구" in df.columns:
                result["시군구"] = "서울특별시 " + df["시군구"].astype(str)

            # 지번: "서울특별시 구 동 번지" 형태로
            if "시군구" in df.columns and "법정동" in df.columns and "지번" in df.columns:
                result["지번"] = (
                    "서울특별시 "
                    + df["시군구"].astype(str) + " "
                    + df["법정동"].astype(str) + " "
                    + df["지번"].astype(str)
                )
            elif "지번" in df.columns:
                result["지번"] = df["지번"].astype(str)

            # 도로명 (API에는 없음 -> 빈값)
            result["도로명"] = ""

            # 거래금액: 숫자 -> 쉼표 포맷
            if "거래금액" in df.columns:
                result["거래금액(만원)"] = df["거래금액"].apply(
                    lambda x: f"{int(x):,}" if pd.notna(x) else ""
                )

            # 계약년월: 계약년도 + 계약월 -> YYYYMM
            if "계약년도" in df.columns and "계약월" in df.columns:
                result["계약년월"] = (
                    df["계약년도"].astype(str)
                    + df["계약월"].apply(lambda x: f"{int(x):02d}" if pd.notna(x) else "00")
                )

            # 직접 매핑 컬럼
            for api_col, csv_col in API_TO_CSV_COLUMNS.items():
                if api_col in df.columns:
                    result[csv_col] = df[api_col]

            # NO 컬럼 (순번)
            result["NO"] = range(1, len(df) + 1)

            # 도로조건, 지분구분 (API에 없음)
            result["도로조건"] = ""
            result["지분구분"] = ""

        elif source == "manual":
            # 수동 CSV는 이미 기존 포맷일 가능성이 높음
            required = ["시군구", "지번", "거래금액(만원)"]
            if all(col in df.columns for col in required):
                result = df.copy()
            else:
                # 컬럼명 매핑 시도 (data.go.kr 다운로드 CSV)
                rename_map = {}
                if "거래금액" in df.columns and "거래금액(만원)" not in df.columns:
                    rename_map["거래금액"] = "거래금액(만원)"
                if rename_map:
                    result = df.rename(columns=rename_map)
                else:
                    result = df.copy()

                # [M2] 매핑 후에도 필수 컬럼 누락 시 빈 DataFrame 반환
                missing = [c for c in required if c not in result.columns]
                if missing:
                    logger.error(
                        f"수동 CSV 필수 컬럼 누락: {missing}. "
                        f"사용 가능 컬럼: {list(result.columns[:10])}..."
                    )
                    return pd.DataFrame()

        # 표제부 매칭 결과 컬럼 초기화
        for col in ["대지위치_표제부", "도로명대지위치_표제부", "매칭단계"]:
            if col not in result.columns:
                result[col] = ""

        return result

    # 호환용 별칭
    def _normalize_columns(self, df: pd.DataFrame, source: str = "api") -> pd.DataFrame:
        return self.normalize_columns(df, source)

    # --- Step 3: 3단계 표제부 매칭 ---

    def bulk_match(self, df_trades: pd.DataFrame) -> pd.DataFrame:
        """전체 거래 데이터에 표제부 매칭을 수행합니다.

        마스킹 지번(**) 복원:
          Stage 1: 정확매칭 (연면적+대지면적+건축년도)
          Stage 2: 건축년도+연면적
          Stage 3: +-tolerance 오차범위
        """
        df = df_trades.copy()

        # 매칭 키 숫자 변환
        df["_연면적"] = pd.to_numeric(df.get("전용/연면적(㎡)"), errors="coerce")
        df["_대지면적"] = pd.to_numeric(df.get("대지면적(㎡)"), errors="coerce")
        df["_건축년도"] = pd.to_numeric(df.get("건축년도"), errors="coerce")
        # 계측: 마스킹 역매칭 실패 사유(성공/비마스킹 행은 "")
        if "역매칭실패사유" not in df.columns:
            df["역매칭실패사유"] = ""

        # 구별로 표제부 로드 & 매칭 (1차: API, 2차: CSV 폴백)
        matched_count = 0
        total_masked = 0
        jibun_road_count = 0  # 비마스킹 지번매칭으로 도로명 확보한 건수
        unmatched_indices: list[tuple[int, str, str]] = []  # (idx, gu, dong)

        total_rows = len(df)
        log_interval = max(total_rows // 10, 1)  # 10% 단위 로그

        for i, (idx, row) in enumerate(df.iterrows()):
            if i > 0 and i % log_interval == 0:
                logger.info(f"표제부 매칭 진행: {i}/{total_rows} ({i/total_rows:.0%})")

            jibun = str(row.get("지번", ""))
            masked = "*" in jibun

            # 시군구에서 구/동 추출 (마스킹·비마스킹 공통)
            sigungu = str(row.get("시군구", ""))
            gu_match = re.search(r"(\S+구)\s*(\S+[동가])?", sigungu)
            if not gu_match:
                if masked:
                    df.at[idx, "역매칭실패사유"] = "시군구_파싱실패"
                continue
            gu_name = gu_match.group(1)
            dong_name = gu_match.group(2) or ""

            # P1: 비마스킹 거래 — 지번이 이미 공개돼 있으므로 '복원'이 아니라
            # 지번 정확일치로 표제부의 도로명/대지위치 필드만 조회해 채운다.
            # (실거래 API는 도로명을 주지 않아, 안 채우면 도로명 필터에서 통째로 누락됨.)
            if not masked:
                hit = self._lookup_pyoje_by_jibun(gu_name, dong_name, jibun)
                if hit and hit["road_address"]:
                    df.at[idx, "대지위치_표제부"] = hit["address"]
                    df.at[idx, "도로명대지위치_표제부"] = hit["road_address"]
                    df.at[idx, "매칭단계"] = "지번매칭(비마스킹)"
                    jibun_road_count += 1
                elif hit:
                    # 지번은 표제부에 있으나 도로명 필드가 비어 있음
                    if hit["address"]:
                        df.at[idx, "대지위치_표제부"] = hit["address"]
                    df.at[idx, "역매칭실패사유"] = "비마스킹_표제부도로명공란"
                else:
                    df.at[idx, "역매칭실패사유"] = "비마스킹_표제부지번없음"
                continue

            total_masked += 1

            building_area = row.get("_연면적")
            land_area = row.get("_대지면적")
            build_year = row.get("_건축년도")

            # 연면적 결측은 하드컷(평단가·스펙매칭 불가). 건축년도 결측은 컷하지
            # 않고 match_single 의 P3(연면적+대지 동시 exact & 후보 유일)로만 회복.
            if pd.isna(building_area):
                df.at[idx, "역매칭실패사유"] = "연면적_결측"
                continue

            # 1차: API 우선 표제부 조회 (+ 부속지번 주석: 다필지/필지세트)
            df_pyoje = self._load_pyoje_data(gu_name, dong_name)
            if df_pyoje.empty:
                df.at[idx, "역매칭실패사유"] = "표제부_없음"
                unmatched_indices.append((idx, gu_name, dong_name))
                continue
            df_pyoje = self._annotate_attached(df_pyoje, gu_name, dong_name)

            reasons: list[str] = []
            match_result = self.match_single(
                df_pyoje, building_area, land_area, build_year,
                masked_jibun=jibun, reason_out=reasons,
            )
            if match_result:
                df.at[idx, "대지위치_표제부"] = match_result["address"]
                df.at[idx, "도로명대지위치_표제부"] = match_result["road_address"]
                df.at[idx, "매칭단계"] = match_result["match_stage"]
                df.at[idx, "역매칭실패사유"] = ""
                matched_count += 1
            else:
                df.at[idx, "역매칭실패사유"] = reasons[0] if reasons else "미분류"
                unmatched_indices.append((idx, gu_name, dong_name))

        # 2차: 미매칭 건에 대해 CSV 폴백 (API 데이터가 불완전할 수 있으므로)
        if unmatched_indices:
            csv_retry_count = 0
            for idx, gu_name, dong_name in unmatched_indices:
                row = df.loc[idx]
                building_area = row.get("_연면적")
                land_area = row.get("_대지면적")
                build_year = row.get("_건축년도")
                if pd.isna(building_area) or pd.isna(build_year):
                    continue

                # CSV는 구 단위 전체 데이터 (API보다 완전)
                df_pyoje_csv = self.load_pyoje_csv(gu_name)
                if df_pyoje_csv.empty:
                    continue

                match_result = self.match_single(
                    df_pyoje_csv, building_area, land_area, build_year,
                    masked_jibun=str(row.get("지번", "")),
                )
                if match_result:
                    df.at[idx, "대지위치_표제부"] = match_result["address"]
                    df.at[idx, "도로명대지위치_표제부"] = match_result["road_address"]
                    df.at[idx, "매칭단계"] = match_result["match_stage"] + " (CSV폴백)"
                    df.at[idx, "역매칭실패사유"] = ""
                    matched_count += 1
                    csv_retry_count += 1

            if csv_retry_count > 0:
                logger.info(f"CSV 폴백 매칭: {csv_retry_count}건 추가 복원")

        # 임시 컬럼 제거
        df = df.drop(columns=["_연면적", "_대지면적", "_건축년도"], errors="ignore")

        if total_masked > 0:
            pct = matched_count / total_masked * 100
            logger.info(
                f"표제부 매칭 완료: 마스킹 {total_masked}건 중 {matched_count}건 복원 "
                f"({pct:.0f}%)"
            )
        else:
            logger.info("표제부 매칭: 마스킹 지번 없음")
        if jibun_road_count > 0:
            logger.info(f"비마스킹 지번매칭: {jibun_road_count}건 도로명 확보")

        return df

    # 호환용 별칭
    def _bulk_match(self, df_trades: pd.DataFrame) -> pd.DataFrame:
        return self.bulk_match(df_trades)

    def _lookup_pyoje_by_jibun(
        self, gu_name: str, dong_name: str, jibun_full: str,
    ) -> Optional[dict[str, str]]:
        """비마스킹 지번을 표제부와 '지번 정확일치'로 조회해 도로명/대지위치를 얻는다.

        마스킹 스펙 역매칭과 달리 지번이 이미 있으므로 퍼지 없이 exact-jibun 으로만
        매칭한다(오매칭 위험 최소). 대표지번 일치 또는 부속지번(_필지세트) 포함이면 채택.
        표제부에 도로명이 비어 있으면 road_address 도 빈 문자열로 반환한다.
        지번 미발견 시 None.
        """
        toks = str(jibun_full).strip().split()
        bunji = norm_bunji(toks[-1]) if toks else ""
        if not bunji:
            return None
        df_pyoje = self._load_pyoje_data(gu_name, dong_name)
        if df_pyoje.empty:
            return None
        df_pyoje = self._annotate_attached(df_pyoje, gu_name, dong_name)
        if "_지번" not in df_pyoje.columns:
            return None

        rep = df_pyoje["_지번"].map(lambda b: bool(b) and jibun_equal(str(b), bunji))
        hits = df_pyoje[rep]
        if hits.empty and "_필지세트" in df_pyoje.columns:
            inset = df_pyoje["_필지세트"].map(
                lambda lots: any(jibun_equal(str(x), bunji) for x in (lots or []))
            )
            hits = df_pyoje[inset]
        if hits.empty:
            return None

        r = hits.iloc[0]
        return {
            "address": re.sub(r"번지$", "", _s(r.get("대지위치"))).strip(),
            "road_address": _s(r.get("도로명대지위치")),
        }

    def _annotate_attached(
        self, df_pyoje: pd.DataFrame, gu_name: str, dong_name: str,
    ) -> pd.DataFrame:
        """표제부 DF에 부속지번 파생 컬럼을 1회 주입합니다 (멱등).

        _지번: 대지위치 말미 번지 / _다필지: 부속지번 대장 기준 합필 여부 /
        _필지세트: 대표+부속 지번 리스트 (마스킹 접두 프리필터용).
        인덱스 로드 실패 시 보수적 기본값(_다필지=False)으로 채워 흐름을 막지 않는다.
        """
        if df_pyoje.empty or "_다필지" in df_pyoje.columns or "대지위치" not in df_pyoje.columns:
            return df_pyoje

        bunji = (
            df_pyoje["대지위치"].astype(str).str.strip()
            .str.extract(r"(\d+(?:-\d+)?)(?:번지)?$", expand=False)
            .fillna("")
        )
        idx: Optional[AttachedJibunIndex] = None
        if dong_name:
            try:
                idx = self.load_attached_index(gu_name, dong_name)
            except Exception as e:  # noqa: BLE001 — 보강 데이터 실패는 치명 아님
                logger.warning(f"부속지번 주석 실패({gu_name} {dong_name}): {redact_secrets(e)}")

        df_pyoje["_지번"] = bunji
        if idx is not None and len(idx) > 0:
            df_pyoje["_다필지"] = bunji.map(lambda b: bool(b) and idx.is_multi_lot(b))
            df_pyoje["_필지세트"] = bunji.map(
                lambda b: sorted(idx.lot_set(b)) if b else []
            )
        else:
            df_pyoje["_다필지"] = False
            df_pyoje["_필지세트"] = bunji.map(lambda b: [b] if b else [])
        return df_pyoje

    def match_single(
        self,
        df_pyoje: pd.DataFrame,
        building_area: float,
        land_area: float,
        build_year: float,
        masked_jibun: str = "",
        reason_out: Optional[list[str]] = None,
    ) -> Optional[dict[str, str]]:
        """단일 거래 건에 대해 3단계 표제부 매칭을 수행합니다.

        masked_jibun: 거래 행의 마스킹 지번('3**'). _필지세트 주석이 있으면
        접두 호환 필지세트를 가진 표제부 행만 후보로 남겨 오매칭을 줄인다.
        reason_out: 계측용 out-param(선택). None 을 반환할 때 그 사유코드를
            append 한다 — 매칭 결과 자체는 불변(비침습). 사유코드:
            프리필터_탈락 / 건축년도_불일치 / 스펙_오차밖.
        """
        tol = self._match_tolerance

        # Stage 0: 마스킹 접두 프리필터 (부속지번 필지세트 기준, 주석 있을 때만)
        if masked_jibun and "*" in masked_jibun and "_필지세트" in df_pyoje.columns:
            keep = df_pyoje["_필지세트"].map(
                lambda lots: masked_prefix_ok_any(masked_jibun, lots or [])
            )
            df_pyoje = df_pyoje[keep]
            if df_pyoje.empty:
                if reason_out is not None:
                    reason_out.append("프리필터_탈락")
                return None

        # P3: 건축년도 결측 — 연면적+대지면적 동시 정확일치 & 후보 유일일 때만
        # 추정매칭 복원(유일성 가드). 2개 이상이면 임의선택 금지 → 포기.
        if pd.isna(build_year):
            if not pd.isna(land_area):
                p3 = df_pyoje[
                    (df_pyoje["연면적_float"] == building_area)
                    & (df_pyoje["대지면적_float"] == land_area)
                ]
                if len(p3) == 1:
                    return self._extract_match(
                        p3.iloc[0],
                        "추정매칭: 연면적+대지 정확 (건축년도 결측·유일후보)",
                    )
            if reason_out is not None:
                reason_out.append("건축년도_결측")
            return None

        # Stage 1: 정확매칭 (연면적 + 대지면적 + 건축년도)
        if not pd.isna(land_area):
            exact = df_pyoje[
                (df_pyoje["건축년도_매칭"] == build_year)
                & (df_pyoje["연면적_float"] == building_area)
                & (df_pyoje["대지면적_float"] == land_area)
            ]
            if len(exact) > 0:
                return self._extract_match(exact.iloc[0], "1단계: 정확매칭")

        # Stage 2: 건축년도 + 연면적
        step2 = df_pyoje[
            (df_pyoje["건축년도_매칭"] == build_year)
            & (df_pyoje["연면적_float"] == building_area)
        ]
        if len(step2) > 0:
            return self._extract_match(step2.iloc[0], "2단계: 건축년도+연면적")

        # Stage 3: 오차범위 +-tolerance
        if not pd.isna(land_area):
            step3 = df_pyoje[
                (df_pyoje["건축년도_매칭"] == build_year)
                & (df_pyoje["연면적_float"] >= building_area * (1 - tol))
                & (df_pyoje["연면적_float"] <= building_area * (1 + tol))
                & (df_pyoje["대지면적_float"] >= land_area * (1 - tol))
                & (df_pyoje["대지면적_float"] <= land_area * (1 + tol))
            ]
            if len(step3) > 0:
                step3 = step3.copy()
                step3["_diff"] = (
                    abs(step3["연면적_float"] - building_area) / max(building_area, 1e-6)
                    + abs(step3["대지면적_float"] - land_area) / max(land_area, 1e-6)
                )
                best = step3.sort_values("_diff").iloc[0]
                return self._extract_match(best, f"3단계: 오차범위+-{tol*100:.0f}%")

        # Stage 3-합필: 다필지(합필) 대지는 거래 대지면적이 필지 구성에 따라
        # 흔들리므로 대지면적 범위 검사를 생략(건축년도+연면적 오차만).
        # 부속지번 대장(_다필지 주석) 기반 — 단일 필지는 여전히 엄격.
        if "_다필지" in df_pyoje.columns:
            step3m = df_pyoje[
                df_pyoje["_다필지"].fillna(False).astype(bool)
                & (df_pyoje["건축년도_매칭"] == build_year)
                & (df_pyoje["연면적_float"] >= building_area * (1 - tol))
                & (df_pyoje["연면적_float"] <= building_area * (1 + tol))
            ]
            if len(step3m) > 0:
                step3m = step3m.copy()
                step3m["_diff"] = (
                    abs(step3m["연면적_float"] - building_area) / max(building_area, 1e-6)
                )
                best = step3m.sort_values("_diff").iloc[0]
                return self._extract_match(
                    best, f"3단계: 오차범위+-{tol*100:.0f}% (합필지·대지면적 생략)"
                )

        # P4: 연면적 ±tol 근접 + 건축년도 일치 + 대지면적 완화(비대칭) — 후보가
        # 정확히 1개일 때만 복원(유일성 가드). 가각전제·도로편입 등으로 건축대지가
        # 실거래 대지와 한방향으로 어긋난 필지를, 연면적 근접+년도 일치가 유일하면
        # 대지 검사 없이 추정매칭 복원. 유일 아니면 아래 사유 기록으로.
        if not pd.isna(land_area):
            p4 = df_pyoje[
                (df_pyoje["건축년도_매칭"] == build_year)
                & (df_pyoje["연면적_float"] >= building_area * (1 - tol))
                & (df_pyoje["연면적_float"] <= building_area * (1 + tol))
            ]
            if len(p4) == 1:
                return self._extract_match(
                    p4.iloc[0],
                    f"추정매칭: 연면적±{tol*100:.0f}%·대지완화 (유일후보)",
                )

        if reason_out is not None:
            # 후보 풀에 '건축년도 일치' 표제부가 아예 없었으면 년도 게이트에서 전멸,
            # 있었는데도 여기까지 왔으면 연면적/대지 오차범위 밖.
            if "건축년도_매칭" in df_pyoje.columns:
                year_hits = len(df_pyoje[df_pyoje["건축년도_매칭"] == build_year])
            else:
                year_hits = 0
            reason_out.append("건축년도_불일치" if year_hits == 0 else "스펙_오차밖")
        return None

    # 호환용 별칭
    def _match_single(
        self, df_pyoje: pd.DataFrame,
        building_area: float, land_area: float, build_year: float,
        masked_jibun: str = "",
    ) -> Optional[dict[str, str]]:
        return self.match_single(
            df_pyoje, building_area, land_area, build_year, masked_jibun=masked_jibun
        )

    @staticmethod
    def _extract_match(row: pd.Series, stage: str) -> dict[str, str]:
        """표제부 매칭 결과에서 주소를 추출합니다."""
        daeji = _s(row.get("대지위치"))
        road = _s(row.get("도로명대지위치"))

        address = re.sub(r"번지$", "", daeji).strip()
        road_clean = road.strip()

        return {
            "address": address,
            "road_address": road_clean,
            "match_stage": stage,
        }

    def _evict_pyoje_cache(self) -> None:
        """캐시가 최대 크기를 초과하면 가장 오래된 엔트리를 제거합니다."""
        # [M5] > 로 변경: 100개까지 허용, 101개째에서 evict
        while len(self._pyoje_cache) > self._pyoje_cache_max:
            oldest_key = next(iter(self._pyoje_cache))
            del self._pyoje_cache[oldest_key]

    # 최근 N개월은 신고가 계속 들어와 값이 변한다(계약 후 30일 이내 신고 의무).
    # 그 이전 월은 해제신고 정정 정도만 생기므로 오래 캐시해도 안전하다.
    _VOLATILE_MONTHS = 2
    _FRESH_TTL_HOURS = 6      # 최근 월
    _STALE_TTL_DAYS = 30      # 지난 월

    def _deals_disk_path(self, gu: str, year_month: str) -> Optional[Path]:
        """월·구 단위 실거래 디스크 캐시 경로 (cache_dir 미설정 시 None)."""
        if not _is_valid_dir(self._cache_dir):
            return None
        return self._cache_dir / "deals" / f"{gu}_{year_month}.csv"

    def _deals_cache_fresh(self, path: Path, year_month: str) -> bool:
        """캐시가 아직 쓸 만한지. 최근 월은 짧게, 지난 월은 길게 본다."""
        try:
            age = datetime.now() - datetime.fromtimestamp(path.stat().st_mtime)
        except OSError:
            return False
        now = datetime.now()
        try:
            y, m = int(year_month[:4]), int(year_month[4:6])
        except (ValueError, IndexError):
            return False
        months_ago = (now.year - y) * 12 + (now.month - m)
        if months_ago <= self._VOLATILE_MONTHS:
            return age.total_seconds() < self._FRESH_TTL_HOURS * 3600
        return age.days < self._STALE_TTL_DAYS

    def fetch_month_cached(self, year_month: str, gu: str) -> pd.DataFrame:
        """한 달치 실거래를 디스크 캐시 경유로 가져온다.

        월별 API 호출이 3~4초라 12개월이면 45초가 걸렸다 — 구·동 조회 첫 응답이
        1분 가까이 걸리던 원인이다. 월 단위 결과는 (최근 월을 빼면) 사실상 불변이라
        디스크에 두면 재조회가 즉시 끝난다.

        캐시 쓰기/읽기 실패는 삼킨다 — 캐시는 최적화지 정확성의 근거가 아니다.
        """
        path = self._deals_disk_path(gu, year_month)
        if path and path.exists() and self._deals_cache_fresh(path, year_month):
            try:
                return pd.read_csv(path, dtype=str)
            except Exception as e:  # noqa: BLE001
                logger.warning(f"실거래 캐시 읽기 실패({path.name}): {e}")

        df = self.fetch_from_api(year_month=year_month, gus=[gu])
        if path is not None:
            try:
                path.parent.mkdir(parents=True, exist_ok=True)
                df.to_csv(path, index=False)
            except Exception as e:  # noqa: BLE001
                logger.warning(f"실거래 캐시 쓰기 실패({path.name}): {e}")
        return df

    def _pyoje_disk_path(self, cache_key: str) -> Optional[Path]:
        """동 단위 표제부 디스크 캐시 경로 (cache_dir 미설정 시 None)."""
        if not _is_valid_dir(self._cache_dir):
            return None
        return self._cache_dir / "pyoje" / f"{cache_key}.csv"

    def _load_pyoje_data(self, gu_name: str, dong_name: str = "") -> pd.DataFrame:
        """표제부 데이터를 로드합니다 (디스크 캐시 → API → 만료 캐시 → CSV 폴백).

        동 단위 조회는 페이지가 많아(큰 동 80페이지+) 디스크 CSV 로 캐시한다
        (staleness_days 적용). API 가 중간 타임아웃으로 실패하면 만료 캐시라도
        폴백해 앵커 전멸을 막는다.

        Args:
            gu_name: 구 이름 (예: "강남구")
            dong_name: 동 이름 (예: "논현동") - API 조회 시 사용
        """
        cache_key = f"{gu_name}_{dong_name}" if dong_name else gu_name
        if cache_key in self._pyoje_cache:
            return self._pyoje_cache[cache_key]

        disk = self._pyoje_disk_path(cache_key) if dong_name else None

        # 0순위: 신선한 디스크 캐시
        if disk and disk.exists():
            age = datetime.now() - datetime.fromtimestamp(disk.stat().st_mtime)
            if age.days <= self._staleness_days:
                try:
                    df = pd.read_csv(disk, encoding="utf-8-sig", low_memory=False)
                    if not df.empty:
                        self._evict_pyoje_cache()
                        self._pyoje_cache[cache_key] = df
                        return df
                except Exception as e:  # noqa: BLE001
                    logger.warning(f"표제부 디스크 캐시 로드 실패({disk.name}): {e}")

        # 1순위: BuildingLedger API
        if dong_name:
            df_api = self._fetch_pyoje_api(gu_name, dong_name)
            if not df_api.empty:
                # 오류로 잘린 부분수신본은 디스크에 굳히지 않는다(다음 조회 때 재시도).
                # 이걸 저장하면 잘린 앵커풀이 staleness 기간 내내 조용히 복원율을 떨어뜨린다.
                if disk and df_api.attrs.get("complete", True):
                    try:
                        disk.parent.mkdir(parents=True, exist_ok=True)
                        df_api.to_csv(disk, index=False, encoding="utf-8-sig")
                    except Exception as e:  # noqa: BLE001
                        logger.warning(f"표제부 디스크 캐시 저장 실패: {e}")
                elif disk:
                    logger.warning(
                        f"표제부 부분수신({cache_key}) — 디스크 캐시 저장 건너뜀"
                    )
                self._evict_pyoje_cache()
                self._pyoje_cache[cache_key] = df_api
                return df_api

            # API 실패 → 만료됐어도 디스크 캐시 폴백
            if disk and disk.exists():
                try:
                    df = pd.read_csv(disk, encoding="utf-8-sig", low_memory=False)
                    if not df.empty:
                        logger.info(f"표제부: API 실패 → 만료 캐시 사용 ({cache_key})")
                        self._evict_pyoje_cache()
                        self._pyoje_cache[cache_key] = df
                        return df
                except Exception:  # noqa: BLE001
                    pass

        # 2순위: 로컬 CSV (구 단위 전체)
        # 구 단위 캐시가 있으면 사용
        if gu_name in self._pyoje_cache:
            return self._pyoje_cache[gu_name]

        df_csv = self.load_pyoje_csv(gu_name)
        if not df_csv.empty:
            return df_csv

        return pd.DataFrame()

    def load_attached_index(self, gu_name: str, dong_name: str) -> AttachedJibunIndex:
        """부속지번 인덱스를 로드합니다 (메모리 → 디스크 JSON → API, 항상 성공).

        다필지(합필) 대지 매칭 정밀화용. API/키 실패 시 빈 인덱스를 반환해
        기존 매칭 흐름을 막지 않는다(부속지번은 보강 데이터).
        디스크 캐시: cache_dir/attached_jibun/{구}_{동}.json (staleness_days 적용,
        API 실패 시 만료 캐시라도 폴백).
        """
        cache_key = f"{gu_name}_{dong_name}"
        if cache_key in self._attached_cache:
            return self._attached_cache[cache_key]

        disk_path: Optional[Path] = None
        if _is_valid_dir(self._cache_dir):
            disk_path = self._cache_dir / "attached_jibun" / f"{cache_key}.json"

        # 1순위: 신선한 디스크 캐시
        if disk_path and disk_path.exists():
            age_days = (
                datetime.now()
                - datetime.fromtimestamp(disk_path.stat().st_mtime)
            ).days
            if age_days <= self._staleness_days:
                try:
                    idx = AttachedJibunIndex.load_json(disk_path)
                    self._attached_cache[cache_key] = idx
                    return idx
                except Exception as e:  # noqa: BLE001
                    logger.warning(f"부속지번 캐시 파싱 실패({disk_path.name}): {e}")

        # 2순위: API 조회
        sigungu_code = SEOUL_GU_CODES.get(gu_name, "")
        bdong_code = self._get_bdong_code(gu_name, dong_name)
        pairs = fetch_attached_jibun(self._get_api_key(), sigungu_code, bdong_code)
        if pairs is not None:
            idx = AttachedJibunIndex(pairs)
            if disk_path:
                try:
                    idx.save_json(disk_path)
                except Exception as e:  # noqa: BLE001
                    logger.warning(f"부속지번 캐시 저장 실패: {e}")
            self._attached_cache[cache_key] = idx
            logger.info(
                f"부속지번 인덱스: {gu_name} {dong_name} - 다필지 대지 {len(idx)}건"
            )
            return idx

        # 3순위: 만료됐어도 디스크 캐시 폴백, 그마저 없으면 빈 인덱스
        if disk_path and disk_path.exists():
            try:
                idx = AttachedJibunIndex.load_json(disk_path)
                self._attached_cache[cache_key] = idx
                logger.info(f"부속지번: API 실패 → 만료 캐시 사용 ({cache_key})")
                return idx
            except Exception:  # noqa: BLE001
                pass
        idx = AttachedJibunIndex()
        self._attached_cache[cache_key] = idx
        return idx

    def _fetch_pyoje_api(self, gu_name: str, dong_name: str) -> pd.DataFrame:
        """BuildingLedger API로 표제부를 조회합니다."""
        api_key = self._get_api_key()
        if not api_key:
            return pd.DataFrame()

        sigungu_code = SEOUL_GU_CODES.get(gu_name, "")
        if not sigungu_code:
            return pd.DataFrame()

        # 법정동코드 조회
        bdong_code = self._get_bdong_code(gu_name, dong_name)
        if not bdong_code:
            logger.debug(f"법정동코드 없음: {gu_name} {dong_name}")
            return pd.DataFrame()

        try:
            import requests
            import xmltodict
            from urllib.parse import unquote

            decoded_key = unquote(api_key)
            url = "http://apis.data.go.kr/1613000/BldRgstHubService/getBrTitleInfo"

            all_items: list[dict] = []
            page = 1
            # 큰 동(구로동 표제부 4천+건)은 20페이지(2000건)로는 잘려서
            # 뒤쪽 지번의 앵커 조회가 조용히 실패한다 → 상한 확대 + 절단 경고.
            max_pages = 100  # 최대 100페이지 (10,000건)
            total_count = 0
            fetch_complete = True  # 오류로 중단되면 False — 부분수신본 캐시 방지

            while page <= max_pages:
                params = {
                    "serviceKey": decoded_key,
                    "sigunguCd": sigungu_code,
                    "bjdongCd": bdong_code,
                    "numOfRows": "100",
                    "pageNo": str(page),
                }
                # 페이지 단위 재시도 — 큰 동은 80페이지+ 라 중간 타임아웃/5xx 1회로
                # 전체를 버리면 앵커가 전멸한다. HTTP 5xx·429 는 data.go.kr 의
                # 일시 장애이므로 지수 백오프로 재시도한다(비200 즉시 포기 금지).
                import time as _time
                resp = None
                for attempt in range(3):
                    try:
                        resp = requests.get(url, params=params, timeout=15)
                    except requests.RequestException as e:
                        resp = None
                        if attempt < 2:
                            _time.sleep(2 ** attempt)
                            continue
                        logger.warning(
                            f"표제부 API 페이지 {page} 실패(3회): {redact_secrets(e)}"
                        )
                        break
                    if resp.status_code in (429, 500, 502, 503, 504) and attempt < 2:
                        _time.sleep(2 ** attempt)
                        continue
                    break
                if resp is None:
                    fetch_complete = False
                    break

                if resp.status_code != 200:
                    logger.warning(
                        f"표제부 API HTTP {resp.status_code} (재시도 후): "
                        f"{gu_name} {dong_name}"
                    )
                    fetch_complete = False
                    break

                try:
                    data = xmltodict.parse(
                        resp.text, disable_entities=True, process_namespaces=False
                    )
                except Exception:
                    logger.warning(f"표제부 API XML 파싱 실패: {gu_name} {dong_name}")
                    fetch_complete = False
                    break

                header = data.get("response", {}).get("header", {})
                if header.get("resultCode") != "00":
                    logger.warning(
                        f"표제부 API 에러: {header.get('resultMsg')}"
                    )
                    fetch_complete = False
                    break

                body = data.get("response", {}).get("body", {})
                items = body.get("items", {}).get("item", [])
                if not items:
                    break
                if isinstance(items, dict):
                    items = [items]

                all_items.extend(items)

                total_count = int(body.get("totalCount", 0))
                if page * 100 >= total_count:
                    break
                page += 1

            if total_count > max_pages * 100:
                logger.warning(
                    f"표제부 절단: {gu_name} {dong_name} 전체 {total_count}건 중 "
                    f"{max_pages * 100}건만 수신 — 뒤쪽 지번은 앵커 실패 가능"
                )

            if not all_items:
                return pd.DataFrame()

            # API 응답 -> 매칭용 DataFrame 변환
            df = pd.DataFrame(all_items)
            result = pd.DataFrame()
            result["대지위치"] = df.get("platPlc", "")
            result["도로명대지위치"] = df.get("newPlatPlc", "")
            result["연면적(㎡)"] = pd.to_numeric(df.get("totArea"), errors="coerce")
            result["대지면적(㎡)"] = pd.to_numeric(df.get("platArea"), errors="coerce")
            result["사용승인일"] = df.get("useAprDay", "")

            # 매칭용 파생 컬럼
            result["건축년도_매칭"] = pd.to_numeric(
                result["사용승인일"].astype(str).str[:4], errors="coerce"
            )
            result["연면적_float"] = result["연면적(㎡)"]
            result["대지면적_float"] = result["대지면적(㎡)"]
            result = result.dropna(subset=["건축년도_매칭", "연면적_float"])

            logger.info(
                f"표제부 API 조회: {gu_name} {dong_name} - {len(result)}건 "
                f"(페이지 {page})"
            )
            # 오류로 중단된 부분수신본은 캐시하지 말라는 신호(_load_pyoje_data 가 확인)
            result.attrs["complete"] = fetch_complete
            return result

        except Exception as e:
            logger.warning(f"표제부 API 조회 실패 ({gu_name} {dong_name}): {redact_secrets(e)}")
            return pd.DataFrame()

    def get_bdong_code(self, gu_name: str, dong_name: str) -> str:
        """법정동 코드(5자리) 조회 공개 API (_get_bdong_code 위임).

        조회 실패·미등재는 빈 문자열(예외 없음). MCP 의 주소 해석 도구처럼
        표제부·실거래를 거치지 않고 코드만 필요한 소비자를 위한 진입점.
        """
        return self._get_bdong_code(gu_name, dong_name)

    def _get_bdong_code(self, gu_name: str, dong_name: str) -> str:
        """법정동 코드를 조회합니다 (PublicDataReader code_bdong 활용)."""
        try:
            if self._bdong_codes is None:
                import PublicDataReader as pdr
                self._bdong_codes = pdr.code_bdong()

            df = self._bdong_codes
            # 서울 + 해당 구 + 말소되지 않은 것
            mask = (
                (df["시도명"] == "서울특별시")
                & (df["시군구명"] == gu_name)
                & (df["읍면동명"] == dong_name)
                & (df["말소일자"].isna() | (df["말소일자"] == ""))
            )
            matches = df[mask]
            if matches.empty:
                return ""

            # 법정동코드에서 시군구(5자리) 이후 5자리 추출
            full_code = str(matches.iloc[0]["법정동코드"])
            # 전체 10자리 중 앞 5자리=시군구, 뒤 5자리=법정동
            bdong_code = full_code[5:10] if len(full_code) >= 10 else ""
            return bdong_code

        except Exception as e:
            logger.debug(f"법정동코드 조회 실패 ({gu_name} {dong_name}): {e}")
            return ""

    def load_pyoje_data(self, gu_name: str, dong_name: str = "") -> pd.DataFrame:
        """표제부 로드 공개 API (API 우선 → CSV 폴백, _load_pyoje_data 위임).

        dong_name 을 주면 건축HUB API 동 단위 조회를 우선 시도한다.
        pyoje_dir CSV 가 없는 환경(예: 경로 이전)에서도 앵커가 동작한다.
        """
        return self._load_pyoje_data(gu_name, dong_name)

    def load_pyoje_csv(self, gu_name: str) -> pd.DataFrame:
        """구별 건축물대장 표제부 CSV를 로드합니다 (캐시 사용)."""
        if gu_name in self._pyoje_cache:
            return self._pyoje_cache[gu_name]

        # path traversal 방지: SEOUL_GU_CODES에 없는 구 이름 거부
        if gu_name not in SEOUL_GU_CODES:
            logger.warning(f"알 수 없는 구 이름 (표제부 로드 스킵): {gu_name}")
            return pd.DataFrame()

        if not _is_valid_dir(self._pyoje_dir) or not self._pyoje_dir.exists():
            return pd.DataFrame()

        matches = list(self._pyoje_dir.glob(f"*{gu_name}*"))
        if not matches:
            return pd.DataFrame()

        csv_path = matches[0]
        try:
            df = pd.read_csv(csv_path, encoding="utf-8-sig", low_memory=False)

            cols_needed = ["대지위치", "도로명대지위치", "연면적(㎡)", "대지면적(㎡)", "사용승인일"]
            for c in cols_needed:
                if c not in df.columns:
                    return pd.DataFrame()

            df = df[cols_needed].copy()
            df["건축년도_매칭"] = pd.to_numeric(
                df["사용승인일"].astype(str).str[:4], errors="coerce"
            )
            df["연면적_float"] = pd.to_numeric(df["연면적(㎡)"], errors="coerce")
            df["대지면적_float"] = pd.to_numeric(df["대지면적(㎡)"], errors="coerce")
            df = df.dropna(subset=["건축년도_매칭", "연면적_float"])

            self._evict_pyoje_cache()
            self._pyoje_cache[gu_name] = df
            logger.debug(f"표제부 CSV 로드: {gu_name} - {len(df)}건")
            return df

        except Exception as e:
            logger.error(f"표제부 CSV 로드 실패 ({gu_name}): {e}")
            return pd.DataFrame()

    # 호환용 별칭
    def _load_pyoje_csv(self, gu_name: str) -> pd.DataFrame:
        return self.load_pyoje_csv(gu_name)

    # --- Step 4: 캐시 관리 ---

    def save_cache(self, df: pd.DataFrame, year_month: str) -> Path:
        """매칭 완료 CSV를 캐시 디렉토리에 저장합니다."""
        if not _is_valid_dir(self._cache_dir):
            raise RuntimeError("cache_dir이 설정되지 않았습니다")
        timestamp = datetime.now().strftime("%Y%m%d")
        filename = f"matched_{year_month}_{timestamp}.csv"
        path = self._cache_dir / filename
        df.to_csv(path, index=False, encoding="utf-8-sig")
        logger.info(f"캐시 저장: {filename} ({len(df)}건)")
        return path

    # 호환용 별칭
    def _save_cache(self, df: pd.DataFrame, year_month: str) -> Path:
        return self.save_cache(df, year_month)

    def cleanup_cache(self, keep_n: int = 3) -> None:
        """오래된 캐시를 삭제합니다 (최근 keep_n개만 유지)."""
        if not _is_valid_dir(self._cache_dir) or not self._cache_dir.exists():
            return
        csvs = sorted(
            self._cache_dir.glob("matched_*.csv"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        for old_csv in csvs[keep_n:]:
            old_csv.unlink()
            logger.info(f"오래된 캐시 삭제: {old_csv.name}")

    # 호환용 별칭
    def _cleanup_cache(self, keep_n: int = 3) -> None:
        self.cleanup_cache(keep_n)

    # --- Helpers ---

    def _get_api_key(self) -> str:
        """설정에서 API 키를 가져옵니다."""
        if not self._config:
            # [H2] 미초기화 상태를 명확히 로깅
            logger.warning("파이프라인 미초기화 - API 키를 가져올 수 없습니다")
            return ""
        key = self._config.api_key
        if not key or key == "여기에_디코딩키를_붙여넣으세요":
            logger.warning("DATA_GO_KR_API_KEY 미설정 - API 조회 불가")
            return ""
        return key
