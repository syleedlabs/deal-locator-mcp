"""실거래 조회가 https + 인증서 검증으로만 나가는지, 스키마가 예전과 같은지.

예전엔 PublicDataReader 를 거쳤는데 그 라이브러리가 `http://` 에 `verify=False` 로
호출해, 인증키(serviceKey 쿼리)가 조회마다 평문으로 네트워크에 실렸다.
지금은 직접 부른다. 네트워크는 타지 않는다(requests.get 대체).
"""

from __future__ import annotations

from typing import Any

import pytest
import requests

from deal_locator.core.config import PipelineConfig
from deal_locator.core.pipeline import RealEstateDataPipeline

_KEY = "TESTKEY_abc123%2Bxyz%3D%3D"  # 가짜 키

_XML_ONE = (
    "<response><header><resultCode>000</resultCode><resultMsg>OK</resultMsg></header>"
    "<body><items><item><sggCd>11650</sggCd><sggNm>서초구</sggNm><umdNm>우면동</umdNm>"
    "<jibun>7*</jibun><buildingType>일반</buildingType><dealAmount>320,000</dealAmount>"
    "<dealYear>2026</dealYear><dealMonth>7</dealMonth><dealDay>1</dealDay>"
    "<buildYear>2004</buildYear><buildingAr>478.26</buildingAr><plottageAr>308</plottageAr>"
    "</item></items><totalCount>1</totalCount></body></response>"
)


class _Resp:
    def __init__(self, text: str, status: int = 200) -> None:
        self.text, self.status_code = text, status


def _pipeline() -> RealEstateDataPipeline:
    p = RealEstateDataPipeline()
    p.initialize(PipelineConfig(api_key=_KEY, cache_dir="", staleness_days=7))
    return p


def test_trade_request_is_https_with_cert_verification(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[tuple[str, Any]] = []

    def fake_get(url: str, params: Any = None, timeout: Any = None, **kw: Any) -> _Resp:
        seen.append((url, kw.get("verify", True)))
        return _Resp(_XML_ONE)

    monkeypatch.setattr(requests, "get", fake_get)

    _pipeline().fetch_from_api(year_month="202607", gus=["서초구"])

    assert seen, "전제: 실거래 요청이 나가야 한다"
    assert all(u.startswith("https://") for u, _ in seen), "평문 http 로 인증키 전송"
    assert all(v is not False for _, v in seen), "TLS 인증서 검증을 끄면 안 된다"


def test_trade_schema_matches_previous_library(monkeypatch: pytest.MonkeyPatch) -> None:
    """파이프라인 하류(정규화·deals-export)가 기대하는 한글 컬럼이 그대로 나와야 한다."""
    monkeypatch.setattr(requests, "get", lambda *a, **k: _Resp(_XML_ONE))

    df = _pipeline().fetch_from_api(year_month="202607", gus=["서초구"])

    assert len(df) == 1
    for col in ("시군구", "법정동", "지번", "거래금액", "건물면적", "대지면적",
                "건축년도", "건물유형", "해제사유발생일", "매도자", "매수자", "_gu_name"):
        assert col in df.columns, f"컬럼 누락: {col}"
    assert df.loc[0, "거래금액"] == 320000, "거래금액 콤마 정규화"
    assert df.loc[0, "_gu_name"] == "서초구"


def test_trade_api_error_is_logged_without_key(monkeypatch: pytest.MonkeyPatch,
                                                caplog: pytest.LogCaptureFixture) -> None:
    bad = ("<response><header><resultCode>30</resultCode>"
           "<resultMsg>SERVICE KEY IS NOT REGISTERED ERROR.</resultMsg></header></response>")
    monkeypatch.setattr(requests, "get", lambda *a, **k: _Resp(bad))
    monkeypatch.setattr("time.sleep", lambda *_: None)

    df = _pipeline().fetch_from_api(year_month="202607", gus=["서초구"])

    assert df.empty
    assert "SERVICE KEY IS NOT REGISTERED" in caplog.text, "전제: 오류 사유는 로그에 남아야 한다"
    assert _KEY not in caplog.text


def test_trade_empty_month_returns_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    empty = ("<response><header><resultCode>000</resultCode></header>"
             "<body><items/><totalCount>0</totalCount></body></response>")
    monkeypatch.setattr(requests, "get", lambda *a, **k: _Resp(empty))

    assert _pipeline().fetch_from_api(year_month="202607", gus=["서초구"]).empty
