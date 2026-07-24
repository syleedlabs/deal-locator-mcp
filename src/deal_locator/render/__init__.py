"""deal_locator.render — 데이터카드 PNG 1장 렌더 (4:5).

전면 사진 + 다크 그라디언트 + 골드 수치의 카드 1장을 만든다. 지도·역명은 넣지
않는다 — 지도 캡처는 지도 서비스 스크래핑이나 별도 API 키를 요구하는데, 공개
배포판이 그걸 강제하면 약관·안정성 부담을 사용자에게 떠넘기게 된다. 지도가 있던
자리는 비우고 스펙표를 아래로 내려, 사진이 더 보이도록 했다.

로고는 넣지 않는다. 이 카드는 사용자(공인중개사)의 자료이지 배포자의 홍보물이
아니다.

동봉 폰트는 Pretendard(SIL Open Font License 1.1, `fonts/OFL.txt`).
렌더는 Playwright(Chromium) — 사용자는 최초 1회 `playwright install chromium` 필요.
"""

from __future__ import annotations

import base64
from pathlib import Path
from typing import Any, Optional

_HERE = Path(__file__).resolve().parent
_FONT_DIR = _HERE / "fonts"
_TEMPLATE = _HERE / "card.html"

CARD_W, CARD_H = 1080, 1350
_SCALE = 2  # 2160×2700 로 산출 — 인스타 업로드 시 재압축돼도 글자가 뭉개지지 않는다
_PAD_TOP = 250

# 신뢰도 → 표기 색. '추정매칭' 이하는 눈에 띄어야 한다 — 카드가 손을 떠난 뒤엔
# 대화로 고지할 방법이 없다.
_CONF_COLOR = {
    "확정": "#8FD3A8",
    "정확매칭": "#8FD3A8",
    "추정매칭": "#F0C177",
    "인접후보": "#F09595",
}
_CONF_DEFAULT = "#C6CBD4"

_FONT_WEIGHTS = {
    "Pretendard-Regular.woff2": 400,
    "Pretendard-Medium.woff2": 500,
    "Pretendard-SemiBold.woff2": 600,
    "Pretendard-Bold.woff2": 700,
    "Pretendard-ExtraBold.woff2": 800,
}


def _font_face_css() -> str:
    """woff2 를 base64 @font-face 로 임베드 — 네트워크·CDN 없이 렌더된다.

    Playwright 의 set_content 는 상대경로 리소스를 못 읽으므로, data URI 가 아니면
    폰트가 통째로 무시되고 시스템 폰트로 대체된다(사용자 환경마다 카드가 달라짐).
    """
    out = []
    for name, weight in _FONT_WEIGHTS.items():
        p = _FONT_DIR / name
        if not p.exists():
            continue
        b64 = base64.b64encode(p.read_bytes()).decode()
        out.append(
            f"@font-face{{font-family:'Pretendard';font-style:normal;"
            f"font-weight:{weight};font-display:swap;"
            f"src:url('data:font/woff2;base64,{b64}') format('woff2');}}"
        )
    return "\n".join(out)


def _price_font_size(price: str) -> int:
    """매매가 문자열 길이에 따라 폰트를 줄인다 — 두 줄로 넘어가면 아래 여백이
    무너져 스펙표가 밀린다. '470억'(3자)과 '1,204억'(7자)이 같은 크기일 수 없다.
    """
    n = len(price)
    if n <= 4:
        return 118
    if n <= 6:
        return 104
    if n <= 8:
        return 92
    return 78


# 매직바이트 → MIME. **확장자를 믿지 않는다** — 확장자만 보고 읽으면 `.png` 로
# 이름 붙인 개인키·설정파일이 그대로 페이지에 임베드된다(정보 유출 경로).
_IMAGE_MAGIC: tuple[tuple[bytes, str], ...] = (
    (b"\x89PNG\r\n\x1a\n", "png"),
    (b"\xff\xd8\xff", "jpeg"),
    (b"GIF87a", "gif"),
    (b"GIF89a", "gif"),
)


def image_mime(path: str | Path) -> str:
    """이미지 파일이면 MIME 서브타입, 아니면 빈 문자열. 내용으로 판별한다."""
    p = Path(path).expanduser()
    try:
        if not p.is_file():
            return ""
        head = p.open("rb").read(16)
    except OSError:
        return ""
    for magic, mime in _IMAGE_MAGIC:
        if head.startswith(magic):
            return mime
    if head[:4] == b"RIFF" and head[8:12] == b"WEBP":
        return "webp"
    return ""


def _photo_css(photo: Optional[str]) -> str:
    if not photo:
        return ""
    mime = image_mime(photo)
    if not mime:
        return ""
    b64 = base64.b64encode(Path(photo).expanduser().read_bytes()).decode()
    return f"url('data:image/{mime};base64,{b64}')"


def render_card(spec: dict[str, Any]) -> str:
    """spec → PNG 파일 경로. 값은 호출측이 조회 실측값으로 채운다(여기선 추정하지 않는다)."""
    from jinja2 import Template
    from markupsafe import Markup
    from playwright.sync_api import sync_playwright

    out_png = Path(spec["out_png"]).expanduser()
    out_png.parent.mkdir(parents=True, exist_ok=True)

    conf = str(spec.get("confidence") or "")
    score = spec.get("confidence_score")
    conf_text = ""
    if conf:
        conf_text = conf + (f" · {score:.2f}" if isinstance(score, (int, float)) else "")

    price = spec.get("price", "")
    # autoescape=True — eyebrow·rows 는 사용자/외부 API 에서 오므로 신뢰 불가다.
    # 끄면 "<img src=x onerror=...>" 한 줄이 그대로 페이지에 주입된다(실측 재현됨).
    # 반대로 우리가 만든 CSS 조각(base64 폰트·사진 data URI, 색상)은 따옴표가
    # 이스케이프되면 CSS 가 깨지므로 Markup 으로 감싸 예외 처리한다.
    tpl = Template(_TEMPLATE.read_text(encoding="utf-8"), autoescape=True)
    html = tpl.render(
        font_face_css=Markup(_font_face_css()),
        photo_css=Markup(_photo_css(spec.get("photo"))),
        pad_top=_PAD_TOP,
        eyebrow=spec.get("eyebrow", ""),
        price=price,
        price_fs=_price_font_size(price),
        rows=spec.get("rows", []),
        conf_text=conf_text,
        conf_color=Markup(_CONF_COLOR.get(conf, _CONF_DEFAULT)),
    )

    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        try:
            # 이 카드는 정적 HTML/CSS 만으로 그려진다 — JS 가 필요 없다.
            # 그래서 아예 끈다. 이스케이프가 뚫리더라도 주입된 스크립트가
            # 실행될 무대가 없다(다층 방어).
            ctx = browser.new_context(
                viewport={"width": CARD_W, "height": CARD_H},
                device_scale_factor=_SCALE,
                java_script_enabled=False,
            )
            page = ctx.new_page()
            # 모든 리소스는 data URI 로 인라인돼 있다 — 바깥으로 나가는 요청은
            # 정상 동작에 없다. 즉 여기서 끊어도 카드는 그대로 나오고,
            # 유출 통로만 사라진다.
            page.route("**/*", lambda route: route.abort())
            page.set_content(html, wait_until="load")
            page.wait_for_timeout(220)   # 임베드 폰트 적용 대기 — 없으면 첫 렌더가 폴백 폰트로 찍힌다
            page.screenshot(path=str(out_png))
        finally:
            browser.close()
    return str(out_png)
