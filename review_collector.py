# -*- coding: utf-8 -*-
"""
쇼핑몰 리뷰 수집기 (requests/BS + Playwright 폴백)

요구사항:
- URL에서 최대 max_pages 반복 수집
- '더보기' 버튼/페이지네이션을 클릭하는 로직 포함(Playwright)
- 크롤링이 막히면 호출부(app.py)에서 Text Area 수동 입력으로 전환 가능
"""

from __future__ import annotations

import asyncio
import os
import re
import subprocess
import sys
import threading
import time
import traceback
from dataclasses import dataclass

# Windows: Playwright가 Node 드라이버 subprocess를 띄울 때 Proactor 루프가 필요함.
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
from typing import Any, Iterable, List
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup

from playwright.sync_api import sync_playwright

# 크롤링 차단/봇 의심을 줄이기 위해 “브라우저처럼 보이는” User-Agent 사용
# (사이트 정책/robots.txt를 준수하는 범위에서만 사용해 주세요.)
CHROME_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)

_playwright_browser_lock = threading.Lock()
_playwright_browsers_ready = False


def _ensure_playwright_browsers() -> None:
    """
    pip로 playwright만 설치되고 Chromium 바이너리가 없는 환경(Streamlit Community Cloud 등) 대비.
    최초 1회 `python -m playwright install chromium` 로 캐시에 받은 뒤 재시도한다.
    """
    global _playwright_browsers_ready
    if _playwright_browsers_ready:
        return

    def _launch_ok() -> bool:
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                browser.close()
            return True
        except Exception as e:
            msg = str(e).lower()
            if "executable doesn't exist" in msg or "looks like playwright was just installed" in msg:
                return False
            raise

    with _playwright_browser_lock:
        if _playwright_browsers_ready:
            return
        if _launch_ok():
            _playwright_browsers_ready = True
            return
        proc = subprocess.run(
            [sys.executable, "-m", "playwright", "install", "chromium"],
            capture_output=True,
            text=True,
            timeout=600,
        )
        if proc.returncode != 0:
            raise RuntimeError(
                "playwright install chromium 실패: "
                + (proc.stderr or proc.stdout or str(proc.returncode))
            )
        if not _launch_ok():
            raise RuntimeError(
                "Chromium 설치 후에도 실행에 실패했습니다. "
                "Streamlit Cloud는 저장소 루트의 packages.txt(시스템 라이브러리)와 "
                "앱 설정의 빌드 명령에 `python -m playwright install chromium` 포함을 확인하세요."
            )
        _playwright_browsers_ready = True


@dataclass
class ReviewItem:
    text: str
    rating: int | None = None  # 1~5, 없으면 None
    author: str | None = None


# 오늘의집·일부 쇼핑몰: '최**님 본문… 박**님 본문…' 이 한 덩어리로 붙는 경우
_MASKED_NICK_HEAD = re.compile(r"[가-힣A-Za-z0-9*·\-]{1,24}\*\*님")


def split_review_text_by_masked_author(text: str, *, min_segment_len: int = 12) -> list[str]:
    """
    한 문자열에 여러 리뷰가 마스킹 닉네임(**님)만으로 이어진 경우 분리한다.
    첫 번째 닉네임 앞의 짧은 메타(날짜 등)는 첫 리뷰에 붙인다.
    """
    text = (text or "").strip()
    if not text:
        return []
    matches = list(_MASKED_NICK_HEAD.finditer(text))
    if len(matches) <= 1:
        return [text] if len(text) >= min_segment_len else []

    out: List[str] = []
    preamble = text[: matches[0].start()].strip()
    for i, m in enumerate(matches):
        start = m.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        seg = text[start:end].strip()
        if preamble and i == 0:
            seg = (preamble + "\n" + seg).strip()
        if len(seg) >= min_segment_len:
            out.append(seg)
    return out


def expand_merged_review_items(items: Iterable[ReviewItem]) -> List[ReviewItem]:
    """
    각 ReviewItem.text에 여러 '**님' 헤드가 있으면 여러 건으로 쪼갠다.
    (크롤/붙여넣기 공통 후처리)
    """
    expanded: List[ReviewItem] = []
    for it in items:
        chunks = split_review_text_by_masked_author(it.text or "")
        if len(chunks) <= 1:
            expanded.append(it)
        else:
            for ch in chunks:
                expanded.append(ReviewItem(text=ch))
    return expanded


# 데모용 제품별 리뷰 풀 (URL 마지막 경로 세그먼트로 구분)
_DEMO_REVIEWS: dict[str, List[ReviewItem]] = {
    "serum": [
        ReviewItem(
            "건조해서 겉당김이 심했는데 이거 쓰고 나서 수분이 꽉 찬 느낌이에요. 잡티도 옅어진 것 같아요.",
            5,
            "user01",
        ),
        ReviewItem(
            "민감 피부인데 속당김 없이 진정돼요. 트러블 올라오던 게 가라앉았어요.",
            5,
            "user02",
        ),
        ReviewItem(
            "대용량이라 가성비 좋고 홈케어로 끝내요. 매일 바르기 부담 없어요.",
            4,
            "user03",
        ),
        ReviewItem(
            "끈적임 없이 흡수 잘 되고 마무리가 산뜻해요. 제형이 묽어서 여름에도 OK.",
            5,
            "user04",
        ),
        ReviewItem(
            "재구매 세 번째예요. 주변에서도 다 물어봐요. 품절될 때마다 대란이던데 이해가 가요.",
            5,
            "user05",
        ),
        ReviewItem(
            "특허 성분 들어갔다고 해서 믿고 샀어요. 전성분도 깔끔하게 공개돼 있어서 좋아요.",
            4,
            "user06",
        ),
        ReviewItem(
            "향이 은은해서 힐링되고 패키지도 고급져요. 브랜드 이미지가 딱 제 스타일.",
            5,
            "user07",
        ),
        ReviewItem(
            "여태 쓰던 세럼은 다 유목민이었는데 이건 정착템이에요. 실패 끝에 찾은 해답.",
            5,
            "user08",
        ),
        ReviewItem(
            "가격 대비 효과는 있는데 용기가 불편해요. 그래도 효과는 인정.",
            3,
            "user09",
        ),
        ReviewItem(
            "기대만큼 드라마틱하진 않아요. 그냥 무난.",
            2,
            "user10",
        ),
    ],
    "cream": [
        ReviewItem("각질 부각 없이 촉촉해요. 겨울에도 버텨요.", 5, "c1"),
        ReviewItem("가격이 착해서 온 가족이 씁니다.", 4, "c2"),
        ReviewItem("유명하다더니 역시 재구매 많은 이유가 있어요.", 5, "c3"),
    ],
}


def _slug_from_url(url: str) -> str:
    """URL에서 마지막 의미 있는 세그먼트 추출 (데모 라우팅용)."""
    url = url.strip()
    if not url:
        return "default"
    # 쿼리 제거
    url = url.split("?")[0].rstrip("/")
    parts = [p for p in re.split(r"/+", url) if p]
    if not parts:
        return "default"
    last = parts[-1].lower()
    # 숫자만인 경우 이전 세그먼트 사용
    if last.isdigit() and len(parts) >= 2:
        last = parts[-2].lower()
    return last


def _fetch_reviews_from_test_html(url: str) -> List[ReviewItem] | None:
    """
    테스트용 고정 HTML 구조에서 리뷰를 파싱합니다.

    기대 마크업 (서버가 아래 구조를 반환하는 경우):
    <div class="product-reviews">
      <article class="review-item" data-rating="5">
        <p class="review-text">리뷰 본문</p>
        <span class="review-author">닉네임</span>
      </article>
    </div>
    """
    parsed = urlparse(url.strip())
    if parsed.scheme not in ("http", "https"):
        return None

    try:
        r = requests.get(
            url,
            timeout=15,
            headers={"User-Agent": CHROME_USER_AGENT},
        )
        r.raise_for_status()
    except requests.RequestException:
        return None

    soup = BeautifulSoup(r.text, "html.parser")
    root = soup.select_one(".product-reviews")
    if root is None:
        return None

    out: List[ReviewItem] = []
    for art in root.select("article.review-item"):
        text_el = art.select_one(".review-text")
        if not text_el:
            continue
        body = text_el.get_text(strip=True)
        if not body:
            continue
        rating = None
        raw_rating = art.get("data-rating")
        if raw_rating and raw_rating.isdigit():
            rating = max(1, min(5, int(raw_rating)))
        author_el = art.select_one(".review-author")
        author = author_el.get_text(strip=True) if author_el else None
        out.append(ReviewItem(text=body, rating=rating, author=author))

    return out if out else None


def _oliveyoung_is_ui_not_review_body(txt: str) -> bool:
    """
    상품 우측/상단 요약, 실시간 조회(00명이 보고 있어요), '리뷰 N,NNN건' 단독 줄 등
    — '리뷰' 키워드가 있어도 사용자 리뷰 본문이 아님.
    """
    s = (txt or "").strip()
    if not s:
        return True
    compact = re.sub(r"\s+", "", s)

    # 실시간 조회수
    if "보고있어요" in compact or "명이보고" in compact:
        return True
    if re.search(r"\d+\s*명\s*이\s*보고", s):
        return True

    # '리뷰 16,186 건' 처럼 건수만
    if re.fullmatch(r"리뷰\s*[\d,]+\s*건\s*", s):
        return True

    hangul_n = len(re.findall(r"[가-힣]", s))

    # 평점·건수·더보기 한 덩어리(상단 요약 블록)
    if hangul_n < 55:
        if "리뷰더보기" in compact or ("리뷰 더보기" in s and "평점" in s):
            return True
        if "평점" in s and re.search(r"리뷰\s*[\d,]+\s*건", s) and hangul_n < 45:
            return True

    # 한글 본문이 거의 없고 숫자·기호·짧은 쇼핑 UI만
    if hangul_n < 28:
        if re.search(r"[\d,]+\s*건", s) or "평점" in s or "더보기" in s:
            return True

    return False


def _extract_oliveyoung_reviews(soup: BeautifulSoup) -> List[ReviewItem]:
    """
    올리브영 상세의 '리뷰&셔터' 영역: 카드 단위로 본문만 모을 때 사용.
    (클래스명은 프론트 변경 시 조정)
    """
    seen: set[str] = set()
    out: List[ReviewItem] = []

    skip_only = (
        "유용한 순",
        "최신순",
        "도움순",
        "평점 높은순",
        "평점 낮은순",
        "리뷰 유형",
        "상품 옵션",
        "피부 필터",
        "내 피부 맞춤",
    )

    selectors = [
        "#gdasList li.review_cont",
        "#gdasList .inner_list > li",
        "#gdasList li",
        ".gdas_list li.review_cont",
        ".gdas_list li",
        "ul.list_inner li.review_cont",
        "[class*='review_list'] li[class*='review']",
        "li.review_cont",
        "#gdasList [class*='review']",
    ]

    for sel in selectors:
        for el in soup.select(sel):
            txt = re.sub(r"\s+", " ", (el.get_text(" ", strip=True) or "")).strip()
            if len(txt) < 25:
                continue
            if len(txt) > 4000:
                txt = txt[:4000]
            if any(x in txt for x in skip_only) and len(txt) < 80:
                continue
            # 상단 요약(별점만·건수만) 배제
            if re.match(r"^[\d\.\s★☆⭐]+건?$", txt[:40].strip()):
                continue
            if _oliveyoung_is_ui_not_review_body(txt):
                continue
            if txt in seen:
                continue
            seen.add(txt)
            out.append(ReviewItem(text=txt[:2000]))

    return out


def _oliveyoung_page_access_issue_message(page: Any) -> str | None:
    """
    Cloudflare/403 등으로 본문이 안 내려오는 경우.
    (자동화 환경에서는 403·챌린지 페이지만 오는 경우가 많음)
    """
    try:
        html = (page.content() or "").lower()
        txt = page.evaluate("() => (document.body && document.body.innerText) || ''") or ""
    except Exception:
        return "페이지 내용을 읽지 못했습니다."
    compact = re.sub(r"\s+", "", txt)
    tl = txt.lower()
    if "ray_id" in tl or "cloudflare" in html or "challenges.cloudflare.com" in html:
        return (
            "올리브영이 봇·자동화 접속을 차단한 것으로 보입니다(Cloudflare 등). "
            "같은 PC의 일반 크롬에서 상품 페이지가 열리는지 확인해 주세요. "
            "열리면 리뷰&셔터 영역을 복사해 앱의 붙여넣기에 넣어 분석할 수 있습니다."
        )
    if "잠시만기다려" in compact or ("잠시만" in txt and "기다려" in txt and len(txt) < 500):
        return "접속 확인/대기 페이지만 표시되었습니다. 차단 또는 네트워크 제한 가능성이 있습니다."
    if len(txt.strip()) < 200 and "올리브영" not in txt and "리뷰" not in txt:
        return "상품 페이지 본문이 비어 있거나 차단된 응답일 수 있습니다."
    return None


def _oliveyoung_extract_review_texts_from_dom(page: Any) -> List[str]:
    """
    리뷰 카드는 레거시 #gdasList 또는 2025+ Shadow DOM 등에 있을 수 있어
    document + 모든 shadowRoot에서 .review_cont 계열을 수집합니다.
    """
    try:
        raw = page.evaluate(
            """
            () => {
              const out = [];
              const seen = new Set();
              function addText(raw) {
                const t = (raw || '').replace(/\\s+/g, ' ').trim();
                if (t.length < 38 || seen.has(t)) return;
                seen.add(t);
                out.push(t);
              }
              function scan(root) {
                if (!root || !root.querySelectorAll) return;
                const sels = [
                  '#gdasList li',
                  '#gdasList .review_cont',
                  '[class*="review_cont"]',
                  '[class*="ReviewCont"]',
                  'li[class*="review"]',
                ];
                sels.forEach(sel => {
                  try {
                    root.querySelectorAll(sel).forEach(el => addText(el.innerText || ''));
                  } catch (e) {}
                });
              }
              function visit(node) {
                if (!node) return;
                scan(node);
                if (node.shadowRoot) {
                  visit(node.shadowRoot);
                }
                const ch = node.children || [];
                for (let i = 0; i < ch.length; i++) visit(ch[i]);
              }
              visit(document.documentElement);
              return out.slice(0, 200);
            }
            """
        )
        return [str(x) for x in (raw or []) if x]
    except Exception:
        return []


def _review_items_from_oliveyoung_texts(texts: List[str]) -> List[ReviewItem]:
    seen: set[str] = set()
    out: List[ReviewItem] = []
    for t in texts:
        t = re.sub(r"\s+", " ", (t or "").strip())
        if len(t) < 38:
            continue
        if _oliveyoung_is_ui_not_review_body(t):
            continue
        if t in seen:
            continue
        seen.add(t)
        out.append(ReviewItem(text=t[:2000]))
    return out


def _text_looks_like_embedded_css(t: str) -> bool:
    """스타일 블록·Yotpo 위젯 CSS가 리뷰 후보로 섞이는 경우."""
    t = (t or "").strip()
    if not t:
        return True
    if "{" in t and "}" in t and ":" in t:
        if t.count("{") >= 1 and any(
            x in t for x in ("display:", "cursor:", "@keyframes", "!important", "animation:")
        ):
            return True
        if ".yotpo" in t or "[v-cloak]" in t:
            return True
    if ".yotpo-" in t and ("{" in t or "display" in t):
        return True
    return False


def _text_is_yotpo_shopify_ui_noise(t: str) -> bool:
    """Yotpo/Shopify 리뷰 위젯 UI·요약·필터 문구 (본문 아님)."""
    low = (t or "").strip().lower()
    if len(low) < 6:
        return True
    exact = {
        "verified buyer",
        "write a review",
        "review highlights",
        "read summary",
        "read summary by topics",
        "search reviews",
        "sort by",
        "with media",
        "all ratings",
        "show more",
        "most relevant",
        "most recent",
        "highest rating",
        "lowest rating",
        "previous review media slide",
        "next review media slide",
    }
    if low in exact:
        return True
    if re.match(r"^\d+\s+reviews?$", low):
        return True
    if re.match(r"^review\s+[\d.]+\s+based on\s+\d+", low):
        return True
    if "customers say" in low and "ai-generated" in low:
        return True
    if "abstract user icon" in low:
        return True
    if "was this review helpful" in low and len(low) < 80:
        return True
    if "published date" in low and len(low) < 120:
        return True
    # Shopify AI 한 줄 요약 블록
    if "offers a unique" in low and "customers praise" in low and len(low) > 200:
        return True
    return False


def _element_is_yotpo_chrome_not_body(el: Any) -> bool:
    """클래스에 review가 들어가도 위젯 껍데기인 요소는 제외."""
    cls = (" ".join(el.get("class", []) or []) + " " + (el.get("id") or "")).lower()
    if "yotpo" not in cls:
        return False
    if any(
        x in cls
        for x in (
            "yotpo-review-body",
            "yotpo-read-more",
            "yotpo-comment",
            "content-review",
        )
    ):
        return False
    if any(
        x in cls
        for x in (
            "yotpo-star",
            "yotpo-sr-",
            "yotpo-bottom",
            "yotpo-widget",
            "yotpo-highly",
            "yotpo-topic",
            "yotpo-reviews-star-ratings",
            "yotpo-scroll",
            "yotpo-filter",
            "yotpo-header",
            "yotpo-summary",
        )
    ):
        return True
    return False


def _extract_yotpo_review_bodies(soup: BeautifulSoup) -> List[ReviewItem]:
    """Yotpo(Shopify 등): 본문 노드만 수집. 위젯 전체 텍스트는 쓰지 않음."""
    selectors = [
        ".yotpo-review-body",
        ".yotpo-read-more",
        "[class*='yotpo-review-body']",
        ".yotpo-comment-content",
        ".content-review",
    ]
    seen: set[str] = set()
    out: List[ReviewItem] = []
    for sel in selectors:
        for el in soup.select(sel):
            txt = el.get_text(" ", strip=True)
            txt = re.sub(r"\s+", " ", txt)
            txt = re.sub(r"\s*Read more\s*$", "", txt, flags=re.IGNORECASE).strip()
            if len(txt) < 28:
                continue
            if _text_looks_like_embedded_css(txt):
                continue
            if _text_is_yotpo_shopify_ui_noise(txt):
                continue
            key = txt[:240]
            if key in seen:
                continue
            seen.add(key)
            out.append(ReviewItem(text=txt[:2000]))
    return out


def _extract_reviews_from_soup(soup: BeautifulSoup, base_url: str | None = None) -> List[ReviewItem]:
    """
    리뷰를 최대한 많이 뽑기 위한 파서.

    1) (우선) 테스트/데모용 고정 마크업: `.product-reviews article.review-item`
    2) (보조) 클래스에 review 관련 단어가 포함된 요소에서 텍스트 추출
    """

    # 스크립트/스타일 안의 CSS·JS가 'review' 클래스 부모에 섞여 나오는 경우 방지
    for tag in soup.find_all(["script", "style", "noscript"]):
        tag.decompose()

    out: List[ReviewItem] = []

    host = (urlparse(base_url).netloc.lower() if base_url else "").strip()

    # Yotpo(lewkin 등 Shopify): 전용 선택자 우선 — 폴백은 위젯·CSS 노이즈가 많음
    if soup.select_one(
        "[class*='yotpo-review'], [class*='yotpo-review-body'], [id*='yotpo']"
    ):
        yb = _extract_yotpo_review_bodies(soup)
        if yb:
            return yb

    def _toun28_extract() -> List[ReviewItem]:
        # toun28은 리뷰 카드가 `.box-review` / `.review` 계열로 렌더되는 경우가 많습니다.
        selectors = [
            ".box-review",
            "div.review",
            ".pop-bestReview",
            ".slider-bestReview-pop",
            "[class*='review' i]",
            "[class*='후기' i]",
        ]
        seen: set[str] = set()
        collected: List[ReviewItem] = []

        def should_skip(txt: str) -> bool:
            skip_markers = [
                "후기작성",
                "베스트 후기",
                "문의",
                "배송",
                "장바구니",
                "환불",
                "교환",
                "상품문의",
                "상품정보",
            ]
            if any(m in txt for m in skip_markers):
                return True
            # '후기 더보기'는 UI 버튼 한 줄만 걸러짐(본문에 같은 문구가 붙은 긴 리뷰는 유지)
            if "후기 더보기" in txt and len(txt) < 55:
                return True
            # 작성자/구분자 없는 텍스트는 보통 노이즈가 많아서 최소 조건
            if "님" not in txt and "★" not in txt and "별" not in txt:
                return True
            return False

        for sel in selectors:
            for el in soup.select(sel):
                txt = (el.get_text(" ", strip=True) or "").strip()
                if not txt:
                    continue
                txt = re.sub(r"\s+", " ", txt)
                if len(txt) < 20 or len(txt) > 1500:
                    continue
                if should_skip(txt):
                    continue
                if txt in seen:
                    continue
                seen.add(txt)
                collected.append(ReviewItem(text=txt[:1200]))

        return collected

    def _ohou_extract() -> List[ReviewItem]:
        # 오늘의집은 프론트렌더/동적로딩이라 class/id에 'review'가 없을 수 있어
        # 전체 문서에서 "리뷰" 문맥 컨테이너를 먼저 찾고, 그 내부 텍스트를 리뷰 키워드로 좁힙니다.
        review_keywords = [
            "배송",
            "사이즈",
            "단단",
            "푹",
            "편하",
            "보송",
            "냄새",
            "좋",
            "별로",
            "재구매",
            "사용",
            "후기",
            "리뷰",
        ]

        # 1) '리뷰'가 포함되고 숫자가 함께 있는 컨테이너(예: 리뷰16,345)를 우선 후보로 선정
        score_candidates: List[tuple[int, Any]] = []
        for el in soup.find_all(True):
            txt = (el.get_text(" ", strip=True) or "")
            if not txt:
                continue
            if "리뷰" not in txt:
                continue
            if not re.search(r"\\d[\\d,]{0,}", txt):
                continue
            # 점수: '리뷰' + 키워드 + 별점 단서
            txt_low = txt.lower()
            kw_hit = sum(1 for k in review_keywords if k in txt)
            star_hit = ("★" in txt) or ("별" in txt)
            score = txt.count("리뷰") * 3 + kw_hit + (2 if star_hit else 0)
            if score >= 5:
                score_candidates.append((score, el))
        score_candidates.sort(key=lambda x: x[0], reverse=True)
        container_candidates = [el for _, el in score_candidates[:3]]

        # 2) 컨테이너가 없으면 전체에서 넓게 잡되 키워드 필터를 강하게
        if not container_candidates:
            container_candidates = [soup]

        seen: set[str] = set()
        collected: List[ReviewItem] = []

        def add_text(t: str):
            t = re.sub(r"\\s+", " ", t).strip()
            if not t:
                return
            if len(t) < 20 or len(t) > 1200:
                return
            if t in seen:
                return
            # 리뷰 키워드가 있는 경우만 채택 (노이즈 억제)
            if not any(k in t for k in review_keywords) and ("★" not in t and "별" not in t):
                return
            # 너무 "공통 UI 텍스트" 느낌은 제외
            if any(m in t for m in ["상품정보", "문의 내역이 없습니다", "배송/환불", "혜택", "업체직접배송"]):
                return
            seen.add(t)
            collected.append(ReviewItem(text=t[:1200]))

        for container in container_candidates:
            # 리뷰 본문은 보통 p/span/div에 들어가 있는 경우가 많음
            for el in container.find_all(["p", "span", "div"]):
                t = el.get_text(" ", strip=True)
                if not t:
                    continue
                add_text(t)

        return collected

    # 사이트별 우선 적용(정확도 우선)
    if host.endswith("toun28.com"):
        return _toun28_extract()
    if "ohou.se" in host:
        return _ohou_extract()
    if "oliveyoung.co.kr" in host:
        oy = _extract_oliveyoung_reviews(soup)
        if oy:
            return oy

    # 우선순위 1) 데모/테스트 마크업
    root = soup.select_one(".product-reviews")
    if root is not None:
        for art in root.select("article.review-item"):
            text_el = art.select_one(".review-text")
            if not text_el:
                continue
            body = text_el.get_text(strip=True)
            if not body:
                continue

            rating = None
            raw_rating = art.get("data-rating")
            if raw_rating and str(raw_rating).isdigit():
                rating = max(1, min(5, int(raw_rating)))

            author = None
            author_el = art.select_one(".review-author")
            if author_el:
                author = author_el.get_text(strip=True) or None

            out.append(ReviewItem(text=body, rating=rating, author=author))
        return out

    # 우선순위 2) fallback: review 관련 클래스/ID를 가진 요소에서 텍스트를 수집
    # - 실제 사이트는 '리뷰'라는 단어가 본문에 없을 수 있어, 텍스트 필터를 너무 빡빡하게 걸지 않습니다.
    candidates: Iterable = soup.select("[class*='review' i], [id*='review' i], [class*='후기' i], [id*='후기' i]")
    review_like_keywords = (
        "review-text",
        "review-body",
        "review-content",
        "review",
        "후기",
        "리뷰",
        "별",
        "★",
        "재구매",
    )

    for el in candidates:
        if _element_is_yotpo_chrome_not_body(el):
            continue
        # 별점/메타가 섞여 있을 수 있으니, 큰 덩어리 텍스트를 짧게 사용
        txt = el.get_text(" ", strip=True)
        if not txt:
            continue
        if _text_looks_like_embedded_css(txt):
            continue
        if _text_is_yotpo_shopify_ui_noise(txt):
            continue
        # 너무 짧거나(노이즈), 리뷰라는 단서가 없으면 건너뜀
        if len(txt) < 10:
            continue
        el_meta = " ".join(
            [
                " ".join(el.get("class", []) or []),
                el.get("id") or "",
                el.get("aria-label") or "",
            ]
        ).lower()
        # 노이즈 억제를 위해: 리뷰 메타/별점 단서가 있거나, 텍스트가 리뷰스럽게 키워드를 포함해야 채택
        if not (
            any(k in el_meta for k in review_like_keywords)
            or "★" in txt
            or any(k in txt for k in ["재구매", "배송", "사이즈", "사용", "좋", "별로"])
            or len(txt) >= 60
        ):
            continue
        # 중복 방지용으로 너무 긴 문장을 잘라서 저장 (분석 품질은 모델이 처리)
        out.append(ReviewItem(text=txt[:1200]))

    out = [
        it
        for it in out
        if not _text_looks_like_embedded_css(it.text or "")
        and not _text_is_yotpo_shopify_ui_noise(it.text or "")
    ]

    if "oliveyoung.co.kr" in host:
        out = [it for it in out if not _oliveyoung_is_ui_not_review_body(it.text or "")]

    return out


def _fetch_html(url: str, timeout_s: int = 20) -> str | None:
    try:
        r = requests.get(
            url,
            timeout=timeout_s,
            headers={
                "User-Agent": CHROME_USER_AGENT,
                "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Connection": "keep-alive",
            },
        )
        r.raise_for_status()
        # 인코딩 자동 추정 실패 시를 대비
        r.encoding = r.encoding or "utf-8"
        return r.text
    except requests.RequestException:
        return None


def _absolute_if_possible(base_url: str | None, href: str | None) -> str | None:
    if not href:
        return None
    href = href.strip()
    if not href:
        return None
    if href.startswith("javascript:"):
        return None
    if base_url:
        return urljoin(base_url, href)
    return href


def _increment_page_param(url: str) -> str | None:
    """
    URL query의 `page`/`p` 파라미터가 숫자일 때만 다음 페이지 URL을 만들어 봅니다.
    """
    parsed = urlparse(url)
    if not parsed.query:
        return None
    qs = parse_qs(parsed.query)
    for key in ("page", "p"):
        if key in qs and qs[key]:
            cur = qs[key][0]
            if str(cur).isdigit():
                qs[key] = [str(int(cur) + 1)]
                new_query = urlencode(qs, doseq=True)
                return urlunparse(
                    (parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment)
                )
    return None


def _guess_next_url(soup: BeautifulSoup, current_url: str) -> str | None:
    """
    '페이지네이션' 또는 '더보기' 버튼의 다음 요청 URL을 최대한 추정합니다.

    중요:
    - 많은 쇼핑몰은 '더보기'가 JS로 동작하며 실제로는 API를 호출합니다.
      이 경우 HTML에 data-url/href가 노출되어야 파싱됩니다.
    """
    # 1) rel=next
    rel_next = soup.select_one("link[rel='next']")
    if rel_next and rel_next.get("href"):
        return _absolute_if_possible(current_url, rel_next.get("href"))

    # 2) href가 있고 다음 페이지로 보이는 링크
    next_text_patterns = ("다음", "next", "›", "»", ">")
    for a in soup.select("a[href]"):
        text = a.get_text(" ", strip=True) or ""
        if not text:
            continue
        if any(p in text.lower() for p in [pat.lower() for pat in next_text_patterns if pat not in (">", "›", "»")]):
            return _absolute_if_possible(current_url, a.get("href"))
        if text.strip() in next_text_patterns:
            return _absolute_if_possible(current_url, a.get("href"))

    # 3) 더보기 버튼/컨테이너에서 data-url/href 탐색
    for btn in soup.select("button, [role='button']"):
        label = btn.get_text(" ", strip=True) or ""
        if not label:
            continue
        if "더보기" in label or "more" in label.lower():
            for attr in ("data-url", "data-href", "data-next", "data-target", "href"):
                if btn.has_attr(attr):
                    v = btn.get(attr)
                    if v:
                        return _absolute_if_possible(current_url, str(v))

    # 4) 그래도 안되면 page/p query 증분으로 추정
    return _increment_page_param(current_url)


def _dedupe_reviews(items: List[ReviewItem]) -> List[ReviewItem]:
    seen = set()
    out: List[ReviewItem] = []
    for it in items:
        key = (it.text or "").strip()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(it)
    return out


def _is_naver_store_host(host: str) -> bool:
    """스마트스토어·브랜드스토어·쇼핑 윈도우 상품 등 네이버 쇼핑 상세(리뷰 탭) 공통."""
    h = (host or "").lower()
    return (
        "brand.naver.com" in h
        or "smartstore.naver.com" in h
        or "shopping.naver.com" in h
    )


def _is_oliveyoung_host(host: str) -> bool:
    return "oliveyoung.co.kr" in (host or "").lower()


def _oliveyoung_url_with_tab_review(url: str) -> str:
    """상품 상세에 tab=review가 없으면 붙여 리뷰&셔터 영역이 로드되도록 함."""
    u = (url or "").strip()
    if not u or re.search(r"[\?&]tab=review\b", u, re.I):
        return u
    return f"{u}{'&' if '?' in u else '?'}tab=review"


def _filter_naver_review_tab_chrome(items: List[ReviewItem]) -> List[ReviewItem]:
    """일반 파서로 잡힌 네이버 상단 안내·통계 덩어리 제거."""
    out: List[ReviewItem] = []
    for it in items:
        if _naver_text_is_review_tab_chrome_not_user_review(it.text or ""):
            continue
        out.append(it)
    return out


def _exc_detail(e: BaseException, *, limit: int = 320) -> str:
    """TimeoutError 등 str(e)가 비는 예외도 화면에 남기기 위함."""
    msg = (str(e) or "").strip()
    if msg:
        return msg[:limit]
    return f"{type(e).__name__}: {repr(e)}"[:limit]


_NAVER_REVIEW_BOILER = (
    "상품평을 작성하시면",
    "구매확정",
    "비밀글입니다",
    "리뷰 작성 시",
    "상품문의",
)

# 네이버 상품 리뷰 블록에 붙는 미디어/메타 라벨(사진·비디오 리뷰 배지, 평점 등)이 본문과 함께 수집되는 경우가 많음
_NAVER_UI_PHRASES = (
    "비디오 리뷰",
    "사진 리뷰",
    "포토 리뷰",
    "동영상 리뷰",
    "영상 리뷰",
    "비디오리뷰",
    "사진리뷰",
    "포토리뷰",
    "동영상리뷰",
)
_NAVER_INLINE_UI_TOKENS = frozenset(
    {
        "비디오",
        "동영상",
        "영상",
        "사진",
        "포토",
        "썸네일",
        "클립",
        "평점",
        "별점",
        "한줄평",
    }
)


def _strip_naver_review_ui_noise(text: str) -> str:
    """리뷰 본문 앞뒤에 붙는 UI 단어(비디오/사진/평점 등)를 제거."""
    t = (text or "").strip()
    if not t:
        return ""
    for ph in _NAVER_UI_PHRASES:
        t = t.replace(ph, " ")
    parts: List[str] = []
    for tok in t.split():
        stripped = tok.strip(".,!?·…:;\"'()[]（）【】")
        if stripped in _NAVER_INLINE_UI_TOKENS:
            continue
        parts.append(tok)
    return re.sub(r"\s+", " ", " ".join(parts)).strip()


def _extract_naver_store_review_body(el: Any) -> str:
    """네이버 스토어 리뷰 링크/블록에서 본문 텍스트만 최대한 추출."""
    ps = el.select("p") if hasattr(el, "select") else []
    chunks: List[str] = []
    for p in ps:
        t = p.get_text(" ", strip=True)
        if len(t) >= 8:
            chunks.append(t)
    if chunks:
        raw = re.sub(r"\s+", " ", " ".join(chunks))
    else:
        raw = re.sub(r"\s+", " ", el.get_text(" ", strip=True))
    return _strip_naver_review_ui_noise(raw)


def _naver_text_is_review_tab_chrome_not_user_review(txt: str) -> bool:
    """
    네이버 스마트스토어 리뷰 탭 상단에 붙는 고정 UI(포인트 안내, 총평점, 분포, 만족도 조사, 항목 이동, PICK 배너 등).
    한 리뷰 본문이 아니며, 큰 컨테이너에 한꺼번에 붙어 수집되는 경우가 많음.
    """
    s = txt or ""
    if not s.strip():
        return True
    compact = re.sub(r"\s+", "", s)

    # 페이지 상단 고정 카피(띄어쓰기 변형 무시)
    if "상품리뷰상품을구매하신분들이작성하신리뷰입니다" in compact:
        return True
    if "로딩중" in compact and ("전체리뷰수" in compact or "평점비율" in compact):
        return True

    # ── 포인트·작성 안내(사이트 소개 문구) ─────────────────
    if "리뷰작성시" in compact and "포인트" in s:
        return True
    if "포인트가적립" in compact:
        return True
    if "50원" in s and "텍스트" in s and "리뷰" in s:
        return True
    if "150원" in s and ("포토" in s or "동영상" in s) and "리뷰" in s:
        return True

    # ── 집계·통계 블록 ─────────────────
    if "사용자총평점" in compact or "사용자 총 평점" in s:
        return True
    if "전체리뷰수" in compact:
        return True
    if "평점비율" in compact:
        return True

    # ── 만족도 한줄 요약 + 표본 수 ─────────────────
    if "다른구매자들은이렇게평가했어요" in compact:
        return True
    if "999+명" in s and ("만족도" in s or "만족 도" in s):
        return True

    # ── 평가 항목 캐러셀/탭 UI ─────────────────
    if "평가항목보기" in compact:
        return True
    if "항목선택됨" in compact:
        return True
    if "이전평가항목" in compact or "다음평가항목" in compact:
        return True
    if re.search(r"\d+\s*항목\s*선택됨", s):
        return True

    # ── 상단 '구매자 작성 리뷰입니다' 안내(긴 덩어리) ─────────────────
    if "구매하신분들이작성하신리뷰" in compact and len(s) > 60:
        return True

    # ── 스토어 PICK / 판매자 선정 ─────────────────
    if "판매자가직접선정" in compact or "판매자가 직접 선정" in s:
        return True
    if "스토어" in s and "PICK" in s.upper():
        if any(k in s for k in ("선정", "베스트", "리뷰입니다")):
            return True

    return False


def _naver_review_el_is_seller_pick_or_section_header(el: Any, txt: str) -> bool:
    """
    판매자 선정 베스트(스토어 PICK) 또는 상단 리뷰 요약/통계 덩어리는 일반 리뷰가 아님.
    (동일 data-shp-contents-type=\"review\" 마크업이 붙는 경우가 있어 별도 제외)
    """
    s = txt or ""
    if not s.strip():
        return True
    if _naver_text_is_review_tab_chrome_not_user_review(s):
        return True
    if "판매자가 직접 선정" in s:
        return True
    if "스토어" in s and "PICK" in s.upper():
        if "선정" in s or "베스트" in s or "리뷰입니다" in s:
            return True
    # 상단 '상품리뷰 안내 + 건수/포인트 안내' 등 (본문 한 건이 아닌 집계 블록)
    if "구매하신 분들이 작성하신 리뷰" in s and len(s) > 100:
        return True
    if "상품리뷰" in s and "포인트" in s and ("텍스트리뷰" in s.replace(" ", "") or "포토" in s) and len(s) > 180:
        return True

    cur: Any = el
    for _ in range(14):
        if cur is None:
            break
        inv = ((cur.get("data-shp-inventory") or "") + " " + (cur.get("data-shp-area") or "")).lower()
        # 네이버 쇼핑에서 PICK/판매자선정 구역에 붙는 인벤토리 패턴(변경 시 보강)
        if any(
            k in inv
            for k in (
                "storepick",
                "sellerpick",
                "seller_pick",
                "bestreview",
                "best_review",
                "storervpick",
                "sprvpick",
            )
        ):
            return True
        cur = getattr(cur, "parent", None)

    return False


def _naver_review_elements_from_soup(soup: BeautifulSoup) -> list[Any]:
    """리뷰 노드 후보: 우선 a 링크, 없으면 동일 속성의 다른 태그."""
    found = soup.select('a[data-shp-contents-type="review"]')
    if found:
        return found
    return soup.select('[data-shp-contents-type="review"]')


def _extract_naver_store_reviews_from_soup(soup: BeautifulSoup) -> List[ReviewItem]:
    """
    네이버 브랜드/스마트스토어: data-shp-contents-type=\"review\" 기반.
    (내부 마크업 변경 시 휴리스틱 보강 필요)
    """
    out: List[ReviewItem] = []
    seen: set[str] = set()

    for el in _naver_review_elements_from_soup(soup):
        inv_chain = ""
        cur = el
        for _ in range(8):
            if cur is None:
                break
            inv_chain += " " + (cur.get("data-shp-inventory") or "")
            cur = cur.parent
        if "qna" in inv_chain.lower():
            continue

        txt = _extract_naver_store_review_body(el)
        if _naver_review_el_is_seller_pick_or_section_header(el, txt):
            continue
        if len(txt) < 12:
            continue
        if any(b in txt for b in _NAVER_REVIEW_BOILER):
            continue
        key = txt[:220]
        if key in seen:
            continue
        seen.add(key)
        out.append(ReviewItem(text=txt[:2000]))

    return out


def _oliveyoung_open_review_shutter_tab(page: Any) -> bool:
    """리뷰&셔터 탭: href(tab=review) 우선, 실패 시 텍스트로 클릭."""
    try:
        clicked = page.evaluate(
            """
            () => {
              const clickEl = (el) => {
                if (!el) return false;
                el.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true }));
                return true;
              };
              const byHref = document.querySelector('a[href*="tab=review"], a[href*="Tab=review"]');
              if (byHref) {
                byHref.click();
                return 'href';
              }

              const nodes = document.querySelectorAll(
                'a, button, [role="tab"], li, span, div[role="tab"], div[class*="tab" i], div[role="button"]'
              );
              for (const n of nodes) {
                const t = (n.textContent || '').replace(/\\s+/g, ' ').trim();
                if (/리뷰\\s*[&＆]\\s*셔터/.test(t)) {
                  const c = n.closest('a, button, [role="tab"], li') || n;
                  if (clickEl(c)) return 'text';
                }
                if (t.includes('리뷰') && t.includes('셔터') && t.length < 120) {
                  const c = n.closest('a, button, [role="tab"], li') || n;
                  if (clickEl(c)) return 'partial';
                }
              }
              return false;
            }
            """
        )
        return bool(clicked)
    except Exception:
        return False


def _oliveyoung_wait_for_review_dom(page: Any, *, timeout_ms: int = 70000) -> int:
    """#gdasList 등 리뷰 목록 노드가 붙을 때까지 폴링. 반환: li 개수."""
    deadline = time.monotonic() + timeout_ms / 1000.0
    best = 0
    while time.monotonic() < deadline:
        try:
            n = page.evaluate(
                """() => {
                  const root = document.querySelector('#gdasList')
                    || document.querySelector('[id*="gdasList" i]')
                    || document.querySelector('[class*="gdas_list" i]');
                  if (!root) return 0;
                  return root.querySelectorAll('li').length;
                }"""
            )
            cnt = int(n) if n is not None else 0
            best = max(best, cnt)
            if cnt >= 2:
                return cnt
        except Exception:
            pass
        try:
            page.evaluate("window.scrollBy(0, 500)")
        except Exception:
            pass
        page.wait_for_timeout(700)
    return best


def _oliveyoung_scroll_review_area(page: Any) -> None:
    """리뷰 목록 지연 로딩용 스크롤."""
    for _ in range(26):
        try:
            page.evaluate(
                """
                () => {
                  window.scrollBy(0, 520);
                  const g = document.querySelector('#gdasList')
                    || document.querySelector('[id*="gdas"]')
                    || document.querySelector('[class*="gdas_list"]')
                    || document.querySelector('[class*="review_list"]');
                  if (g) {
                    try { g.scrollTop = Math.min(g.scrollTop + 450, g.scrollHeight); } catch (e) {}
                  }
                }
                """
            )
        except Exception:
            pass
        page.wait_for_timeout(140)
    page.wait_for_timeout(800)


def _oliveyoung_prepare_review_panel(page: Any) -> None:
    """
    올리브영: '리뷰&셔터' 탭 → 리뷰 DOM 대기 → 스크롤.
    (requests만으로는 거의 비어 있으므로 Playwright 전용 경로에서 호출)
    """
    _oliveyoung_open_review_shutter_tab(page)
    page.wait_for_timeout(2000)
    _oliveyoung_wait_for_review_dom(page, timeout_ms=75000)
    _oliveyoung_scroll_review_area(page)


def _playwright_prepare_review_tab(page: Any, shopping_url: str) -> None:
    """오늘의집·올리브영 등: 리뷰 탭을 눌러 본문 영역을 띄운 뒤 스크롤."""
    parsed = urlparse(shopping_url)
    host = (parsed.netloc or "").lower()

    if "ohou.se" in host:
        for fn in (
            lambda: page.locator('a[href*="#REVIEW"]').first.click(timeout=6000),
            lambda: page.get_by_role("tab", name=re.compile(r"리뷰")).first.click(timeout=6000),
            lambda: page.locator("a").filter(has_text=re.compile(r"리뷰\s*\d")).first.click(timeout=6000),
        ):
            try:
                fn()
                break
            except Exception:
                continue

    if "oliveyoung.co.kr" in host:
        _oliveyoung_prepare_review_panel(page)

    page.wait_for_timeout(1200)
    try:
        page.evaluate(
            """
            () => {
              const el = document.querySelector('#REVIEW');
              if (el) el.scrollIntoView({ block: 'center', behavior: 'instant' });
              else window.scrollTo(0, Math.floor(document.body.scrollHeight * 0.45));
            }
            """
        )
    except Exception:
        pass
    page.wait_for_timeout(800)


def _naver_click_review_page(page: Any, page_num: int) -> bool:
    """리뷰 영역의 페이지 번호(1·2·3…) 클릭. 여러 pgn 블록이 있을 때 리뷰 쪽만 타겟."""
    if page_num < 1:
        return False
    sel_id = str(page_num)
    js = """
    (targetPage) => {
      const id = String(targetPage);
      const candidates = Array.from(
        document.querySelectorAll('[data-shp-contents-type="pgn"][data-shp-contents-id]')
      );
      const inReview = (el) => {
        if (!el) return false;
        if (el.closest && el.closest('#REVIEW')) return true;
        const inv = (el.getAttribute('data-shp-inventory') || '') +
          (el.closest('[data-shp-inventory]')?.getAttribute('data-shp-inventory') || '');
        if (/review|rtopspick|sprvp/i.test(inv)) return true;
        const area = el.getAttribute('data-shp-area') || '';
        if (/\\.pgn|review|rv/i.test(area)) return true;
        return false;
      };
      for (const el of candidates) {
        if (el.getAttribute('data-shp-contents-id') !== id) continue;
        if (!inReview(el)) continue;
        el.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true }));
        return true;
      }
      for (const el of candidates) {
        if (el.getAttribute('data-shp-contents-id') !== id) continue;
        el.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true }));
        return true;
      }
      return false;
    }
    """
    try:
        return bool(page.evaluate(js, sel_id))
    except Exception:
        return False


def _naver_click_page_number_menubar(page: Any, page_num: int) -> bool:
    """페이지 숫자만 있는 링크(1·2·3…) 클릭 — Locator 미사용."""
    if page_num < 1:
        return False
    try:
        return bool(
            page.evaluate(
                """
                (want) => {
                  const w = String(want);
                  const root = document.querySelector('#REVIEW') || document.body;
                  const links = root.querySelectorAll('a[href="#"], a[role="menuitem"], a');
                  for (const a of links) {
                    const t = (a.textContent || '').trim().replace(/,/g, '');
                    if (t === w) { a.click(); return true; }
                  }
                  return false;
                }
                """,
                str(page_num),
            )
        )
    except Exception:
        return False


def _naver_scroll_review_panel(page: Any) -> None:
    """리뷰가 많은 상품은 지연 로딩되므로 REVIEW 근처를 여러 번 스크롤."""
    try:
        page.evaluate(
            """
            () => {
              const el = document.querySelector('#REVIEW') || document.body;
              el.scrollIntoView({ block: 'start', behavior: 'instant' });
            }
            """
        )
    except Exception:
        pass
    for _ in range(4):
        # Windows 등 일부 환경에서 page.mouse.wheel()이 NotImplementedError를 던질 수 있음
        try:
            page.evaluate("window.scrollBy(0, 900)")
        except Exception:
            pass
        page.wait_for_timeout(600)


def _naver_count_review_nodes(page: Any) -> int:
    try:
        n = page.evaluate(
            """() => document.querySelectorAll('[data-shp-contents-type="review"]').length"""
        )
        return int(n) if n is not None else 0
    except Exception:
        return 0


def _naver_wait_for_review_nodes(page: Any, timeout_ms: int = 90000) -> bool:
    """
    리뷰 DOM이 붙을 때까지 대기.
    wait_for_selector / Locator는 일부 환경에서 NotImplementedError가 나므로 폴링만 사용.
    """
    deadline = time.monotonic() + (timeout_ms / 1000.0)
    while time.monotonic() < deadline:
        if _naver_count_review_nodes(page) > 0:
            return True
        _naver_scroll_review_panel(page)
        page.wait_for_timeout(700)
    return False


def _naver_scroll_until_tabs_visible(page: Any) -> None:
    """상세정보 / 리뷰 / Q&A 탭이 아래쪽에 있을 때 스크롤로 노출."""
    for _ in range(14):
        try:
            page.evaluate("window.scrollBy(0, 400)")
        except Exception:
            pass
        page.wait_for_timeout(300)


def _naver_open_review_tab(page: Any) -> bool:
    """
    두 번째 탭(리뷰 N,NNN건 등) 클릭.
    Locator/role 클릭은 환경에 따라 NotImplementedError가 나므로 evaluate로만 처리.
    """
    _naver_scroll_until_tabs_visible(page)
    try:
        ok = page.evaluate(
            """
            () => {
              const a = document.querySelector('a[href*="#REVIEW"]');
              if (a) { a.click(); return true; }
              const all = document.querySelectorAll('a, button, [role="tab"]');
              for (const n of all) {
                const t = (n.textContent || '').replace(/\\s+/g, ' ').trim();
                if (/^리뷰\\s*[\\d,]*/.test(t) && t.length < 60) { n.click(); return true; }
              }
              return false;
            }
            """
        )
        page.wait_for_timeout(2000)
        return bool(ok)
    except Exception:
        return False


def _naver_ensure_all_reviews_tab(page: Any) -> None:
    """'스토어PICK' 등이 아닌 '전체보기' 탭을 눌러 일반 리뷰 목록을 쓰도록 함."""
    try:
        page.evaluate(
            """
            () => {
              const root = document.querySelector('#REVIEW') || document.body;
              const nodes = root.querySelectorAll('a, button, [role="tab"], [role="menuitem"], span');
              for (const n of nodes) {
                const t = (n.textContent || '').replace(/\\s+/g, ' ').trim();
                if (t === '전체보기' || t.startsWith('전체보기')) {
                  n.click();
                  return true;
                }
              }
              return false;
            }
            """
        )
    except Exception:
        pass
    page.wait_for_timeout(1500)


def _naver_deep_scroll_review_section(page: Any) -> None:
    """#REVIEW 내부·창 스크롤로 지연 로딩되는 일반 리뷰 노드를 더 불러옴."""
    try:
        page.evaluate(
            """
            () => {
              const root = document.querySelector('#REVIEW');
              if (root) {
                for (let i = 0; i < 18; i++) {
                  root.scrollBy(0, 650);
                }
                try { root.scrollTop = root.scrollHeight; } catch (e) {}
              }
              for (let i = 0; i < 12; i++) {
                window.scrollBy(0, 550);
              }
            }
            """
        )
    except Exception:
        pass
    page.wait_for_timeout(900)


def _playwright_goto_relaxed(
    page: Any,
    url: str,
    *,
    timeout_ms: int = 90000,
    soft_load_wait_ms: int = 28000,
) -> Any:
    """
    wait_until='load' 단독 사용 시, 광고·추적 스크립트 때문에 load 이벤트가 늦거나
    사실상 오지 않아 Page.goto가 장시간 타임아웃나는 경우가 있다.
    domcontentloaded까지 확보한 뒤, 가능하면 load만 짧게 추가 대기한다.
    """
    resp = page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
    page.wait_for_timeout(400)
    try:
        page.wait_for_load_state("load", timeout=soft_load_wait_ms)
    except Exception:
        pass
    return resp


def _playwright_collect_naver_store(
    shopping_url: str,
    *,
    max_pages: int,
    max_reviews: int,
) -> tuple[List[ReviewItem], str]:
    """네이버 브랜드/스마트스토어: 리뷰 탭 → 페이지 번호 클릭으로 수집."""
    all_items: List[ReviewItem] = []
    pages_done = 0

    # sync_playwright() 종료 후에 browser/context를 다시 닫으면 예외(NotImplementedError 등)가 날 수 있음.
    # 반드시 with 블록 안에서만 close 한다.
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
            ],
        )
        context = browser.new_context(
            user_agent=CHROME_USER_AGENT,
            locale="ko-KR",
            viewport={"width": 1365, "height": 900},
        )
        try:
            context.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"
            )
            page = context.new_page()
            _playwright_goto_relaxed(page, shopping_url, timeout_ms=120000)
            page.wait_for_timeout(5000)
            page.wait_for_timeout(3000)

            _naver_open_review_tab(page)
            page.wait_for_timeout(2200)
            _naver_ensure_all_reviews_tab(page)
            try:
                page.evaluate(
                    """
                    () => {
                      const el = document.querySelector('#REVIEW');
                      if (el) el.scrollIntoView({ block: 'start', behavior: 'instant' });
                    }
                    """
                )
            except Exception:
                pass
            _naver_deep_scroll_review_section(page)
            _naver_scroll_review_panel(page)
            _naver_wait_for_review_nodes(page, timeout_ms=90000)

            for page_idx in range(1, max(1, max_pages) + 1):
                if page_idx > 1:
                    clicked = _naver_click_review_page(page, page_idx)
                    if not clicked:
                        clicked = _naver_click_page_number_menubar(page, page_idx)
                    if not clicked:
                        break
                    page.wait_for_timeout(2800)
                    _naver_deep_scroll_review_section(page)
                    _naver_wait_for_review_nodes(page, timeout_ms=45000)

                html = page.content()
                soup = BeautifulSoup(html, "html.parser")
                batch = _extract_naver_store_reviews_from_soup(soup)
                if page_idx == 1 and not batch:
                    page.wait_for_timeout(5000)
                    _naver_scroll_review_panel(page)
                    _naver_wait_for_review_nodes(page, timeout_ms=60000)
                    html = page.content()
                    soup = BeautifulSoup(html, "html.parser")
                    batch = _extract_naver_store_reviews_from_soup(soup)
                if batch:
                    all_items.extend(batch)
                    all_items = _dedupe_reviews(all_items)
                pages_done += 1

                if len(all_items) >= max_reviews:
                    break
        finally:
            try:
                context.close()
            except Exception:
                pass
            try:
                browser.close()
            except Exception:
                pass

    all_items = _dedupe_reviews(all_items)[:max_reviews]
    note = f"네이버 스토어 Playwright: {pages_done}페이지, 총 {len(all_items)}건 (판매자 PICK·상단 요약 블록 제외)"
    return all_items, note


def _playwright_collect_oliveyoung(
    shopping_url: str,
    *,
    max_pages: int,
    max_reviews: int,
) -> tuple[List[ReviewItem], str]:
    """
    올리브영: 리뷰는 JS·Shadow DOM·(간헐적) 봇 차단.
    tab=review → 리뷰&셔터 → DOM evaluate(섀도 포함) → 실패 시 BeautifulSoup.

    환경 변수 PLAYWRIGHT_OLIVEYOUNG_HEADED=1 이면 headless=False 로 시도(차단 완화되는 경우 있음).
    """
    all_items: List[ReviewItem] = []
    rounds = 0
    url = _oliveyoung_url_with_tab_review(shopping_url)
    headed = os.environ.get("PLAYWRIGHT_OLIVEYOUNG_HEADED", "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=not headed,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
            ],
        )
        context = browser.new_context(
            user_agent=CHROME_USER_AGENT,
            locale="ko-KR",
            viewport={"width": 1365, "height": 900},
        )
        try:
            context.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"
            )
            page = context.new_page()
            resp = _playwright_goto_relaxed(page, url, timeout_ms=120000)
            if resp is not None and resp.status >= 400:
                note = (
                    f"올리브영 HTTP {resp.status} (자동 수집이 거부됐을 수 있습니다). "
                    "일반 브라우저에서 페이지가 열리는지 확인하거나 붙여넣기를 이용해 주세요."
                )
                return [], note

            page.wait_for_timeout(4500)
            blocked_msg = _oliveyoung_page_access_issue_message(page)
            if blocked_msg:
                return [], f"올리브영: {blocked_msg}"

            _oliveyoung_prepare_review_panel(page)
            try:
                page.evaluate(
                    """
                    () => {
                      const el = document.querySelector('#gdasList, [class*="gdas"]')
                        || document.getElementById('gdasList');
                      if (el) el.scrollIntoView({ block: 'start', behavior: 'instant' });
                      else window.scrollTo(0, Math.floor(document.body.scrollHeight * 0.35));
                    }
                    """
                )
            except Exception:
                pass
            page.wait_for_timeout(1200)

            for _ in range(max(1, max_pages)):
                rounds += 1
                texts = _oliveyoung_extract_review_texts_from_dom(page)
                batch = _review_items_from_oliveyoung_texts(texts)
                if not batch:
                    html = page.content()
                    soup = BeautifulSoup(html, "html.parser")
                    batch = _extract_oliveyoung_reviews(soup)

                if batch:
                    all_items.extend(batch)
                    all_items = _dedupe_reviews(all_items)
                if len(all_items) >= max_reviews:
                    break

                more = False
                try:
                    more = bool(
                        page.evaluate(
                            """
                            () => {
                              const btns = Array.from(document.querySelectorAll('a, button'));
                              for (const b of btns) {
                                const t = (b.textContent || '').trim();
                                if (!t.includes('더보기')) continue;
                                if (b.closest && b.closest('#gdasList')) { b.click(); return true; }
                                if (/gdas|review|list/i.test(b.className || '')) { b.click(); return true; }
                              }
                              return false;
                            }
                            """
                        )
                    )
                except Exception:
                    pass
                if more:
                    page.wait_for_timeout(2200)
                    _oliveyoung_scroll_review_area(page)
                    continue
                try:
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                except Exception:
                    pass
                page.wait_for_timeout(1600)
                if not batch and rounds >= 2:
                    break
        finally:
            try:
                context.close()
            except Exception:
                pass
            try:
                browser.close()
            except Exception:
                pass

    all_items = _dedupe_reviews(all_items)[:max_reviews]
    note = f"올리브영 Playwright: {rounds}회 시도, 총 {len(all_items)}건"
    if not all_items:
        note += (
            ". 0건이면 사이트가 자동화를 차단했거나 마크업이 바뀐 경우입니다. "
            "환경 변수 PLAYWRIGHT_OLIVEYOUNG_HEADED=1 로 재시도하거나 Text Area에 리뷰를 붙여 넣어 주세요."
        )
    return all_items, note


def _is_toun28_host(host: str) -> bool:
    return "toun28.com" in (host or "").lower()


def _toun28_open_review_tab(page: Any) -> None:
    """상품 페이지에서 구매후기 탭·영역으로 전환(Playwright locator 우선, 실패 시 DOM 스캔)."""
    try:
        page.locator('a, button, [role="tab"]').filter(
            has_text=re.compile(r"구매후기\s*\(\s*\d+\s*\)")
        ).first.click(timeout=9000)
        page.wait_for_timeout(1100)
        return
    except Exception:
        pass
    try:
        page.get_by_role("tab", name=re.compile(r"구매후기")).first.click(timeout=7000)
        page.wait_for_timeout(1100)
        return
    except Exception:
        pass
    try:
        page.evaluate(
            """
            () => {
              const nodes = document.querySelectorAll('a, button, [role="tab"], li, span, div, p');
              for (const el of nodes) {
                const t = (el.textContent || '').replace(/\\s+/g, ' ').trim();
                if (/구매후기\\s*\\(\\s*\\d+\\s*\\)/.test(t) || /^구매후기\\s*\\d/.test(t)) {
                  (el.closest('a, button, [role="tab"]') || el).click();
                  return true;
                }
              }
              for (const el of nodes) {
                const t = (el.textContent || '').replace(/\\s+/g, ' ').trim();
                if (t === '구매후기' || (t.startsWith('구매후기') && t.length < 28)) {
                  (el.closest('a, button, [role="tab"]') || el).click();
                  return true;
                }
              }
              return false;
            }
            """
        )
    except Exception:
        pass
    page.wait_for_timeout(900)


def _toun28_click_more_reviews(page: Any) -> bool:
    """모달/리스트의 '후기 더보기' 클릭(톤28 공식몰) — 보이는 요소·스크롤 후 재시도."""
    try:
        btn = page.get_by_text("후기 더보기", exact=True).first
        btn.scroll_into_view_if_needed(timeout=3000)
        btn.click(timeout=4000)
        return True
    except Exception:
        pass
    try:
        btn2 = page.get_by_text(re.compile(r"^\s*후기\s*더보기\s*$")).first
        btn2.scroll_into_view_if_needed(timeout=3000)
        btn2.click(timeout=4000)
        return True
    except Exception:
        pass
    try:
        loc = page.locator("button, a").filter(has_text=re.compile(r"^\s*후기\s*더보기\s*$")).first
        loc.scroll_into_view_if_needed(timeout=3000)
        loc.click(timeout=4000)
        return True
    except Exception:
        pass
    try:
        return bool(
            page.evaluate(
                """
                () => {
                  const cand = Array.from(document.querySelectorAll('a, button, span, div'));
                  for (const el of cand) {
                    const t = (el.textContent || '').replace(/\\s+/g, ' ').trim();
                    if (t === '후기 더보기' || (t.includes('후기') && t.includes('더보기') && t.length <= 22)) {
                      const b = el.closest('button') || el.closest('a') || el;
                      try { b.scrollIntoView({ block: 'center', behavior: 'instant' }); b.click(); return true; } catch (e) {}
                    }
                  }
                  return false;
                }
                """
            )
        )
    except Exception:
        return False


def _toun28_extract_review_texts_from_dom(page: Any) -> List[str]:
    """
    동적 로딩된 리뷰 카드 — innerText 기준 수집(BeautifulSoup만으로는 누락되는 경우가 많음).
    """
    try:
        raw = page.evaluate(
            """
            () => {
              const out = [];
              const seen = new Set();
              const add = (t) => {
                const s = (t || '').replace(/\\s+/g, ' ').trim();
                if (s.length < 28 || seen.has(s)) return;
                const nick = /[가-힣A-Za-z0-9*·\\-]{1,20}\\*\\*님/.test(s);
                const longKo = (s.match(/[가-힣]/g) || []).length >= 18;
                if (!nick && !longKo) return;
                seen.add(s);
                out.push(s);
              };
              const sels = [
                '.box-review', '[class*="box-review"]', '[class*="reviewList"]',
                '[class*="review_list"]', 'article', 'li[class*="review"]'
              ];
              sels.forEach(sel => {
                try {
                  document.querySelectorAll(sel).forEach(el => add(el.innerText || ''));
                } catch (e) {}
              });
              return out.slice(0, 900);
            }
            """
        )
        return [str(x) for x in (raw or []) if x]
    except Exception:
        return []


def _review_items_from_toun28_dom_texts(texts: List[str]) -> List[ReviewItem]:
    seen: set[str] = set()
    out: List[ReviewItem] = []
    for t in texts:
        t = re.sub(r"\s+", " ", (t or "").strip())
        if len(t) < 28 or len(t) > 4500:
            continue
        if t in seen:
            continue
        seen.add(t)
        out.append(ReviewItem(text=t[:2000]))
    return out


def _toun28_scroll_review_area(page: Any) -> None:
    try:
        page.evaluate(
            """
            () => {
              const markers = ['후기', 'review', 'Review', '구매후기'];
              let root = null;
              for (const m of markers) {
                const els = Array.from(document.querySelectorAll('h2, h3, h4, section, div, article'));
                for (const el of els) {
                  if ((el.innerText || '').includes(m) && el.innerText.length < 80) {
                    root = el;
                    break;
                  }
                }
                if (root) break;
              }
              const r = root || document.body;
              for (let i = 0; i < 14; i++) {
                try { r.scrollBy(0, 420); } catch (e) {}
                window.scrollBy(0, 380);
              }
              try { window.scrollTo(0, document.body.scrollHeight); } catch (e) {}
            }
            """
        )
    except Exception:
        pass
    page.wait_for_timeout(500)


def _playwright_collect_toun28(
    shopping_url: str,
    *,
    max_pages: int,
    max_reviews: int,
) -> tuple[List[ReviewItem], str]:
    """
    톤28(renew) 상품: 리뷰 대부분이 '후기 더보기'로 지연 로딩됨.
    requests 초기 HTML에는 베스트 후기 소수만 포함되는 경우가 많음.
    """
    all_items: List[ReviewItem] = []
    clicks = 0
    max_rounds = min(200, max(80, max_pages * 55))

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
            ],
        )
        context = browser.new_context(
            user_agent=CHROME_USER_AGENT,
            locale="ko-KR",
            viewport={"width": 1365, "height": 900},
        )
        try:
            context.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"
            )
            page = context.new_page()
            _playwright_goto_relaxed(page, shopping_url, timeout_ms=120000)
            page.wait_for_timeout(2000)
            try:
                page.wait_for_load_state("networkidle", timeout=20000)
            except Exception:
                pass
            page.wait_for_timeout(1200)
            _toun28_open_review_tab(page)
            page.wait_for_timeout(1500)
            try:
                page.evaluate(
                    "() => { window.scrollTo(0, Math.floor(document.body.scrollHeight * 0.42)); }"
                )
            except Exception:
                pass
            page.wait_for_timeout(600)
            _toun28_scroll_review_area(page)
            page.wait_for_timeout(800)

            prev_n = -1
            stall = 0
            for _ in range(max_rounds):
                html = page.content()
                soup = BeautifulSoup(html, "html.parser")
                batch_soup = _extract_reviews_from_soup(soup, base_url=shopping_url) or []
                batch_dom = _review_items_from_toun28_dom_texts(
                    _toun28_extract_review_texts_from_dom(page)
                )
                batch = _dedupe_reviews(batch_soup + batch_dom)
                if batch:
                    all_items.extend(batch)
                    all_items = _dedupe_reviews(all_items)
                if len(all_items) >= max_reviews:
                    all_items = all_items[:max_reviews]
                    break
                n = len(all_items)
                if n == prev_n:
                    stall += 1
                else:
                    stall = 0
                prev_n = n
                if stall >= 16:
                    break

                try:
                    page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
                except Exception:
                    pass
                page.wait_for_timeout(400)
                clicked = _toun28_click_more_reviews(page)
                if clicked:
                    clicks += 1
                else:
                    _toun28_scroll_review_area(page)
                    page.wait_for_timeout(500)
                    clicked = _toun28_click_more_reviews(page)
                    if clicked:
                        clicks += 1
                    else:
                        stall += 1
                page.wait_for_timeout(1400 if clicked else 800)
        finally:
            try:
                context.close()
            except Exception:
                pass
            try:
                browser.close()
            except Exception:
                pass

    all_items = _dedupe_reviews(all_items)[:max_reviews]
    note = (
        f"톤28 Playwright: 후기 더보기 클릭 {clicks}회, 총 {len(all_items)}건 "
        f"(상한 {max_reviews}건·최대 {max_rounds}회 시도)"
    )
    return all_items, note


def collect_reviews(
    shopping_url: str,
    *,
    max_pages: int = 10,
    max_reviews: int = 300,
) -> tuple[List[ReviewItem], str]:
    """
    쇼핑몰 URL을 받아 리뷰 목록을 반환합니다.

    우선순위:
    1) 네이버 쇼핑 도메인 → 전용 Playwright(리뷰 탭·페이지네이션)
    2) 올리브영 → 전용 Playwright(tab=review·리뷰&셔터·#gdasList 대기·스크롤). 리뷰는 JS 렌더링이라 requests만으로는 보통 비어 있음
    3) 톤28 공식몰 → 전용 Playwright(구매후기 탭·후기 더보기 반복). 초기 HTML만으로는 소수만 파싱되는 경우가 많음
    4) 그 외 → requests 순회 후 파싱, 실패 시 일반 Playwright 폴백
    """
    shopping_url = (shopping_url or "").strip()
    failure_detail = ""

    if shopping_url and shopping_url.lower().startswith(("http://", "https://")):
        try:
            _ensure_playwright_browsers()
        except Exception as e:
            return [], f"Playwright Chromium 준비 실패: {_exc_detail(e)}"
        parsed0 = urlparse(shopping_url)
        host0 = (parsed0.netloc or "").lower()
        # 네이버 브랜드/스마트스토어는 전용 Playwright 경로(리뷰 탭·페이지네이션) 우선
        if _is_naver_store_host(host0):
            try:
                items_n, note_n = _playwright_collect_naver_store(
                    shopping_url, max_pages=max_pages, max_reviews=max_reviews
                )
                if items_n:
                    return items_n, note_n
                failure_detail = f" ({note_n})"
            except Exception as e:
                # Streamlit 화면에는 요약만 나가므로, 원인 추적용으로 터미널에 전체 Traceback 출력
                print("[collect_reviews] 네이버 전용 수집 예외 (아래 Traceback을 복사해 주세요)", flush=True)
                traceback.print_exc()
                failure_detail = f" (네이버 전용 수집 오류: {_exc_detail(e)})"

        if _is_oliveyoung_host(host0):
            try:
                items_oy, note_oy = _playwright_collect_oliveyoung(
                    shopping_url, max_pages=max_pages, max_reviews=max_reviews
                )
                if items_oy:
                    return items_oy, note_oy
                failure_detail = (failure_detail or "") + f" ({note_oy})"
            except Exception as e:
                print("[collect_reviews] 올리브영 전용 수집 예외 (아래 Traceback을 복사해 주세요)", flush=True)
                traceback.print_exc()
                failure_detail = (failure_detail or "") + f" (올리브영 전용 수집 오류: {_exc_detail(e)})"

        if _is_toun28_host(host0):
            try:
                items_t28, note_t28 = _playwright_collect_toun28(
                    shopping_url, max_pages=max_pages, max_reviews=max_reviews
                )
                if items_t28:
                    return items_t28, note_t28
                failure_detail = (failure_detail or "") + f" ({note_t28})"
            except Exception as e:
                print("[collect_reviews] 톤28 전용 수집 예외 (아래 Traceback을 복사해 주세요)", flush=True)
                traceback.print_exc()
                failure_detail = (failure_detail or "") + f" (톤28 전용 수집 오류: {_exc_detail(e)})"

        visited: set[str] = set()
        current_url = shopping_url
        all_items: List[ReviewItem] = []

        pages_fetched = 0
        for _ in range(max(1, max_pages)):
            if current_url in visited:
                break
            visited.add(current_url)

            html = _fetch_html(current_url)
            if not html:
                break

            soup = BeautifulSoup(html, "html.parser")
            if _is_naver_store_host(host0):
                items = _extract_naver_store_reviews_from_soup(soup) or _filter_naver_review_tab_chrome(
                    _extract_reviews_from_soup(soup, base_url=current_url)
                )
            else:
                items = _extract_reviews_from_soup(soup, base_url=current_url)
            if items:
                all_items.extend(items)
                pages_fetched += 1

            all_items = _dedupe_reviews(all_items)
            if len(all_items) >= max_reviews:
                all_items = all_items[:max_reviews]
                break

            next_url = _guess_next_url(soup, current_url)
            if not next_url or next_url in visited:
                break
            current_url = next_url

        all_items = _dedupe_reviews(all_items)
        # 상한까지 채웠으면 즉시 반환. 그 외(동적 더보기로 일부만 잡힌 경우)는 Playwright 폴백으로 이어짐
        if all_items and len(all_items) >= max_reviews:
            note = f"크롤링 성공: {pages_fetched}페이지, 총 {len(all_items)}건"
            return all_items[:max_reviews], note

        # requests 기반으로는 수집이 실패/부족할 수 있어 Playwright 폴백을 시도합니다.
        def extract_reviews_from_playwright(page) -> List[ReviewItem]:
            html = page.content()
            soup2 = BeautifulSoup(html, "html.parser")
            parsed_h = urlparse(shopping_url)
            h2 = (parsed_h.netloc or "").lower()
            if _is_naver_store_host(h2):
                got = _extract_naver_store_reviews_from_soup(soup2)
                if got:
                    return got
                return _filter_naver_review_tab_chrome(
                    _extract_reviews_from_soup(soup2, base_url=shopping_url) or []
                )
            return _extract_reviews_from_soup(soup2, base_url=shopping_url) or []

        def try_click_load_more(page) -> bool:
            # 더보기/리뷰 더보기 계열 텍스트를 우선 시도
            load_more_texts = [
                "후기 더보기",
                "리뷰 더보기",
                "더보기",
                "더 보기",
                "Load more",
                "Load More",
                "Show more",
            ]

            for t in load_more_texts:
                try:
                    loc = page.get_by_role(
                        "button",
                        name=re.compile(re.escape(t), re.IGNORECASE),
                    )
                    if loc.count() > 0 and loc.first.is_visible():
                        loc.first.click()
                        return True
                except Exception:
                    pass

                try:
                    loc2 = page.get_by_text(t, exact=False)
                    if loc2.count() > 0 and loc2.first.is_visible():
                        # 버튼이 아니더라도 텍스트가 포함된 요소를 클릭할 수 있어
                        # 실패하면 다음 후보로 넘어갑니다.
                        loc2.first.click()
                        return True
                except Exception:
                    pass

            return False

        def try_click_next(page) -> bool:
            next_texts = ["다음", "Next", ">", "›", ">>"]
            for t in next_texts:
                try:
                    loc = page.get_by_role(
                        "link",
                        name=re.compile(re.escape(t), re.IGNORECASE),
                    )
                    if loc.count() > 0 and loc.first.is_visible():
                        loc.first.click()
                        return True
                except Exception:
                    pass

                try:
                    loc2 = page.get_by_text(t, exact=False)
                    if loc2.count() > 0 and loc2.first.is_visible():
                        loc2.first.click()
                        return True
                except Exception:
                    pass
            return False

        def playwright_scrape() -> tuple[List[ReviewItem], int]:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(
                    user_agent=CHROME_USER_AGENT,
                    locale="ko-KR",
                )
                page = context.new_page()
                page.goto(shopping_url, wait_until="domcontentloaded", timeout=30000)
                page.wait_for_timeout(1500)
                _playwright_prepare_review_tab(page, shopping_url)

                all_items2: List[ReviewItem] = []
                pages_like = 0
                stable_rounds = 0
                last_count = 0

                for _ in range(max(1, max_pages)):
                    # 현재 DOM 기반으로 최대한 수집
                    extracted = extract_reviews_from_playwright(page)
                    if extracted:
                        all_items2.extend(extracted)
                        all_items2 = _dedupe_reviews(all_items2)

                    if len(all_items2) >= max_reviews:
                        break

                    pages_like += 1
                    new_count = len(all_items2)
                    if new_count == last_count:
                        stable_rounds += 1
                    else:
                        stable_rounds = 0
                    last_count = new_count

                    if stable_rounds >= 3:
                        break

                    # 1) 더보기 클릭 시도
                    clicked = try_click_load_more(page)
                    if clicked:
                        page.wait_for_timeout(1500)
                        continue

                    # 2) 페이지네이션(다음) 클릭 시도
                    clicked2 = try_click_next(page)
                    if clicked2:
                        page.wait_for_timeout(1500)
                        continue

                    # 3) 그래도 안되면 무한스크롤 바닥으로 이동
                    try:
                        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        page.wait_for_timeout(1500)
                    except Exception:
                        pass

                context.close()
                browser.close()
                return all_items2, pages_like

        try:
            items2, pages_like = playwright_scrape()
            items2 = _dedupe_reviews(items2)[:max_reviews]
            if items2:
                merged = _dedupe_reviews((all_items or []) + items2)[:max_reviews]
                note = f"Playwright 폴백 성공: {pages_like}회 시도, 총 {len(merged)}건"
                if all_items:
                    note += f" (requests 선수집 {len(all_items)}건과 병합)"
                return merged, note
        except Exception:
            pass

    # 요청은 되었지만 파싱/네비게이션/폴백 모두 실패

    # 자동 데모 폴백은 실제 URL 분석 결과를 왜곡하므로 비활성화합니다.
    # (필요 시 사용자가 Text Area로 직접 리뷰를 붙여넣어 분석)
    return [], (
        "실사이트 리뷰 수집 실패: 크롤링 차단/동적 렌더링/선택자 불일치 가능성이 있습니다."
        + (failure_detail or "")
    )


def reviews_to_plain_text(items: List[ReviewItem]) -> str:
    """Gemini 입력용 텍스트 블록."""
    lines = []
    for i, r in enumerate(items, 1):
        meta = []
        if r.rating is not None:
            meta.append(f"별점 {r.rating}/5")
        if r.author:
            meta.append(r.author)
        head = f"[리뷰 {i}]"
        if meta:
            head += " (" + ", ".join(meta) + ")"
        lines.append(f"{head}\n{r.text}")
    return "\n\n".join(lines)
