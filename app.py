# -*- coding: utf-8 -*-
"""
리뷰 기반 광고 소구점 자동 추출기 — Streamlit 대시보드

【 API 키 넣는 위치 】
──────────────────────────────────────────────────────────────────
이 파일을 수정해서 키를 넣지 마세요. (코드에 키가 남을 수 있습니다)

방법 B) 권장: 프로젝트 폴더에 `.streamlit` 폴더를 만들고 그 안에 `secrets.toml` 파일을 만듭니다.

    예시 경로 (Windows):
    c:\\Users\\본인계정\\...\\커서ai test\\.streamlit\\secrets.toml

    secrets.toml 내용 예:
    GOOGLE_API_KEY = "여기에 Google AI Studio에서 복사한 키를 붙여넣기"

    → Streamlit은 실행 시 자동으로 이 값을 읽습니다. (코드에 키를 안 남김)

방법 C) Windows 환경 변수에 `GOOGLE_API_KEY` 를 등록해도 됩니다.

Google AI Studio API 키 발급: https://aistudio.google.com/apikey

【 Vertex AI (GCP 크레딧) 】
──────────────────────────────────────────────────────────────────
- `pip install google-cloud-aiplatform`
- Vertex 요금은 GCP 프로젝트(결제·크레딧)에서 차감됩니다. AI Studio API 키와 과금이 다릅니다.
- 인증: `gcloud auth application-default login` 또는 서비스 계정 JSON 후
  `GOOGLE_APPLICATION_CREDENTIALS` 환경 변수.
- secrets / 환경 변수: `VERTEX_PROJECT_ID`, `VERTEX_LOCATION`(예: us-central1, asia-northeast3)

【 워드맵 색상 】
──────────────────────────────────────────────────────────────────
- 긍·부·중 색은 내장 사전 없이 Gemini로 키워드(명사·형용사)마다 단어 자체 극성을 분류합니다.
──────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import asyncio
import html
import json
import math
import os
import random
import re
import statistics
import sys
import textwrap
from collections import Counter
from typing import Any, Optional

# Windows + Streamlit: 기본 이벤트 루프가 subprocess 미지원이면 Playwright(sync)가
# asyncio.create_subprocess_exec 단계에서 NotImplementedError를 낸다.
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

import plotly.graph_objects as go
import streamlit as st

from google import genai
from google.genai import types as genai_types

from review_collector import ReviewItem, collect_reviews, expand_merged_review_items

# 브라우저 탭 제목(상단 탭). 여기만 바꾼 뒤 저장하고 새로고침. set_page_config + document.title 동기화용.
APP_PAGE_TITLE = "리뷰 기반 광고 인사이트 대시보드 by jiwoo"

# ---------------------------------------------------------------------------
# 8가지 마케팅 소구점 (Gemini 프롬프트·UI 라벨과 동일하게 유지)
# ---------------------------------------------------------------------------
ANGLE_DEFS: list[tuple[str, str]] = [
    ("efficacy", "① 효능 중심 — 문제/상황이 실제로 좋아짐"),
    ("pain_avoidance", "② 고통 회피 — 불편/스트레스가 줄어듦"),
    ("value_efficiency", "③ 가성비/효율 — 비용·시간·노력이 절감됨"),
    ("convenience", "④ 사용 편의성 — 쓰는 과정이 쉽고 마무리가 깔끔함"),
    ("social_proof", "⑤ 사회적 증거 — 대세감과 신뢰도"),
    ("ingredients_tech", "⑥ 성분/기술력 — 근거 있는 소재·기술"),
    ("emotion_experience", "⑦ 감성/경험 — 사용 중 기분/분위기/브랜드 체감"),
    ("settler", "⑧ 유목민 정착 — 실패 끝에 맞는 해결책을 찾음"),
]

ANGLE_IDS = [a[0] for a in ANGLE_DEFS]
ANGLE_LABEL_BY_ID = {a[0]: a[1] for a in ANGLE_DEFS}
ANGLE_ID_SET = frozenset(ANGLE_IDS)
# UI 라벨 앞 번호(①…) → 내부 id (모델이 라벨 문자열을 줄 때 대비)
_ANGLE_CIRCLE_TO_ID: dict[str, str] = {
    "①": "efficacy",
    "②": "pain_avoidance",
    "③": "value_efficiency",
    "④": "convenience",
    "⑤": "social_proof",
    "⑥": "ingredients_tech",
    "⑦": "emotion_experience",
    "⑧": "settler",
}


def _coerce_mentioned_angle_ids(raw: Any) -> list[Any]:
    """
    Gemini가 mentioned_angle_ids를 배열이 아니라 한 문자열로 줄 때가 있음.
    그대로 for 문에 넣으면 글자 단위 순회되어 집계가 전부 0이 됨 → 리스트로 통일.
    """
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return []
        if s.startswith("["):
            try:
                j = json.loads(s)
                if isinstance(j, list):
                    return j
            except json.JSONDecodeError:
                pass
        parts = [p.strip() for p in re.split(r"[,;|]", s) if p.strip()]
        if len(parts) > 1:
            return parts
        # "efficacy pain_avoidance" 같이 공백만 구분된 경우
        if " " in s and "," not in s and ";" not in s and "|" not in s:
            sp = [p.strip() for p in s.split() if p.strip()]
            if len(sp) > 1:
                return sp
        return [s]
    if isinstance(raw, (int, float)):
        return [raw]
    return []


def _normalize_angle_id(raw: Any) -> str | None:
    """내부 소구점 키(efficacy 등)로만 맞춤."""
    if raw is None:
        return None
    if isinstance(raw, bool):
        return None
    if isinstance(raw, int):
        if 1 <= raw <= len(ANGLE_IDS):
            return ANGLE_IDS[raw - 1]
        return None
    if isinstance(raw, float) and raw.is_integer():
        return _normalize_angle_id(int(raw))
    s = str(raw).strip()
    if not s:
        return None
    if s in ANGLE_ID_SET:
        return s
    sl = s.lower().replace("-", "_")
    if sl in ANGLE_ID_SET:
        return sl
    if s.isdigit():
        n = int(s)
        if 1 <= n <= len(ANGLE_IDS):
            return ANGLE_IDS[n - 1]
    for ch, aid in _ANGLE_CIRCLE_TO_ID.items():
        if ch in s:
            return aid
    return None


# LLM이 mentioned_angle_ids를 비워 둘 때 리뷰 본문으로 소구점 건수 보조(키워드 스코어)
_ANGLE_HEURISTIC_KEYWORDS: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "efficacy",
        (
            "효과",
            "효능",
            "나아",
            "개선",
            "촉촉",
            "보습",
            "흡수",
            "탄력",
            "잡티",
            "안색",
            "좋아졌",
            "차오",
            "광채",
            "속건조",
            "건조",
            "피부",
        ),
    ),
    (
        "pain_avoidance",
        (
            "자극",
            "불편",
            "트러블",
            "뾰루지",
            "가려",
            "따가",
            "번거",
            "스트레스",
            "불쾌",
            "냄새",
            "아프",
            "따끔",
        ),
    ),
    (
        "value_efficiency",
        (
            "가격",
            "가성비",
            "저렴",
            "세일",
            "할인",
            "용량",
            "양이",
            "대용량",
            "절약",
            "돈",
        ),
    ),
        (
            "convenience",
            (
                "편리",
                "편하",
                "발림",
                "간편",
                "쉽",
                "휴대",
                "뚜껑",
                "용기",
                "발라",
                "바르",
            ),
        ),
    (
        "social_proof",
        (
            "유명",
            "인기",
            "추천",
            "후기",
            "입소문",
            "대세",
            "베스트",
            "재구매",
            "단골",
            "리뷰",
        ),
    ),
    (
        "ingredients_tech",
        (
            "성분",
            "원료",
            "기술",
            "특허",
            "무첨가",
            "저자극",
            "약산성",
            "비건",
            "인증",
            "함유",
        ),
    ),
    (
        "emotion_experience",
        (
            "향",
            "분위기",
            "기분",
            "패키지",
            "디자인",
            "고급",
            "예쁘",
            "감성",
            "선물",
            "만족",
        ),
    ),
    (
        "settler",
        (
            "여러",
            "다 써보",
            "찾",
            "맞는",
            "드디어",
            "포기",
            "실패",
            "애쓰",
            "겨우",
            "계속",
        ),
    ),
)


def _heuristic_angle_ids_for_review_text(text: str) -> list[str]:
    """본문에 키워드가 있으면 해당 소구점 id를 스코어 상위 몇 개만 반환."""
    t = (text or "").lower()
    if not t.strip():
        return []
    scores: dict[str, int] = {aid: 0 for aid in ANGLE_IDS}
    for aid, kws in _ANGLE_HEURISTIC_KEYWORDS:
        for kw in kws:
            if kw in t:
                scores[aid] += 1
    ranked = sorted(scores.items(), key=lambda x: (-x[1], x[0]))
    out = [aid for aid, sc in ranked if sc > 0][:4]
    return out


DEFAULT_MODEL_NAME = "gemini-2.5-flash"
CHUNK_MAX_CHARS_DEFAULT = 12000


def normalize_gemini_model_name(model_name: str) -> str:
    """
    google-genai는 model 파라미터에 'models/' 접두사를 넣지 않는 형태를 기대합니다.
    사용자가 실수로 'models/gemini-...'를 넣었을 때를 방지합니다.
    """
    m = (model_name or "").strip()
    if not m:
        return DEFAULT_MODEL_NAME
    # 사용자가 'models/gemini-1.5-pro'처럼 넣었을 때 방지
    if m.startswith("models/"):
        m = m[len("models/") :].strip()
    return m


def _extract_first_json_object(text: str) -> dict[str, Any]:
    """
    Gemini 응답이 코드블록 등으로 감싸진 경우를 대비해 첫 JSON 객체를 뽑습니다.
    """
    raw = (text or "").strip()
    if not raw:
        raise json.JSONDecodeError("empty response", raw, 0)

    # ```json ... ``` 형태 제거
    if raw.startswith("```"):
        # 코드펜스 시작 제거
        raw = raw.split("\n", 1)[-1]
    if raw.endswith("```"):
        raw = raw[: -3].strip()

    # 1) 응답 전체가 JSON인 경우
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass

    # 2) 첫 '{'부터, JSON 문자열(따옴표/이스케이프)까지 고려해 균형 잡힌 '}'까지 추출
    start = raw.find("{")
    if start == -1:
        raise json.JSONDecodeError("no '{' found for json object", raw, 0)

    in_str = False
    escape = False
    depth = 0
    end = None

    for i in range(start, len(raw)):
        ch = raw[i]
        if in_str:
            if escape:
                escape = False
                continue
            if ch == "\\":
                escape = True
                continue
            if ch == '"':
                in_str = False
                continue
            continue

        if ch == '"':
            in_str = True
            continue

        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                end = i
                break

    if end is None:
        # 끝까지 균형을 못 맞추면, 기존처럼 마지막 '}'까지라도 시도(최후 수단)
        last = raw.rfind("}")
        if last == -1:
            raise json.JSONDecodeError("unterminated json object", raw, 0)
        candidate = raw[start : last + 1]
    else:
        candidate = raw[start : end + 1]

    return json.loads(candidate)


# 워드맵: Kiwi — 명사(NNG·NNP) + 용언(VV·VA) + NNG+XSV 합성(노력하다 등) 집계
_KIWI_INSTANCE = None
# 용언 어미 꼬리(하나의 용언에 붙은 것으로 보고 소구 이후 토큰만 스킵)
_WORDMAP_VERB_TAIL_TAGS = frozenset({"EP", "EC", "EF", "ETN", "ETM"})
# 명사 1글자 허용(의미 단위가 짧아도 쓰는 경우)
_WORDMAP_SHORT_NOUN_ALLOW = frozenset({"잠", "꿈", "숨", "빛"})
# 용언 사전형(lemma)으로 두면 의미가 거의 없는 보조·동형 반복
_WORDMAP_LEMMA_STOP = frozenset(
    {
        "같다",
        "하다",
        "있다",
        "없다",
        "싶다",
    }
)
# 목적어(명사)+이/가 뒤에 오는 준동사류 — 단독 '나다' 대신 '거품 나다' 등으로 집계
_WORDMAP_LIGHT_VERB_AFTER_NOUN = frozenset(
    {
        "나다",
        "나오다",
    }
)
_EF_DA_TOKEN = None  # kiwi.join(NNG+XSV+_) 사전형용 '다' EF 토큰 캐시
# build_wordmap_keywords 직후 UI에서 참고(폴백 여부)
WORDMAP_EXTRACT_MODE: str = "kiwi"


def _get_kiwi():
    global _KIWI_INSTANCE
    if _KIWI_INSTANCE is None:
        from kiwipiepy import Kiwi

        _KIWI_INSTANCE = Kiwi()
    return _KIWI_INSTANCE


def _get_ef_da_token(kiwi: Any) -> Any:
    """'노력하다' 분석 결과의 마지막 EF(다) — NNG+XSV+다 사전형 join에 재사용."""
    global _EF_DA_TOKEN
    if _EF_DA_TOKEN is None:
        toks = list(kiwi.tokenize("노력하다"))
        _EF_DA_TOKEN = toks[-1]
    return _EF_DA_TOKEN


_WORDMAP_EN_TOKEN_RE = re.compile(r"[A-Za-z]+(?:'[a-z]+)?")


def _wordmap_english_stopwords() -> set[str]:
    """
    영어 워드맵용 불용어: be동사·조동사·관사·대부분 전치사/접속사·대명사 등
    (내용어 명사·형용사·동사는 남김).
    """
    return {
        # be
        "am",
        "is",
        "are",
        "was",
        "were",
        "be",
        "been",
        "being",
        "aint",
        # have / do (조동사)
        "has",
        "had",
        "having",
        "do",
        "does",
        "did",
        "doing",
        "done",
        # 조동사
        "will",
        "would",
        "shall",
        "should",
        "can",
        "could",
        "may",
        "might",
        "must",
        "need",
        # 관사
        "a",
        "an",
        "the",
        # 대명사·지시어
        "i",
        "you",
        "he",
        "she",
        "it",
        "we",
        "they",
        "me",
        "him",
        "her",
        "us",
        "them",
        "my",
        "your",
        "his",
        "its",
        "our",
        "their",
        "mine",
        "yours",
        "hers",
        "ours",
        "theirs",
        "this",
        "that",
        "these",
        "those",
        "who",
        "whom",
        "whose",
        "which",
        "what",
        "whatever",
        "whoever",
        "where",
        "when",
        "why",
        "how",
        "there",
        "here",
        # 접속·절
        "and",
        "or",
        "but",
        "nor",
        "if",
        "because",
        "although",
        "though",
        "while",
        "unless",
        "until",
        "since",
        "so",
        "than",
        "then",
        "that",
        # 부정·양태
        "not",
        "no",
        "yes",
        "very",
        "just",
        "only",
        "also",
        "too",
        "really",
        "quite",
        "even",
        "still",
        "already",
        "ever",
        "never",
        "always",
        "often",
        "sometimes",
        "maybe",
        "perhaps",
        # 전치사(의미 없는 연결)
        "at",
        "by",
        "for",
        "from",
        "in",
        "into",
        "of",
        "off",
        "on",
        "onto",
        "out",
        "over",
        "up",
        "down",
        "with",
        "without",
        "about",
        "above",
        "after",
        "against",
        "before",
        "below",
        "between",
        "beyond",
        "during",
        "across",
        "around",
        "through",
        "toward",
        "towards",
        "under",
        "upon",
        "via",
        "within",
        "as",
        "to",
        # 기타 기능어
        "all",
        "any",
        "both",
        "each",
        "every",
        "few",
        "more",
        "most",
        "other",
        "some",
        "such",
        "same",
        "another",
        "either",
        "neither",
        "own",
        "noone",
        "nobody",
        "nothing",
        "nowhere",
        "anyone",
        "anything",
        "everyone",
        "everything",
        "somewhere",
        "someone",
        "something",
        # 반복·쇼핑 UI에 자주 붙는 단어
        "review",
        "reviews",
        "product",
        "products",
        "buyer",
        "verified",
        "read",
        "more",
        "less",
        "show",
        "sort",
        "filter",
        # 축약(아포스트로피 제거 후)
        "dont",
        "doesnt",
        "didnt",
        "wont",
        "cant",
        "couldnt",
        "wouldnt",
        "shouldnt",
        "isnt",
        "arent",
        "wasnt",
        "werent",
        "havent",
        "hasnt",
        "hadnt",
        "im",
        "ive",
        "ill",
        "youre",
        "youve",
        "youll",
        "theyre",
        "weve",
        "its",
        "thats",
        "whats",
    }


def _count_wordmap_english(text: str, stopwords: set[str], counter: Counter[str]) -> None:
    """영어 리뷰: 단어 단위 빈도(소문자). Apostrophe는 제거해 dont 등과 매칭."""
    raw = (text or "").strip()
    if not raw:
        return
    for m in _WORDMAP_EN_TOKEN_RE.finditer(raw):
        tok = m.group(0).lower().replace("'", "")
        if len(tok) < 2 or tok.isdigit() or tok in stopwords:
            continue
        counter[tok] += 1


def _wordmap_stopwords() -> set[str]:
    return {
        "그리고",
        "그냥",
        "정말",
        "진짜",
        "너무",
        "조금",
        "많이",
        "이거",
        "이건",
        "있어요",
        "같네요",
        "같습니다",
        "합니다",
        "입니다",
        "제품",
        "상품",
        "사용",
        "사용감",
        "구매",
        "재구매",
        "배송",
        "포장",
        "오늘의집",
        # 쇼핑몰 리뷰 UI·미디어 배지가 본문에 섞일 때 자주 나오는 명사(제품 속성과 무관한 경우가 많음)
        "비디오",
        "동영상",
        "영상",
        "사진",
        "포토",
        "썸네일",
        "평점",
        "별점",
        "한줄평",
    }


def _wordmap_accept_noun(w: str, stopwords: set[str]) -> bool:
    if not w or w.isdigit() or w in stopwords:
        return False
    if len(w) >= 2:
        return True
    return w in _WORDMAP_SHORT_NOUN_ALLOW


def _wordmap_accept_verb_lemma(w: str, stopwords: set[str]) -> bool:
    if not w or w.isdigit() or w in stopwords:
        return False
    if w in _WORDMAP_LEMMA_STOP:
        return False
    return len(w) >= 2


def _skip_trailing_verb_morphemes(tokens: list[Any], k: int, n: int) -> int:
    while k < n and tokens[k].tag in _WORDMAP_VERB_TAIL_TAGS:
        k += 1
    return k


def _wordmap_try_noun_jks_light_verb(
    tokens: list[Any],
    i: int,
    n: int,
    stopwords: set[str],
    counter: Counter[str],
) -> int | None:
    """
    NNG/NNP + JKS(이/가) + (부사*) + VV(lemma ∈ LIGHT_VERB) → '명사 용언' 한 키워드.
    예: 거품이 나다 → '거품 나다', 향기가 나오다 → '향기 나오다'
    """
    if i >= n or tokens[i].tag not in ("NNG", "NNP"):
        return None
    j = i + 1
    if j >= n or tokens[j].tag != "JKS":
        return None
    j += 1
    while j < n and tokens[j].tag in ("MAG", "MM", "MAJ"):
        j += 1
    if j >= n or tokens[j].tag != "VV":
        return None
    lemma = (getattr(tokens[j], "lemma", None) or tokens[j].form or "").strip()
    if lemma not in _WORDMAP_LIGHT_VERB_AFTER_NOUN:
        return None
    noun = (tokens[i].form or "").strip()
    if not _wordmap_accept_noun(noun, stopwords):
        return None
    phrase = f"{noun} {lemma}"
    if len(phrase.replace(" ", "")) < 3:
        return None
    counter[phrase] += 1
    return _skip_trailing_verb_morphemes(tokens, j + 1, n)


def _count_wordmap_kiwi(text: str, stopwords: set[str], counter: Counter[str]) -> None:
    """
    NNG·NNP(명사), VV·VA(용언 lemma), NNG(+XPN)*+XSV 합성(노력+하→노력하다) 집계.
    보조 용언 VX·의존 어미 등은 단독 키워드로 넣지 않음.
    """
    kiwi = _get_kiwi()
    raw = (text or "").strip()
    if not raw:
        return
    tokens = list(kiwi.tokenize(raw))
    n = len(tokens)
    ef_da = _get_ef_da_token(kiwi)
    i = 0
    while i < n:
        jumped = _wordmap_try_noun_jks_light_verb(tokens, i, n, stopwords, counter)
        if jumped is not None:
            i = jumped
            continue

        t = tokens[i]

        # (XPN|NNG)+ + XSV → 사전형(…하다/…되다 등)으로 합쳐 1키워드
        if t.tag in ("XPN", "NNG"):
            j = i
            while j < n and tokens[j].tag in ("XPN", "NNG"):
                j += 1
            if j < n and tokens[j].tag == "XSV":
                head = tokens[i : j + 1]
                try:
                    w = kiwi.join(head + [ef_da])
                except Exception:
                    w = kiwi.join(head)
                if _wordmap_accept_verb_lemma(w, stopwords):
                    counter[w] += 1
                i = _skip_trailing_verb_morphemes(tokens, j + 1, n)
                continue

        if t.tag == "VV":
            lemma = (getattr(t, "lemma", None) or t.form or "").strip()
            # '나다' 등은 보통 명사+이/가와 묶어 위에서 집계 — 단독은 제외(의미 단위 분리 방지)
            if lemma in _WORDMAP_LIGHT_VERB_AFTER_NOUN:
                i += 1
                continue
            if _wordmap_accept_verb_lemma(lemma, stopwords):
                counter[lemma] += 1
            i += 1
            continue

        if t.tag == "VA":
            lemma = (getattr(t, "lemma", None) or t.form or "").strip()
            if _wordmap_accept_verb_lemma(lemma, stopwords):
                counter[lemma] += 1
            i += 1
            continue

        # 보조 용언(VX 등) — 단독 키워드로 쓰지 않음 (예: 싶다)
        if t.tag == "VX":
            i += 1
            continue

        if t.tag in ("NNG", "NNP"):
            w = (t.form or "").strip()
            if _wordmap_accept_noun(w, stopwords):
                counter[w] += 1
            i += 1
            continue

        i += 1


def _count_tokens_regex(text: str, stopwords: set[str], counter: Counter[str]) -> None:
    token_re = re.compile(r"[가-힣A-Za-z0-9]{2,}")
    txt = (text or "").strip().lower()
    if not txt:
        return
    for tok in token_re.findall(txt):
        if tok.isdigit() or tok in stopwords:
            continue
        counter[tok] += 1


def _auto_select_wordmap_keywords(ranked: list[tuple[str, int]]) -> list[tuple[str, int]]:
    """
    최고 빈도 대비 상대적으로 약한 꼬리는 빼고, 의미 있게 반복된 키워드만 남깁니다.
    개수는 고정하지 않고 분포에 맞춥니다(대략 4~14개).
    """
    if not ranked:
        return []
    f_max = ranked[0][1]
    # 1회만 나온 단어가 잔뜩이면 상위 몇 개만
    if f_max <= 1:
        return ranked[: min(8, len(ranked))]
    # 상위 빈도의 ~16% 미만이면 '중요 반복'으로 보기 어렵다고 보고 제외 (최소 2회)
    floor = max(2, int(f_max * 0.16 + 0.5))
    picked = [(w, f) for w, f in ranked if f >= floor]
    hard_min, hard_max = 4, 14
    if len(picked) < hard_min:
        picked = ranked[: min(hard_min, len(ranked))]
    if len(picked) > hard_max:
        picked = picked[:hard_max]
    return picked


def build_wordmap_keywords(items: list[ReviewItem]) -> list[tuple[str, int]]:
    """
    리뷰 원문에서 반복 등장 단어를 빈도 기반으로 추출합니다.
    - 영어 리뷰가 주이면: 영어 단어만(소문자), be동사·관사·대명사 등 불용어 제외.
    - 그 외: Kiwi로 명사·용언 등(한국어), 실패 시 정규식 폴백.
    표시 개수는 빈도 분포로 자동 결정합니다.
    """
    if not items:
        return []

    global WORDMAP_EXTRACT_MODE

    counter: Counter[str] = Counter()

    if _reviews_primarily_english(items):
        WORDMAP_EXTRACT_MODE = "english"
        en_sw = _wordmap_english_stopwords()
        for it in items:
            _count_wordmap_english(it.text or "", en_sw, counter)
    else:
        stopwords = _wordmap_stopwords()
        try:
            _get_kiwi()
        except ImportError:
            WORDMAP_EXTRACT_MODE = "regex"
            for it in items:
                _count_tokens_regex(it.text or "", stopwords, counter)
        else:
            WORDMAP_EXTRACT_MODE = "kiwi"
            for it in items:
                _count_wordmap_kiwi(it.text or "", stopwords, counter)

    if not counter:
        return []

    ranked = counter.most_common(60)
    return _auto_select_wordmap_keywords(ranked)


def _wordmap_half_extents(words: list[str], sizes: list[float]) -> tuple[list[float], list[float]]:
    """
    플롯 정규화 좌표(0~1)에서 각 라벨의 반가로·반세로.
    Plotly 실제 글자 박스보다 약간 크게 잡아 겹침을 줄임(큰 폰트일수록 여유↑).
    """
    hws: list[float] = []
    hhs: list[float] = []
    for i in range(len(words)):
        s = max(10.0, sizes[i])
        w = words[i]
        nch = max(1, len(w))
        # 가로: 글자 수·폰트 크기에 비례(한글 가로폭 반영)
        hw = 0.0142 * (s / 22.0) * (1.18 + 0.41 * nch)
        hh = 0.0122 * (s / 22.0)
        sz_boost = 1.0 + 0.12 * max(0.0, min(1.2, (s - 18.0) / 32.0))
        # Plotly 텍스트 박스보다 넉넉히 잡아 겹침 완화(특히 큰 글자·긴 키워드)
        hws.append(hw * sz_boost * 1.16)
        hhs.append(hh * sz_boost * 1.12)
    return hws, hhs


def _wordmap_label_mass(sizes: list[float], i: int) -> float:
    """큰 키워드가 더 ‘무거워’ 밀어낼 때 덜 움직이도록 하는 가중치."""
    return max(11.0, float(sizes[i])) ** 1.75


def _wordmap_aabb_overlap(
    x1: float,
    y1: float,
    hw1: float,
    hh1: float,
    x2: float,
    y2: float,
    hw2: float,
    hh2: float,
    gap: float,
) -> bool:
    """두 축정렬 사각형(중심·반크기)이 gap을 두고도 겹치면 True."""
    return abs(x1 - x2) < hw1 + hw2 + gap and abs(y1 - y2) < hh1 + hh2 + gap


def _wordmap_pair_min_gap(sizes: list[float], i: int, j: int, base_gap: float) -> float:
    """
    주변 키워드 폰트 크기가 클수록 박스 간 최소 여백을 넓혀 겹침·가독성을 맞춤.
    base_gap은 평균 크기(약 22pt) 기준 최소값.
    """
    si = max(10.0, float(sizes[i]))
    sj = max(10.0, float(sizes[j]))
    m = 0.5 * (si + sj)
    ref = 44.0  # 대략 중간~큰 글자 기준
    scale = 0.78 + 0.52 * (m / ref) ** 1.15
    return base_gap * scale


def _wordmap_relax_aabb_overlaps(
    xs: list[float],
    ys: list[float],
    hws: list[float],
    hhs: list[float],
    sizes: list[float],
    *,
    gap: float = 0.022,
    iterations: int = 320,
    margin: float = 0.028,
) -> tuple[list[float], list[float]]:
    """
    겹치는 라벨을 축 방향으로 분리. 질량(폰트 크기 기반)이 큰 키워드는 덜 움직이고
    주변 작은 키워드가 더 밀려남(‘큰 단어가 자리를 차지’하는 느낌).
    """
    n = len(xs)
    if n < 2:
        return xs, ys

    xo = list(xs)
    yo = list(ys)
    masses = [_wordmap_label_mass(sizes, k) for k in range(n)]

    for it in range(iterations):
        moved = False
        for i in range(n):
            for j in range(i):
                gij = _wordmap_pair_min_gap(sizes, i, j, gap)
                if not _wordmap_aabb_overlap(
                    xo[i], yo[i], hws[i], hhs[i], xo[j], yo[j], hws[j], hhs[j], gij
                ):
                    continue
                dx = abs(xo[i] - xo[j])
                dy = abs(yo[i] - yo[j])
                need_x = hws[i] + hws[j] + gij
                need_y = hhs[i] + hhs[j] + gij
                pen_x = need_x - dx
                pen_y = need_y - dy
                if pen_x <= 0 or pen_y <= 0:
                    continue
                mi, mj = masses[i], masses[j]
                inv = 1.0 / (mi + mj)
                # 관통이 짧은 축부터 완전히 벌림 + 큰 질량이 적게 이동
                if pen_x <= pen_y:
                    sep = pen_x * 0.55 + 2e-4
                    wi, wj = sep * mj * inv, sep * mi * inv
                    if xo[i] >= xo[j]:
                        xo[i] += wi
                        xo[j] -= wj
                    else:
                        xo[i] -= wi
                        xo[j] += wj
                else:
                    sep = pen_y * 0.55 + 2e-4
                    wi, wj = sep * mj * inv, sep * mi * inv
                    if yo[i] >= yo[j]:
                        yo[i] += wi
                        yo[j] -= wj
                    else:
                        yo[i] -= wi
                        yo[j] += wj
                moved = True
        for k in range(n):
            xo[k] = min(1.0 - margin, max(margin, xo[k]))
            yo[k] = min(1.0 - margin, max(margin, yo[k]))
        if not moved:
            break
        # 후반부에 미세 진동으로 국소 최소에 걸린 겹침 풀기
        if it > iterations // 2 and moved and it % 45 == 0:
            for k in range(n):
                xo[k] += (hash((k, it)) % 7 - 3) * 1.2e-4
                yo[k] += (hash((k + 3, it)) % 7 - 3) * 1.2e-4

    return xo, yo


def _wordmap_scatter_positions(words: list[str], sizes: list[float]) -> tuple[list[float], list[float]]:
    """
    빈도 높은 단어를 먼저 중심에 두고, 나선·무작위로 산발 배치.
    텍스트는 가로로 길어 원형 거리만으로는 겹침이 남으므로 축정렬 사각형(AABB)으로 충돌 검사.
    """
    n = len(words)
    if n == 0:
        return [], []
    if n != len(sizes):
        raise ValueError("words and sizes length mismatch")

    hws, hhs = _wordmap_half_extents(words, sizes)
    box_gap = 0.016

    rnd = random.Random(hash(tuple(words)) % (2**32))

    xs: list[float] = [0.5]
    ys: list[float] = [0.5]
    if n == 1:
        return xs, ys

    margin = 0.042
    golden = 2.39996322972865332  # ~137.5°

    def fits_at(i: int, x: float, y: float) -> bool:
        for j in range(i):
            if _wordmap_aabb_overlap(x, y, hws[i], hhs[i], xs[j], ys[j], hws[j], hhs[j], box_gap):
                return False
        return True

    for i in range(1, n):
        hw_i, hh_i = hws[i], hhs[i]
        placed = False
        for attempt in range(220):
            if attempt < 120:
                ang = i * golden + rnd.uniform(-0.55, 0.55)
                r_spiral = (
                    0.026 + math.sqrt(i) * 0.024 + rnd.uniform(0, 0.018) + (attempt // 25) * 0.008
                ) * 0.72
            else:
                ang = rnd.uniform(0, 2 * math.pi)
                r_spiral = 0.08 + rnd.uniform(0, 0.16) + (attempt - 120) * 0.0024

            x = 0.5 + math.cos(ang) * r_spiral * 0.92
            y = 0.5 + math.sin(ang) * r_spiral * 0.42
            x += rnd.uniform(-0.012, 0.012)
            y += rnd.uniform(-0.012, 0.012)
            x = max(margin + hw_i, min(1.0 - margin - hw_i, x))
            y = max(margin + hh_i, min(1.0 - margin - hh_i, y))

            if fits_at(i, x, y):
                xs.append(x)
                ys.append(y)
                placed = True
                break

        if not placed:
            for _ in range(80):
                x = rnd.uniform(margin + hw_i, 1.0 - margin - hw_i)
                y = rnd.uniform(margin + hh_i, 1.0 - margin - hh_i)
                if fits_at(i, x, y):
                    xs.append(x)
                    ys.append(y)
                    placed = True
                    break
            if not placed:
                ang = i * golden + rnd.random() * 0.5
                r_fallback = min(0.2, 0.11 + (i % 6) * 0.018)
                xs.append(max(margin + hw_i, min(1.0 - margin - hw_i, 0.5 + math.cos(ang) * r_fallback)))
                ys.append(max(margin + hh_i, min(1.0 - margin - hh_i, 0.5 + math.sin(ang) * r_fallback * 0.42)))

    return xs, ys


def _wordmap_winsorize(vals: list[float], *, low_pct: float = 0.1, high_pct: float = 0.9) -> list[float]:
    """한두 개의 극단 좌표가 전체 스케일을 벌려 동떨어져 보이는 현상을 줄입니다."""
    n = len(vals)
    if n < 4:
        return list(vals)
    sv = sorted(vals)
    lo = sv[int(low_pct * (n - 1))]
    hi = sv[int(high_pct * (n - 1))]
    return [min(max(v, lo), hi) for v in vals]


def _wordmap_rect_normalize(
    xs: list[float],
    ys: list[float],
    *,
    x_margin: float = 0.05,
    y_band: tuple[float, float] = (0.26, 0.74),
) -> tuple[list[float], list[float]]:
    """
    산발 좌표를 가로 전폭·세로는 짧은 띠(직사각형 가시 영역)로 선형 매핑.
    min/max 대신 윈저라이즈된 범위로 스케일해 한 단어만 멀리 있는 경우를 덩어리 안으로 모음.
    """
    if not xs or not ys or len(xs) != len(ys):
        return xs, ys
    n = len(xs)
    if n == 1:
        return [0.5], [(y_band[0] + y_band[1]) / 2]

    wx = _wordmap_winsorize(xs, low_pct=0.08, high_pct=0.92)
    wy = _wordmap_winsorize(ys, low_pct=0.08, high_pct=0.92)

    lo_x, hi_x = min(wx), max(wx)
    lo_y, hi_y = min(wy), max(wy)
    span_x = max(hi_x - lo_x, 1e-6)
    span_y = max(hi_y - lo_y, 1e-6)

    # 가로·세로 모두 중앙 쪽 직사각형에만 맵(전체 폭을 쓰지 않아 이후 중앙 정렬과 맞음)
    tx_lo, tx_hi = x_margin + 0.06, 1.0 - x_margin - 0.06
    ty_lo, ty_hi = y_band[0], y_band[1]

    nx = [tx_lo + (x - lo_x) / span_x * (tx_hi - tx_lo) for x in wx]
    ny = [ty_lo + (y - lo_y) / span_y * (ty_hi - ty_lo) for y in wy]
    return nx, ny


def _wordmap_pull_to_center(
    xs: list[float],
    ys: list[float],
    *,
    strength: float = 0.68,
    cx: float = 0.5,
) -> tuple[list[float], list[float]]:
    """
    정규화 직후 좌표를 (cx, 세로 중앙) 쪽으로 당겨 덩어리를 화면 중앙에 모음.
    strength: 1에 가까울수록 원래 배치, 작을수록 중앙에 밀집.
    """
    if not xs or not ys or len(xs) != len(ys):
        return xs, ys
    cy = statistics.median(ys)
    out_x = [cx + (x - cx) * strength for x in xs]
    out_y = [cy + (y - cy) * strength for y in ys]
    eps = 0.02
    return [
        min(1.0 - eps, max(eps, x)) for x in out_x
    ], [min(1.0 - eps, max(eps, y)) for y in out_y]


def _wordmap_contract_toward_centroid(
    xs: list[float], ys: list[float], *, factor: float = 0.86
) -> tuple[list[float], list[float]]:
    """클러스터를 무게중심 쪽으로 모아 단어 간 시각적 간격을 줄임(factor 작을수록 더 촘촘)."""
    n = len(xs)
    if n < 2 or len(ys) != n:
        return xs, ys
    cx = sum(xs) / n
    cy = sum(ys) / n
    return [cx + (x - cx) * factor for x in xs], [cy + (y - cy) * factor for y in ys]


def _wordmap_sentiment_colors(words: list[str], llm_polarity: dict[str, str] | None = None) -> list[str]:
    """Gemini가 준 단어별 극성만 사용. 없거나 실패 시 중립(노랑)."""
    pos_rgba = "rgba(33, 102, 220, 0.95)"
    neg_rgba = "rgba(215, 48, 48, 0.95)"
    neu_rgba = "rgba(215, 175, 28, 0.95)"

    llm_polarity = llm_polarity or {}
    out: list[str] = []
    for w in words:
        p = llm_polarity.get(w, "neutral").strip().lower()
        if p == "positive":
            out.append(pos_rgba)
        elif p == "negative":
            out.append(neg_rgba)
        else:
            out.append(neu_rgba)
    return out


def _wordmap_sentiment_legend_html() -> str:
    """워드맵 긍정/부정/중립 색상 안내(_wordmap_sentiment_colors와 동일 RGB)."""
    pos = "rgb(33, 102, 220)"
    neg = "rgb(215, 48, 48)"
    neu = "rgb(215, 175, 28)"
    return f"""
<div style="width:100%;box-sizing:border-box;display:flex;justify-content:center;margin:0.2rem 0 0.25rem 0;padding:0 4px;">
<div style="width:100%;max-width:min(100%,520px);background:#FFFFFF;border:1px solid #E2E8F0;border-radius:12px;padding:10px 18px;
display:flex;flex-wrap:wrap;justify-content:space-evenly;align-items:center;gap:12px 20px;
font-size:0.92rem;font-weight:500;color:#0F172A;box-shadow:0 1px 2px rgba(15,23,42,0.05);">
<span style="display:inline-flex;align-items:center;gap:8px;min-width:5.5em;justify-content:center;">
<span style="width:18px;height:18px;border-radius:5px;background:{pos};display:inline-flex;align-items:center;justify-content:center;color:#fff;font-size:11px;line-height:1;flex-shrink:0;">✓</span>
긍정
</span>
<span style="display:inline-flex;align-items:center;gap:8px;min-width:5.5em;justify-content:center;">
<span style="width:18px;height:18px;border-radius:5px;background:{neg};display:inline-flex;align-items:center;justify-content:center;color:#fff;font-size:11px;line-height:1;flex-shrink:0;">✓</span>
부정
</span>
<span style="display:inline-flex;align-items:center;gap:8px;min-width:5.5em;justify-content:center;">
<span style="width:18px;height:18px;border-radius:5px;background:{neu};display:inline-flex;align-items:center;justify-content:center;color:#fff;font-size:11px;line-height:1;flex-shrink:0;">✓</span>
중립
</span>
</div>
</div>
"""


def _items_to_review_blob(items: list[ReviewItem]) -> str:
    lines: list[str] = []
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


def _build_review_and_usp_for_ad_copies(
    items: list[ReviewItem],
    result: dict[str, Any],
    *,
    max_review_chars: int = 12000,
) -> str:
    """추천 광고 카피 프롬프트용: 긍정 리뷰 위주 + 분석 요약(소구점·키워드)."""
    per = result.get("per_review") or []
    pos_texts: list[str] = []
    seen_idx: set[int] = set()
    for row in per:
        if row.get("sentiment") != "positive":
            continue
        idx = row.get("review_index")
        if not isinstance(idx, int) or idx < 0 or idx >= len(items):
            continue
        if idx in seen_idx:
            continue
        seen_idx.add(idx)
        t = (items[idx].text or "").strip()
        if t:
            pos_texts.append(t)
    if not pos_texts:
        for it in items:
            t = (it.text or "").strip()
            if t:
                pos_texts.append(t)
    blob = "\n\n---\n\n".join(pos_texts)
    if len(blob) > max_review_chars:
        blob = blob[:max_review_chars] + "\n…(이하 생략)"

    top5 = result.get("top5_angle_ids") or []
    labels = [ANGLE_LABEL_BY_ID[aid] for aid in top5 if aid in ANGLE_LABEL_BY_ID]
    kws = [k for k in (result.get("top_keywords") or []) if (k or "").strip()]
    sent = result.get("sentiment") or {}

    parts = [
        f"[긍정 리뷰 비율] 긍정 {sent.get('positive_percent', '-')}% / 부정 {sent.get('negative_percent', '-')}%",
        f"[핵심 소구점(분석)] {', '.join(labels) if labels else '(없음)'}",
        f"[핵심 키워드] {', '.join(kws[:20]) if kws else '(없음)'}",
        "",
        "[긍정 리뷰 원문 샘플]",
        blob,
    ]
    return "\n".join(parts)


# 추천 광고 카피 섹션 제목(표시: 1. 직접 판매 … — copies[0]~[3]와 대응)
_AD_COPY_SECTION_TITLES: tuple[str, str, str, str] = (
    "직접 판매",
    "USP 집중형",
    "불편 해결",
    "혜택 강조",
)

# 추천 광고 카피 CTA: 공백 포함 최대 글자 수
_AD_COPY_CTA_MAX_LEN = 20


def _ad_copy_body_three_sentences(body: str) -> str:
    """
    본문을 최대 세 문장(세 줄)으로 맞춤.
    화면 한 블록은 '번호·제목' 1줄 + 본문 3줄 + CTA 1줄 = 총 5줄이 되도록 함.
    """
    b = (body or "").strip()
    if not b:
        return ""
    parts = re.split(r"(?<=[\.!?…])\s+", b)
    parts = [p.strip() for p in parts if p.strip()]
    if not parts:
        return b
    return "\n".join(parts[:3])


_AD_COPY_JSON_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "copies": {
            "type": "array",
            "minItems": 4,
            "maxItems": 4,
            "items": {
                "type": "object",
                "properties": {
                    "body": {
                        "type": "string",
                        "description": "본문 세 문장(각 마침표 종료). 표시 시 3줄+CTA로 총 5줄 블록",
                    },
                    "cta": {
                        "type": "string",
                        "maxLength": _AD_COPY_CTA_MAX_LEN,
                        "description": f"직접 행동 유도(동사형으로 끝남), 공백 포함 {_AD_COPY_CTA_MAX_LEN}자 이내, 끝에 느낌표 없이",
                    },
                },
                "required": ["body", "cta"],
                "additionalProperties": False,
            },
        }
    },
    "required": ["copies"],
    "additionalProperties": False,
}

_AD_COPY_CTA_MAX_LEN_EN = 28

_AD_COPY_JSON_SCHEMA_EN: dict[str, Any] = {
    "type": "object",
    "properties": {
        "copies": {
            "type": "array",
            "minItems": 4,
            "maxItems": 4,
            "items": {
                "type": "object",
                "properties": {
                    "body": {
                        "type": "string",
                        "description": "Three sentences in English, each ending with a period.",
                    },
                    "cta": {
                        "type": "string",
                        "maxLength": _AD_COPY_CTA_MAX_LEN_EN,
                        "description": f"Short action CTA, max {_AD_COPY_CTA_MAX_LEN_EN} chars including spaces, no trailing !",
                    },
                },
                "required": ["body", "cta"],
                "additionalProperties": False,
            },
        }
    },
    "required": ["copies"],
    "additionalProperties": False,
}


def _ad_copy_strip_cta_trailing_bang(cta: str) -> str:
    """CTA 끝의 느낌표 제거(요청 시 표시 일관)."""
    s = (cta or "").strip()
    while s.endswith("!"):
        s = s[:-1].rstrip()
    return s


def _ad_copy_normalize_cta(cta: str, *, max_len: int = _AD_COPY_CTA_MAX_LEN) -> str:
    """
    CTA 표시용: 느낌표 제거 후 길이 상한 적용.
    공백으로 구분된 단어 단위로만 잘라 단어 중간이 잘리지 않게 한다(영어 CTA 등).
    """
    s = _ad_copy_strip_cta_trailing_bang(cta)
    s = s.strip()
    if not s:
        return ""
    if len(s) <= max_len:
        return s
    words = s.split()
    if not words:
        return s[:max_len]
    chosen: list[str] = []
    total = 0
    for w in words:
        add = len(w) if not chosen else 1 + len(w)
        if total + add <= max_len:
            chosen.append(w)
            total += add
        else:
            break
    if chosen:
        return " ".join(chosen)
    # 첫 토큰만으로도 상한 초과(드묾) — 상한까지 자름
    return words[0][:max_len]


def _ad_copy_blocks_from_json(
    raw: dict[str, Any],
    *,
    cta_max_len: int = _AD_COPY_CTA_MAX_LEN,
) -> list[tuple[int, str, str, str]]:
    """(번호, 제목, 본문 줄바꿈 포함, cta) 블록 목록."""
    copies = raw.get("copies") if isinstance(raw, dict) else None
    if not isinstance(copies, list):
        return []
    out: list[tuple[int, str, str, str]] = []
    for idx in range(4):
        title = _AD_COPY_SECTION_TITLES[idx]
        block = copies[idx] if idx < len(copies) else None
        if not isinstance(block, dict):
            continue
        body = (block.get("body") or "").strip()
        cta = _ad_copy_normalize_cta((block.get("cta") or "").strip(), max_len=cta_max_len)
        if not body and not cta:
            continue
        body_fmt = _ad_copy_body_three_sentences(body)
        out.append((idx + 1, title, body_fmt, cta))
    return out


def _format_ad_copies_from_json(
    raw: dict[str, Any],
    *,
    cta_max_len: int = _AD_COPY_CTA_MAX_LEN,
) -> str:
    """
    JSON 응답을 평문으로 변환(재시도·빈 값 판별용).
    - 각 블록은 5줄: `1. 제목` / 본문 1줄 / 본문 1줄 / 본문 1줄 / CTA.
    """
    parts: list[str] = []
    for num, title, body_fmt, cta in _ad_copy_blocks_from_json(raw, cta_max_len=cta_max_len):
        parts.append(f"{num}. {title}\n{body_fmt}\n{cta}")
    return "\n\n".join(parts)


def _format_ad_copies_html(
    raw: dict[str, Any],
    *,
    cta_max_len: int = _AD_COPY_CTA_MAX_LEN,
) -> str:
    """Streamlit markdown용 HTML: 번호·제목은 굵게, 본문·CTA는 이스케이프."""
    chunks: list[str] = []
    for num, title, body_fmt, cta in _ad_copy_blocks_from_json(raw, cta_max_len=cta_max_len):
        body_html = "<br>".join(html.escape(line) for line in body_fmt.split("\n")) if body_fmt else ""
        chunks.append(
            '<div style="margin-bottom:1.1em;">'
            f'<p style="margin:0 0 0.25em 0;"><strong>{num}. {html.escape(title)}</strong></p>'
            f'<p style="margin:0 0 0.25em 0;white-space:pre-wrap;">{body_html}</p>'
            f'<p style="margin:0;">{html.escape(cta)}</p>'
            "</div>"
        )
    return "".join(chunks)


def _st_html_fragment(fragment: str) -> str:
    """
    Streamlit `st.markdown`은 CommonMark 규칙상 줄 앞에 공백 4칸 이상이면
    해당 블록을 코드 블록으로 처리해, HTML이 그대로 화면에 노출된다.
    들여쓰기를 제거한 뒤 렌더링한다.
    """
    return textwrap.dedent(fragment).strip()


def _ad_copy_raw_to_card_blocks(
    raw: dict[str, Any],
    *,
    cta_max_len: int = _AD_COPY_CTA_MAX_LEN,
) -> list[dict[str, str]]:
    """카드 UI용: 제목·본문(줄바꿈 유지)·CTA."""
    blocks: list[dict[str, str]] = []
    for _num, title, body_fmt, cta in _ad_copy_blocks_from_json(raw, cta_max_len=cta_max_len):
        blocks.append({"title": title, "body": body_fmt, "cta": cta})
    return blocks


def inject_dashboard_styles() -> None:
    """추천 광고 카피·키워드 Top10 칩/카드용 공통 스타일(페이지당 1회)."""
    st.markdown(
        _st_html_fragment(
            """
    <style>
    /* 공통 섹션 카드 */
    .gs-section-card{
        background:#ffffff;
        border:1px solid #E2E8F0;
        border-radius:18px;
        padding:22px 22px 18px 22px;
        box-shadow:0 1px 2px rgba(15, 23, 42, 0.04);
    }

    .gs-section-title{
        margin:0 0 16px 0;
        font-size:1.8rem;
        line-height:1.2;
        font-weight:800;
        letter-spacing:-0.02em;
        color:#0F172A;
    }

    .gs-section-sub{
        margin:0 0 14px 0;
        font-size:0.92rem;
        color:#64748B;
    }

    /* 키워드 칩 */
    .gs-chip-wrap{
        display:flex;
        flex-wrap:wrap;
        gap:10px 10px;
    }

    .gs-chip{
        display:inline-flex;
        align-items:center;
        gap:8px;
        padding:10px 14px;
        border-radius:999px;
        background:linear-gradient(180deg, #F8FBFF 0%, #EEF4FF 100%);
        border:1px solid #D6E4FF;
        color:#1D4ED8;
        font-size:0.95rem;
        font-weight:600;
        line-height:1.25;
        white-space:nowrap;
        min-height:2.75rem;
        box-sizing:border-box;
    }

    .gs-chip-no{
        width:22px;
        height:22px;
        border-radius:999px;
        background:#DBEAFE;
        color:#1D4ED8;
        font-size:0.78rem;
        font-weight:700;
        display:inline-flex;
        align-items:center;
        justify-content:center;
        flex-shrink:0;
    }

    /* 키워드 카드: 하단 여백 + 디버그와 간격 */
    .gs-section-card.gs-keyword-card{
        margin-bottom:16px;
        padding-bottom:34px;
    }

    .gs-debug-spacer{
        height:12px;
    }

    /* 광고 카피 카드 그리드: 같은 행 높이 맞춤 + CTA 하단 정렬 */
    .gs-copy-grid{
        display:grid;
        grid-template-columns:repeat(2, minmax(0, 1fr));
        gap:16px;
        align-items:stretch;
    }

    @media (max-width: 900px){
        .gs-copy-grid{
            grid-template-columns:1fr;
        }
    }

    .gs-copy-card{
        background:#ffffff;
        border:1px solid #E2E8F0;
        border-radius:18px;
        padding:20px 22px 24px 22px;
        box-shadow:0 1px 2px rgba(15, 23, 42, 0.04);
        box-sizing:border-box;
        min-height:240px;
        height:100%;
        display:flex;
        flex-direction:column;
    }

    .gs-copy-top{
        display:flex;
        align-items:center;
        gap:10px;
        margin-bottom:17px;
        flex-shrink:0;
    }

    .gs-copy-index{
        width:28px;
        height:28px;
        border-radius:999px;
        background:#2563EB;
        color:#ffffff;
        font-size:0.84rem;
        font-weight:700;
        display:flex;
        align-items:center;
        justify-content:center;
        flex-shrink:0;
    }

    .gs-copy-title{
        font-size:1.09rem;
        font-weight:900;
        color:#0F172A;
        line-height:1.22;
        letter-spacing:-0.02em;
        flex:1;
        min-width:0;
    }

    .gs-copy-body{
        font-size:0.97rem;
        font-weight:500;
        color:#1E293B;
        line-height:1.475;
        word-break:keep-all;
        padding:0 3px;
        flex:1 1 auto;
        min-height:0;
    }

    .gs-copy-body .gs-copy-line{
        margin:0 0 0.34em 0;
    }

    .gs-copy-body .gs-copy-line:last-child{
        margin-bottom:0;
    }

    .gs-copy-cta-wrap{
        margin-top:auto;
        flex-shrink:0;
        padding-top:2px;
    }

    .gs-copy-divider{
        height:1px;
        background:rgba(226, 232, 240, 0.65);
        margin:0 0 12px 0;
    }

    .gs-copy-cta-label{
        margin:0 0 5px 0;
        font-size:0.62rem;
        font-weight:600;
        color:#B8C0CC;
        letter-spacing:0.06em;
        text-transform:uppercase;
    }

    .gs-copy-cta{
        margin:0;
        font-size:0.96rem;
        font-weight:800;
        color:#0F172A;
        line-height:1.55;
        word-break:keep-all;
    }

    /* 사이드바 접기(<<): Material 아이콘(stIconMaterial) — 메인 스타일보다 뒤에 두어 emotion과 순서 경쟁 완화 */
    [data-testid="stSidebarCollapseButton"] {
        visibility: visible !important;
    }
    [data-testid="stSidebarCollapseButton"] [data-testid="stIconMaterial"],
    [data-testid="stSidebarCollapseButton"] [data-testid="stIconMaterial"] * {
        color: #FFFFFF !important;
        opacity: 1 !important;
    }
    </style>
    """
        ),
        unsafe_allow_html=True,
    )


def _to_lines(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(x) for x in value if str(x).strip()]
    return [line for line in str(value).splitlines() if line.strip()]


def _body_lines_for_card_display(body: str) -> list[str]:
    """카드 본문: 문장 단위로 줄을 나눔(마침표·물음표·느낌표·말줄임 후 공백 기준)."""
    b = (body or "").strip()
    if not b:
        return []
    normalized = re.sub(r"\s+", " ", b)
    parts = re.split(r"(?<=[\.!?…])\s+", normalized)
    parts = [p.strip() for p in parts if p.strip()]
    if len(parts) >= 2:
        return parts
    if "\n" in b:
        return [line.strip() for line in b.splitlines() if line.strip()]
    return [b]


def render_keyword_chips(keywords: list[str], title: str = "리뷰 핵심 키워드 (Top 10)") -> None:
    chips_html = "".join(
        f'<span class="gs-chip"><span class="gs-chip-no">{i}</span>{html.escape(str(keyword))}</span>'
        for i, keyword in enumerate(keywords, start=1)
    )

    st.markdown(
        _st_html_fragment(
            f"""<div class="gs-section-card gs-keyword-card">
<h3 class="gs-section-title">{html.escape(title)}</h3>
<div class="gs-chip-wrap">{chips_html}</div>
</div>"""
        ),
        unsafe_allow_html=True,
    )


def render_ad_copy_cards(
    copy_blocks: list[dict[str, Any]],
    title: str = "추천 광고 카피",
    subtitle: str | None = None,
) -> None:
    cards_html: list[str] = []

    for idx, block in enumerate(copy_blocks, start=1):
        copy_title = html.escape(str(block.get("title", f"카피 {idx}")))
        body_lines = _body_lines_for_card_display(block.get("body", ""))
        cta_text = html.escape(str(block.get("cta", "")))

        lines_html = "".join(
            f'<p class="gs-copy-line">{html.escape(line)}</p>' for line in body_lines
        )

        card_html = _st_html_fragment(
            f"""<div class="gs-copy-card">
<div class="gs-copy-top">
<div class="gs-copy-index">{idx}</div>
<div class="gs-copy-title">{copy_title}</div>
</div>
<div class="gs-copy-body">{lines_html}</div>
<div class="gs-copy-cta-wrap">
<div class="gs-copy-divider"></div>
<p class="gs-copy-cta-label">CTA</p>
<p class="gs-copy-cta">{cta_text}</p>
</div>
</div>"""
        )
        cards_html.append(card_html)

    subtitle_html = (
        f'<p class="gs-section-sub">{html.escape(subtitle)}</p>'
        if subtitle
        else ""
    )

    st.markdown(
        _st_html_fragment(
            f"""<div class="gs-section-card">
<h3 class="gs-section-title">{html.escape(title)}</h3>
{subtitle_html}
<div class="gs-copy-grid">{"".join(cards_html)}</div>
</div>"""
        ),
        unsafe_allow_html=True,
    )


def generate_recommended_ad_copies(
    *,
    items: list[ReviewItem],
    result: dict[str, Any],
    api_keys: list[str],
    model: str,
    use_vertex: bool = False,
    vertex_project_id: str | None = None,
    vertex_location: str | None = None,
) -> tuple[str, list[dict[str, str]]]:
    """
    Gemini로 리뷰·USP 기반 퍼포먼스 광고 카피 4종(순서 고정) 생성.
    자유 텍스트는 출력이 잘리거나 개수가 부족해질 수 있어 JSON 스키마로 4쌍(body+cta)을 강제한다.
    반환: (평문, 카드 UI용 블록 dict 목록 — title/body/cta).
    """
    review_and_usp_data = _build_review_and_usp_for_ad_copies(items, result)
    primary_en = _reviews_primarily_english(items)
    ad_copy_schema: dict[str, Any] = _AD_COPY_JSON_SCHEMA_EN if primary_en else _AD_COPY_JSON_SCHEMA

    if primary_en:
        prompt = f"""# Role
You are a senior performance copywriter. Combine the USP and review data into high-CTR ad copy.

# Input Data
{review_and_usp_data}

# Task — exactly 4 objects in "copies" (fixed order; UI labels are added separately)
# copies[0] → direct response, [1] → USP-led, [2] → pain/solution, [3] → benefit-led
copies[0]: Direct response — break hesitation and drive immediate action.
copies[1]: USP-led — lead with the clearest differentiator from reviews/USP.
copies[2]: Pain/solution — emphasize before/after feel and relief.
copies[3]: Benefit-led — tangible payoff and everyday win.

Rules (each copy: 1 title row + 3 body lines + 1 CTA on screen):
- **Tone:** Warm, conversational English. Not stiff corporate. No Korean-style endings.
- **Body:** Exactly three sentences. Each ends with a period. Different focus per sentence. Ground in the reviews/USP; do not invent facts.
- Do not put section labels like "direct response" inside body or CTA.
- **cta:** Max {_AD_COPY_CTA_MAX_LEN_EN} characters including spaces. Short action phrase (e.g. Shop the glow, See shades, Get yours today). No trailing exclamation mark.
- Output JSON only, matching the schema.

Aim for ~60 characters per sentence; total body roughly within ~220 characters.
"""
    else:
        prompt = f"""# Role
너는 리뷰 기반 10년차 퍼포먼스 카피라이터다. USP와 리뷰 데이터를 결합해 매체 규격에 맞는 고효율(CTR 중심) 광고 카피를 생성한다.

# Input Data
{review_and_usp_data}

# Task (반드시 copies 배열 4개, 순서 고정 — 화면에는 아래 제목으로 매핑됨)
# copies[0] → 직접 판매, [1] → USP 집중형, [2] → 불편 해결, [3] → 혜택 강조
copies[0]: 직접 판매 톤 — 망설임을 끊고 즉각적인 구매와 행동을 촉구하는 직설적 전략
copies[1]: USP 집중형 톤 — 제품만이 가진 독보적인 기능이나 차별점을 가장 앞세우는 전략
copies[2]: 불편 해결 톤 — 제품 사용 전후의 극적인 변화와 체감을 강조하는 전략
copies[3]: 혜택 강조 톤 — 제품 사용 후 얻게 될 실질적인 이득과 삶의 질 변화를 보여주는 전략

규칙 (각 copies 항목마다 화면은 **총 5줄**: 제목 1줄 + 본문 3줄 + CTA 1줄):
- **어투(필수):** 딱딱한 설명체는 **절대 쓰지 않는다**. (~입니다, ~합니다, ~됩니다, ~드립니다 등 금지) 다정하고 친근한 **구어체·해요체**로 써서 공감을 이끌어낸다. (~해요, ~예요, ~인가요?, ~보세요, ~드려요 등) 맞춤법·띄어쓰기는 엄격히 지키고, 과한 반말체나 과장·자극적·선정적 표현은 쓰지 않는다.
- **표현(필수):** 추상적인 단어만 나열하지 말고, 가능하면 **구체적인 묘사**와 **비유**를 들어 장면이 떠오르게 쓴다. 아래 **구체적 상황 묘사** 항목과 함께 적용한다.
- **포맷(필수):** 가독성을 위해 **세 문장 각각이 한 덩어리**로 읽히게 쓴다(문장마다 주제·호흡을 나누고, 한 문장에 여러 생각을 꾸겨 넣지 않는다). 화면에는 본문이 세 줄로 나뉘어 보이므로, 줄마다 숨을 고르고 읽히게 한다.
- **구체적 상황 묘사:** '좋다', '최고다' 같은 추상적 칭찬만 쓰지 말고, 입력된 리뷰·USP에 나온 **실사용 맥락**을 빌려 구체적으로 쓴다. 제품군에 관계없이 통하는 방식으로, 예를 들어 사용 전후 체감 차이, 걸리는 시간·횟수, 만져보거나 써 본 감각, 일상 속 장면 한 줄 등 **검증 가능한 디테일**을 세 문장에 나누어 녹인다(리뷰에 없는 사실은 지어내지 않는다).
- body·cta 안에 '직접 판매' 등 섹션 제목 문구를 넣지 않는다(시스템이 번호·제목을 붙임).
- body는 **반드시 세 문장만** 쓴다. 각 문장은 마침표(.)로 끝내고, 서로 다른 내용이어야 한다. 리뷰 어조를 녹인다.
- 네 문장 이상 쓰지 않는다(후처리에서 앞 세 문장만 사용됨).
- cta는 **공백 포함 20자 이내**로 쓴다. **반드시 구매·신청·예약·확인·바로 가기** 등 사용자가 즉시 실행할 수 있는 **동사형 행동**으로 끝내 직접 액션을 유도한다(설명만 하고 끝내지 않는다). 예: 오늘 혜택 한눈에 확인하기, 공식 스토어에서 구매하기. 문장 끝에 느낌표(!)는 쓰지 않는다.
- 큰따옴표/작은따옴표로 본문 전체를 감싸지 않는다.
- JSON 스키마만 출력.

문장당 공백 포함 약 60자 이내·body 합계 약 220자 이내를 권장한다.
"""
    loc = (vertex_location or "us-central1").strip() or "us-central1"
    max_out = 8192

    def run_once() -> dict[str, Any]:
        if use_vertex:
            pid = (vertex_project_id or "").strip()
            if not pid:
                raise ValueError("Vertex AI 사용 시 GCP 프로젝트 ID가 필요합니다.")
            return _call_vertex_json(
                model=model,
                prompt=prompt,
                temperature=0.42,
                max_output_tokens=max_out,
                response_json_schema=ad_copy_schema,
                project_id=pid,
                location=loc,
            )
        if not api_keys:
            raise ValueError("API 키 목록이 비어 있습니다.")
        last_exc: Exception | None = None
        for k in api_keys:
            try:
                client = genai.Client(api_key=k)
                return _call_gemini_json(
                    client=client,
                    model=model,
                    prompt=prompt,
                    temperature=0.42,
                    max_output_tokens=max_out,
                    response_json_schema=ad_copy_schema,
                )
            except Exception as e:
                if _is_api_key_invalid_error(e):
                    last_exc = e
                    continue
                raise
        if last_exc is not None:
            raise last_exc
        raise RuntimeError("모든 API key 호출이 실패했습니다.")

    cta_max_len = _AD_COPY_CTA_MAX_LEN_EN if primary_en else _AD_COPY_CTA_MAX_LEN
    raw = run_once()
    text = _format_ad_copies_from_json(raw, cta_max_len=cta_max_len)
    # 스키마는 맞지만 비어 있거나 파싱 실패 시 1회 재시도
    if not text.strip() or len([x for x in (raw.get("copies") or []) if isinstance(x, dict)]) < 4:
        prompt_retry = (
            prompt
            + "\n\n[중요] 이전 JSON이 비었거나 copies가 4개 미만이었다. "
            "반드시 copies 길이 4, 각 항목에 body·cta 비어 있지 않게 다시 출력."
        )
        if use_vertex:
            pid = (vertex_project_id or "").strip() or ""
            raw = _call_vertex_json(
                model=model,
                prompt=prompt_retry,
                temperature=0.35,
                max_output_tokens=max_out,
                response_json_schema=ad_copy_schema,
                project_id=pid,
                location=loc,
            )
        else:
            last_exc2: Exception | None = None
            for k in api_keys:
                try:
                    client = genai.Client(api_key=k)
                    raw = _call_gemini_json(
                        client=client,
                        model=model,
                        prompt=prompt_retry,
                        temperature=0.35,
                        max_output_tokens=max_out,
                        response_json_schema=ad_copy_schema,
                    )
                    break
                except Exception as e:
                    if _is_api_key_invalid_error(e):
                        last_exc2 = e
                        continue
                    raise
            else:
                if last_exc2 is not None:
                    raise last_exc2
        text = _format_ad_copies_from_json(raw, cta_max_len=cta_max_len)

    return text, _ad_copy_raw_to_card_blocks(raw, cta_max_len=cta_max_len)


def _chunk_reviews(items: list[ReviewItem], *, max_chars: int) -> list[list[ReviewItem]]:
    chunks: list[list[ReviewItem]] = []
    cur: list[ReviewItem] = []
    cur_chars = 0

    for it in items:
        add = len(it.text or "") + 200
        if cur and cur_chars + add > max_chars:
            chunks.append(cur)
            cur = [it]
            cur_chars = add
        else:
            cur.append(it)
            cur_chars += add
    if cur:
        chunks.append(cur)
    return chunks


def _parse_pasted_reviews(text: str, *, max_items: int) -> list[ReviewItem]:
    """
    사용자가 리뷰를 복사/붙여넣으면 가능한 한 잘게 쪼개어 ReviewItem으로 만듭니다.
    - [리뷰 1] ... 형태가 있으면 그 경계로 분리
    - 그 외엔 빈 줄 단위 블록 분리
    """
    t = (text or "").strip()
    if not t:
        return []

    parts: list[str]
    if "[리뷰" in t:
        import re

        split = re.split(r"\[리뷰\s*\d+[^\]]*\]", t)
        parts = [s.strip() for s in split if s.strip()]
    else:
        import re

        parts = [p.strip() for p in re.split(r"\n\s*\n+", t) if p.strip()]

    out: list[ReviewItem] = []
    for p in parts:
        if len(p) < 10:
            continue
        out.append(ReviewItem(text=p))
        if len(out) >= max_items:
            break
    return out


def _call_gemini_json(
    *,
    client: genai.Client,
    model: str,
    prompt: str,
    temperature: float,
    max_output_tokens: int,
    response_json_schema: dict[str, Any] | None = None,
) -> dict[str, Any]:
    # JSON이 깨져서 파싱 실패하는 케이스를 대비해 1회 재시도합니다.
    last_text = ""
    for attempt in range(2):
        resp = client.models.generate_content(
            model=model,
            contents=prompt,
            config=genai_types.GenerateContentConfig(
                temperature=temperature,
                max_output_tokens=max_output_tokens,
                top_p=0.9,
                response_mime_type="application/json",
                response_json_schema=response_json_schema,
            ),
        )
        # SDK가 이미 JSON을 파싱해 제공하는 경우(resp.parsed 우선) 수동 json.loads를 피합니다.
        parsed = getattr(resp, "parsed", None)
        if parsed is not None:
            if isinstance(parsed, dict):
                return parsed
            # pydantic 모델일 수도 있으므로 dict로 변환
            if hasattr(parsed, "model_dump"):
                return parsed.model_dump(exclude_none=True)  # type: ignore[attr-defined]
            # 그 외는 최후 수단으로 str->json 파싱
        text = resp.text or ""
        last_text = text
        try:
            return _extract_first_json_object(text)
        except json.JSONDecodeError:
            if attempt == 1:
                # 마지막 응답 일부를 메시지로 남겨 디버깅 가능하게 합니다.
                snippet = (last_text or "")[:500]
                raise json.JSONDecodeError(
                    f"Gemini JSON 파싱 실패(마지막 시도). 앞부분: {snippet}",
                    last_text,
                    0,
                )
            # 재시도 프롬프트: JSON만 다시 출력
            prompt = (
                prompt
                + "\n\n[중요] 이전 응답의 JSON이 깨졌습니다. 반드시 JSON만 다시 출력해 주세요."
            )


_VERTEX_INIT_KEY: tuple[str, str] | None = None


def _ensure_vertex_init(project_id: str, location: str) -> None:
    """동일 세션에서 프로젝트·리전이 바뀌면 다시 init 합니다."""
    global _VERTEX_INIT_KEY
    pid = project_id.strip()
    loc = (location.strip() or "us-central1")
    if not pid:
        raise ValueError("Vertex AI: GCP 프로젝트 ID가 비어 있습니다.")
    key = (pid, loc)
    if _VERTEX_INIT_KEY == key:
        return
    try:
        import vertexai
    except ImportError as e:
        raise ImportError(
            "Vertex AI를 쓰려면 `pip install google-cloud-aiplatform` 후 다시 실행하세요."
        ) from e
    vertexai.init(project=pid, location=loc)
    _VERTEX_INIT_KEY = key


def _call_vertex_json(
    *,
    model: str,
    prompt: str,
    temperature: float,
    max_output_tokens: int,
    response_json_schema: dict[str, Any] | None,
    project_id: str,
    location: str,
) -> dict[str, Any]:
    """Vertex AI Gemini — GCP 크레딧/결제 계정 기준 과금."""
    try:
        from vertexai.generative_models import GenerativeModel, GenerationConfig
    except ImportError as e:
        raise ImportError(
            "Vertex AI를 쓰려면 `pip install google-cloud-aiplatform` 후 다시 실행하세요."
        ) from e

    _ensure_vertex_init(project_id, location)
    gen_model = GenerativeModel(model)

    last_text = ""
    for attempt in range(2):
        if response_json_schema is not None:
            config = GenerationConfig(
                temperature=temperature,
                max_output_tokens=max_output_tokens,
                top_p=0.9,
                response_mime_type="application/json",
                response_schema=response_json_schema,
            )
        else:
            config = GenerationConfig(
                temperature=temperature,
                max_output_tokens=max_output_tokens,
                top_p=0.9,
            )

        resp = gen_model.generate_content(
            prompt,
            generation_config=config,
        )
        text = (getattr(resp, "text", None) or "").strip()
        last_text = text
        try:
            return _extract_first_json_object(text)
        except json.JSONDecodeError:
            if attempt == 1:
                snippet = (last_text or "")[:500]
                raise json.JSONDecodeError(
                    f"Vertex Gemini JSON 파싱 실패(마지막 시도). 앞부분: {snippet}",
                    last_text,
                    0,
                )
            prompt = (
                prompt
                + "\n\n[중요] 이전 응답의 JSON이 깨졌습니다. 반드시 JSON만 다시 출력해 주세요."
            )


def _call_gemini_text(
    *,
    client: genai.Client,
    model: str,
    prompt: str,
    temperature: float,
    max_output_tokens: int,
) -> str:
    """JSON 없이 순수 텍스트 응답."""
    resp = client.models.generate_content(
        model=model,
        contents=prompt,
        config=genai_types.GenerateContentConfig(
            temperature=temperature,
            max_output_tokens=max_output_tokens,
            top_p=0.9,
        ),
    )
    return (getattr(resp, "text", None) or "").strip()


def _call_vertex_text(
    *,
    model: str,
    prompt: str,
    temperature: float,
    max_output_tokens: int,
    project_id: str,
    location: str,
) -> str:
    try:
        from vertexai.generative_models import GenerativeModel, GenerationConfig
    except ImportError as e:
        raise ImportError(
            "Vertex AI를 쓰려면 `pip install google-cloud-aiplatform` 후 다시 실행하세요."
        ) from e

    _ensure_vertex_init(project_id, location)
    gen_model = GenerativeModel(model)
    config = GenerationConfig(
        temperature=temperature,
        max_output_tokens=max_output_tokens,
        top_p=0.9,
    )
    resp = gen_model.generate_content(prompt, generation_config=config)
    return (getattr(resp, "text", None) or "").strip()


def _wordmap_classify_nouns_polarity_llm(
    words: list[str],
    *,
    api_keys: list[str],
    use_vertex: bool,
    vertex_project_id: str | None,
    vertex_location: str | None,
    model: str,
) -> dict[str, str]:
    """
    워드맵 키워드(명사·형용사) 전체에 대해 제품·카테고리와 무관하게 단어 자체의 긍·부·중 극성을 Gemini로 분류.
    """
    if not words:
        return {}

    schema: dict[str, Any] = {
        "type": "object",
        "properties": {
            "items": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "word": {"type": "string"},
                        "polarity": {"type": "string", "enum": ["positive", "negative", "neutral"]},
                    },
                    "required": ["word", "polarity"],
                    "additionalProperties": False,
                },
            }
        },
        "required": ["items"],
        "additionalProperties": False,
    }
    words_json = json.dumps(words, ensure_ascii=False)
    prompt = f"""제품 리뷰에서 추출한 명사·형용사(키워드) 목록입니다. 카테고리는 화장품·가전·식품·의류·유아용품 등 어떤 것이든 될 수 있습니다.

각 단어를 **단어 자체만** 보고 일반적인 소비자 리뷰 맥락에서:
- positive: 긍정적 평가·만족·호의에 가까운 뉘앙스
- negative: 불만·불편·불호·부작용에 가까운 뉘앙스
- neutral: 사실·속성·재료명·측정·용도 등 감성이 분명하지 않음

입력에 나온 단어를 빠짐없이 items에 넣고, polarity는 소문자만 쓰세요.

단어 목록: {words_json}
"""
    loc = (vertex_location or "us-central1").strip() or "us-central1"

    if use_vertex:
        pid = (vertex_project_id or "").strip()
        if not pid:
            return {}
        raw = _call_vertex_json(
            model=model,
            prompt=prompt,
            temperature=0.15,
            max_output_tokens=2048,
            response_json_schema=schema,
            project_id=pid,
            location=loc,
        )
    else:
        if not api_keys:
            return {}
        last_exc: Exception | None = None
        raw: dict[str, Any] | None = None
        for k in api_keys:
            try:
                client = genai.Client(api_key=k)
                raw = _call_gemini_json(
                    client=client,
                    model=model,
                    prompt=prompt,
                    temperature=0.15,
                    max_output_tokens=2048,
                    response_json_schema=schema,
                )
                break
            except Exception as e:
                last_exc = e
                continue
        if raw is None:
            raise last_exc or RuntimeError("워드맵 극성 분류에 사용할 API 호출에 실패했습니다.")

    out: dict[str, str] = {}
    for it in raw.get("items", []) or []:
        if not isinstance(it, dict):
            continue
        w = (it.get("word") or "").strip()
        p = (it.get("polarity") or "neutral").strip().lower()
        if w and p in ("positive", "negative", "neutral"):
            out[w] = p
    return out


def _build_chunk_prompt(chunk_blob: str) -> str:
    schema_hint = {
        "chunk_review_count": 0,
        "sentiment": {"positive_percent": 0, "negative_percent": 0},
        "angle_weights": {aid: 0 for aid in ANGLE_IDS},
        "chunk_summary_for_final": "짧은 요약 문자열",
    }
    angles_block = "\n".join(f"- `{k}`: {v}" for k, v in ANGLE_DEFS)
    return f"""당신은 퍼포먼스 마케팅 분석가입니다. 아래는 리뷰 **일부 청크**입니다.

## 리뷰 청크
{chunk_blob}

## 분류 기준 (반드시 아래 8개 키만 사용)
{angles_block}

## 작업 (반드시 JSON만 출력)
1) 이 청크에서 긍정/부정 감성을 positive_percent/negative_percent(합 100)로 추정하세요.
2) 8개 angle이 얼마나 자주/핵심으로 등장했는지 angle_weights(각 0~100 상대중요도)로 추정하세요.
3) 최종 분석에 쓰기 위해 chunk_summary_for_final에 "제품에서 실제로 관찰된 문맥"을 중심으로 짧게 요약하세요. (5~12줄, 과장/허위 금지)

스키마 예시(JSON만):
{json.dumps(schema_hint, ensure_ascii=False, indent=2)}
"""


def _build_final_prompt(
    condensed_summaries: str,
    *,
    angle_weights_hint: dict[str, float],
    sentiment_hint: dict[str, float],
    top3_hint: list[str],
) -> str:
    schema_hint = {
        "sentiment": {"positive_percent": 0, "negative_percent": 0},
        "angle_weights": {aid: 0 for aid in ANGLE_IDS},
        "top3_angle_ids": ["", "", ""],
        "ad_copies_by_angle": {aid: ["", ""] for aid in ANGLE_IDS},
    }
    angles_block = "\n".join(f"- `{k}`: {v}" for k, v in ANGLE_DEFS)
    return f"""당신은 퍼포먼스 마케팅 분석가입니다. 아래는 여러 청크를 요약한 리뷰 근거입니다.

## 요약된 리뷰 근거
{condensed_summaries}

## 분류 기준 (반드시 아래 8개 키만 사용)
{angles_block}

## 힌트(참고용)
- sentiment_hint: {json.dumps(sentiment_hint, ensure_ascii=False)}
- angle_weights_hint: {json.dumps(angle_weights_hint, ensure_ascii=False)}
- top3_hint: {json.dumps(top3_hint, ensure_ascii=False)}

## 최종 작업 (반드시 JSON만 출력)
1) 전체 리뷰 기준으로 sentiment positive/negative percent를 추정하세요. (합 100)
2) angle_weights를 8개 키에 대해 0~100 상대중요도로 추정하세요. (힌트를 최대한 반영)
3) top3_angle_ids는 위 8개 키 중 angle_weights 상위 3개로 결정하세요.
4) 각 angle마다 CTR을 높일 수 있도록 자극적이지만 과장/허위가 아닌 광고 카피를 **정확히 2개**씩 만드세요.

주의: 리뷰에 없는 사실(특정 수치/의학적 효능/100% 보장 등)은 단정하지 마세요. 리뷰의 문맥에 근거해 표현하세요.

스키마 예시(JSON만):
{json.dumps(schema_hint, ensure_ascii=False, indent=2)}
"""


def _reviews_primarily_english(items: list[ReviewItem]) -> bool:
    """리뷰 샘플이 영어 중심이면 키워드·카피를 영어로 출력하기 위한 휴리스틱."""
    blob = " ".join((it.text or "") for it in items[:45])
    if len(blob.strip()) < 50:
        return False
    hangul = len(re.findall(r"[\uac00-\ud7a3]", blob))
    latin = len(re.findall(r"[A-Za-z]", blob))
    return latin >= 100 and latin > hangul * 3


def analyze_reviews_with_gemini(
    api_keys: list[str],
    items: list[ReviewItem],
    *,
    model: str,
    max_chunks: int,
    chunk_max_chars: int,
    product_name: str = "해당 제품",
    use_vertex: bool = False,
    vertex_project_id: str | None = None,
    vertex_location: str | None = None,
) -> dict[str, Any]:
    """
    - 리뷰 단위로 감성(positive/negative)과 8개 Angle 언급 여부를 분류
    - 리뷰 언급 빈도를 합산해서 Top3 Angle 선정
    - Top3 Angle별 CTR용 광고 카피 2개씩 생성
    """
    if not use_vertex and not api_keys:
        raise ValueError("API key 목록이 비어 있습니다. 환경변수 GOOGLE_API_KEY/GOOGLE_API_KEYS를 설정해 주세요.")
    if use_vertex and not (vertex_project_id or "").strip():
        raise ValueError(
            "Vertex AI 사용 시 GCP 프로젝트 ID가 필요합니다. "
            "사이드바에 입력하거나 환경 변수 `VERTEX_PROJECT_ID` / secrets를 설정해 주세요."
        )

    loc = (vertex_location or "us-central1").strip() or "us-central1"
    primary_en = _reviews_primarily_english(items)

    def call_llm_json(
        *,
        prompt: str,
        temperature: float,
        max_output_tokens: int,
        response_json_schema: dict[str, Any] | None,
    ) -> dict[str, Any]:
        if use_vertex:
            return _call_vertex_json(
                model=model,
                prompt=prompt,
                temperature=temperature,
                max_output_tokens=max_output_tokens,
                response_json_schema=response_json_schema,
                project_id=(vertex_project_id or "").strip(),
                location=loc,
            )
        last_exc: Exception | None = None
        for k in api_keys:
            try:
                client = genai.Client(api_key=k)
                return _call_gemini_json(
                    client=client,
                    model=model,
                    prompt=prompt,
                    temperature=temperature,
                    max_output_tokens=max_output_tokens,
                    response_json_schema=response_json_schema,
                )
            except Exception as e:
                if _is_api_key_invalid_error(e):
                    last_exc = e
                    continue
                raise
        if last_exc is not None:
            raise last_exc
        raise RuntimeError("모든 API key 호출이 실패했습니다.")

    # Gemini가 반드시 이 JSON 형태로만 응답하도록 강제합니다.
    classifier_response_schema: dict[str, Any] = {
        "type": "object",
        "properties": {
            "items": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "review_index": {"type": "integer"},
                        "sentiment": {"type": "string", "enum": ["positive", "negative"]},
                        # enum을 두면 모델/SDK가 보수적으로 빈 배열만 내는 경우가 있어 string만 둠(코드에서 id 정규화)
                        "mentioned_angle_ids": {"type": "array", "items": {"type": "string"}},
                    },
                    "required": ["review_index", "sentiment", "mentioned_angle_ids"],
                    "additionalProperties": False,
                },
            }
        },
        "required": ["items"],
        "additionalProperties": False,
    }

    def build_angle_classifier_prompt(chunk_items: list[ReviewItem], offset: int) -> str:
        angles_block = "\n".join(f"- `{aid}`: {label}" for aid, label in ANGLE_DEFS)
        reviews_payload = [
            {"review_index": offset + i, "text": (it.text or "").strip()}
            for i, it in enumerate(chunk_items)
            if (it.text or "").strip()
        ]

        schema_hint = {
            "items": [
                {
                    "review_index": 0,
                    "sentiment": "positive|negative",
                    "mentioned_angle_ids": ["efficacy", "pain_avoidance"],
                }
            ]
        }

        if primary_en:
            return f"""
You are a performance marketing analyst and review classifier.
Read each review and classify sentiment (positive/negative) and which Angles (marketing appeals) are mentioned.

## Angle definitions (use ONLY these 8 ids in mentioned_angle_ids)
{angles_block}

## Sentiment
- positive: clear satisfaction, recommendation, or repurchase intent
- negative: clear dissatisfaction, flaws, weak effect, or hesitation

## Angle rules
- mentioned_angle_ids must contain only these English ids: efficacy, pain_avoidance, value_efficiency, convenience, social_proof, ingredients_tech, emotion_experience, settler
- When the review touches product experience, include at least one angle when possible (empty array only if truly off-topic).
- Multiple angles per review are allowed.

## Input
Product name: {product_name}

Reviews (JSON array):
{json.dumps(reviews_payload, ensure_ascii=False)}

## Output (JSON only, no markdown)
{json.dumps(schema_hint, ensure_ascii=False, indent=2)}
""".strip()

        return f"""
너는 한국어 퍼포먼스 마케팅 분석가이자 리뷰 분류기야.
아래 리뷰들을 각각 읽고 감성(positive/negative)과 8가지 Angle(소구점) 언급 여부를 분류해라.

## Angle 기준 (반드시 아래 8개 키만 사용)
{angles_block}

## 감성 기준
- positive: 만족/추천/재구매 의사 등 체감이 명확
- negative: 불만/단점/효과 부족/재구매 망설임이 명확

## Angle 언급 기준
- mentioned_angle_ids에는 **아래 8개 영문 id만** 넣어라: efficacy, pain_avoidance, value_efficiency, convenience, social_proof, ingredients_tech, emotion_experience, settler
- 제품 사용·체험·만족/불만이 드러나면 **가능하면 1개 이상** 넣어라(빈 배열은 정말 무관할 때만).
- 한 리뷰에 여러 소구가 겹치면 모두 넣어도 된다.

## 입력
제품명: {product_name}

리뷰 목록(JSON 배열):
{json.dumps(reviews_payload, ensure_ascii=False)}

## 출력 (반드시 JSON만, 코드블록 없이)
{json.dumps(schema_hint, ensure_ascii=False, indent=2)}
""".strip()

    def build_ad_copy_prompt(top_angle_ids: list[str]) -> str:
        angle_lines = "\n".join([f"- {aid}: {label}" for aid, label in ANGLE_DEFS if aid in top_angle_ids])
        schema_hint = {"ad_copies_by_angle": {aid: ["카피1", "카피2", "카피3", "카피4", "카피5"] for aid in top_angle_ids}}
        return f"""
너는 한국어 성과형 퍼포먼스 카피라이터야.
아래 Top 5 Angle(소구점) 각각에 대해 CTR을 높일 수 있는 광고 카피를 “각 5개” 만들어라.

## 카피 생성 가이드 (핵심만, 간결하게)
- 각 카피는 1~2문장 허용 (내용이 부족하면 2문장까지 가능)
- 핵심 구조: "상품 특징/근거 → 소비자가 체감하는 효용(이득)"
- 효용은 구체적으로: 시간 절약, 번거로움 감소, 만족감 증가, 실패/불안 감소 등
- 글자수: 공백 제외 30~65자 권장 (너무 장황하면 축약)
- 과장/허위 금지 (특정 수치/치료/완치/100% 보장 등 표현 금지)
- 신뢰 기반 문장: “리뷰에서 많이 보인 포인트”를 자연스럽게 반영
- 각 5개 카피는 서로 표현이 겹치지 않게(중복 문장/동일 어휘 반복 최소화)

## 톤앤매너(요청사항)
1) 자연스럽고 일상적인 말투(대화하듯 읽히게)
3) 신뢰 기반 표현("최고/완벽" 같은 검증 어려운 표현 지양)
4) 사용자 존중(명령형/강요형 지양)

## 금지/주의
- 과장/허위 의료, "완치/치료" 같은 표현 금지
- 강요형: "지금 당장 구매", "무조건" 같은 문장 금지
- 리뷰에 없는 단정적 수치/의학적 효능을 단정하지 마라

## 입력
제품명: {product_name}

Top 5 Angle:
{angle_lines}

## 출력 (반드시 JSON만, 코드블록 없이)
{json.dumps(schema_hint, ensure_ascii=False, indent=2)}
""".strip()

    chunks = _chunk_reviews(items, max_chars=chunk_max_chars)
    chunks = chunks[: max(1, max_chunks)]

    sentiment_counts = {"positive": 0, "negative": 0}
    angle_counts: dict[str, int] = {aid: 0 for aid in ANGLE_IDS}
    per_review_rows: list[dict[str, Any]] = []

    offset = 0
    for chunk_items in chunks:
        prompt = build_angle_classifier_prompt(chunk_items, offset=offset)
        offset += len(chunk_items)

        try:
            partial = call_llm_json(
                prompt=prompt,
                temperature=0.2,
                max_output_tokens=6000,
                response_json_schema=classifier_response_schema,
            )
        except json.JSONDecodeError:
            # 분류 JSON이 깨지면, 해당 청크만 스킵하고 다음 청크로 계속 진행
            # (전체 앱이 멈추는 것을 방지하기 위함)
            continue

        raw_items = partial.get("items")
        if not isinstance(raw_items, list):
            raw_items = []
        for item in raw_items:
            if not isinstance(item, dict):
                continue
            sentiment = item.get("sentiment")
            if sentiment not in ("positive", "negative"):
                continue
            sentiment_counts[sentiment] += 1

            mentioned = _coerce_mentioned_angle_ids(item.get("mentioned_angle_ids"))
            mentioned_clean: list[str] = []
            for aid in mentioned:
                norm = _normalize_angle_id(aid)
                if norm is not None and norm in angle_counts and norm not in mentioned_clean:
                    mentioned_clean.append(norm)

            for aid in mentioned_clean:
                angle_counts[aid] += 1

            per_review_rows.append(
                {
                    "review_index": item.get("review_index"),
                    "sentiment": sentiment,
                    "mentioned_angle_ids": mentioned_clean,
                }
            )

    # LLM이 전부 빈 배열만 주면 건수가 0으로만 나오는 경우 → 본문 키워드로 보조 집계
    if sum(angle_counts.values()) == 0:
        angle_counts = {aid: 0 for aid in ANGLE_IDS}
        for it in items:
            txt = (it.text or "").strip()
            if not txt:
                continue
            for aid in _heuristic_angle_ids_for_review_text(txt):
                angle_counts[aid] += 1

    total_sent = max(1, sentiment_counts["positive"] + sentiment_counts["negative"])
    pos_pct = round((sentiment_counts["positive"] / total_sent) * 100.0, 1)
    neg_pct = round(100.0 - pos_pct, 1)

    top5 = [aid for aid, _ in sorted(angle_counts.items(), key=lambda x: x[1], reverse=True)[:5]]

    # ---------------------------------------------------------
    # 리뷰 효용 키워드 Top10 요약(광고 카피 대신)
    # ---------------------------------------------------------
    def safe_extract_top_keywords_from_text(text: str) -> list[str]:
        """
        json.loads 실패 시에도, 응답 문자열에서 "top_keywords"의 인용 문자열들을 최대한 수습합니다.
        (응답이 중간에 잘리는 경우 unterminated string이라도 완결된 따옴표 항목은 뽑을 수 있음)
        """
        if not text:
            return []
        try:
            start = text.find('"top_keywords"')
            if start == -1:
                start = text.find("top_keywords")
            if start == -1:
                return []
            sub = text[start : start + 4000]
            # sub 안에서 완결된 큰따옴표 문자열만 최대한 수집
            import re

            candidates = re.findall(r'"([^"]+)"', sub)
            # 한국어/공백/기호 포함 여부를 너무 빡빡하게 필터하지는 않음
            out: list[str] = []
            seen: set[str] = set()
            for c in candidates:
                c2 = (c or "").strip()
                if not c2:
                    continue
                # JSON 키 이름 "top_keywords"가 첫 매칭으로 들어오는 경우 제외
                if c2 == "top_keywords":
                    continue
                if c2 in seen:
                    continue
                # 너무 긴 문장은 제외(키워드용)
                if len(c2) > 40:
                    continue
                seen.add(c2)
                out.append(c2)
                if len(out) >= 10:
                    break
            return out
        except Exception:
            return []

    all_review_texts = [it.text.strip() for it in items if (it.text or "").strip()]
    # 토큰/요청량 보호: 1회 프롬프트에 너무 많은 리뷰를 넣지 않음
    # (리뷰 10~200건에서도 안정적으로 동작시키기 위함)
    max_review_for_keyword = 40
    max_chars_for_keyword = 25000
    selected_texts: list[str] = []
    running = 0
    for t in all_review_texts[:max_review_for_keyword]:
        if running + len(t) > max_chars_for_keyword:
            break
        selected_texts.append(t)
        running += len(t)

    if primary_en:
        keyword_prompt = f"""
You extract concise marketing keywords from customer reviews.

Requirements:
- Output exactly 10 items in top_keywords.
- Each keyword: 1–3 words in **English** (e.g. "non-sticky shine", "daily lip").
- Category-agnostic phrasing; merge synonyms and dedupe.
- Exclude brand names, product proper nouns, and retailer names.
- No medical claims or guaranteed outcomes.

Output JSON only (no markdown).
Review excerpts:
{chr(10).join(selected_texts[:40])}
""".strip()
    else:
        keyword_prompt = f"""
너는 "리뷰 효용 키워드 추출기"야.
아래 리뷰에서 소비자들이 반복해서 말하는 체감 효용/개선 포인트를 뽑아라.

요구사항:
- 총 10개를 반드시 출력: top_keywords (각 키워드는 1~3단어로 짧게, 예: "안색 개선", "잡티 제거"처럼)
- 스킨케어/식품/가전 등 어떤 카테고리에도 범용적으로 적용 가능한 표현이어야 함
- 브랜드명/상품명/고유명사는 제외
- 유사 표현은 합치고 중복 제거
- 과장/의학적 단정/100% 보장 같은 표현 금지

출력은 JSON만(코드블록 없이)으로 해.
리뷰 텍스트(일부):
{chr(10).join(selected_texts[:40])}
""".strip()

    keywords_response_schema: dict[str, Any] = {
        "type": "object",
        "properties": {
            "top_keywords": {
                "type": "array",
                "items": {"type": "string"},
                "minItems": 10,
                "maxItems": 10,
            }
        },
        "required": ["top_keywords"],
        "additionalProperties": False,
    }

    try:
        kw_resp = call_llm_json(
            prompt=keyword_prompt,
            temperature=0.2,
            max_output_tokens=2500,
            response_json_schema=keywords_response_schema,
        )
        top_keywords = kw_resp.get("top_keywords") or []
    except json.JSONDecodeError as e:
        # JSON이 깨지더라도, 응답 문자열에서 top_keywords 항목만 최대한 수습합니다.
        raw = getattr(e, "doc", "") or ""
        top_keywords = safe_extract_top_keywords_from_text(raw)
    if not isinstance(top_keywords, list):
        top_keywords = []
    # 모델이 10개를 못 채웠을 때를 대비(안전장치)
    top_keywords = [str(x).strip() for x in top_keywords]
    # 오탐·오출력: 리터럴 "top_keywords" 문자열 제거
    top_keywords = [x for x in top_keywords if x.lower() != "top_keywords"][:10]
    while len(top_keywords) < 10:
        top_keywords.append("")

    return {
        "sentiment": {"positive_percent": pos_pct, "negative_percent": neg_pct},
        "angle_weights": angle_counts,
        "top5_angle_ids": top5,
        "top_keywords": top_keywords,
        "per_review": per_review_rows,
    }


def _get_api_keys() -> list[str]:
    """
    여러 API 키를 지원합니다.
    - Streamlit secrets: GOOGLE_API_KEY (단일) 또는 GOOGLE_API_KEYS (쉼표/세미콜론 구분)
    - 환경변수: GOOGLE_API_KEY (단일) 또는 GOOGLE_API_KEYS (복수, 쉼표/세미콜론 구분)
    - fallback: GEMINI_API_KEY / GEMINI_API_KEYS 도 함께 봅니다.
    """

    import re

    def split_keys(s: str) -> list[str]:
        parts = re.split(r"[;,]\s*", s.strip())
        return [p.strip() for p in parts if p.strip()]

    keys: list[str] = []
    # secrets
    try:
        if hasattr(st, "secrets"):
            if st.secrets.get("GOOGLE_API_KEY"):
                keys.extend(split_keys(str(st.secrets["GOOGLE_API_KEY"])))
            if st.secrets.get("GOOGLE_API_KEYS"):
                keys.extend(split_keys(str(st.secrets["GOOGLE_API_KEYS"])))
    except Exception:
        pass

    # env
    env_single = os.environ.get("GOOGLE_API_KEY", "").strip()
    if env_single:
        keys.extend(split_keys(env_single))
    env_multi = os.environ.get("GOOGLE_API_KEYS", "").strip()
    if env_multi:
        keys.extend(split_keys(env_multi))

    # alt names
    env_single2 = os.environ.get("GEMINI_API_KEY", "").strip()
    if env_single2:
        keys.extend(split_keys(env_single2))
    env_multi2 = os.environ.get("GEMINI_API_KEYS", "").strip()
    if env_multi2:
        keys.extend(split_keys(env_multi2))

    # uniq (순서 유지)
    out: list[str] = []
    seen: set[str] = set()
    for k in keys:
        if not k or k in seen:
            continue
        seen.add(k)
        out.append(k)
    return out


def _vertex_env_defaults() -> tuple[str, str]:
    """VERTEX_PROJECT_ID / VERTEX_LOCATION — secrets 우선, 다음 환경 변수."""
    project = os.environ.get("VERTEX_PROJECT_ID", "").strip()
    location = os.environ.get("VERTEX_LOCATION", "us-central1").strip() or "us-central1"
    try:
        if hasattr(st, "secrets"):
            sp = st.secrets.get("VERTEX_PROJECT_ID")
            if sp:
                project = str(sp).strip()
            sl = st.secrets.get("VERTEX_LOCATION")
            if sl:
                location = str(sl).strip() or location
    except Exception:
        pass
    return project, location


def _is_api_key_invalid_error(e: Exception) -> bool:
    msg = str(e).upper()
    return ("API_KEY_INVALID" in msg) or ("API KEY" in msg and "INVALID" in msg) or ("NOT_AUTHENTICATED" in msg)


def _pick_one_working_api_key(api_keys: list[str]) -> tuple[list[str], str | None]:
    """
    api_keys 중 실제로 동작하는 키 1개만 골라서 반환합니다.
    (models.list()는 매우 가벼운 호출이라 1~1개만 확인합니다.)
    """
    if not api_keys:
        return [], None

    for k in api_keys:
        try:
            client_tmp = genai.Client(api_key=k)
            pager = client_tmp.models.list(config={"page_size": 1, "query_base": True})
            # google-genai Pager는 보통 .page에 첫 페이지 결과가 들어있습니다.
            page = getattr(pager, "page", None)
            if page and len(page) > 0:
                return [k], k
        except Exception:
            # 키가 invalid이면 다음 키로 넘어갑니다.
            # (다른 에러는 그대로 무시하고 다음 키를 시도)
            continue

    # 다 돌려도 못 찾으면, 원본 중 1개라도 반환(기존 에러 메시지 확인용)
    return [api_keys[0]], None


def _insight_bordered_card(key: str):
    """st.container(border=True, key=...) — 1.56+: flex 카드는 .st-key-* + [data-testid=stVerticalBlock] 한 요소에 렌더됨."""
    return st.container(border=True, key=key)


def main() -> None:
    st.set_page_config(
        page_title=APP_PAGE_TITLE,
        layout="wide",
        initial_sidebar_state="expanded",
    )
    st.html(
        f"<script>document.title = {json.dumps(APP_PAGE_TITLE)}</script>",
        unsafe_allow_javascript=True,
        width=1,
    )
    st.markdown(
        """
<style>
  /* 추천 팔레트: 페이지 #F6F7FB · 카드 #FFFFFF · 타이틀 #0F172A · 서브 #64748B · 보더 #D7DCE5 · 주버튼 #2563EB */
  .stApp, [data-testid="stAppViewContainer"] {
    background-color: #F6F7FB !important;
  }
  header[data-testid="stHeader"] {
    background: transparent;
  }

  /* 왼쪽 설정 사이드바 — 다크 네이비·슬레이트 (아래 규칙이 패널 색을 덮어씀) */
  section[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #18253A 0%, #0F1A2B 55%, #0F1A2B 100%) !important;
  }
  section[data-testid="stSidebar"] > div {
    background: transparent !important;
    border-right: 1px solid rgba(255, 255, 255, 0.08) !important;
  }
  section[data-testid="stSidebar"] h1,
  section[data-testid="stSidebar"] h2,
  section[data-testid="stSidebar"] h3 {
    color: #E5EDF8 !important;
  }
  section[data-testid="stSidebar"] label,
  section[data-testid="stSidebar"] [data-testid="stWidgetLabel"] p,
  section[data-testid="stSidebar"] [data-testid="stWidgetLabel"] span {
    color: #E5EDF8 !important;
  }
  section[data-testid="stSidebar"] .stMarkdown,
  section[data-testid="stSidebar"] .stMarkdown p,
  section[data-testid="stSidebar"] .stMarkdown strong,
  section[data-testid="stSidebar"] .stMarkdown li {
    color: #E5EDF8 !important;
  }
  section[data-testid="stSidebar"] .stCaption,
  section[data-testid="stSidebar"] [data-testid="stCaption"] {
    color: #A9B7C9 !important;
  }
  section[data-testid="stSidebar"] .stRadio > label,
  section[data-testid="stSidebar"] .stRadio > div > label,
  section[data-testid="stSidebar"] .stRadio [role="radiogroup"] label,
  section[data-testid="stSidebar"] .stRadio [data-baseweb="radio"] label,
  section[data-testid="stSidebar"] [data-baseweb="radio"] label,
  section[data-testid="stSidebar"] ul[role="radiogroup"] label,
  section[data-testid="stSidebar"] .stRadio label p,
  section[data-testid="stSidebar"] .stRadio label span,
  section[data-testid="stSidebar"] .stRadio div[role="radiogroup"] label p,
  section[data-testid="stSidebar"] .stRadio div[role="radiogroup"] label span {
    color: #E5EDF8 !important;
  }
  section[data-testid="stSidebar"] .stRadio [data-baseweb="radio"] div {
    color: #E5EDF8 !important;
  }
  section[data-testid="stSidebar"] input,
  section[data-testid="stSidebar"] textarea {
    background-color: #36465A !important;
    color: #E5EDF8 !important;
    border-color: rgba(255, 255, 255, 0.14) !important;
    caret-color: #E5EDF8 !important;
  }
  section[data-testid="stSidebar"] input::placeholder,
  section[data-testid="stSidebar"] textarea::placeholder {
    color: #A9B7C9 !important;
    opacity: 1 !important;
  }
  section[data-testid="stSidebar"] [data-baseweb="input"] input::placeholder,
  section[data-testid="stSidebar"] [data-baseweb="textarea"] textarea::placeholder {
    color: #A9B7C9 !important;
    opacity: 1 !important;
  }
  section[data-testid="stSidebar"] input[type="password"]::placeholder {
    color: #A9B7C9 !important;
    opacity: 1 !important;
  }
  section[data-testid="stSidebar"] input:focus,
  section[data-testid="stSidebar"] textarea:focus,
  section[data-testid="stSidebar"] input:focus-visible,
  section[data-testid="stSidebar"] textarea:focus-visible {
    box-shadow: inset 0 0 0 1px #3B4B60 !important;
    outline: none !important;
  }
  section[data-testid="stSidebar"] [data-baseweb="select"] > div,
  section[data-testid="stSidebar"] [data-baseweb="input"] > div {
    background-color: #36465A !important;
    border-color: rgba(255, 255, 255, 0.14) !important;
  }
  section[data-testid="stSidebar"] [data-baseweb="select"] span,
  section[data-testid="stSidebar"] [data-testid="stSelectValue"] {
    color: #E5EDF8 !important;
  }
  section[data-testid="stSidebar"] small,
  section[data-testid="stSidebar"] .stTooltipIcon,
  section[data-testid="stSidebar"] [data-testid="stHelp"] {
    color: #A9B7C9 !important;
  }
  section[data-testid="stSidebar"] [data-testid="stCaption"],
  section[data-testid="stSidebar"] [data-testid="stCaption"] p,
  section[data-testid="stSidebar"] [data-testid="stCaption"] span,
  section[data-testid="stSidebar"] [data-testid="stCaption"] code,
  section[data-testid="stSidebar"] div[data-testid="stCaption"] p,
  section[data-testid="stSidebar"] div[data-testid="stCaption"] span {
    color: #A9B7C9 !important;
  }
  /*
   * 사이드바 접기(<<): 1.56은 Material 폰트 아이콘(data-testid=stIconMaterial) + theme fadedText60 이라
   * svg fill 규칙이 먹지 않음 → stIconMaterial 에 color:#FFF 로 지정.
   * 데스크톱 기본은 헤더 호버 시에만 visibility:visible → 항상 보이게 덮어씀.
   */
  [data-testid="stSidebarCollapseButton"] {
    visibility: visible !important;
    color: #FFFFFF !important;
    opacity: 1 !important;
  }
  [data-testid="stSidebarCollapseButton"] [data-testid="stIconMaterial"],
  [data-testid="stSidebarCollapseButton"] [data-testid="stIconMaterial"] * {
    color: #FFFFFF !important;
    opacity: 1 !important;
  }
  [data-testid="stSidebarCollapseButton"] svg,
  [data-testid="stSidebarCollapseButton"] svg * {
    fill: #FFFFFF !important;
    stroke: #FFFFFF !important;
  }

  /*
   * 1.56+: bordered container는 별도 BorderWrapper 없이 StyledFlexContainerBlock 한 노드에 border+키 클래스가 붙음.
   * data-testid는 stVerticalBlock(세로)이며, key는 .st-key-{key} 로 동일 요소에 합쳐짐.
   * 메인의 두 인사이트 카드만 타겟(전역 stVerticalBlock 스타일 금지).
   */
  [data-testid="stMain"] .st-key-input_exec_card[data-testid="stVerticalBlock"],
  [data-testid="stMain"] .st-key-wm_top5_card[data-testid="stVerticalBlock"] {
    background: #FFFFFF !important;
    border: 1px solid #D7DCE5 !important;
    border-radius: 16px !important;
    padding: 20px 24px !important;
    box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04), 0 2px 6px rgba(15, 23, 42, 0.06) !important;
  }
  [data-testid="stMain"] .st-key-input_exec_card[data-testid="stVerticalBlock"] [data-testid="column"],
  [data-testid="stMain"] .st-key-wm_top5_card[data-testid="stVerticalBlock"] [data-testid="column"],
  section.main .st-key-input_exec_card[data-testid="stVerticalBlock"] [data-testid="column"],
  section.main .st-key-wm_top5_card[data-testid="stVerticalBlock"] [data-testid="column"],
  .main .st-key-input_exec_card[data-testid="stVerticalBlock"] [data-testid="column"],
  .main .st-key-wm_top5_card[data-testid="stVerticalBlock"] [data-testid="column"] {
    align-self: stretch !important;
  }
  /* 상품 URL·리뷰 붙여넣기 (메인 입력 카드) */
  [data-testid="stMain"] .stTextInput [data-baseweb="input"] input,
  [data-testid="stMain"] .stTextInput input,
  [data-testid="stMain"] .stTextArea textarea,
  section.main .stTextInput [data-baseweb="input"] input,
  section.main .stTextInput input,
  section.main .stTextArea textarea,
  .main .stTextInput [data-baseweb="input"] input,
  .main .stTextInput input,
  .main .stTextArea textarea {
    border: 1px solid #CBD5E1 !important;
    border-radius: 10px !important;
  }
  .stButton > button[kind="primary"],
  button[kind="primary"] {
    background-color: #2563EB !important;
    border-color: #2563EB !important;
    color: #FFFFFF !important;
  }
  .stButton > button[kind="primary"]:hover,
  button[kind="primary"]:hover {
    background-color: #1d4ed8 !important;
    border-color: #1d4ed8 !important;
    color: #FFFFFF !important;
  }
.insight-hero {
  border: 1px solid #E2E8F0;
  border-radius: 14px;
  padding: 1.35rem 1.5rem 1.15rem 1.5rem;
  margin-bottom: 1.25rem;
  background: #FFFFFF;
  box-shadow: 0 1px 2px rgba(15, 23, 42, 0.05);
}
.insight-hero .hero-title {
  font-size: 1.65rem;
  font-weight: 700;
  letter-spacing: -0.02em;
  color: #0F172A;
  margin: 0 0 0.45rem 0;
  line-height: 1.28;
}
.insight-hero .hero-sub {
  font-size: 0.98rem;
  color: #64748B;
  margin: 0;
  line-height: 1.55;
}
.insight-card-title {
  font-size: 1.08rem;
  font-weight: 600;
  color: #0F172A;
  margin: 0 0 0.85rem 0;
  letter-spacing: -0.01em;
}
.insight-muted {
  font-size: 0.88rem;
  color: #64748B;
  margin-top: 0.35rem;
}
</style>
""",
        unsafe_allow_html=True,
    )
    inject_dashboard_styles()
    st.markdown(
        f"""
<div class="insight-hero">
  <p class="hero-title">{html.escape(APP_PAGE_TITLE)}</p>
  <p class="hero-sub">리뷰에서 소비자 언어를 추출해 광고 소구점과 카피로 전환합니다</p>
</div>
""",
        unsafe_allow_html=True,
    )

    def _extract_model_ids_from_list_resp(resp: Any) -> list[str]:
        """
        google-genai에서 models.list는 Pager를 반환합니다.
        - resp.page: 현재 페이지의 Model 객체들
        - 버전에 따라 resp.models / resp.items 같은 키가 있을 수 있어 방어적으로 처리합니다.
        """
        models = None
        if hasattr(resp, "page"):
            models = getattr(resp, "page", None)
        if not models:
            models = getattr(resp, "models", None)
        if not models and isinstance(resp, dict):
            models = resp.get("models") or resp.get("items")
        if not models:
            return []

        out: list[str] = []
        for m in models:
            raw = None
            # google-genai types.Model은 보통 name을 가집니다.
            if hasattr(m, "name"):
                raw = getattr(m, "name")
            elif hasattr(m, "id"):
                raw = getattr(m, "id")
            elif isinstance(m, dict):
                raw = m.get("name") or m.get("id")
            if not raw:
                raw = str(m)

            raw = str(raw).strip()
            # 예: models/gemini-1.5-pro 또는 publishers/google/models/gemini-...
            if raw.startswith("models/"):
                raw = raw[len("models/") :]
            if "models/" in raw:
                raw = raw.split("models/", 1)[-1]
            out.append(raw)

        # 중복 제거(순서 유지)
        seen: set[str] = set()
        uniq: list[str] = []
        for x in out:
            if not x or x in seen:
                continue
            seen.add(x)
            uniq.append(x)
            seen.add(x)
        return uniq

    with st.sidebar:
        st.header("설정")
        llm_backend = st.radio(
            "Gemini 백엔드",
            options=["Google AI Studio (API 키)", "Vertex AI (GCP 크레딧)"],
            index=0,
            help=(
                "AI Studio는 `GOOGLE_API_KEY`로 호출합니다. "
                "Vertex는 `google-cloud-aiplatform` + GCP 인증으로 호출하며 과금이 Vertex 프로젝트에 붙습니다."
            ),
        )
        use_vertex = llm_backend.startswith("Vertex")

        v_def_project, v_def_loc = _vertex_env_defaults()
        vertex_project_input = st.text_input(
            "Vertex GCP 프로젝트 ID",
            value=v_def_project,
            placeholder="예: my-gcp-project-id",
            disabled=not use_vertex,
            help="Console의 프로젝트 ID. secrets의 VERTEX_PROJECT_ID 또는 여기 입력.",
        )
        vertex_location_input = st.text_input(
            "Vertex 리전",
            value=v_def_loc,
            placeholder="us-central1 / asia-northeast3",
            disabled=not use_vertex,
            help="Gemini가 배포된 리전(예: 서울은 asia-northeast3).",
        )

        if use_vertex:
            st.caption(
                "Vertex: `pip install google-cloud-aiplatform` 후 "
                "`gcloud auth application-default login` 등으로 ADC를 설정하세요."
            )
        else:
            st.markdown(
                """
**API 키**는 아래 중 한 곳에만 두면 됩니다.
1. `.streamlit/secrets.toml` 의 `GOOGLE_API_KEY` (**권장**)
2. PC 환경 변수 `GOOGLE_API_KEY`
3. 키가 위에 없을 때만: 아래 입력란 (임시 테스트용)
"""
            )
        api_key_sidebar = st.text_input(
            "Google API 키 (선택)",
            type="password",
            placeholder="비워두면 secrets / 환경 변수 사용",
            help="Google AI Studio에서 발급한 키. Vertex 모드에서는 사용하지 않습니다.",
            disabled=use_vertex,
        )
        model_name = st.text_input(
            "Gemini 모델명",
            value=DEFAULT_MODEL_NAME,
            help=(
                "AI Studio·Vertex 공통 짧은 이름(예: gemini-2.5-flash, gemini-1.5-flash-002). "
                "Vertex에서 해당 리전에 배포된 모델이어야 합니다."
            ),
        )

        normalized_model_name = normalize_gemini_model_name(model_name)
        if normalized_model_name != model_name:
            st.caption(f"모델 이름에서 `models/` 접두사를 제거했어요: `{normalized_model_name}`")
        else:
            backend_label = "Vertex AI" if use_vertex else "Google AI Studio"
            st.caption(f"{backend_label} · 모델: `{normalized_model_name}`")

        st.divider()
        st.subheader("모델 목록 (디버그 · AI Studio만)")
        list_models = st.button("지원 모델 목록 조회", disabled=use_vertex)
        if list_models and not use_vertex:
            try:
                api_keys_dbg = _get_api_keys()
                if api_key_sidebar and api_key_sidebar.strip():
                    api_keys_dbg = [api_key_sidebar.strip()] + api_keys_dbg
                api_keys_dbg, _ = _pick_one_working_api_key(api_keys_dbg)
                dbg_key = api_keys_dbg[0] if api_keys_dbg else None
                if not dbg_key:
                    st.error("API 키가 없습니다. 환경변수 `GOOGLE_API_KEY` 또는 secrets로 먼저 설정해 주세요.")
                else:
                    with st.spinner("지원 모델 목록을 조회 중입니다..."):
                        client_tmp = genai.Client(api_key=dbg_key)
                        resp = client_tmp.models.list(config={"page_size": 50, "query_base": True})
                        model_ids = _extract_model_ids_from_list_resp(resp)
                        st.success(f"조회 완료: {len(model_ids)}개")
                        st.write("예: " + ", ".join(model_ids[:25]))

                        if normalized_model_name and normalized_model_name not in model_ids:
                            candidates = [m for m in model_ids if "gemini-1.5" in m][:10]
                            if candidates:
                                st.warning("현재 모델이 목록에 없어요. 후보 예시: " + ", ".join(candidates))
            except Exception as e:
                st.error(f"모델 목록 조회 실패: {e}")
        elif list_models and use_vertex:
            st.info("Vertex 모드에서는 Console의 모델 문서를 참고하세요.")

    api_keys = _get_api_keys()
    if not use_vertex:
        if api_key_sidebar and api_key_sidebar.strip():
            api_keys = [api_key_sidebar.strip()] + api_keys
        api_keys, api_key_selected = _pick_one_working_api_key(api_keys)
    else:
        api_keys = []
    api_key = api_keys[0] if api_keys else None

    input_shell = _insight_bordered_card("input_exec_card")
    with input_shell:
        st.markdown(
            '<p class="insight-card-title">입력 및 실행</p>',
            unsafe_allow_html=True,
        )
        shopping_url = st.text_input(
            "상품 URL",
            value="",
            placeholder="https://… 분석할 상품 페이지 주소를 입력하세요.",
            help="URL 크롤링이 막히면 아래에 리뷰를 직접 붙여넣을 수 있습니다.",
        )
        review_source = st.radio(
            "리뷰 소스",
            options=["URL 크롤링", "붙여넣기", "URL + 붙여넣기"],
            index=0,
            horizontal=True,
            help="붙여넣기만 선택하면 크롤링 없이 텍스트만 분석합니다.",
        )

        try:
            s_col1, s_col2 = st.columns(2, vertical_alignment="center")
        except TypeError:
            s_col1, s_col2 = st.columns(2)
        with s_col1:
            max_pages = st.slider(
                "최대 페이지 수 (페이지네이션·더보기)",
                min_value=1,
                max_value=10,
                value=3,
                step=1,
            )
        with s_col2:
            max_total_reviews = st.slider(
                "최대 리뷰 수 (비용 보호)",
                min_value=10,
                max_value=100,
                value=20,
                step=10,
            )
        # 페이지 10까지면 텍스트가 과도하지 않을 거라 가정하고, 청크 제한 UI는 제거합니다.
        # (내부적으로는 모델 컨텍스트 여유를 위해 충분히 크게 잡아둡니다.)
        max_chunks = 999
        chunk_max_chars = 200000

        pasted_reviews = st.text_area(
            "리뷰 직접 붙여넣기",
            height=220,
            placeholder="예: 쇼핑몰 리뷰를 붙여넣어 주세요.\n\n[리뷰 1] … 형태로 붙여넣으면 더 잘 분리됩니다.",
        )

        try:
            b_col1, b_col2 = st.columns([1, 2.2], vertical_alignment="center")
        except TypeError:
            b_col1, b_col2 = st.columns([1, 2.2])
        with b_col1:
            run = st.button("광고 인사이트 생성하기", type="primary", use_container_width=True)
        with b_col2:
            st.markdown(
                '<p class="insight-muted">리뷰에서 핵심 소구점, 키워드, 카피를 추출합니다.</p>',
                unsafe_allow_html=True,
            )

    if not run:
        st.info("리뷰 소스와 URL·텍스트를 설정한 뒤 **광고 인사이트 생성하기**를 눌러 주세요.")
        return

    ep, el0 = _vertex_env_defaults()
    v_project = (vertex_project_input or "").strip() or ep
    v_location = (vertex_location_input or "").strip() or el0
    if use_vertex:
        if not v_project:
            st.error("Vertex AI: GCP 프로젝트 ID를 입력하거나 secrets / 환경 변수 `VERTEX_PROJECT_ID`를 설정하세요.")
            return
    elif not api_keys:
        st.error("Google AI Studio: API 키가 없습니다. 사이드바에 입력하거나 `GOOGLE_API_KEY`를 설정하세요.")
        return

    use_url = review_source in ("URL 크롤링", "URL + 붙여넣기")
    use_paste = review_source in ("붙여넣기", "URL + 붙여넣기")

    items: list[ReviewItem] = []
    notes: list[str] = []

    if use_url:
        if not (shopping_url or "").strip():
            st.error("리뷰 소스가 URL을 포함하는데 상품 URL이 비어 있습니다.")
            return
        with st.spinner(f"리뷰 크롤링 중… (최대 {max_pages}페이지, 최대 {max_total_reviews}건)"):
            crawled, note = collect_reviews(shopping_url, max_pages=max_pages, max_reviews=max_total_reviews)
            items.extend(crawled)
            notes.append(note)

    if use_paste:
        with st.spinner("붙여넣기 텍스트 파싱 중…"):
            remaining = max_total_reviews - len(items)
            pasted_items = _parse_pasted_reviews(pasted_reviews, max_items=max(0, remaining))
            items.extend(pasted_items)
            if pasted_items:
                notes.append(f"붙여넣기 파싱 성공: {len(pasted_items)}건")

    # 간단 중복 제거(텍스트 기반)
    dedup_seen: set[str] = set()
    deduped: list[ReviewItem] = []
    for it in items:
        k = (it.text or "").strip()
        if not k or k in dedup_seen:
            continue
        dedup_seen.add(k)
        deduped.append(it)
        if len(deduped) >= max_total_reviews:
            break
    # 한 블록에 '최**님 … 박**님 …'처럼 여러 리뷰가 붙은 경우(오늘의집 등) 분리
    merged_split = expand_merged_review_items(deduped)
    dedup_seen2: set[str] = set()
    items = []
    for it in merged_split:
        k = (it.text or "").strip()
        if not k or k in dedup_seen2:
            continue
        dedup_seen2.add(k)
        items.append(it)
        if len(items) >= max_total_reviews:
            break

    if not items:
        if notes:
            st.warning("분석할 리뷰가 없습니다. " + " / ".join(notes))
        else:
            st.warning("분석할 리뷰가 없습니다. URL을 입력하거나 Text Area에 붙여넣어 주세요.")
        st.info(
            "후속 액션: 크롤링이 차단되었을 수 있어요. "
            "페이지의 리뷰 영역을 직접 복사해서 넣은 뒤 다시 실행해 주세요."
        )
        return

    st.success("리뷰 준비 완료: " + (", ".join(notes) if notes else f"{len(items)}건"))
    if use_url and len(items) < 50:
        st.warning("크롤링으로 리뷰가 충분히 모이지 않았을 수 있어요. 아래 Text Area에 추가로 붙여넣으면 분석 품질이 좋아집니다.")

    with st.expander("리뷰 샘플 (앞부분만 보기)"):
        st.text(_items_to_review_blob(items[: min(30, len(items))]))

    st.caption(f"총 리뷰: {len(items)}건 (페이지 수: 최대 {max_pages}까지 수집)")

    with st.spinner("Gemini 분석 중… (리뷰 단위 분류 + 집계)"):
        try:
            result = analyze_reviews_with_gemini(
                api_keys,
                items,
                model=normalized_model_name,
                max_chunks=max_chunks,
                chunk_max_chars=chunk_max_chars,
                use_vertex=use_vertex,
                vertex_project_id=v_project if use_vertex else None,
                vertex_location=v_location if use_vertex else None,
            )
        except json.JSONDecodeError as e:
            st.error(f"JSON 파싱 실패: {e}")
            return
        except Exception as e:
            st.error(f"Gemini 호출 오류: {e}")
            return

    # 워드맵 vs 소구점 표: 표가 너무 좁으면 가로 스크롤이 생기므로 표 쪽 비중을 충분히 둠
    _wm_top5_outer = _insight_bordered_card("wm_top5_card")
    with _wm_top5_outer:
        try:
            col1, col2 = st.columns([1, 1.18], vertical_alignment="top")
        except TypeError:
            col1, col2 = st.columns([1, 1.18])
        with col1:
            st.subheader("주요 키워드 워드맵")
            word_items = build_wordmap_keywords(items)
            if WORDMAP_EXTRACT_MODE == "regex":
                st.caption(
                    "Kiwi(`kiwipiepy`)를 불러오지 못했습니다. `pip install kiwipiepy` 후 앱을 다시 실행하면 명사·용언 기준 워드맵이 적용됩니다."
                )
            if not word_items:
                st.caption("워드맵을 만들 키워드가 부족합니다.")
            else:
                words = [w for w, _ in word_items]
                freqs = [f for _, f in word_items]
                max_f = max(freqs) if freqs else 1
                min_f = min(freqs) if freqs else 1
                sizes: list[float] = []
                for f in freqs:
                    if max_f == min_f:
                        sizes.append(44.0)
                    else:
                        # 상위는 크게, 다만 감마를 낮춰 중·하위도 함께 키워(참고: 1위 대비 2~4배급 대비)
                        t = (f - min_f) / (max_f - min_f)
                        t = t**1.14
                        sizes.append(15.5 + t * 32.5)
    
                n_kw = len(words)
                size_scale = 0.91 if n_kw > 12 else 0.95 if n_kw > 8 else 1.0
                sizes = [max(13.5, min(50.0, s * size_scale)) for s in sizes]
    
                xs, ys = _wordmap_scatter_positions(words, sizes)
                # 세로는 중앙보다 위쪽 띠에 배치(완성 후 y축 자동 범위로 아래 빈 여백 제거)
                y_lo, y_hi = (0.34, 0.86) if n_kw > 11 else (0.36, 0.84)
                xs, ys = _wordmap_rect_normalize(xs, ys, x_margin=0.07, y_band=(y_lo, y_hi))
                xs, ys = _wordmap_pull_to_center(xs, ys, strength=0.48)
                # 정규화/당김 후에도 겹칠 수 있어 축정렬 박스 기준으로 분리
                _hwm, _hhm = _wordmap_half_extents(words, sizes)
                xs, ys = _wordmap_relax_aabb_overlaps(
                    xs, ys, _hwm, _hhm, sizes, gap=0.02, iterations=480, margin=0.018
                )
                xs, ys = _wordmap_contract_toward_centroid(xs, ys, factor=0.9)
                xs, ys = _wordmap_relax_aabb_overlaps(
                    xs, ys, _hwm, _hhm, sizes, gap=0.015, iterations=260, margin=0.014
                )
                xs, ys = _wordmap_relax_aabb_overlaps(
                    xs, ys, _hwm, _hhm, sizes, gap=0.012, iterations=200, margin=0.012
                )
                # 키워드 덩어리를 살짝 위로 올린 뒤, y축을 데이터 범위에만 맞춰 차트 아래쪽 빈 공간 제거
                ys = [min(0.99, max(0.02, y + 0.08)) for y in ys]
    
                llm_pol: dict[str, str] = {}
                with st.spinner("워드맵: 키워드 극성 분류 중…"):
                    try:
                        llm_pol = _wordmap_classify_nouns_polarity_llm(
                            words,
                            api_keys=api_keys,
                            use_vertex=use_vertex,
                            vertex_project_id=v_project if use_vertex else None,
                            vertex_location=v_location if use_vertex else None,
                            model=normalized_model_name,
                        )
                    except Exception as e:
                        st.caption(f"워드맵 색: 극성 분류에 실패해 모두 중립으로 표시합니다. ({e})")
                colors = _wordmap_sentiment_colors(words, llm_polarity=llm_pol)
    
                y_min, y_max = min(ys), max(ys)
                pad_y = 0.045
                y_axis0 = max(0.0, y_min - pad_y)
                y_axis1 = min(1.0, y_max + pad_y)
                if y_axis1 - y_axis0 < 0.2:
                    mid_y = (y_axis0 + y_axis1) / 2
                    y_axis0 = max(0.0, mid_y - 0.1)
                    y_axis1 = min(1.0, mid_y + 0.1)
    
                # 세로 픽셀: 과도한 높이를 줄이되, 글자 크기 대비 최소 높이 유지
                wm_height = min(300, max(215, 198 + min(n_kw, 14) * 4))
                fig_word = go.Figure(
                    data=[
                        go.Scatter(
                            x=xs,
                            y=ys,
                            mode="text",
                            text=words,
                            textfont={"size": sizes, "color": colors},
                            textposition="middle center",
                            hovertext=[f"{w}: {f}회" for w, f in word_items],
                            hoverinfo="text",
                            cliponaxis=False,
                        )
                    ]
                )
                # 가로로 넓은 컨테이너에서 양축 constrain/domain이 맞물리면 세로가 정사각형으로 맞춰져
                # 위·아래에 큰 빈 여백이 생길 수 있음 → scaleanchor 해제, 상단 마진 최소화
                fig_word.update_xaxes(visible=False, range=[0, 1], fixedrange=True, showgrid=False, zeroline=False)
                fig_word.update_yaxes(
                    visible=False,
                    range=[y_axis0, y_axis1],
                    fixedrange=True,
                    showgrid=False,
                    zeroline=False,
                )
                fig_word.update_layout(
                    height=wm_height,
                    margin=dict(l=6, r=6, t=2, b=4),
                    plot_bgcolor="white",
                    paper_bgcolor="white",
                    xaxis=dict(scaleanchor=None, scaleratio=None),
                    yaxis=dict(scaleanchor=None, scaleratio=None),
                )
                st.plotly_chart(fig_word, use_container_width=True)
                st.markdown(_wordmap_sentiment_legend_html(), unsafe_allow_html=True)
            

        with col2:
            st.subheader("핵심 소구점 (Top 5)")
            weights = result.get("angle_weights") or {}
            top5 = result.get("top5_angle_ids") or []
            rows = []
            for rank, aid in enumerate(top5[:5], 1):
                if aid not in ANGLE_LABEL_BY_ID:
                    continue
                rows.append(
                    {"순위": rank, "소구점": ANGLE_LABEL_BY_ID[aid], "리뷰 언급(건)": weights.get(aid, 0)}
                )
            if rows:
                st.dataframe(
                    rows,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "순위": st.column_config.NumberColumn("순위", width="small"),
                        "소구점": st.column_config.TextColumn("소구점", width="large"),
                        "리뷰 언급(건)": st.column_config.NumberColumn("리뷰 언급(건)", width="small"),
                    },
                )
            else:
                st.write("Top5 정보를 가져오지 못했습니다. `top5_angle_ids` 형식을 확인하세요.")

    st.divider()
    with st.spinner("Gemini로 리뷰 기반 추천 광고 카피 생성 중…"):
        try:
            ad_copy_text, ad_copy_blocks = generate_recommended_ad_copies(
                items=items,
                result=result,
                api_keys=api_keys,
                model=normalized_model_name,
                use_vertex=use_vertex,
                vertex_project_id=v_project if use_vertex else None,
                vertex_location=v_location if use_vertex else None,
            )
        except Exception as e:
            st.caption(f"추천 광고 카피를 생성하지 못했습니다. ({e})")
        else:
            if ad_copy_blocks:
                render_ad_copy_cards(ad_copy_blocks)
            elif ad_copy_text.strip():
                st.caption("생성된 카피 블록을 표시할 수 없습니다.")
            else:
                st.caption("생성된 카피가 비어 있습니다.")

    st.divider()
    top_keywords = [k for k in (result.get("top_keywords") or []) if (k or "").strip()][:10]
    if not top_keywords:
        st.caption("키워드 요약을 가져오지 못했습니다.")
    else:
        render_keyword_chips(top_keywords)

    st.markdown(
        _st_html_fragment("""<div class="gs-debug-spacer"></div>"""),
        unsafe_allow_html=True,
    )
    try:
        _debug_box = st.container(border=True)
    except TypeError:
        _debug_box = st.container()
    with _debug_box:
        with st.expander("원본 JSON (디버그)"):
            st.json(result)


if __name__ == "__main__":
    main()
