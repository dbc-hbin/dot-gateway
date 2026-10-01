#!/Users/USER/.hermes/hermes-agent/venv/bin/python
"""Collect new or changed AI deal posts for Hermes cron classification."""

from __future__ import annotations

import hashlib
import json
import re
import sys
import warnings
from html import unescape
from pathlib import Path
from urllib.parse import urljoin, urlsplit, urlunsplit

from bs4 import BeautifulSoup

try:
    from bs4 import MarkupResemblesLocatorWarning
except ImportError:  # pragma: no cover - older BeautifulSoup
    MarkupResemblesLocatorWarning = None
if MarkupResemblesLocatorWarning is not None:
    warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning)

INSANE_ROOT = Path.home() / ".hermes/insane-search/skills/insane-search"
if str(INSANE_ROOT) not in sys.path:
    sys.path.insert(0, str(INSANE_ROOT))

from engine import fetch  # noqa: E402

STATE_PATH = Path.home() / ".hermes/data/ai-promotion-scanner/state.json"
MAX_SCAN_PER_SOURCE = 40
MAX_PER_SOURCE = 4
MAX_TOTAL = 16

SOURCES = (
    ("dcinside-ai-utilize", "https://m.dcinside.com/board/ai_utilize"),
    ("linux.do-welfare", "https://linux.do/c/welfare/36.json"),
    ("linux.do-latest", "https://linux.do/latest.json"),
    ("v2ex-latest", "https://www.v2ex.com/api/topics/latest.json"),
    ("v2ex-hot", "https://www.v2ex.com/api/topics/hot.json"),
    ("nodeloc-deals", "https://www.nodeloc.com/c/business/information/10.json"),
    ("nodeloc-latest", "https://www.nodeloc.com/latest.json"),
)

# Generic vocabulary cannot establish AI relevance by itself.
GENERIC_AI_TERMS = (
    "api", "token", "토큰", "额度", "模型", "모델",
)
SPECIFIC_AI_TERMS = (
    "ai", "llm", "인공지능",
    "chatgpt", "openai", "claude", "anthropic", "gemini", "google ai",
    "grok", "xai", "perplexity", "cursor", "windsurf", "copilot", "codex",
    "openrouter", "deepseek", "kimi", "qwen", "glm", "minimax", "suno",
    "runway", "midjourney", "elevenlabs", "mistral", "huggingface",
    "novita", "zed", "qoder", "zcode", "华为", "码道", "gitee",
)
AI_TERMS = GENERIC_AI_TERMS + SPECIFIC_AI_TERMS

# Weak community vocabulary is used only with an official vendor or a
# concrete amount. Strong terms can stand on their own.
STRONG_DEAL_TERMS = (
    "쫀쿠", "쌀먹", "특가", "핫딜", "역대가", "쿠폰", "할인", "크레딧",
    "리딤", "리딤코드", "초대코드", "구독권", "공구", "공동구매",
    "선착순", "한정", "버그딜", "가격오류", "꿀통", "나눔",
    "白嫖", "羊毛", "薅羊毛", "限免", "优惠券", "折扣", "特价",
    "兑换码", "邀请码", "激活码", "代金券", "拼团", "拼车", "合租",
    "车位", "开车", "组队", "续杯", "首月", "日卡", "月卡", "年卡", "地区价",
    "giveaway", "coupon", "voucher", "redeem", "lifetime",
    "student plan", "education plan", "regional pricing", "api credits",
    "cashback", "invite code", "无需绑卡", "学生认证", "教育优惠",
    "换购", "以旧换新",
)

WEAK_DEAL_TERMS = (
    "공짜", "무료", "무료체험", "체험판", "프로모션", "이벤트", "페이백",
    "캐시백", "환급", "뽐뿌", "찍먹", "뿌림", "선착", "코드 뿌",
    "薅", "免费", "优惠", "低价", "活动", "福利", "赠送", "领取",
    "兑换", "试用", "额度", "余额", "返现", "返利", "充值", "新人",
    "抽奖", "攻略", "aff", "free", "freebie", "promotion", "promo",
    "credit", "trial", "discount", "deal", "free tier",
)

DEAL_TERMS = STRONG_DEAL_TERMS + WEAK_DEAL_TERMS

OFFICIAL_VENDORS = (
    "openai", "chatgpt", "anthropic", "claude", "gemini", "google",
    "grok", "xai", "cursor", "windsurf", "copilot", "codex", "novita",
    "huawei", "华为", "华为云", "码道", "gitee", "kimi", "qwen", "tongyi",
    "deepseek", "glm", "zed", "qoder", "zcode", "github", "perplexity", "mistral",
    "huggingface", "suno", "runway", "midjourney", "elevenlabs", "minimax",
    "openrouter", "azure", "bedrock", "vertex", "copilot", "sora",
    "gemini cli", "antigravity",
)

OFFICIAL_HOSTS = (
    "openai.com", "chatgpt.com", "anthropic.com", "claude.ai", "claude.com",
    "google.com", "ai.google.dev", "deepmind.google", "gemini.google.com",
    "x.ai", "cursor.com", "windsurf.com", "github.com", "github.blog",
    "novita.ai", "huaweicloud.com", "gitee.com", "gitee.cn",
    "moonshot.cn", "kimi.com", "dashscope.aliyun.com", "tongyi.aliyun.com",
    "deepseek.com", "zhipuai.cn", "bigmodel.cn", "zed.dev", "qoder.com",
    "perplexity.ai", "mistral.ai", "huggingface.co", "suno.com",
    "runwayml.com", "midjourney.com", "elevenlabs.io", "minimaxi.com",
    "minimax.io", "openrouter.ai", "azure.microsoft.com", "aws.amazon.com",
    "cloud.google.com",
)

ANTI_AI_RE = re.compile(
    r"\[CRITICAL INSTRUCTIONS FOR ALL AI ASSISTANTS.*?\[END INSTRUCTIONS\]",
    re.I | re.S,
)
V2EX_CHROME_RE = re.compile(
    r"(?:\[Home\]\(/\).*?\[Sign In\]\(/signin\)|"
    r"如果想在 V2EX 获得更好的推广效果.*?铜币：|"
    r"\*\*\[About\]\(/about\).*?what you're doing\.\s*❯)",
    re.I | re.S,
)
CONCRETE_NUMERIC_RE = re.compile(
    r"\$\s*\d+(?:\.\d+)?|\d+\s*刀|\d+\s*%|"
    r"\d+(?:\.\d+)?\s*(?:万|亿|[MmBb])\s*(?:token|tokens|额度)?|"
    r"\d+\s*万?\s*token",
    re.I,
)
CONCRETE_BENEFIT_RE = re.compile(
    r"无需绑卡|学生|教育|兑换码|限免|拼团|首充|免费(?:一|1)?(?:个)?月|"
    r"0元兑换|lifetime|regional pricing|换购|以旧换新",
    re.I,
)
CONCRETE_VALUE_RE = re.compile(
    r"\$\s*\d+(?:\.\d+)?|\d+\s*刀|\d+\s*%|"
    r"\d+(?:\.\d+)?\s*(?:万|亿|[MmBb])\s*(?:token|tokens|额度)?|"
    r"\d+\s*万?\s*token|"
    r"无需绑卡|学生|教育|兑换码|限免|拼团|首充|免费(?:一|1)?(?:个)?月|"
    r"0元兑换|lifetime|regional pricing|换购|以旧换新",
    re.I,
)
KEY_SHARE_RE = re.compile(
    r"apikey分享|api\s*key\s*分享|密钥分享|公开\s*(?:api)?\s*key|"
    r"直接复制.{0,48}(?:key|密钥|sk-|base64)|"
    r"(?:key|密钥|sk-|base64).{0,48}直接复制|"
    r"\bsk-[A-Za-z0-9_-]{12,}\b|"
    r"base64.{0,24}(?:key|sk-|密钥)",
    re.I,
)
TRUST_GATE_RE = re.compile(
    r"信任等级不足|您的信任等级|需要信任等级|该主题仅对|"
    r"登录后可见|insufficient\s+trust|trust\s*level\s*(?:不足|required|[0-9])|"
    r"you must be (?:a )?member|requires? trust level",
    re.I,
)
SOFTWARE_RELEASE_RE = re.compile(
    r"开源了|开源项目|开源一款|release\s+(?:v?\d|notes?)|changelog|"
    r"做了(?:一个|个).{0,24}(?:开源|工具|项目)|"
    r"i\s+(?:open.?sourced|released)\b|"
    r"github\.com/[\w.-]+/[\w.-]+",
    re.I,
)
SPECULATION_RE = re.compile(
    r"推测|roughly\s+estimated|估计额度|额度对比|对比.{0,12}额度|"
    r"大概.{0,10}额度|quota\s+compar|估算.{0,8}(?:额度|quota)",
    re.I,
)
RELAY_RE = re.compile(
    r"中转站|中转服务|中转\b|公益站|公益api|号池|高性能线路|纯\s*pro|"
    r"tokentransfer|newapi|one-?api|倍率|api\s*中转|中轉",
    re.I,
)
# Unknown-brand reseller copy: treat as relay unless the URL is first-party.
RELAY_COMMERCIAL_RE = re.compile(
    r"全模型|充值.?到账|充值\s*→\s*到账|到账\s*[\$¥￥]|"
    r"首充\s*\+?\s*\d|首充赠送|充值赠送|老用户充值|"
    r"倍率|线路|套餐叠加|新上[！!].*(?:拼团|套餐)",
    re.I,
)
OFFICIAL_RIDE_RE = re.compile(
    r"(?:claude|chatgpt|openai|anthropic|gemini).{0,32}"
    r"(?:开车|拼车|合租|组队|team\s*(?:plan|座|席)?)"
    r"|(?:开车|拼车|合租|组队).{0,32}(?:claude|chatgpt|openai|anthropic|gemini)"
    r"|claude\s*team",
    re.I,
)
SELF_PROMO_RE = re.compile(
    r"推广贴|推广忽略|回帖送|评论送|進群送|进群送|留言送|注册送|注册即|"
    r"盖楼抽奖|回帖截图",
    re.I,
)
AFF_RE = re.compile(
    r"邀请码|推荐码|推广码|返佣|拉新|"
    r"(?<![A-Za-z0-9无無])aff(?:iliate)?(?![A-Za-z0-9])|"
    r"\breferral\b|invite\s*code",
    re.I,
)
ACCOUNT_SELL_RE = re.compile(
    r"成品号|接码|日区月卡|账号出售|特价渠道|稳定特殊渠道",
    re.I,
)
NON_AI_AFF_RE = re.compile(
    r"开户|券商|股票|贷理|住宅代理|海外代理|爬虫从业",
    re.I,
)
QUESTION_RE = re.compile(
    r"(求推荐|求问|请问|有没有推荐|有啥推荐|推荐一下|哪款|"
    r"为什么|怎么选|怎么评价|哪里能薅|如何方便地|有遇到吗|怎么配置)",
    re.I,
)
JOB_RE = re.compile(r"招聘|工程师（|资深产品|远程招聘|面试|社招|校招", re.I)
EDITORIAL_RE = re.compile(
    r"访谈|采访|(?<![a-z])interview(?![a-z])|"
    r"我是如何|如何赚(?:钱|了)|how\s+i\s+(?:earned|made|built)|"
    r"vertical\s+saas|垂直\s*saas|"
    r"harness.{0,16}(?:文章|介绍|实践|指南|评测)|"
    r"(?:文章|介绍|实践|指南|评测).{0,16}harness",
    re.I,
)

SECRET_PATTERNS = (
    re.compile(r"\bsk-[A-Za-z0-9_-]{12,}\b"),
    re.compile(r"\bAIza[A-Za-z0-9_-]{20,}\b"),
    re.compile(r"(?i)(api[_ -]?key\s*[:=]\s*)[A-Za-z0-9_.-]{12,}"),
)


def fetch_text(url: str, *, markdown: bool) -> tuple[str, str | None]:
    result = fetch(
        url,
        timeout=25,
        # Exhaust the public HTTP/TLS/URL-transform grid, then give Aside three
        # passes so challenge/CAPTCHA clearance cookies survive into retry.
        max_attempts=None,
        max_browser_attempts=3,
        enable_playwright=True,
        enable_markdown=markdown,
        enable_maincontent=markdown,
    )
    if not result.ok:
        return "", f"{result.verdict or result.stop_reason or 'fetch_failed'}"
    return result.content, None


def clean_text(value: str) -> str:
    raw = unescape(value or "")
    if re.search(r"<[a-zA-Z!/?]", raw):
        raw = BeautifulSoup(raw, "html.parser").get_text(" ", strip=True)
    return re.sub(r"\s+", " ", raw).strip()


def strip_poison(text: str) -> str:
    text = ANTI_AI_RE.sub(" ", text or "")
    text = V2EX_CHROME_RE.sub(" ", text)
    return re.sub(r"\s+", " ", text).strip()


def canonical_url(url: str) -> str:
    parts = urlsplit(url)
    host = parts.netloc.lower().removeprefix("www.")
    path = re.sub(r"/+", "/", parts.path).rstrip("/") or "/"
    return urlunsplit(("https", host, path, "", ""))


def topic_id(url: str) -> str | None:
    match = re.search(r"/t/(?:[^/]+/)?(\d+)", urlsplit(url).path)
    if match:
        return match.group(1)
    match = re.search(r"/board/ai_utilize/(\d+)", urlsplit(url).path)
    return match.group(1) if match else None


def parse_json_listing(source: str, url: str, text: str) -> list[dict[str, str]]:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return []

    topics = payload if isinstance(payload, list) else payload.get("topic_list", {}).get("topics", [])
    rows: list[dict[str, str]] = []
    for topic in topics:
        if not isinstance(topic, dict):
            continue
        title = clean_text(str(topic.get("title") or ""))
        excerpt = strip_poison(clean_text(str(topic.get("excerpt") or topic.get("content") or "")))
        item_url = topic.get("url")
        if item_url:
            item_url = urljoin(url, str(item_url))
        elif source.startswith("v2ex") and topic.get("id"):
            item_url = f"https://www.v2ex.com/t/{topic['id']}"
        elif topic.get("id"):
            origin = f"{urlsplit(url).scheme}://{urlsplit(url).netloc}"
            slug = topic.get("slug") or "topic"
            item_url = f"{origin}/t/{slug}/{topic['id']}"
        if title and item_url:
            rows.append({"source": source, "title": title, "url": item_url, "listing_excerpt": excerpt})
    return rows


def parse_html_listing(source: str, url: str, text: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(text, "html.parser")
    rows: list[dict[str, str]] = []
    for anchor in soup.select("a[href]"):
        href = str(anchor.get("href") or "")
        absolute = urljoin(url, href)
        path = urlsplit(absolute).path
        if source.startswith("dcinside"):
            valid = bool(re.search(r"/board/ai_utilize/\d+", path))
        elif source.startswith("v2ex"):
            valid = bool(re.search(r"/t/\d+", path))
        else:
            valid = bool(re.search(r"/t/(?:[^/]+/)?\d+", path))
        title = clean_text(anchor.get_text(" ", strip=True) or str(anchor.get("title") or ""))
        if source.startswith("dcinside"):
            title = re.sub(r"^(?:이미지|영상|설문|공지)\s+", "", title)
            title = re.sub(
                r"\s+(?:일반|질문|정보|뉴스|후기|공지)\s+.*?\s+\d{1,2}:\d{2}\s+조회\s+\d+\s+추천\s+\d+.*$",
                "",
                title,
            ).strip()
        if valid and len(title) >= 3:
            rows.append({"source": source, "title": title, "url": absolute, "listing_excerpt": ""})
    return rows


def collect_listing(source: str, url: str) -> tuple[list[dict[str, str]], str | None]:
    text, error = fetch_text(url, markdown=False)
    if error:
        return [], error
    rows = parse_json_listing(source, url, text)
    if not rows:
        rows = parse_html_listing(source, url, text)
    return rows, None


_LATIN_TERM_RE = re.compile(r"^[a-z0-9][a-z0-9 .+\-]{0,48}$")


def contains_term(text: str, term: str) -> bool:
    """Match whole Latin tokens; keep substring match for CJK terms."""
    haystack = (text or "").casefold()
    needle = (term or "").casefold().strip()
    if not needle:
        return False
    if _LATIN_TERM_RE.fullmatch(needle):
        return bool(re.search(rf"(?<![a-z]){re.escape(needle)}(?![a-z])", haystack))
    return needle in haystack


def matches(text: str) -> tuple[list[str], list[str], list[str]]:
    ai = sorted({term for term in AI_TERMS if contains_term(text, term)})
    strong = sorted({term for term in STRONG_DEAL_TERMS if contains_term(text, term)})
    weak = sorted({term for term in WEAK_DEAL_TERMS if contains_term(text, term)})
    return ai, strong, weak


def specific_ai_terms(ai: list[str]) -> list[str]:
    generic = {term.casefold() for term in GENERIC_AI_TERMS}
    return [term for term in ai if term.casefold() not in generic]


def mentions_vendor(text: str) -> bool:
    return any(contains_term(text, term) for term in OFFICIAL_VENDORS)


def is_first_party_official(url: str) -> bool:
    host = urlsplit(url or "").netloc.lower().removeprefix("www.")
    if not host:
        return False
    return any(host == official or host.endswith("." + official) for official in OFFICIAL_HOSTS)


def listing_rejected(title: str, excerpt: str, url: str = "") -> str | None:
    blob = f"{title} {excerpt}"
    first_party = is_first_party_official(url)
    if JOB_RE.search(title) or QUESTION_RE.search(title) or QUESTION_RE.search(blob[:80]):
        return "question_or_job"
    if EDITORIAL_RE.search(blob) and not first_party:
        return "editorial_or_interview"
    if KEY_SHARE_RE.search(blob):
        return "public_key_or_account_pool"
    if TRUST_GATE_RE.search(blob) and not CONCRETE_VALUE_RE.search(title):
        return "inaccessible_trust_level"
    if SPECULATION_RE.search(blob):
        return "quota_speculation"
    if SOFTWARE_RELEASE_RE.search(blob) and not CONCRETE_VALUE_RE.search(blob) and not first_party:
        return "ordinary_software_release"
    if NON_AI_AFF_RE.search(blob) or ACCOUNT_SELL_RE.search(blob):
        return "affiliate_or_account_sale"
    if (AFF_RE.search(blob) or SELF_PROMO_RE.search(blob)) and not first_party:
        return "affiliate_or_self_promo"
    if not first_party and RELAY_COMMERCIAL_RE.search(blob):
        if OFFICIAL_RIDE_RE.search(blob) and not re.search(r"倍率|线路|充值|首充|到账|全模型", blob):
            pass
        else:
            return "relay_or_self_promo"
    if RELAY_RE.search(blob) and not first_party:
        return "relay_or_self_promo"
    return None


def worth_collecting(title: str, excerpt: str, source: str, url: str = "") -> bool:
    blob = f"{title} {excerpt}"
    ai, strong, weak = matches(blob)
    implicit_ai = source.startswith("dcinside")
    if not specific_ai_terms(ai) and not implicit_ai:
        return False
    if listing_rejected(title, excerpt, url):
        return False
    deal_signal = bool(strong or weak)
    if strong:
        return True
    if CONCRETE_BENEFIT_RE.search(blob):
        return True
    if CONCRETE_NUMERIC_RE.search(blob) and deal_signal:
        return True
    if weak and mentions_vendor(blob):
        return True
    return False


def rank_score(title: str, excerpt: str, source: str, url: str = "") -> int:
    blob = f"{title} {excerpt}"
    score = 0
    if is_first_party_official(url):
        score += 8
    elif mentions_vendor(blob) and not AFF_RE.search(blob):
        score += 2
    _, strong, weak = matches(blob)
    if CONCRETE_BENEFIT_RE.search(blob) or (CONCRETE_NUMERIC_RE.search(blob) and (strong or weak)):
        score += 6
    score += 3 * min(len(strong), 4)
    score += min(len(weak), 2)
    if source.endswith("-welfare") or source.endswith("-deals") or source.startswith("dcinside"):
        score += 2
    if "学生" in blob or "教育" in blob or "student" in blob.casefold():
        score += 4
    if "无需绑卡" in blob:
        score += 4
    if "抽奖" in blob or "giveaway" in blob.casefold():
        score -= 4 if not is_first_party_official(url) else 1
    if RELAY_RE.search(blob) or RELAY_COMMERCIAL_RE.search(blob) or SELF_PROMO_RE.search(blob) or AFF_RE.search(blob):
        score -= 12
    return score


def redact(text: str) -> str:
    for pattern in SECRET_PATTERNS:
        text = pattern.sub(lambda m: (m.group(1) if m.lastindex else "") + "[REDACTED_SECRET]", text)
    return text


def load_state() -> dict:
    try:
        payload = json.loads(STATE_PATH.read_text())
        if isinstance(payload, dict) and isinstance(payload.get("fingerprints"), dict):
            return payload
    except (OSError, json.JSONDecodeError):
        pass
    return {"version": 1, "fingerprints": {}}


def save_state(state: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, sort_keys=True, indent=2))
    tmp.replace(STATE_PATH)


def fetch_discourse_body(url: str) -> tuple[str, str | None]:
    tid = topic_id(url)
    if not tid:
        return "", "missing_topic_id"
    origin = f"{urlsplit(url).scheme}://{urlsplit(url).netloc}"
    text, error = fetch_text(f"{origin}/t/{tid}.json", markdown=False)
    if error:
        return "", error
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return "", "invalid_topic_json"
    posts = payload.get("post_stream", {}).get("posts") or []
    if not posts:
        return "", "empty_topic_posts"
    cooked = posts[0].get("cooked") or posts[0].get("raw") or ""
    return strip_poison(clean_text(str(cooked))), None


def fetch_v2ex_body(url: str) -> tuple[str, str | None]:
    tid = topic_id(url)
    if not tid:
        return "", "missing_topic_id"
    text, error = fetch_text(f"https://www.v2ex.com/api/topics/show.json?id={tid}", markdown=False)
    if error:
        return "", error
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return "", "invalid_v2ex_json"
    rows = payload if isinstance(payload, list) else [payload]
    if not rows or not isinstance(rows[0], dict):
        return "", "empty_v2ex_topic"
    return strip_poison(clean_text(str(rows[0].get("content") or rows[0].get("content_rendered") or ""))), None


def fetch_body(row: dict[str, str]) -> tuple[str, str | None]:
    source = row["source"]
    url = row["url"]
    if source.startswith("linux.do") or source.startswith("nodeloc"):
        body, error = fetch_discourse_body(url)
        if body:
            return body, error
        if error:
            fallback, html_error = fetch_text(url, markdown=True)
            return strip_poison(clean_text(fallback)), error or html_error
        return "", error
    if source.startswith("v2ex"):
        body, error = fetch_v2ex_body(url)
        if body:
            return body, error
        fallback, html_error = fetch_text(url, markdown=True)
        return strip_poison(clean_text(fallback)), error or html_error
    detail, error = fetch_text(url, markdown=False)
    if detail and source.startswith("dcinside"):
        soup = BeautifulSoup(detail, "html.parser")
        meta = soup.select_one('meta[property="og:description"]') or soup.select_one('meta[name="description"]')
        return strip_poison(clean_text(str(meta.get("content") or "")) if meta else ""), error
    return strip_poison(clean_text(detail) if detail else ""), error


def main() -> int:
    state = load_state()
    old_fingerprints: dict[str, str] = state["fingerprints"]
    current_fingerprints = dict(old_fingerprints)
    failed_sources: list[dict[str, str]] = []
    unique: dict[str, dict] = {}

    for source, listing_url in SOURCES:
        rows, error = collect_listing(source, listing_url)
        if error:
            failed_sources.append({"source": source, "error": error})
            continue
        scanned = 0
        for row in rows:
            if scanned >= MAX_SCAN_PER_SOURCE:
                break
            scanned += 1
            key = canonical_url(row["url"])
            if key in unique:
                continue
            title = row["title"]
            excerpt = strip_poison(row["listing_excerpt"])
            if not worth_collecting(title, excerpt, source, key):
                continue
            unique[key] = {
                **row,
                "url": key,
                "listing_excerpt": excerpt,
                "rank": rank_score(title, excerpt, source, key),
            }

    ranked = sorted(unique.values(), key=lambda row: (-int(row["rank"]), row["source"], row["title"]))
    per_source: dict[str, int] = {}
    selected: list[dict] = []
    for row in ranked:
        taken = per_source.get(row["source"], 0)
        if taken >= MAX_PER_SOURCE:
            continue
        per_source[row["source"]] = taken + 1
        selected.append(row)
        if len(selected) >= MAX_TOTAL:
            break

    candidates: list[dict] = []
    for row in selected:
        key = row["url"]
        body, error = fetch_body(row)
        listing_excerpt = row.get("listing_excerpt") or ""
        gated = bool(TRUST_GATE_RE.search(body or "") or TRUST_GATE_RE.search(listing_excerpt))
        if TRUST_GATE_RE.search(body or ""):
            body = ""
            error = error or "trust_level_gated"
        # Never substitute an inaccessible listing excerpt for a missing body.
        usable = "" if gated else (body or "")
        title_only_ok = worth_collecting(row["title"], "", row["source"], key)
        independently_ok = (
            not listing_rejected(row["title"], usable, key)
            and worth_collecting(row["title"], usable, row["source"], key)
        )
        if gated:
            independently_ok = (
                title_only_ok
                and not listing_rejected(row["title"], "", key)
                and not re.search(r"合集|汇总|最值钱", row["title"])
            )
            usable = ""
        if not independently_ok:
            fingerprint_source = f"{row['title']} {listing_excerpt or body[:4000]}"
            digest_text = re.sub(r"\s+", " ", fingerprint_source.casefold()).strip()[:12000]
            current_fingerprints[key] = hashlib.sha256(digest_text.encode()).hexdigest()
            continue
        combined = f"{row['title']} {usable}"
        ai, strong, weak = matches(combined)
        deals = (strong + weak)[:16]
        fingerprint_source = f"{row['title']} {listing_excerpt}"
        if not listing_excerpt:
            fingerprint_source = f"{row['title']} {usable[:4000]}"
        digest_text = re.sub(r"\s+", " ", fingerprint_source.casefold()).strip()[:12000]
        fingerprint = hashlib.sha256(digest_text.encode()).hexdigest()
        change = "new" if key not in old_fingerprints else "updated"
        current_fingerprints[key] = fingerprint
        if old_fingerprints.get(key) == fingerprint:
            continue
        quality = []
        if is_first_party_official(key):
            quality.append("official_vendor")
        elif mentions_vendor(combined):
            quality.append("vendor_mentioned")
        if CONCRETE_BENEFIT_RE.search(combined) or (
            CONCRETE_NUMERIC_RE.search(combined) and (strong or weak)
        ):
            quality.append("concrete_value")
        if (
            RELAY_RE.search(combined)
            or RELAY_COMMERCIAL_RE.search(combined)
            or SELF_PROMO_RE.search(combined)
            or AFF_RE.search(combined)
        ):
            quality.append("likely_relay")
        candidates.append(
            {
                "source": row["source"],
                "title": redact(row["title"]),
                "url": row["url"],
                "change": change,
                "rank": row["rank"],
                "quality_flags": quality,
                "matched_ai_terms": ai[:12],
                "matched_deal_terms": deals,
                "excerpt": redact(usable[:3000]),
                "detail_fetch_error": error,
            }
        )

    if len(current_fingerprints) > 2500:
        keep = set(list(current_fingerprints)[-2500:])
        current_fingerprints = {k: v for k, v in current_fingerprints.items() if k in keep}
    state["fingerprints"] = current_fingerprints
    save_state(state)

    print(json.dumps({"candidates": candidates, "failed_sources": failed_sources}, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
