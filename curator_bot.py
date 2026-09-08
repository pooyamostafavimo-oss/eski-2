"""
News Curator Bot — replacement version
---------------------------------------
Improvements over the original:
1) Gemini receives text/caption only; no image/video processing or media API cost.
2) Threat/assassination/terror/political news is treated as news, not discarded
   merely because the text contains violent words. The rewrite remains neutral
   and does not turn a threat into an instruction or endorsement.
3) Conservative event-level deduplication: exact hash + normalized event_key +
   high Jaccard similarity. Common entities alone are NOT enough to mark a
   story as duplicate.
4) No hard 30-message bottleneck: per-channel processing is incremental and
   checkpoints last_id after every processed album/message, so a busy channel
   is drained over successive runs without losing unprocessed messages.
5) Source attribution is preserved as "منبع: @channel" rather than scrubbed.
6) Existing key rotation, FloodWait handling, stats, watermark and GitHub
   Actions-friendly state are retained.

Environment variables are compatible with the original project.
"""

import asyncio
import hashlib
import json
import os
import random
import re
import sys
import tempfile
import time
from pathlib import Path

import requests
from telethon import TelegramClient
from telethon.errors import FloodWaitError
from telethon.sessions import StringSession


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

API_ID = int(os.environ["TG_API_ID"])
API_HASH = os.environ["TG_API_HASH"]
SESSION_STRING = os.environ["TG_SESSION_STRING"]
TARGET_CHANNEL = os.environ["TARGET_CHANNEL"]

WATERMARK_TEXT = os.environ.get("WATERMARK_TEXT", "").strip() or "@KosSherijat_69"

MAX_TEXT_LEN = 4096
MAX_CAPTION_LEN = 1024

GEMINI_API_KEYS = [
    k.strip()
    for k in os.environ.get("GEMINI_API_KEYS", "").split(",")
    if k.strip()
]
if not GEMINI_API_KEYS:
    single = os.environ.get("GEMINI_API_KEY", "").strip()
    if single:
        GEMINI_API_KEYS = [single]
if not GEMINI_API_KEYS:
    print("[ERROR] No Gemini API key configured.")
    sys.exit(1)

GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-3.6-flash")
GEMINI_CALL_DELAY_SECONDS = float(
    os.environ.get("GEMINI_CALL_DELAY_SECONDS", "4")
)

DEFAULT_SOURCE_CHANNELS = [
    "Newz021",
    "withyashar",
    "pm_afshaa",
    "IranianMinds",
    "hutt_news",
    "BadCandom",
    "FUTBALLI18",
    "drtel",
    "nikotiinn_text",
    "nabe_saman",
    "Tala_Dollar_ir",
    "SoccerrWorld",
]
SOURCE_CHANNELS = [
    c.strip()
    for c in os.environ.get("SOURCE_CHANNELS", "").split(",")
    if c.strip()
] or DEFAULT_SOURCE_CHANNELS

# This is now a throughput cap, not a "only look at 30" rule.
# Increase it if your channels are very busy. Unprocessed messages remain
# available because last_id is checkpointed after every group.
MAX_MESSAGES_PER_CHANNEL = int(
    os.environ.get("MAX_MESSAGES_PER_CHANNEL", "150")
)

MAX_DEDUP_HASHES = int(os.environ.get("MAX_DEDUP_HASHES", "1000"))


RUN_TIMEOUT_SECONDS = float(
    os.environ.get("RUN_TIMEOUT_SECONDS", "3300")
)
POST_GAP_MIN_SECONDS = float(
    os.environ.get("POST_GAP_MIN_SECONDS", "120")
)
POST_GAP_MAX_SECONDS = float(
    os.environ.get("POST_GAP_MAX_SECONDS", "600")
)

CHANNEL_FAIL_ALERT_THRESHOLD = int(
    os.environ.get("CHANNEL_FAIL_ALERT_THRESHOLD", "3")
)
ADMIN_CHAT = os.environ.get("ADMIN_CHAT_ID", "").strip() or "me"

STATS_INTERVAL_SECONDS = float(
    os.environ.get("STATS_INTERVAL_SECONDS", str(7 * 24 * 3600))
)

STATE_FILE = Path(__file__).parent / "last_ids.json"

_key_cursor = 0


# ---------------------------------------------------------------------------
# Prompt
# ---------------------------------------------------------------------------

CLASSIFY_PROMPT = r"""
تو یک ویراستار حرفه‌ای خبر برای یک کانال تلگرامی فارسی هستی.

وظیفه:
از متن و کپشن پیام تشخیص بده آیا این پیام یک خبر، گزارش،
اظهارنظر سیاسی، رویداد مهم، درگیری، جنگ، تهدید، ترور، قتل، حمله، عملیات
امنیتی، بحران، حادثه، یا اتفاق واقعی قابل انتشار است یا نه.

نکته بسیار مهم:
وجود کلمات خشونت‌آمیز مثل «کشتن»، «ترور»، «بمب»، «حمله»، «اعدام»، «قتل»
به‌تنهایی دلیل رد خبر نیست. اگر متن درباره یک تهدید، اظهارنظر، گزارش یا
رویداد واقعی است، آن را به‌عنوان خبر مرتبط نگه دار.

اما:
- تبلیغ یا تشویق خودت به خشونت نکن.
- دستورالعمل عملی برای آسیب‌رساندن تولید نکن.
- اگر منبع درباره تهدید یا ترور حرف زده، آن را به شکل گزارش خنثی بازنویسی کن.
- چیزی که در منبع نیست اختراع نکن.
- اگر ادعا قطعی نیست، از عباراتی مثل «ادعا شد»، «گفته شد»، «به گزارش منبع»
  استفاده کن.
- «تهدید به قتل/ترور» را با «قتل اتفاق افتاد» اشتباه نگیر.

دسته‌ها:
- "political": سیاست داخلی/خارجی، حکومت‌ها، انتخابات، مقامات، دیپلماسی
- "war": جنگ، درگیری نظامی، حمله، عملیات نظامی، بحران ژئوپلیتیک
- "security": ترور، تهدید، قتل، بازداشت امنیتی، عملیات امنیتی، حمله مسلحانه
- "important": خبر مهم غیرنظامی/غیرجنگی
- "strange": اتفاق عجیب، باورنکردنی یا غیرمعمول
- "funny": اتفاق طنزآمیز واقعی
- "random": خبر متفرقه ولی واقعی
- "none": غیرخبر

رد کن اگر:
- صرفاً چت شخصی یا درد دل روزمره است.
- تبلیغ، اسپم یا دعوت به کانال/ربات است.
- صرفاً نظرسنجی/سؤال مخاطبان است.
- متن آن‌قدر مبهم است که هیچ رویداد واقعی قابل تشخیصی ندارد.
- صرفاً یک شعار بدون رویداد یا اظهارنظر مشخص است.

مدیا:
به تصویر یا ویدیو دسترسی نداری؛ فقط بر اساس متن و کپشن قضاوت کن و درباره
محتوای مدیا حدس نزن.

متن خروجی:
- فارسی روان و کوتاه.
- خبر را خلاصه و طبیعی کن.
- برای خبرهای سیاسی/امنیتی/ترور، لحن خنثی و خبری باشد.
- تهدید یا دعوت به خشونت را به‌عنوان «اظهار/تهدید مطرح‌شده» گزارش کن، نه به
  عنوان توصیه یا دعوت.
- 0 تا 2 ایموجی؛ برای خبرهای خشن/مرگ/ترور معمولاً بدون ایموجی یا فقط ⚠️.
- نام منبع را در متن نهایی ننویس؛ برنامه خودش آن را به انتهای پست اضافه می‌کند.

برای dedup یک event_key کوتاه و پایدار بساز. این کلید باید شامل «فاعل/شخص اصلی
+ عمل/رویداد + هدف/موضوع اصلی» باشد. نام‌های مشترک مثل «ترامپ» یا «ایران»
به‌تنهایی کافی نیستند. اگر دو خبر درباره یک شخص ولی درباره دو رویداد متفاوت
هستند، event_key آنها باید متفاوت باشد.

اگر خبر درباره ادعای ترور/قتل/تهدید است، event_key باید خود «تهدید/ادعا» را
نمایندگی کند، نه اینکه آن را به‌عنوان قتل قطعی ثبت کند.

فقط JSON معتبر برگردان، بدون markdown:
{
  "relevant": true/false,
  "category": "political"|"war"|"security"|"important"|"strange"|"funny"|"random"|"none",
  "action": "copy"|"rewrite",
  "text": "متن آماده انتشار",
  "event_key": "فاعل|عمل|موضوع",
  "confidence": 0.0
}

پیام:
---
{MESSAGE}
---
"""


# ---------------------------------------------------------------------------
# State / formatting
# ---------------------------------------------------------------------------

def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            print("[WARN] State file is invalid; starting with empty state.")
    return {}


def save_state(state: dict) -> None:
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(
        json.dumps(state, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    tmp.replace(STATE_FILE)


def add_watermark(text: str, is_caption: bool) -> str:
    if not WATERMARK_TEXT:
        return text
    limit = MAX_CAPTION_LEN if is_caption else MAX_TEXT_LEN
    footer = f"\n\n{WATERMARK_TEXT}"
    room = limit - len(footer)
    if room <= 0:
        return WATERMARK_TEXT[:limit]
    trimmed = (text or "")[:room].rstrip()
    return f"{trimmed}{footer}" if trimmed else WATERMARK_TEXT


def source_label(channel: str) -> str:
    c = channel.strip()
    return c if c.startswith("@") else f"@{c}"


def add_source_attribution(text: str, channel: str, is_caption: bool) -> str:
    """
    Preserve attribution instead of pretending the source does not exist.
    """
    if not channel:
        return text or ""
    attribution = f"منبع: {source_label(channel)}"
    limit = MAX_CAPTION_LEN if is_caption else MAX_TEXT_LEN
    base = (text or "").strip()

    if attribution in base:
        return base[:limit]

    if not base:
        return attribution[:limit]

    room = limit - len(attribution) - 2
    if room <= 0:
        return attribution[:limit]
    return f"{base[:room].rstrip()}\n\n{attribution}"


# ---------------------------------------------------------------------------
# Dedup
# ---------------------------------------------------------------------------

_TOPIC_STOPWORDS = {
    "the", "a", "an", "of", "in", "on", "at", "to", "for", "and", "or",
    "با", "در", "به", "از", "و", "را", "که", "این", "یک", "برای", "شد",
    "شدن", "کرد", "کردن", "درباره", "علیه",
}


def normalize_text(text: str) -> str:
    text = (text or "").lower()
    text = re.sub(r"https?://\S+", " ", text)
    text = re.sub(r"@\w+", " ", text)
    text = text.replace("ي", "ی").replace("ك", "ک")
    text = re.sub(r"[^\w\u0600-\u06FF]+", " ", text)
    return " ".join(text.split())


def content_hash(text: str) -> str:
    return hashlib.sha256(
        normalize_text(text).encode("utf-8")
    ).hexdigest()


def event_tokens(event_key: str) -> frozenset:
    words = re.findall(
        r"[\w\u0600-\u06FF]+",
        (event_key or "").lower(),
    )
    return frozenset(
        w for w in words
        if w not in _TOPIC_STOPWORDS and len(w) > 1
    )


def event_already_posted(event_key: str, posted_events: list) -> bool:
    """
    Conservative matching:
    1) Exact normalized event_key = duplicate.
    2) Otherwise require high Jaccard similarity AND at least 3 shared
       significant tokens.
    This prevents two different Trump/Iran stories from colliding just because
    they share a famous entity.
    """
    tokens = event_tokens(event_key)
    if len(tokens) < 3:
        return False

    normalized_key = normalize_text(event_key)

    for item in posted_events:
        if isinstance(item, dict):
            prev_key = normalize_text(item.get("event_key", ""))
            prev_tokens = frozenset(item.get("tokens", []))
        else:
            prev_key = ""
            prev_tokens = frozenset(item)

        if prev_key and prev_key == normalized_key:
            return True

        if len(prev_tokens) < 3:
            continue

        intersection = len(tokens & prev_tokens)
        union = len(tokens | prev_tokens)
        jaccard = intersection / max(union, 1)

        # Require both several shared tokens and high overall similarity.
        if intersection >= 3 and jaccard >= 0.72:
            return True

    return False


def remember_event(posted_events: list, event_key: str) -> None:
    tokens = list(event_tokens(event_key))
    if len(tokens) < 3:
        return
    posted_events.append(
        {
            "event_key": event_key[:200],
            "tokens": tokens,
            "ts": int(time.time()),
        }
    )


# ---------------------------------------------------------------------------
# Gemini media + request
# ---------------------------------------------------------------------------

def _mask_key(key: str) -> str:
    return f"...{key[-4:]}" if len(key) > 4 else "****"


def safe_result(data: dict) -> dict:
    if not isinstance(data, dict):
        return {
            "relevant": False,
            "category": "none",
            "action": "rewrite",
            "text": "",
            "event_key": "",
            "confidence": 0,
        }

    allowed_categories = {
        "political", "war", "security", "important",
        "strange", "funny", "random", "none",
    }
    category = data.get("category", "none")
    if category not in allowed_categories:
        category = "none"

    action = data.get("action", "rewrite")
    if action not in {"copy", "rewrite"}:
        action = "rewrite"

    return {
        "relevant": bool(data.get("relevant")),
        "category": category,
        "action": action,
        "text": str(data.get("text") or "").strip(),
        "event_key": str(
            data.get("event_key") or data.get("topic_key") or ""
        ).strip(),
        "confidence": float(data.get("confidence", 0) or 0),
    }


def classify_with_gemini(
    message_text: str,
    dead_keys: set,
) -> dict:
    global _key_cursor

    active_keys = [k for k in GEMINI_API_KEYS if k not in dead_keys]
    if not active_keys:
        return safe_result({})

    message_text = (message_text or "")[:8000]
    prompt = CLASSIFY_PROMPT.replace("{MESSAGE}", message_text)

    parts = [{"text": prompt}]

    payload = {
        "contents": [{"parts": parts}],
        "generationConfig": {
            "temperature": 0.25,
            "responseMimeType": "application/json",
        },
    }

    n_keys = len(active_keys)

    for attempt in range(n_keys):
        key = active_keys[(_key_cursor + attempt) % n_keys]
        url = (
            "https://generativelanguage.googleapis.com/v1beta/models/"
            f"{GEMINI_MODEL}:generateContent?key={key}"
        )

        resp = None
        for retry in range(3):
            try:
                resp = requests.post(url, json=payload, timeout=90)
            except requests.RequestException as e:
                print(
                    f"[WARN] Gemini request error on {_mask_key(key)} "
                    f"(retry {retry + 1}/3): {e}"
                )
            else:
                if resp.status_code < 500:
                    break
                print(
                    f"[WARN] Gemini server error {resp.status_code} "
                    f"on {_mask_key(key)}"
                )

            if retry < 2:
                time.sleep(2 * (retry + 1))

        if resp is None:
            continue

        if resp.status_code == 429:
            print(
                f"[WARN] Gemini key {_mask_key(key)} rate-limited; "
                "trying next key."
            )
            continue

        if resp.status_code in (400, 401, 403):
            err_status = ""
            err_msg = resp.text[:300]
            try:
                err = resp.json().get("error", {})
                err_status = err.get("status", "")
                err_msg = err.get("message", err_msg)
            except ValueError:
                pass

            bad_key = (
                err_status in {"PERMISSION_DENIED", "UNAUTHENTICATED"}
                or "api key" in err_msg.lower()
                or "api_key" in err_status.lower()
            )
            if bad_key:
                dead_keys.add(key)
                print(
                    f"[WARN] Disabling invalid Gemini key "
                    f"{_mask_key(key)}"
                )
            else:
                print(
                    f"[WARN] Gemini {resp.status_code}: "
                    f"{err_msg[:200]}"
                )
            continue

        if not resp.ok:
            print(
                f"[WARN] Gemini call failed: {resp.status_code} "
                f"{resp.text[:200]}"
            )
            continue

        _key_cursor = (
            _key_cursor + attempt + 1
        ) % max(n_keys, 1)

        try:
            data = resp.json()
            raw = data["candidates"][0]["content"]["parts"][0]["text"].strip()
            parsed = json.loads(raw)
            return safe_result(parsed)
        except Exception as e:
            print(f"[WARN] Gemini returned invalid JSON: {e}")
            return safe_result({})

    return safe_result({})


def preflight_check_keys(dead_keys: set) -> None:
    test_payload = {
        "contents": [{"parts": [{"text": "Reply with just OK."}]}],
        "generationConfig": {
            "temperature": 0,
            "maxOutputTokens": 5,
        },
    }

    for key in list(GEMINI_API_KEYS):
        if key in dead_keys:
            continue

        url = (
            "https://generativelanguage.googleapis.com/v1beta/models/"
            f"{GEMINI_MODEL}:generateContent?key={key}"
        )
        try:
            resp = requests.post(url, json=test_payload, timeout=30)
        except requests.RequestException:
            continue

        if resp.status_code in (400, 401, 403):
            try:
                err = resp.json().get("error", {})
                status = err.get("status", "")
                msg = err.get("message", "")
            except ValueError:
                status, msg = "", resp.text[:200]

            if (
                status in {"PERMISSION_DENIED", "UNAUTHENTICATED"}
                or "api key" in msg.lower()
            ):
                dead_keys.add(key)


# ---------------------------------------------------------------------------
# Telegram helpers
# ---------------------------------------------------------------------------

def group_albums(messages):
    groups = []
    current = []
    current_gid = None

    for msg in messages:
        gid = msg.grouped_id
        if gid is not None and gid == current_gid:
            current.append(msg)
        else:
            if current:
                groups.append(current)
            current = [msg]
            current_gid = gid

    if current:
        groups.append(current)

    return groups


async def post_group(
    client: TelegramClient,
    group: list,
    final_text: str,
    channel: str,
) -> None:
    media_list = [m.media for m in group if m.media]
    is_caption = bool(media_list)

    text_to_send = add_source_attribution(
        final_text or "",
        channel,
        is_caption,
    )
    text_to_send = add_watermark(
        text_to_send,
        is_caption=is_caption,
    )

    async def _send():
        if media_list:
            await client.send_file(
                TARGET_CHANNEL,
                file=media_list if len(media_list) > 1 else media_list[0],
                caption=text_to_send,
            )
        elif text_to_send.strip():
            await client.send_message(TARGET_CHANNEL, text_to_send)

    try:
        await _send()
    except FloodWaitError as e:
        print(
            f"[INFO] Telegram FloodWait: sleeping {e.seconds}s"
        )
        await asyncio.sleep(e.seconds + 1)
        await _send()


async def send_admin_alert(client: TelegramClient, text: str) -> None:
    try:
        await client.send_message(
            ADMIN_CHAT,
            f"🛠 [News Curator Bot]\n{text}",
        )
    except Exception as e:
        print(f"[WARN] Admin alert failed: {e}")


def record_post_stat(state: dict, category: str, channel: str) -> None:
    stats = state.setdefault(
        "_stats",
        {
            "since": time.time(),
            "by_category": {},
            "by_channel": {},
        },
    )
    stats.setdefault("by_category", {})
    stats.setdefault("by_channel", {})
    stats["by_category"][category] = (
        stats["by_category"].get(category, 0) + 1
    )
    stats["by_channel"][channel] = (
        stats["by_channel"].get(channel, 0) + 1
    )


def maybe_build_stats_summary(state: dict):
    if STATS_INTERVAL_SECONDS <= 0:
        return None

    stats = state.get("_stats", {})
    since = stats.get("since", time.time())

    if time.time() - since < STATS_INTERVAL_SECONDS:
        return None

    by_category = stats.get("by_category", {})
    by_channel = stats.get("by_channel", {})
    total = sum(by_category.values())

    days = round((time.time() - since) / 86400, 1)
    lines = [
        f"📊 خلاصه {days} روز اخیر — {total} پست منتشر شد:"
    ]

    if by_category:
        lines.append("دسته‌ها:")
        for cat, count in sorted(
            by_category.items(),
            key=lambda x: -x[1],
        ):
            lines.append(f"• {cat}: {count}")

    if by_channel:
        lines.append("منابع:")
        for ch, count in sorted(
            by_channel.items(),
            key=lambda x: -x[1],
        ):
            lines.append(f"• {ch}: {count}")

    state["_stats"] = {
        "since": time.time(),
        "by_category": {},
        "by_channel": {},
    }
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def _run() -> None:
    if not SOURCE_CHANNELS:
        print("[ERROR] No SOURCE_CHANNELS configured.")
        sys.exit(1)

    state = load_state()

    posted_hashes = state.get("_posted_hashes", [])
    posted_hashes_set = set(posted_hashes)

    posted_events = state.get("_posted_events", [])

    channel_fail_counts = state.setdefault(
        "_channel_fail_counts", {}
    )
    alerted_channels = set(
        state.get("_alerted_dead_channels", [])
    )
    dead_keys = set(
        state.get("_dead_gemini_keys", [])
    )

    client = TelegramClient(
        StringSession(SESSION_STRING),
        API_ID,
        API_HASH,
    )
    await client.start()

    preflight_check_keys(dead_keys)

    if len(dead_keys) >= len(GEMINI_API_KEYS):
        await send_admin_alert(
            client,
            "همه کلیدهای Gemini نامعتبر/منقضی هستند.",
        )

    try:
        for channel in SOURCE_CHANNELS:
            last_id = int(state.get(channel, 0))

            try:
                # We fetch a larger window now. Crucially, last_id is NOT
                # advanced to newest_seen at once. It is advanced after each
                # processed group, so a timeout cannot silently skip messages.
                raw_messages = []

                async for msg in client.iter_messages(
                    channel,
                    min_id=last_id,
                    limit=MAX_MESSAGES_PER_CHANNEL,
                ):
                    if msg.text or msg.media:
                        raw_messages.append(msg)

                channel_fail_counts[channel] = 0

            except FloodWaitError as e:
                print(
                    f"[INFO] FloodWait reading {channel}: "
                    f"{e.seconds}s"
                )
                await asyncio.sleep(e.seconds + 1)
                continue

            except Exception as e:
                print(
                    f"[WARN] Failed to read {channel}: {e}"
                )
                channel_fail_counts[channel] = (
                    channel_fail_counts.get(channel, 0) + 1
                )

                if (
                    channel_fail_counts[channel]
                    >= CHANNEL_FAIL_ALERT_THRESHOLD
                    and channel not in alerted_channels
                ):
                    await send_admin_alert(
                        client,
                        f"چنل «{channel}» چند بار پیاپی خطا داده است.",
                    )
                    alerted_channels.add(channel)

                state["_channel_fail_counts"] = channel_fail_counts
                state["_alerted_dead_channels"] = sorted(
                    alerted_channels
                )
                save_state(state)
                continue

            raw_messages.reverse()
            groups = group_albums(raw_messages)

            for group in groups:
                # If a previous group was processed and checkpointed,
                # never process it again.
                group_last_id = max(m.id for m in group)
                if group_last_id <= last_id:
                    continue

                source_text = next(
                    (m.text for m in group if m.text),
                    "",
                ) or ""

                try:
                    result = classify_with_gemini(
                        source_text,
                        dead_keys,
                    )
                except Exception as e:
                    print(
                        f"[WARN] Gemini classification failed "
                        f"for {channel}: {e}"
                    )
                    result = safe_result({})
                finally:
                    await asyncio.sleep(
                        GEMINI_CALL_DELAY_SECONDS
                    )

                # Always checkpoint THIS group after attempting it.
                # This prevents one bad message from blocking the channel
                # forever and prevents a run timeout from skipping future
                # messages.
                last_id = group_last_id
                state[channel] = last_id

                if not result["relevant"]:
                    save_state(state)
                    continue

                final_text = (
                    result["text"].strip()
                    or source_text.strip()
                )

                if not final_text and not any(
                    m.media for m in group
                ):
                    save_state(state)
                    continue

                # Event-level dedup is checked BEFORE exact text.
                event_key = result["event_key"]
                if event_key and event_already_posted(
                    event_key,
                    posted_events,
                ):
                    print(
                        f"[SKIP] Duplicate event from {channel}: "
                        f"{event_key}"
                    )
                    save_state(state)
                    continue

                if (
                    final_text
                    and len(final_text.strip()) > 15
                ):
                    h = content_hash(final_text)
                    if h in posted_hashes_set:
                        print(
                            f"[SKIP] Exact duplicate from {channel}"
                        )
                        save_state(state)
                        continue

                try:
                    await post_group(
                        client,
                        group,
                        final_text,
                        channel,
                    )
                except Exception as e:
                    print(
                        f"[WARN] Failed to post from {channel}: {e}"
                    )
                    save_state(state)
                    continue

                if (
                    final_text
                    and len(final_text.strip()) > 15
                ):
                    h = content_hash(final_text)
                    posted_hashes_set.add(h)
                    posted_hashes.append(h)

                if event_key:
                    remember_event(
                        posted_events,
                        event_key,
                    )

                record_post_stat(
                    state,
                    result.get("category", "unknown"),
                    channel,
                )

                print(
                    f"[OK] Posted "
                    f"({result.get('category')}/"
                    f"{result.get('action')}) from {channel}"
                )

                # Persist after EVERY group, not only after a whole channel.
                if len(posted_hashes) > MAX_DEDUP_HASHES:
                    posted_hashes = posted_hashes[
                        -MAX_DEDUP_HASHES:
                    ]

                if len(posted_events) > MAX_DEDUP_HASHES:
                    posted_events = posted_events[
                        -MAX_DEDUP_HASHES:
                    ]

                state["_posted_hashes"] = posted_hashes
                state["_posted_events"] = posted_events
                state["_dead_gemini_keys"] = sorted(dead_keys)
                state["_channel_fail_counts"] = (
                    channel_fail_counts
                )
                state["_alerted_dead_channels"] = sorted(
                    alerted_channels
                )
                save_state(state)

                gap = random.uniform(
                    POST_GAP_MIN_SECONDS,
                    POST_GAP_MAX_SECONDS,
                )
                await asyncio.sleep(gap)

            # End-of-channel checkpoint.
            state[channel] = last_id
            state["_posted_hashes"] = posted_hashes
            state["_posted_events"] = posted_events
            state["_dead_gemini_keys"] = sorted(dead_keys)
            state["_channel_fail_counts"] = channel_fail_counts
            state["_alerted_dead_channels"] = sorted(
                alerted_channels
            )
            save_state(state)

        summary = maybe_build_stats_summary(state)
        if summary:
            await send_admin_alert(client, summary)
            save_state(state)

    finally:
        await client.disconnect()
        save_state(state)


async def main() -> None:
    try:
        await asyncio.wait_for(
            _run(),
            timeout=RUN_TIMEOUT_SECONDS,
        )
    except asyncio.TimeoutError:
        print(
            f"[ERROR] Run exceeded {RUN_TIMEOUT_SECONDS:.0f}s. "
            "Progress was checkpointed after each processed group."
        )


if __name__ == "__main__":
    asyncio.run(main())
