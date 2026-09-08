"""
News Curator Bot
-----------------

Reads new messages (text, photos, videos, albums) from a list of source
Telegram channels using a Telethon *user* session (a normal Bot API bot
can't read channels it isn't admin of), asks Gemini to classify each
message into a category (war / funny / random / important / strange /
none) and, if relevant, either copies it as-is or rewrites/summarizes
the caption (Gemini decides which), then posts the result — including
any photo/video/album — to your own channel.

Posting is done with the same Telethon user session (not a separate Bot
API bot): Telegram lets a client copy media server-side via
send_file(file=<original media object>) without re-uploading bytes,
which handles photos/videos/documents/albums transparently.

Extra features:
- Multiple Gemini API keys with round-robin + automatic fallback when
  one key hits its rate limit (429). If a key turns out to be
  invalid/expired/revoked (not just rate-limited), it is permanently
  disabled and remembered in last_ids.json so future runs don't waste
  time retrying it. Only a SHA-256 fingerprint of the key is ever
  written to last_ids.json (which the workflow commits to git) - never
  the raw key itself.
- An optional random pause after each post (POST_GAP_MIN_SECONDS /
  POST_GAP_MAX_SECONDS, both 0/disabled by default) so posts don't land
  at suspiciously regular, bot-like intervals.
- A small delay after every Gemini call (classification, embedding, and
  duplicate-event judging alike) to stay under free-tier RPM caps.
- Event-level duplicate detection (see below) — the same real-world
  story reported with completely different wording by different source
  channels is only posted once, while genuinely different stories about
  the same broad topic (same country, same company, same person) are
  NOT wrongly treated as duplicates.
- Telegram albums (multi-photo/video posts) are kept together as a
  single post instead of being split into separate messages.
- Telegram FloodWait errors are handled by waiting it out instead of
  crashing the run.
- A watermark/signature (e.g. your channel link) is appended to the end
  of every post, text or media caption.
- Strict relevance filtering: casual personal chit-chat, jokes between
  friends, ads/self-promotion, and other non-news content are rejected
  instead of being posted.
- Natural, human writing style: captions get 1-3 fitting emojis and a
  conversational (not robotic) tone, while staying factual — no filler
  or made-up details just to sound livelier.
- Source-trace scrubbing: channel signatures, @mentions-only lines,
  t.me links, and "forwarded from"/"منبع:" style attributions are
  stripped from the final text as a safety net, so posts don't look
  like an obvious copy-paste from another channel.
- Retry-with-backoff on transient network/5xx errors before rotating to
  a different Gemini key (a temporary blip isn't treated like a bad key).
- A quick "preflight" ping to every key at the start of the run, so a
  dead key is caught in one request instead of being rediscovered on
  every single message.
- Per-channel failure tracking with a one-time Telegram alert (to
  ADMIN_CHAT_ID, or your own Saved Messages by default) if a source
  channel fails to read several runs in a row — a likely sign it was
  banned/deleted and should be removed from SOURCE_CHANNELS.
- A global run timeout (RUN_TIMEOUT_SECONDS) so a slow run can't still
  be going when the next scheduled run starts.
- State is checkpointed to last_ids.json after every source channel
  (not just at the very end), so a crash/timeout partway through doesn't
  lose progress already made.
- An optional periodic (default weekly) summary of posts-by-category and
  posts-by-channel, sent to ADMIN_CHAT_ID.

Duplicate detection — how it actually works
--------------------------------------------
Two layers, cheapest first:

1. Exact-text hash (fast path, free): if the *final* posted text is
   byte-for-byte identical (after whitespace/case normalization) to
   something already posted, it's skipped immediately. Catches literal
   re-posts / re-forwards of the same source message.

2. Event-fingerprint matching (semantic, for everything else): plain
   "this is about the same broad topic" is NOT enough to call two
   messages duplicates — e.g. "Trump met Netanyahu" and "Trump commented
   on the Gaza war" share subject/country but are different events, and
   must both be posted. Conversely "Bitcoin hit $100K" and "BTC crossed
   the 100,000 dollar mark" are worded completely differently but ARE
   the same event.

   So instead of a single topic slug, Gemini extracts a structured
   event fingerprint for every relevant message (event_type, subject,
   action, object, location, time, numbers, event_status). That
   fingerprint is turned into a short canonical sentence and embedded
   (Gemini embeddings). For a new message, we only compare against
   *recent* stored events (an event more than EVENT_DEDUP_WINDOW_SECONDS
   old is not a candidate — a new earthquake report days after an old
   one is a new event, not a dup) whose embedding is close enough
   (cosine similarity >= EVENT_SIMILARITY_THRESHOLD) to be worth
   checking at all — this is just a cheap pre-filter, not the final
   answer.

   Only for those few candidates do we spend an actual Gemini call
   asking it to judge, with explicit few-shot examples, whether the two
   messages describe the *same real-world event* — not just a similar
   topic. Only a "yes" from that judge call marks the new message as a
   duplicate. This keeps the expensive step rare (most messages have no
   close candidate at all) while avoiding both failure modes: dumb
   exact-text hashing (misses reworded dupes) and dumb topic-only
   matching (wrongly merges distinct events that share a topic).

Designed to run on a schedule (GitHub Actions cron, every 1-2 hours).
State (last seen message id per source channel, exact-text hashes, and
recent event fingerprints/embeddings for dedup) is kept in
last_ids.json, which the workflow commits back to the repo after each
run so the next run knows where to continue from.
"""

import asyncio
import hashlib
import json
import math
import os
import random
import re
import sys
import time
from pathlib import Path
from typing import Callable, Optional

import requests
from telethon import TelegramClient
from telethon.errors import FloodWaitError
from telethon.sessions import StringSession

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Telethon (user account) credentials - from https://my.telegram.org
API_ID = int(os.environ["TG_API_ID"])
API_HASH = os.environ["TG_API_HASH"]
SESSION_STRING = os.environ["TG_SESSION_STRING"]

TARGET_CHANNEL = os.environ["TARGET_CHANNEL"]  # e.g. "@my_channel"

# Watermark/signature appended to the end of every post (e.g. your
# channel's own link). Leave WATERMARK_TEXT empty to disable.
WATERMARK_TEXT = os.environ.get("WATERMARK_TEXT", "").strip() or "https://t.me/KosSherijat_69"

# Telegram limits: 4096 chars for a plain text message, 1024 for a
# media caption. We trim the generated text so the watermark always fits.
MAX_TEXT_LEN = 4096
MAX_CAPTION_LEN = 1024


def add_watermark(text: str, is_caption: bool) -> str:
    if not WATERMARK_TEXT:
        return text
    limit = MAX_CAPTION_LEN if is_caption else MAX_TEXT_LEN
    footer = f"\n\n{WATERMARK_TEXT}"
    room = limit - len(footer)
    if room < 0:
        return WATERMARK_TEXT[:limit]
    trimmed = text[:room].rstrip() if len(text) > room else text
    return f"{trimmed}{footer}" if trimmed else WATERMARK_TEXT


# Gemini - supports multiple comma-separated keys for round-robin +
# automatic fallback when one hits its rate limit.
GEMINI_API_KEYS = [
    k.strip() for k in os.environ.get("GEMINI_API_KEYS", "").split(",") if k.strip()
]
if not GEMINI_API_KEYS:
    # Backward-compatible single-key env var
    single = os.environ.get("GEMINI_API_KEY", "").strip()
    if single:
        GEMINI_API_KEYS = [single]
if not GEMINI_API_KEYS:
    print("[ERROR] No Gemini API key configured (GEMINI_API_KEYS or GEMINI_API_KEY).")
    sys.exit(1)

GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-3.6-flash")
# Cheap embedding model used only for the event-dedup candidate pre-filter.
EMBEDDING_MODEL = os.environ.get("EMBEDDING_MODEL", "text-embedding-004")

# Delay between Gemini calls, to stay under free-tier requests-per-minute caps.
GEMINI_CALL_DELAY_SECONDS = float(os.environ.get("GEMINI_CALL_DELAY_SECONDS", "4"))

# Source channels to curate from (usernames, no @ needed but both work).
# Hardcoded defaults below; can still be overridden with the SOURCE_CHANNELS
# env var (comma-separated) if set.
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
    c.strip() for c in os.environ.get("SOURCE_CHANNELS", "").split(",") if c.strip()
] or DEFAULT_SOURCE_CHANNELS

# How many messages max to pull per channel per run (safety cap)
MAX_MESSAGES_PER_CHANNEL = int(os.environ.get("MAX_MESSAGES_PER_CHANNEL", "30"))

# How many recent exact-text content hashes to remember for the fast-path
# duplicate check.
MAX_DEDUP_HASHES = int(os.environ.get("MAX_DEDUP_HASHES", "500"))

# --- Event-fingerprint dedup tuning -----------------------------------
# Only events posted within this many seconds are ever considered as
# duplicate candidates for a new message. A "similar" story outside this
# window is treated as a new, unrelated event (e.g. a fresh earthquake
# report days after an old one).
EVENT_DEDUP_WINDOW_SECONDS = float(
    os.environ.get("EVENT_DEDUP_WINDOW_SECONDS", "").strip() or str(3 * 24 * 3600)
)
# Cosine-similarity cutoff for an event to even be considered a candidate
# worth spending a Gemini "same event?" judge call on. This is a cheap
# pre-filter, not the final duplicate decision.
EVENT_SIMILARITY_THRESHOLD = float(os.environ.get("EVENT_SIMILARITY_THRESHOLD", "0.80"))
# At most this many closest candidates get an actual judge call.
EVENT_CANDIDATE_TOP_K = int(os.environ.get("EVENT_CANDIDATE_TOP_K", "3"))
# How many recent events to keep in state for comparison (independent of
# the time window, as a hard cap on state file size).
MAX_STORED_EVENTS = int(os.environ.get("MAX_STORED_EVENTS", "400"))

# Safety cap on total run time so a slow run can't still be going when the
# next scheduled run starts (cron is hourly by default). Leaves headroom
# before the ~1h cron interval.
RUN_TIMEOUT_SECONDS = float(os.environ.get("RUN_TIMEOUT_SECONDS", "").strip() or "3300")

# How many consecutive failed reads before we loudly flag a source channel
# as possibly dead/banned/removed (instead of just quietly skipping it
# every run forever).
CHANNEL_FAIL_ALERT_THRESHOLD = int(os.environ.get("CHANNEL_FAIL_ALERT_THRESHOLD", "").strip() or "3")

# Optional random pause after each successful post (in addition to
# GEMINI_CALL_DELAY_SECONDS), so posts don't land at suspiciously
# regular, bot-like intervals. Both default to 0 (disabled) if unset.
POST_GAP_MIN_SECONDS = float(os.environ.get("POST_GAP_MIN_SECONDS", "").strip() or "0")
POST_GAP_MAX_SECONDS = float(os.environ.get("POST_GAP_MAX_SECONDS", "").strip() or "0")

# Where to send operational alerts (all-keys-dead, a source channel
# repeatedly failing, etc.) and the periodic stats summary. Optional -
# defaults to "me" (your own Telegram "Saved Messages"), since posting
# alerts to your own audience in TARGET_CHANNEL isn't appropriate.
ADMIN_CHAT = os.environ.get("ADMIN_CHAT_ID", "").strip() or "me"

# How often (in seconds) to send a "posts by category/channel" summary to
# ADMIN_CHAT. Default 7 days. Set to 0 to disable.
STATS_INTERVAL_SECONDS = float(
    os.environ.get("STATS_INTERVAL_SECONDS", "").strip() or str(7 * 24 * 3600)
)

STATE_FILE = Path(__file__).parent / "last_ids.json"

CLASSIFY_PROMPT = """You are a strict content curator for a Telegram news
channel. You only let through content that reads like an actual news
item, report, or notable real-world story — never personal chit-chat.

You will be given the text/caption of a message from another Telegram
channel (it may also have a photo or video attached, which you can't see,
but the text below is all of the written content).

First, mark it NOT relevant (category "none") if it is any of:
- personal chit-chat, a joke between friends, or a first-person
  daily-life remark/complaint that isn't an actual news story (example:
  "منم روم نمیشه دِین رفیقمو بدم" — a casual personal comment, not news)
- an ad, self-promotion, or an invitation to join another channel/bot
- a question, poll, or something addressed directly to the channel's
  own audience
- just a link, hashtag spam, or a channel signature with no real content
- too vague, generic, or short to be a real story

If it's genuinely a news-like item, decide which single category best fits:
- "war": war / military conflict / geopolitical crisis news
- "funny": funny or amusing real-world event, meme-worthy story
- "random": random/miscellaneous notable daily event, not war/funny/strange
- "important": an important event worth knowing about (non-war)
- "strange": a strange, bizarre, or surprising real-world event

If relevant, also decide whether to COPY it (keep the original wording
and facts basically as-is, only touching it up lightly — fixing
formatting, removing source traces, adding fitting emojis) or REWRITE it
(condense, clean up, translate if needed) into a short, punchy caption.
Prefer REWRITE whenever the original text contains anything that reveals
its source — a channel name, @username, t.me link, "forwarded from",
"منبع:"/"به نقل از", or any self-promotion — so the result reads like an
original post instead of an obvious copy-paste. Never include a source
channel's name, @username, or link in the output text, whether you copy
or rewrite.

Writing style for the final "text" (whether copied or rewritten):
- Keep it natural and easy to read, not stiff or robotic — like a real
  person sharing news with friends, not a press release.
- The content must stay coherent and make actual sense: don't invent,
  exaggerate, or pad with filler/nonsense just to sound lively. If the
  source text is thin, keep the caption short rather than making things
  up.
- Add 1-3 emojis that genuinely fit the story's content and category
  (e.g. 🔥⚠️ for war/important, 😂 for funny, 😳🤯 for strange, 📰 for
  random) — placed naturally (start of a line, next to the key fact),
  never spammy, never more than one emoji per short sentence, and never
  if the story is somber/tragic enough that emojis would feel out of
  place (e.g. deaths, disasters — keep those plain or use at most a
  single sober emoji like ⚠️).

If the input text is empty (e.g. a photo/video with no caption), it's
still fine to mark it relevant if you have no reason to think otherwise
— just return an empty "text".

Also, if relevant, extract an "fingerprint" object describing the single
concrete real-world event being reported (used only internally, to later
tell apart "same event reported twice" from "different event, same
general topic"):
- "event_type": short category of what happened (e.g. "price_record",
  "meeting", "military_strike", "election_result", "injury",
  "earthquake", "statement")
- "subject": the main actor/entity the event is about (person, company,
  country, team, asset, etc.)
- "action": the specific thing that happened, as a short verb phrase
  (e.g. "crossed", "met with", "struck", "won", "resigned")
- "object": what the action was directed at / involves, if any (e.g. the
  other party in a meeting, the target of a strike, the threshold
  crossed)
- "location": place the event happened, if mentioned, else null
- "time": date/time of the event if mentioned (any format, else null)
- "numbers": array of key numeric values mentioned (prices, casualty
  counts, scores, percentages, magnitudes) as strings, empty array if none
- "event_status": one short word for the state of the event if relevant,
  e.g. "new_record", "ongoing", "resolved", "denied", "confirmed", else null

Be concrete and specific in the fingerprint — it must be possible to
tell two DIFFERENT events about the same subject apart just from it
(e.g. two different earthquakes, or two different meetings between the
same two people on different days, must get different "time"/"numbers"/
"action" values, not identical fingerprints). Leave fingerprint fields
null/empty and use an empty object {} if not relevant.

Respond with ONLY valid JSON, no markdown fences, no extra text, in this
exact shape:
{"relevant": true/false, "category": "war"|"funny"|"random"|"important"|"strange"|"none", "action": "copy"|"rewrite", "text": "final ready-to-post caption, or empty string", "fingerprint": {"event_type": "", "subject": "", "action": "", "object": "", "location": null, "time": null, "numbers": [], "event_status": null}}

Message:
---
{MESSAGE}
---
"""

# Few-shot judge: decides whether two messages report the SAME real-world
# event, not just a similar topic/subject. This is deliberately the exact
# kind of distinction a pure topic-slug or text-similarity match gets
# wrong, so it's spelled out with contrastive examples.
JUDGE_PROMPT = """Two news items are given below, each as (original text +
extracted fingerprint). Decide whether they report the SAME specific
real-world event — not merely the same general topic, subject, country,
or person.

Similarity of topic, subject, country, or wording is NOT sufficient by
itself. Only answer "same event" if both items are clearly describing
one and the same concrete happening (same action, same target/threshold/
result, same approximate time if a time is given).

Examples:
- "ترامپ با نتانیاهو دیدار کرد" vs "ترامپ درباره جنگ غزه صحبت کرد"
  -> DIFFERENT events (same person, different happening)
- "بیت‌کوین به ۱۰۰ هزار دلار رسید" vs "BTC از مرز ۱۰۰,۰۰۰ دلار عبور کرد"
  -> SAME event (same threshold, same asset, same milestone), even
     though the wording is completely different
- "زلزله ۶ ریشتری در ژاپن" vs "زلزله دیگری با قدرت ۵ ریشتر در ژاپن"
  -> DIFFERENT events (different magnitude = different earthquake)
- "فلان شرکت ۱۰٪ رشد کرد" vs "سهام همان شرکت ۱۰٪ افزایش یافت"
  -> SAME event (same company, same figure, same kind of move)
- "فلان بازیکن مصدوم شد" vs "همان بازیکن از مصدومیت برگشت"
  -> DIFFERENT events (injury vs. recovery are opposite happenings)

Item A:
text: {TEXT_A}
fingerprint: {FP_A}

Item B:
text: {TEXT_B}
fingerprint: {FP_B}

Respond with ONLY valid JSON, no markdown fences, no extra text:
{"same_event": true/false}
"""

# Round-robin cursor over API keys, shared across calls in this run.
_key_cursor = 0


def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    return {}


def save_state(state: dict) -> None:
    STATE_FILE.write_text(
        json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8"
    )


async def send_admin_alert(client: TelegramClient, text: str) -> None:
    """Best-effort notification to ADMIN_CHAT (defaults to your own Saved
    Messages) so problems don't sit unnoticed in GitHub Actions logs.
    Never raises - an alert failing shouldn't crash the actual bot run."""
    try:
        await client.send_message(ADMIN_CHAT, f"🛠 [News Curator Bot]\n{text}")
    except Exception as e:  # noqa: BLE001
        print(f"[WARN] Couldn't send admin alert: {e}")


def maybe_build_stats_summary(state: dict) -> Optional[str]:
    """Returns a summary string (and resets the counters in `state`) if
    STATS_INTERVAL_SECONDS has elapsed since the last summary, else None."""
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

    lines = [f"📊 خلاصه‌ی {days} روز اخیر — {total} پست منتشر شد:"]
    if by_category:
        lines.append("بر اساس دسته:")
        for cat, count in sorted(by_category.items(), key=lambda x: -x[1]):
            lines.append(f"  • {cat}: {count}")
    if by_channel:
        lines.append("بر اساس منبع:")
        for ch, count in sorted(by_channel.items(), key=lambda x: -x[1]):
            lines.append(f"  • {ch}: {count}")

    # Reset counters for the next period.
    state["_stats"] = {"since": time.time(), "by_category": {}, "by_channel": {}}
    return "\n".join(lines)


def record_post_stat(state: dict, category: str, channel: str) -> None:
    stats = state.setdefault("_stats", {"since": time.time(), "by_category": {}, "by_channel": {}})
    stats.setdefault("by_category", {})
    stats.setdefault("by_channel", {})
    stats["by_category"][category] = stats["by_category"].get(category, 0) + 1
    stats["by_channel"][channel] = stats["by_channel"].get(channel, 0) + 1


# Lines that are made up ONLY of a source-channel signature (a bare
# @mention, a bare t.me link, "Forwarded from ...", "منبع: ...", a
# "join our channel" plug, etc.) get dropped entirely. This is a safety
# net on top of the Gemini prompt, in case the model leaves one in.
_SOURCE_TRACE_LINE_PATTERNS = [
    re.compile(r"(?im)^\s*forwarded from\b.*$"),
    re.compile(r"(?im)^\s*فوروارد(?:\s*شده)?\s*از\b.*$"),
    re.compile(r"(?im)^\s*(منبع|به نقل از|به گزارش)\s*[:：].*$"),
    re.compile(r"(?im)^[\s\W]*@[A-Za-z0-9_]{4,32}[\s\W]*$"),
    re.compile(r"(?im)^[\s\W]*https?://t\.me/\S+[\s\W]*$"),
    re.compile(r"(?im)^\s*(join|عضویت در کانال|کانال ما|چنل ما)\b.*$"),
]
# A bare t.me link that shows up in the middle of an otherwise-fine line
# is stripped inline rather than dropping the whole line.
_INLINE_TME_LINK = re.compile(r"https?://t\.me/\S+", re.IGNORECASE)


def strip_source_traces(text: str) -> str:
    """Remove channel signatures, bare @mentions/t.me links, and
    forwarded/attribution lines so a post doesn't look like an obvious
    copy-paste from another channel."""
    if not text:
        return text
    kept_lines = [
        line for line in text.splitlines()
        if not any(p.match(line) for p in _SOURCE_TRACE_LINE_PATTERNS)
    ]
    cleaned = "\n".join(kept_lines)
    cleaned = _INLINE_TME_LINK.sub("", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned.strip()


def content_hash(text: str) -> str:
    normalized = " ".join(text.strip().lower().split())
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _mask_key(key: str) -> str:
    """Never print a full API key in logs."""
    return f"...{key[-4:]}" if len(key) > 4 else "****"


def _key_fingerprint(key: str) -> str:
    """Never store a raw API key on disk (last_ids.json gets committed to
    git by the workflow) - only its hash, so a disabled/dead key can still
    be recognized on the next run without ever leaking the real value."""
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def _gemini_request(url_for_key: Callable[[str], str], payload: dict, dead_keys: set) -> Optional[dict]:
    """Shared round-robin/retry/dead-key logic for any Gemini REST call
    (generateContent or embedContent). `url_for_key` builds the full URL
    for a given API key. Returns the parsed JSON response body, or None
    if every usable key failed for this call."""
    global _key_cursor

    active_keys = [k for k in GEMINI_API_KEYS if _key_fingerprint(k) not in dead_keys]
    if not active_keys:
        return None

    n_keys = len(active_keys)
    for attempt in range(n_keys):
        key = active_keys[(_key_cursor + attempt) % n_keys]
        url = url_for_key(key)

        # A transient network error or a 5xx from Google doesn't mean the
        # key is bad — retry the SAME key a couple of times with a short
        # backoff before giving up on it and moving to the next one.
        resp = None
        for retry in range(3):
            try:
                resp = requests.post(url, json=payload, timeout=60)
            except requests.RequestException as e:
                print(f"[WARN] Gemini request error on key {_mask_key(key)} "
                      f"(retry {retry + 1}/3): {e}")
                resp = None
            else:
                if resp.status_code < 500:
                    break  # not a transient server error, stop retrying
                print(f"[WARN] Gemini server error {resp.status_code} on key "
                      f"{_mask_key(key)} (retry {retry + 1}/3)")
            if retry < 2:
                time.sleep(2 * (retry + 1))

        if resp is None:
            continue

        if resp.status_code == 429:
            print(f"[WARN] Gemini key {_mask_key(key)} rate-limited, trying next key")
            continue

        if resp.status_code in (400, 401, 403):
            err_status, err_msg = "", resp.text[:300]
            try:
                err = resp.json().get("error", {})
                err_status = err.get("status", "")
                err_msg = err.get("message", err_msg)
            except ValueError:
                pass
            looks_like_bad_key = (
                err_status in ("PERMISSION_DENIED", "UNAUTHENTICATED")
                or "api key" in err_msg.lower()
                or "api_key" in err_status.lower()
            )
            if looks_like_bad_key:
                print(f"[WARN] Gemini key {_mask_key(key)} looks invalid/expired "
                      f"({err_status or resp.status_code}: {err_msg[:120]}); "
                      f"disabling it permanently.")
                dead_keys.add(_key_fingerprint(key))
            else:
                print(f"[WARN] Gemini call failed: {resp.status_code} {err_msg[:200]}")
            continue

        if not resp.ok:
            print(f"[WARN] Gemini call failed: {resp.status_code} {resp.text[:200]}")
            continue

        # Success — advance the cursor so the next call starts from the
        # next key (spreads load evenly across keys).
        _key_cursor = (_key_cursor + attempt + 1) % max(n_keys, 1)
        return resp.json()

    return None


def _parse_json_response(raw: str) -> Optional[dict]:
    raw = (raw or "").strip()
    if raw.startswith("```"):
        raw = raw.strip("`")
        raw = raw.replace("json\n", "", 1).replace("json", "", 1)
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def classify_with_gemini(message_text: str, dead_keys: set) -> dict:
    """Classify a message and, if relevant, extract its event fingerprint.
    Returns a safe "not relevant" default if every usable key fails."""
    if not [k for k in GEMINI_API_KEYS if _key_fingerprint(k) not in dead_keys]:
        print("[ERROR] No usable Gemini keys left — all are disabled as "
              "invalid/expired. Add a new key to GEMINI_API_KEYS.")
        return {"relevant": False, "category": "none", "action": "copy", "text": "", "fingerprint": {}}

    prompt = CLASSIFY_PROMPT.replace("{MESSAGE}", (message_text or "")[:4000])
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.6},
    }
    url_for_key = lambda key: (
        f"https://generativelanguage.googleapis.com/v1beta/models/"
        f"{GEMINI_MODEL}:generateContent?key={key}"
    )
    data = _gemini_request(url_for_key, payload, dead_keys)
    if data is None:
        print("[WARN] All usable Gemini keys failed/rate-limited for this message; skipping.")
        return {"relevant": False, "category": "none", "action": "copy", "text": "", "fingerprint": {}}

    try:
        raw = data["candidates"][0]["content"]["parts"][0]["text"]
    except (KeyError, IndexError):
        return {"relevant": False, "category": "none", "action": "copy", "text": "", "fingerprint": {}}

    parsed = _parse_json_response(raw)
    if parsed is None:
        return {"relevant": False, "category": "none", "action": "copy", "text": "", "fingerprint": {}}
    parsed.setdefault("fingerprint", {})
    return parsed


def embed_text(text: str, dead_keys: set) -> Optional[list]:
    """Get an embedding vector for `text` via Gemini's embedding model.
    Returns None if it's empty or every usable key fails — callers must
    treat that as "skip the semantic dedup check for this message" rather
    than crash the run."""
    text = (text or "").strip()
    if not text:
        return None

    payload = {"content": {"parts": [{"text": text[:2000]}]}}
    url_for_key = lambda key: (
        f"https://generativelanguage.googleapis.com/v1beta/models/"
        f"{EMBEDDING_MODEL}:embedContent?key={key}"
    )
    data = _gemini_request(url_for_key, payload, dead_keys)
    if data is None:
        return None
    try:
        return data["embedding"]["values"]
    except (KeyError, TypeError):
        return None


def cosine_similarity(a: list, b: list) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(y * y for y in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def fingerprint_to_text(fp: dict) -> str:
    """Turn a structured event fingerprint into one short canonical
    sentence, used both for embedding and for showing the judge call a
    compact summary of "what event is this"."""
    if not fp:
        return ""
    parts = [
        fp.get("subject"),
        fp.get("action"),
        fp.get("object"),
        fp.get("location"),
        fp.get("time"),
        " ".join(fp.get("numbers") or []),
        fp.get("event_status"),
    ]
    return " ".join(str(p).strip() for p in parts if p).strip()


def judge_same_event(text_a: str, fp_a: dict, text_b: str, fp_b: dict, dead_keys: set) -> bool:
    """Ask Gemini whether two messages report the same real-world event.
    Defaults to False (not a duplicate) on any failure — an infra hiccup
    here should never silently suppress a real, new story."""
    prompt = (
        JUDGE_PROMPT
        .replace("{TEXT_A}", (text_a or "")[:800])
        .replace("{FP_A}", json.dumps(fp_a or {}, ensure_ascii=False))
        .replace("{TEXT_B}", (text_b or "")[:800])
        .replace("{FP_B}", json.dumps(fp_b or {}, ensure_ascii=False))
    )
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.0},
    }
    url_for_key = lambda key: (
        f"https://generativelanguage.googleapis.com/v1beta/models/"
        f"{GEMINI_MODEL}:generateContent?key={key}"
    )
    data = _gemini_request(url_for_key, payload, dead_keys)
    if data is None:
        return False
    try:
        raw = data["candidates"][0]["content"]["parts"][0]["text"]
    except (KeyError, IndexError):
        return False
    parsed = _parse_json_response(raw)
    if not parsed:
        return False
    return bool(parsed.get("same_event", False))


def prune_events(events: list, now: float) -> list:
    """Drop events older than the dedup window, then cap to the most
    recent MAX_STORED_EVENTS regardless of age (state-size safety net)."""
    fresh = [e for e in events if now - e.get("ts", 0) <= EVENT_DEDUP_WINDOW_SECONDS]
    if len(fresh) > MAX_STORED_EVENTS:
        fresh = fresh[-MAX_STORED_EVENTS:]
    return fresh


async def find_duplicate_event(
    new_text: str,
    new_fp: dict,
    new_embedding: Optional[list],
    events: list,
    dead_keys: set,
) -> Optional[dict]:
    """Returns the matching stored event dict if `new_text`/`new_fp` is
    judged to be the same real-world event as one already posted recently,
    else None. Cheap embedding similarity narrows candidates; only those
    get an actual LLM judge call.

    Each judge call is a blocking HTTP request, so it's run in a worker
    thread (asyncio.to_thread) to avoid stalling the event loop that the
    Telethon client relies on, and is followed by the same
    GEMINI_CALL_DELAY_SECONDS pause used after every other Gemini call, so
    a message with several close candidates can't burst past the
    free-tier RPM cap."""
    if new_embedding is None or not events:
        return None

    scored = []
    for ev in events:
        sim = cosine_similarity(new_embedding, ev.get("embedding") or [])
        if sim >= EVENT_SIMILARITY_THRESHOLD:
            scored.append((sim, ev))
    if not scored:
        return None

    scored.sort(key=lambda pair: -pair[0])
    for _, candidate in scored[:EVENT_CANDIDATE_TOP_K]:
        is_dup = await asyncio.to_thread(
            judge_same_event,
            new_text, new_fp,
            candidate.get("raw_text", ""), candidate.get("fingerprint", {}),
            dead_keys,
        )
        await asyncio.sleep(GEMINI_CALL_DELAY_SECONDS)
        if is_dup:
            return candidate
    return None


def preflight_check_keys(dead_keys: set) -> None:
    """Quick, cheap ping to each not-yet-dead key before the main loop, so
    an invalid/expired key is caught in ~1 request instead of being
    re-discovered on every message until it happens to be picked."""
    test_payload = {
        "contents": [{"parts": [{"text": "Reply with just the word OK."}]}],
        "generationConfig": {"temperature": 0, "maxOutputTokens": 5},
    }
    for key in list(GEMINI_API_KEYS):
        if _key_fingerprint(key) in dead_keys:
            continue
        url = (
            f"https://generativelanguage.googleapis.com/v1beta/models/"
            f"{GEMINI_MODEL}:generateContent?key={key}"
        )
        try:
            resp = requests.post(url, json=test_payload, timeout=30)
        except requests.RequestException as e:
            print(f"[WARN] Preflight check couldn't reach Gemini for key "
                  f"{_mask_key(key)}: {e} (will still try it normally later)")
            continue
        if resp.status_code in (400, 401, 403):
            try:
                err = resp.json().get("error", {})
                err_status, err_msg = err.get("status", ""), err.get("message", "")
            except ValueError:
                err_status, err_msg = "", resp.text[:200]
            if (err_status in ("PERMISSION_DENIED", "UNAUTHENTICATED")
                    or "api key" in err_msg.lower()):
                print(f"[WARN] Preflight: Gemini key {_mask_key(key)} is "
                      f"invalid/expired; disabling it before the run starts.")
                dead_keys.add(_key_fingerprint(key))


def group_albums(messages):
    """Group consecutive messages that share the same grouped_id (Telegram
    albums) into single lists, so they're posted together as one album
    instead of as separate messages."""
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


async def post_group(client: TelegramClient, group: list, final_text: str) -> None:
    """Post a message or album to TARGET_CHANNEL, retrying once if
    Telegram asks us to wait (FloodWaitError). The watermark is applied
    here so it lands on every single post, media or text."""
    media_list = [m.media for m in group if m.media]
    is_caption = bool(media_list)
    text_to_send = add_watermark(final_text or "", is_caption=is_caption)

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
        print(f"[INFO] FloodWait: sleeping {e.seconds}s as Telegram requested")
        await asyncio.sleep(e.seconds + 1)
        await _send()

    if POST_GAP_MAX_SECONDS > 0:
        gap = random.uniform(POST_GAP_MIN_SECONDS, max(POST_GAP_MIN_SECONDS, POST_GAP_MAX_SECONDS))
        await asyncio.sleep(gap)


async def process_message_group(
    client: TelegramClient,
    channel: str,
    group: list,
    state: dict,
    dead_keys: set,
    events: list,
    posted_hashes: list,
    posted_hashes_set: set,
) -> Optional[dict]:
    """Classify one message/album group, run it through both dedup layers,
    and post it if it's relevant and new.

    Mutates `events` / `posted_hashes` / `posted_hashes_set` / `state`
    (stats) in place so callers share dedup state across a whole run.
    Returns the classify result dict if the group was posted, else None.

    This is the single source of truth for "classify + dedup + post" —
    shared by the full hourly run (_run, below) and the urgent quick-scan
    companion script (urgent_scan.py), so a message that gets fast-tracked
    by the quick scan is judged with exactly the same quality/dedup logic
    as one handled by the normal run. Only *which* messages reach this
    function differs between the two.
    """
    source_text = next((m.text for m in group if m.text), "") or ""

    try:
        result = await asyncio.to_thread(classify_with_gemini, source_text, dead_keys)
    except Exception as e:  # noqa: BLE001
        print(f"[WARN] Gemini call failed for a message in {channel}: {e}")
        return None
    finally:
        await asyncio.sleep(GEMINI_CALL_DELAY_SECONDS)

    if not result.get("relevant"):
        return None

    final_text = result.get("text") or source_text
    final_text = strip_source_traces(final_text)

    # If cleanup left no text and there's no media either, there's
    # nothing worth posting.
    if not final_text.strip() and not any(m.media for m in group):
        print(f"[SKIP] Nothing left to post from {channel} after cleanup")
        return None

    # --- Layer 1: exact-text fast path (free) ---------------
    if final_text and len(final_text.strip()) > 15:
        h = content_hash(final_text)
        if h in posted_hashes_set:
            print(f"[SKIP] Exact-text duplicate from {channel}, already posted")
            return None

    # --- Layer 2: event-fingerprint semantic dedup -----------
    fingerprint = result.get("fingerprint") or {}
    fp_text = fingerprint_to_text(fingerprint)
    embedding_source = fp_text or final_text
    new_embedding = None
    if embedding_source.strip():
        new_embedding = await asyncio.to_thread(embed_text, embedding_source, dead_keys)
        await asyncio.sleep(GEMINI_CALL_DELAY_SECONDS)
    if new_embedding is None:
        print(f"[WARN] Couldn't get embedding for a message from {channel}; "
              f"skipping semantic dedup check for it")
    else:
        dup_event = await find_duplicate_event(final_text, fingerprint, new_embedding, events, dead_keys)
        if dup_event is not None:
            print(f"[SKIP] Same underlying event already posted "
                  f"(judged duplicate) from {channel}")
            return None

    # Not a duplicate by either layer — record it before posting so a
    # later message in the same run can also match against it.
    if final_text and len(final_text.strip()) > 15:
        h = content_hash(final_text)
        posted_hashes_set.add(h)
        posted_hashes.append(h)
    if new_embedding is not None:
        events.append({
            "id": content_hash(fp_text or final_text)[:16],
            "fingerprint": fingerprint,
            "raw_text": final_text[:800],
            "embedding": new_embedding,
            "ts": time.time(),
        })

    try:
        await post_group(client, group, final_text)
    except Exception as e:  # noqa: BLE001
        print(f"[WARN] Failed to post a message from {channel}: {e}")
        return None

    record_post_stat(state, result.get("category", "unknown"), channel)
    print(f"[OK] Posted ({result.get('category')}/{result.get('action')}) "
          f"from {channel}")
    return result


async def _run() -> None:
    if not SOURCE_CHANNELS:
        print("[ERROR] No SOURCE_CHANNELS configured. Set the env var (comma-separated).")
        sys.exit(1)

    state = load_state()

    posted_hashes = state.get("_posted_hashes", [])
    posted_hashes_set = set(posted_hashes)

    now = time.time()
    events = prune_events(state.get("_events", []), now)

    channel_fail_counts = state.setdefault("_channel_fail_counts", {})
    alerted_channels = set(state.get("_alerted_dead_channels", []))
    dead_keys = set(state.get("_dead_gemini_keys", []))
    if dead_keys:
        print(f"[INFO] Skipping {len(dead_keys)} previously-disabled Gemini key(s).")

    client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
    await client.start()

    await asyncio.to_thread(preflight_check_keys, dead_keys)
    if len(dead_keys) >= len(GEMINI_API_KEYS):
        msg = ("همه‌ی کلیدهای Gemini نامعتبر/منقضی شدن؛ یه کلید جدید به "
               "GEMINI_API_KEYS اضافه کن.")
        print(f"[ERROR] {msg}")
        await send_admin_alert(client, msg)

    for channel in SOURCE_CHANNELS:
        last_id = state.get(channel, 0)
        newest_seen = last_id
        raw_messages = []
        try:
            async for msg in client.iter_messages(
                channel, min_id=last_id, limit=MAX_MESSAGES_PER_CHANNEL
            ):
                if msg.text or msg.media:
                    raw_messages.append(msg)
                    newest_seen = max(newest_seen, msg.id)
            channel_fail_counts[channel] = 0
        except FloodWaitError as e:
            print(f"[INFO] FloodWait while reading {channel}: sleeping {e.seconds}s")
            await asyncio.sleep(e.seconds + 1)
        except Exception as e:  # noqa: BLE001
            print(f"[WARN] Failed to read {channel}: {e}")
            channel_fail_counts[channel] = channel_fail_counts.get(channel, 0) + 1
            if (channel_fail_counts[channel] >= CHANNEL_FAIL_ALERT_THRESHOLD
                    and channel not in alerted_channels):
                alert = (f"چنل «{channel}» {channel_fail_counts[channel]} بار "
                         f"پیاپی fail شده (احتمالاً حذف/بن شده یا دیگه در "
                         f"دسترس نیست). شاید بخوای از SOURCE_CHANNELS حذفش کنی.")
                print(f"[WARN] {alert}")
                await send_admin_alert(client, alert)
                alerted_channels.add(channel)
            state["_channel_fail_counts"] = channel_fail_counts
            state["_alerted_dead_channels"] = sorted(alerted_channels)
            save_state(state)
            continue

        # iter_messages returns newest-first; reverse to chronological
        # order, then group albums together.
        raw_messages.reverse()
        groups = group_albums(raw_messages)

        for group in groups:
            await process_message_group(
                client, channel, group, state, dead_keys, events,
                posted_hashes, posted_hashes_set,
            )

        state[channel] = newest_seen

        # Checkpoint after each channel so a timeout/crash partway through
        # doesn't lose all progress from channels already finished.
        if len(posted_hashes) > MAX_DEDUP_HASHES:
            posted_hashes = posted_hashes[-MAX_DEDUP_HASHES:]
            posted_hashes_set = set(posted_hashes)
        events = prune_events(events, time.time())

        state["_posted_hashes"] = posted_hashes
        state["_events"] = events
        state["_dead_gemini_keys"] = sorted(dead_keys)
        state["_channel_fail_counts"] = channel_fail_counts
        state["_alerted_dead_channels"] = sorted(alerted_channels)
        save_state(state)

    summary = maybe_build_stats_summary(state)
    if summary:
        await send_admin_alert(client, summary)

    await client.disconnect()
    save_state(state)


async def main() -> None:
    try:
        await asyncio.wait_for(_run(), timeout=RUN_TIMEOUT_SECONDS)
    except asyncio.TimeoutError:
        print(f"[ERROR] Run exceeded the {RUN_TIMEOUT_SECONDS:.0f}s safety "
              f"timeout and was stopped early so it won't overlap the next "
              f"scheduled run. Progress up to the last completed channel "
              f"was already saved.")


if __name__ == "__main__":
    asyncio.run(main())
