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
  one key hits its rate limit (429).
- A small delay between Gemini calls to stay under free-tier RPM caps.
- Duplicate-content detection: if the same story/caption shows up in
  more than one source channel, it's only posted once.
- Telegram albums (multi-photo/video posts) are kept together as a
  single post instead of being split into separate messages.
- Telegram FloodWait errors are handled by waiting it out instead of
  crashing the run.

Designed to run on a schedule (GitHub Actions cron, every 1-2 hours).
State (last seen message id per source channel, plus recently-posted
content hashes for dedup) is kept in last_ids.json, which the workflow
commits back to the repo after each run so the next run knows where to
continue from.
"""

import asyncio
import hashlib
import json
import os
import sys
from pathlib import Path

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

GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.0-flash")

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

# How many recent content hashes to remember for duplicate detection
MAX_DEDUP_HASHES = int(os.environ.get("MAX_DEDUP_HASHES", "500"))

STATE_FILE = Path(__file__).parent / "last_ids.json"

CLASSIFY_PROMPT = """You are a content curator for a Telegram channel.
You will be given the text/caption of a message from another Telegram
channel (it may also have a photo or video attached, which you can't see,
but the text below is all of the written content).

Decide which single category best fits this message:
- "war": war / military conflict / geopolitical crisis news
- "funny": funny or amusing everyday event, meme-worthy story
- "random": random/miscellaneous notable daily event, not war/funny/strange
- "important": an important event worth knowing about (non-war)
- "strange": a strange, bizarre, or surprising event
- "none": doesn't fit any of the above, irrelevant

If it fits one of the five categories above, also decide whether to COPY
it as-is (it's already clear, well-written, and concise) or REWRITE it
(condense, clean up, fix formatting, translate if needed) into a short,
punchy caption. Do NOT mention or link back to any source channel in the
output text. If the input text is empty (e.g. a photo/video with no
caption), it's still fine to mark it relevant if you have no reason to
think otherwise — just return an empty "text".

Respond with ONLY valid JSON, no markdown fences, no extra text, in this
exact shape:
{"relevant": true/false, "category": "war"|"funny"|"random"|"important"|"strange"|"none", "action": "copy"|"rewrite", "text": "final ready-to-post caption, or empty string"}

Message:
---
{MESSAGE}
---
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


def content_hash(text: str) -> str:
    normalized = " ".join(text.strip().lower().split())
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def classify_with_gemini(message_text: str) -> dict:
    """Try each configured Gemini key in round-robin order; if a key is
    rate-limited (429), fall back to the next one. Returns a safe default
    (not relevant) if every key fails."""
    global _key_cursor

    prompt = CLASSIFY_PROMPT.replace("{MESSAGE}", (message_text or "")[:4000])
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.4},
    }

    n_keys = len(GEMINI_API_KEYS)
    for attempt in range(n_keys):
        key = GEMINI_API_KEYS[(_key_cursor + attempt) % n_keys]
        url = (
            f"https://generativelanguage.googleapis.com/v1beta/models/"
            f"{GEMINI_MODEL}:generateContent?key={key}"
        )
        try:
            resp = requests.post(url, json=payload, timeout=60)
        except requests.RequestException as e:
            print(f"[WARN] Gemini request error on key #{attempt}: {e}")
            continue

        if resp.status_code == 429:
            print(f"[WARN] Gemini key #{(_key_cursor + attempt) % n_keys} rate-limited, "
                  f"trying next key")
            continue

        if not resp.ok:
            print(f"[WARN] Gemini call failed: {resp.status_code} {resp.text[:200]}")
            continue

        # Success — advance the cursor so the next call starts from the
        # next key (spreads load evenly across keys).
        _key_cursor = (_key_cursor + attempt + 1) % n_keys

        data = resp.json()
        try:
            raw = data["candidates"][0]["content"]["parts"][0]["text"]
        except (KeyError, IndexError):
            return {"relevant": False, "category": "none", "action": "copy", "text": ""}

        raw = raw.strip()
        if raw.startswith("```"):
            raw = raw.strip("`")
            raw = raw.replace("json\n", "", 1).replace("json", "", 1)

        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return {"relevant": False, "category": "none", "action": "copy", "text": ""}

    print("[WARN] All Gemini keys failed/rate-limited for this message; skipping.")
    return {"relevant": False, "category": "none", "action": "copy", "text": ""}


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
    Telegram asks us to wait (FloodWaitError)."""
    media_list = [m.media for m in group if m.media]

    try:
        if media_list:
            await client.send_file(
                TARGET_CHANNEL,
                file=media_list if len(media_list) > 1 else media_list[0],
                caption=final_text or "",
            )
        elif final_text.strip():
            await client.send_message(TARGET_CHANNEL, final_text)
    except FloodWaitError as e:
        print(f"[INFO] FloodWait: sleeping {e.seconds}s as Telegram requested")
        await asyncio.sleep(e.seconds + 1)
        # one retry after waiting
        if media_list:
            await client.send_file(
                TARGET_CHANNEL,
                file=media_list if len(media_list) > 1 else media_list[0],
                caption=final_text or "",
            )
        elif final_text.strip():
            await client.send_message(TARGET_CHANNEL, final_text)


async def main() -> None:
    if not SOURCE_CHANNELS:
        print("[ERROR] No SOURCE_CHANNELS configured. Set the env var (comma-separated).")
        sys.exit(1)

    state = load_state()
    posted_hashes = state.get("_posted_hashes", [])
    posted_hashes_set = set(posted_hashes)

    client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
    await client.start()

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
        except FloodWaitError as e:
            print(f"[INFO] FloodWait while reading {channel}: sleeping {e.seconds}s")
            await asyncio.sleep(e.seconds + 1)
        except Exception as e:  # noqa: BLE001
            print(f"[WARN] Failed to read {channel}: {e}")
            continue

        # iter_messages returns newest-first; reverse to chronological
        # order, then group albums together.
        raw_messages.reverse()
        groups = group_albums(raw_messages)

        for group in groups:
            # Use the first non-empty caption/text found in the group.
            source_text = next((m.text for m in group if m.text), "") or ""

            try:
                result = classify_with_gemini(source_text)
            except Exception as e:  # noqa: BLE001
                print(f"[WARN] Gemini call failed for a message in {channel}: {e}")
                continue
            finally:
                await asyncio.sleep(GEMINI_CALL_DELAY_SECONDS)

            if not result.get("relevant"):
                continue

            final_text = result.get("text") or source_text

            # Duplicate-content check (skip very short/empty text, not
            # useful for dedup and would collide too easily).
            if final_text and len(final_text.strip()) > 15:
                h = content_hash(final_text)
                if h in posted_hashes_set:
                    print(f"[SKIP] Duplicate content from {channel}, already posted")
                    continue
                posted_hashes_set.add(h)
                posted_hashes.append(h)

            try:
                await post_group(client, group, final_text)
            except Exception as e:  # noqa: BLE001
                print(f"[WARN] Failed to post a message from {channel}: {e}")
                continue

            print(f"[OK] Posted ({result.get('category')}/{result.get('action')}) "
                  f"from {channel}")

        state[channel] = newest_seen

    # Cap the dedup history so the state file doesn't grow forever.
    if len(posted_hashes) > MAX_DEDUP_HASHES:
        posted_hashes = posted_hashes[-MAX_DEDUP_HASHES:]
    state["_posted_hashes"] = posted_hashes

    await client.disconnect()
    save_state(state)


if __name__ == "__main__":
    asyncio.run(main())
