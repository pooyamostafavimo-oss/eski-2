"""
Urgent Quick-Scan
------------------
A lightweight companion to curator_bot.py, meant to run on a much shorter
cron than the main run (e.g. every 10-15 minutes instead of hourly), so
breaking/urgent news doesn't sit waiting for the next full run.

Design goals:
- Zero API cost for the vast majority of messages. Before spending a
  single Gemini call, every new message is checked against a cheap local
  keyword list (URGENT_KEYWORDS). Only a match gets classified at all.
- No duplicate work, no quality loss. A matched message goes through the
  exact same classify -> dedup -> post pipeline as the full hourly run
  (process_message_group, imported directly from curator_bot.py) — this
  script never re-implements or approximates that logic.
- Never interferes with the main run's state. It keeps its own separate
  per-channel bookmark (_urgent_checked_id) for "already glanced at by
  the quick scan", completely independent of the main last-seen-id
  cursor the full hourly run uses (state[channel]). The full run still
  reads and classifies every message from scratch later, regardless of
  what the quick scan already did — nothing is ever skipped by the full
  run because of this script. If this script already posted something,
  the full run's own exact-text-hash layer catches it as a duplicate for
  near-zero extra cost (one classify call, no judge call needed since
  the text matches exactly).

In short: this script can only make urgent news appear sooner. It cannot
cause a story to be missed, and it cannot lower the bar for what gets
posted — the categories, dedup layers, and writing-style rules are all
identical to the full run.

Suggested cron (separate GitHub Actions workflow, or an extra `schedule`
entry pointing at this script): every 10-15 minutes.
"""

import asyncio
import re
import time

from telethon.errors import FloodWaitError

import curator_bot as cb

# Categories worth fast-tracking. Everything else still gets posted
# normally by the next full hourly run — this script simply doesn't
# bother checking for them early.
URGENT_CATEGORIES = {
    c.strip() for c in cb.os.environ.get("URGENT_CATEGORIES", "war,important").split(",")
    if c.strip()
}

# Cheap local pre-filter — no API call. Deliberately broad/recall-biased:
# a false positive here only costs one classify call (which itself can
# still say "not urgent" or "not relevant"); a false negative just means
# the story waits for the next full run instead of being fast-tracked,
# which is the same as if this script didn't exist at all. Customize via
# env var (comma-separated), e.g. to add more languages or topics.
DEFAULT_URGENT_KEYWORDS = [
    # Persian
    "فوری", "حمله", "انفجار", "کشته", "زخمی", "جنگ", "موشک", "بمباران",
    "درگیری", "سقوط", "زلزله", "کودتا", "استعفا", "ترور", "اضطراری",
    "هشدار", "بحران", "محاصره",
    # English
    "breaking", "urgent", "explosion", "attack", "killed", "strike",
    "missile", "earthquake", "war", "crisis", "collapse", "assassinat",
]
URGENT_KEYWORDS = [
    kw.strip().lower()
    for kw in cb.os.environ.get("URGENT_KEYWORDS", "").split(",")
    if kw.strip()
] or [kw.lower() for kw in DEFAULT_URGENT_KEYWORDS]

_URGENT_PATTERN = re.compile(
    "|".join(re.escape(kw) for kw in URGENT_KEYWORDS), re.IGNORECASE
)

# Keep this small — the point is a quick, cheap glance, not a full sweep
# (the full hourly run already covers everything with MAX_MESSAGES_PER_CHANNEL).
QUICK_SCAN_MAX_MESSAGES_PER_CHANNEL = int(
    cb.os.environ.get("QUICK_SCAN_MAX_MESSAGES_PER_CHANNEL", "15")
)


def looks_urgent(text: str) -> bool:
    if not text:
        return False
    return bool(_URGENT_PATTERN.search(text))


async def _run() -> None:
    if not cb.SOURCE_CHANNELS:
        print("[ERROR] No SOURCE_CHANNELS configured.")
        return

    state = cb.load_state()
    posted_hashes = state.get("_posted_hashes", [])
    posted_hashes_set = set(posted_hashes)
    events = cb.prune_events(state.get("_events", []), time.time())
    dead_keys = set(state.get("_dead_gemini_keys", []))
    urgent_checked = state.setdefault("_urgent_checked_id", {})

    client = cb.TelegramClient(cb.StringSession(cb.SESSION_STRING), cb.API_ID, cb.API_HASH)
    await client.start()

    any_posted = False

    for channel in cb.SOURCE_CHANNELS:
        # Never look further back than the main run already has, and
        # never re-glance at a message this script already checked.
        min_id = max(state.get(channel, 0), urgent_checked.get(channel, 0))
        newest_seen = min_id
        raw_messages = []
        try:
            async for msg in client.iter_messages(
                channel, min_id=min_id, limit=QUICK_SCAN_MAX_MESSAGES_PER_CHANNEL
            ):
                if msg.text or msg.media:
                    raw_messages.append(msg)
                    newest_seen = max(newest_seen, msg.id)
        except FloodWaitError as e:
            print(f"[INFO] FloodWait while quick-scanning {channel}: sleeping {e.seconds}s")
            await asyncio.sleep(e.seconds + 1)
            continue
        except Exception as e:  # noqa: BLE001
            print(f"[WARN] Quick-scan couldn't read {channel}: {e}")
            continue

        raw_messages.reverse()
        groups = cb.group_albums(raw_messages)

        for group in groups:
            source_text = next((m.text for m in group if m.text), "") or ""
            if looks_urgent(source_text):
                result = await cb.process_message_group(
                    client, channel, group, state, dead_keys, events,
                    posted_hashes, posted_hashes_set,
                )
                if result is not None and result.get("category") in URGENT_CATEGORIES:
                    any_posted = True
                    print(f"[URGENT] Fast-tracked a {result.get('category')} post from {channel}")

        urgent_checked[channel] = newest_seen

    state["_urgent_checked_id"] = urgent_checked
    state["_posted_hashes"] = posted_hashes[-cb.MAX_DEDUP_HASHES:]
    state["_events"] = cb.prune_events(events, time.time())
    state["_dead_gemini_keys"] = sorted(dead_keys)
    cb.save_state(state)

    await client.disconnect()

    if not any_posted:
        print("[INFO] Quick-scan: nothing urgent found this pass.")


if __name__ == "__main__":
    asyncio.run(_run())
