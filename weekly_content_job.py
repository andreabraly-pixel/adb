#!/usr/bin/env python3
"""
Weekly LinkedIn Content Job
────────────────────────────
Schedule : Every Friday
           Pulls Mon–Fri signals from all 7 feed channels.

Output   : 3 LinkedIn post drafts in Andrea's voice, DM'd to Andrea.

Required env vars:
    SLACK_BOT_TOKEN    – bot token
    ANTHROPIC_API_KEY  – for post generation
"""

import argparse
import json
import os
import re
import sys
from datetime import datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

import anthropic as _anthropic_mod
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# ── Config ────────────────────────────────────────────────────────────────────

SLACK_BOT_TOKEN   = os.environ["SLACK_BOT_TOKEN"]
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
CST               = ZoneInfo("America/Chicago")
ANDREA_USER_ID    = "U09BU8CHL2W"
INPUT_FILE        = "weekly_content_input.json"
OUTPUT_FILE       = "weekly_content_output.json"

CHANNELS = [
    {"id": "C0ACUBVBNAZ", "name": "feed-hiring-alerts",               "label": "new hires"},
    {"id": "C0AC2QKCZMJ", "name": "feed-job-postings",                "label": "job postings"},
    {"id": "C0A11N5NQ3D", "name": "feed-funding-alerts",              "label": "funding rounds"},
    {"id": "C0ADXCQ6CBC", "name": "feed-mergers-acquisitions-alerts", "label": "M&A activity"},
    {"id": "C0AE86C0JUU", "name": "feed-ipo-alerts",                  "label": "IPOs"},
    {"id": "C0A1M3T271P", "name": "feed-website-visitors",            "label": "website visitors"},
    {"id": "C070FHEKWSV", "name": "feed-outbound-signals",            "label": "outbound signals"},
]

# ── Andrea's voice examples ───────────────────────────────────────────────────

_VOICE_EXAMPLES = """\
Post 1 (human connection over AI):
the more we rely on technology and AI in 2026, the more we NEED to pause to remember that HUMANS are the core aspect of why we do what we do.

I know we all love *generating shareholder value* and all but the real work is in the conversations NOT recorded by AI notetakers.

meet people, share dinner and drinks, have meaningful conversations, connect authentically.

---

Post 2 (vulnerability + full circle):
Something not a lot of people know is that in 2024, I had a signed offer letter and start date for an analyst position at JPMorganChase.

I ended up turning it down in favor of growth roles that led me into tech and startups.

Full circle moment: now that I work with FP&A leaders and finance executives building better planning and forecasting systems, I'm rebuilding my financial modeling skills from the ground up to better speak that language.

What other resources would you recommend for strengthening modeling skills?

---

Post 3 (honest sales take):
OUTBOUNDING IS SO TOUGH Y'ALL

But every time I feel dumb for reaching out to folks and getting left on read or no reply, I think about this quote.

I believe this is the single most important thing to live by as a salesperson. (also leading with value of course 🙈 )

Happy Friday 💪

---

Post 4 (thought leadership with structure):
Strategic finance is shifting its focus from number-crunching to creating and communicating valuable insights to your stakeholders, investors, and leadership.

These teams aren't just building better models. They're getting everyone aligned on what the numbers actually mean.

Three signs your finance function is truly strategic:
Your leadership team comes to you before making decisions, not after
Non-finance teams can clearly articulate your growth drivers
You spend more time on "what should we do?" than "what happened?"

---

Post 5 (hot take / personal):
HOT TAKE: LATE NIGHT GRIND >>>>> EARLY MORNING GRIND

I will never ever have a 4 or 5am wakeup time. I get on work at 9am like normal.

But something just clicks in my brain after lunch and it grows every passing hour...

Suddenly I'm grinding at work and I can't stop.

Who's with me?

Morning people, tell me I'm dumb and wrong in the comments below 🤪

---

Post 6 (community / real event):
The best conversations I've had to date haven't happened over Zoom.

They happened last night in a wine cellar at Lonesome Dove in Austin, over steak cooked medium rare, with finance leaders who want to think out loud with people who get it.

No agenda, no slides. Just people talking about real issues. How to become more strategic and think critically in the age of AI, how to deliver value everyday, and more.
"""

# ── Date helpers ──────────────────────────────────────────────────────────────

def get_week_range(start: Optional[str] = None, end: Optional[str] = None):
    if start:
        oldest = datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=CST)
        latest = (
            datetime.strptime(end, "%Y-%m-%d").replace(
                hour=23, minute=59, second=59, tzinfo=CST
            )
            if end else datetime.now(CST)
        )
        return oldest.timestamp(), latest.timestamp()
    today  = datetime.now(CST).replace(hour=23, minute=59, second=59, microsecond=0)
    monday = (today - timedelta(days=today.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    return monday.timestamp(), today.timestamp()


# ── Slack helpers ─────────────────────────────────────────────────────────────

def fetch_history(client: WebClient, channel_id: str, oldest: float, latest: float):
    msgs, cursor = [], None
    while True:
        kwargs = dict(
            channel=channel_id, oldest=str(oldest), latest=str(latest),
            limit=200, inclusive=False,
        )
        if cursor:
            kwargs["cursor"] = cursor
        try:
            resp = client.conversations_history(**kwargs)
        except SlackApiError as e:
            print(f"[WARN] history({channel_id}): {e.response['error']}", file=sys.stderr)
            break
        msgs.extend(resp.get("messages") or [])
        if not resp.get("has_more"):
            break
        cursor = (resp.get("response_metadata") or {}).get("next_cursor")
    return msgs


# ── Signal extraction ─────────────────────────────────────────────────────────

def _display(raw: str) -> str:
    m = re.match(r"<[^|>]+\|([^>]+)>", raw.strip())
    return m.group(1).strip() if m else raw.strip()


def extract_signals(messages: list, label: str) -> list:
    signals = []
    for msg in messages:
        if msg.get("subtype"):
            continue
        text = msg.get("text") or ""
        if not text:
            continue

        company_m  = re.search(r"\*Company:\s*(<[^>]+>|[^\n*]+)\*?", text)
        org_m      = re.search(r"\*Organization\*:\s*(<[^>]+>|[^\n]+)", text)
        name_m     = re.search(r"-\s*Name:\s*(<[^>]+>|[^\n]+)", text)
        contact_m  = re.search(r"\*Contact\*:\s*(<[^>]+>|[^\n]+)", text)
        title_m    = re.search(r"-\s*Title:\s*([^\n]+)", text)
        role_m     = re.search(r"\*Role\*:\s*([^\n]+)", text)
        headline_m = re.match(r"^\*([^*\n]{10,120})\*", text.strip())

        company = _display(company_m.group(1)) if company_m else (
                  _display(org_m.group(1))     if org_m     else "")
        name    = _display(name_m.group(1))    if name_m    else (
                  _display(contact_m.group(1)) if contact_m else "")
        title   = title_m.group(1).strip()     if title_m   else (
                  role_m.group(1).strip()      if role_m    else "")
        headline = headline_m.group(1).strip() if headline_m else ""

        parts = []
        if headline and not name:
            parts.append(headline)
        if name:
            parts.append(name)
        if title:
            parts.append(title)
        if company:
            parts.append(f"@ {company}")

        if parts:
            signals.append(f"[{label}] " + ", ".join(parts))

    return signals


# ── Content generation ────────────────────────────────────────────────────────

_SYSTEM_PROMPT = (
    "You are a ghostwriter for Andrea Braly, an SDR at a fintech startup that sells FP&A software "
    "to finance leaders and CFOs.\n\n"
    "Andrea's voice characteristics:\n"
    "- Casual and conversational, NOT corporate or polished\n"
    "- Uses lowercase intentionally for emphasis\n"
    "- Short punchy paragraphs with lots of white space between them\n"
    "- Honest and self-aware about the hard parts of sales and outbound\n"
    "- Ends posts with a genuine question or CTA to spark comments\n"
    "- Emojis used sparingly and naturally (1-3 per post max)\n"
    "- Humor is self-deprecating and relatable, never forced\n"
    "- Core audience: FP&A leaders, finance directors, CFOs at growth-stage companies\n"
    "- Mixes personal moments with professional insight\n"
    "- NEVER sounds like a product pitch or marketing copy\n"
    "- Does NOT name her company or product — she can add that herself\n\n"
    "Voice examples from her actual posts:\n\n"
    + _VOICE_EXAMPLES
    + "\n\nGiven a list of market signals from the week (new hires, funding rounds, M&A, etc.), "
    "generate exactly 3 distinct LinkedIn post drafts:\n\n"
    "1. OBSERVATION — a pattern noticed across this week's signals tied to a broader finance/sales insight\n"
    "2. HOT TAKE — a contrarian or honest opinion inspired by something in the data\n"
    "3. PERSONAL STORY — a first-person moment or realization tied to what she's seeing\n\n"
    "Return ONLY valid JSON in this exact format, nothing else:\n"
    '{"posts": ['
    '{"type": "observation", "draft": "..."},'
    '{"type": "hot_take", "draft": "..."},'
    '{"type": "personal_story", "draft": "..."}'
    "]}"
)


def generate_posts(signals: list, date_range: str) -> list:
    if not signals:
        print("[WARN] No signals — skipping generation", file=sys.stderr)
        return []

    client   = _anthropic_mod.Anthropic(api_key=ANTHROPIC_API_KEY)
    sample   = signals[:60]
    user_msg = (
        f"Week of {date_range}. Market signals this week:\n\n"
        + "\n".join(sample)
        + "\n\nGenerate 3 LinkedIn post drafts."
    )

    resp = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=2000,
        system=[{"type": "text", "text": _SYSTEM_PROMPT, "cache_control": {"type": "ephemeral"}}],
        messages=[{"role": "user", "content": user_msg}],
    )

    raw = resp.content[0].text.strip()
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)

    return json.loads(raw).get("posts", [])


# ── Slack delivery ────────────────────────────────────────────────────────────

_POST_LABELS = {
    "observation":    "📊 Observation",
    "hot_take":       "🔥 Hot Take",
    "personal_story": "💬 Personal Story",
}


def format_message(posts: list, date_range: str) -> str:
    parts = [f":pencil: *Your LinkedIn post ideas — week of {date_range}*\n"]
    for post in posts:
        label = _POST_LABELS.get(post.get("type", ""), "Post")
        draft = post.get("draft", "").strip()
        parts.append(f"*{label}*\n```\n{draft}\n```")
    parts.append("_Edit and post whichever resonates. These are drafts — make them yours._ ✌️")
    return "\n\n".join(parts)


# ── input.json / output.json ──────────────────────────────────────────────────

def write_input_json(args, oldest: float, latest: float) -> None:
    with open(INPUT_FILE, "w") as f:
        json.dump({
            "run_at":     datetime.now(CST).isoformat(),
            "start_date": args.start or datetime.fromtimestamp(oldest, tz=CST).strftime("%Y-%m-%d"),
            "end_date":   args.end   or datetime.fromtimestamp(latest - 1, tz=CST).strftime("%Y-%m-%d"),
            "dry_run":    args.dry_run,
            "channels":   [ch["name"] for ch in CHANNELS],
        }, f, indent=2)


def write_output_json(signal_counts: dict, posts: list, oldest: float, latest: float) -> None:
    with open(OUTPUT_FILE, "w") as f:
        json.dump({
            "run_at": datetime.now(CST).isoformat(),
            "date_range": {
                "start": datetime.fromtimestamp(oldest, tz=CST).strftime("%Y-%m-%d"),
                "end":   datetime.fromtimestamp(latest - 1, tz=CST).strftime("%Y-%m-%d"),
            },
            "signals_by_channel": signal_counts,
            "total_signals":      sum(signal_counts.values()),
            "posts_generated":    len(posts),
            "posts": [
                {"type": p.get("type"), "preview": (p.get("draft") or "")[:100] + "…"}
                for p in posts
            ],
        }, f, indent=2)


# ── Main ──────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="Weekly LinkedIn content generator for Andrea")
    p.add_argument("--start",   metavar="YYYY-MM-DD")
    p.add_argument("--end",     metavar="YYYY-MM-DD")
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


def main():
    args           = parse_args()
    client         = WebClient(token=SLACK_BOT_TOKEN)
    oldest, latest = get_week_range(args.start, args.end)

    start_str  = datetime.fromtimestamp(oldest, tz=CST).strftime("%b %-d")
    end_str    = datetime.fromtimestamp(latest, tz=CST).strftime("%b %-d")
    date_range = f"{start_str}–{end_str}"

    write_input_json(args, oldest, latest)

    print(f"Pulling signals for {date_range}…", file=sys.stderr)
    all_signals, signal_counts = [], {}
    for ch in CHANNELS:
        print(f"  #{ch['name']} …", file=sys.stderr)
        msgs  = fetch_history(client, ch["id"], oldest, latest)
        sigs  = extract_signals(msgs, ch["label"])
        signal_counts[ch["name"]] = len(sigs)
        all_signals.extend(sigs)
        print(f"    → {len(sigs)} signal(s)", file=sys.stderr)

    print(f"\n{len(all_signals)} total signals. Generating posts…", file=sys.stderr)
    posts = generate_posts(all_signals, date_range)
    print(f"  → {len(posts)} post(s) generated", file=sys.stderr)

    message = format_message(posts, date_range)

    if args.dry_run:
        print("\n[dry-run] Message preview:\n", file=sys.stderr)
        print(message, file=sys.stderr)
    else:
        dm    = client.conversations_open(users=[ANDREA_USER_ID])
        dm_id = dm["channel"]["id"]
        client.chat_postMessage(channel=dm_id, text=message)
        print("DM sent to Andrea.", file=sys.stderr)

    write_output_json(signal_counts, posts, oldest, latest)


if __name__ == "__main__":
    main()
