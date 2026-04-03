#!/usr/bin/env python3
"""
Andrea Daily Leads Job
──────────────────────
Schedule : Mon–Fri 08:00 CST
           Monday pulls Sat + Sun + Mon-minus-1  (3 days back)
           Tue–Fri pulls the previous calendar day only

Channels : 6 feed-* channels  →  messages assigned to each rep
           feed-website-visitors  →  assigned to rep; contacts live in threads
           feed-outbound-signals  →  ALL posts, sent to all reps

Output   : Per-rep CSV DM'd to each rep. Andrea is @mentioned in her summary.

Required env var:
    SLACK_BOT_TOKEN   – bot token with the scopes listed in README
"""

import argparse
import csv
import io
import json
import os
import re
import sys
from datetime import datetime, timedelta
from typing import Optional, Tuple
from zoneinfo import ZoneInfo

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# ── Config ────────────────────────────────────────────────────────────────────

SLACK_BOT_TOKEN = os.environ["SLACK_BOT_TOKEN"]
CST             = ZoneInfo("America/Chicago")

REPS = [
    {"name": "Andrea Braly",    "id": "U09BU8CHL2W", "mention": True},
    {"name": "Daniel Mendez",   "id": "U0A0E2971HV", "mention": True},
    {"name": "Melissa Houston", "id": "U094BN3GBCG", "mention": True},
]

CHANNELS = [
    {"id": "C0ACUBVBNAZ", "name": "feed-hiring-alerts",               "filter": "andrea", "threads": False},
    {"id": "C0AC2QKCZMJ", "name": "feed-job-postings",                "filter": "andrea", "threads": False},
    {"id": "C0A11N5NQ3D", "name": "feed-funding-alerts",              "filter": "andrea", "threads": False},
    {"id": "C0ADXCQ6CBC", "name": "feed-mergers-acquisitions-alerts", "filter": "andrea", "threads": False},
    {"id": "C0AE86C0JUU", "name": "feed-ipo-alerts",                  "filter": "andrea", "threads": False},
    {"id": "C0A1M3T271P", "name": "feed-website-visitors",            "filter": "andrea", "threads": True},
    {"id": "C070FHEKWSV", "name": "feed-outbound-signals",            "filter": "all",    "threads": False},
]

HEADERS = [
    "First Name", "Last Name", "Title", "Company", "Industry",
    "Employees",  "Email",     "Phone", "LinkedIn", "Source", "Date",
]

# Ordered from most to least senior — used only for the DM summary
SENIORITY_TIERS = [
    ("c-suite",    ["ceo", "cto", "cfo", "coo", "cpo", "ciso", "chief"]),
    ("vp",         ["vp ", "vice president", "svp", "evp"]),
    ("director",   ["director", "dir "]),
    ("manager",    ["manager", "mgr"]),
    ("individual", []),
]

# ── Date helpers ──────────────────────────────────────────────────────────────

def get_date_range(start: Optional[str] = None, end: Optional[str] = None) -> Tuple[float, float]:
    """
    Returns (oldest_ts, latest_ts) in Unix time.

    If --start / --end are supplied (YYYY-MM-DD), uses those dates.
    Otherwise falls back to the normal daily schedule:
      Monday  → 3 days back (covers Sat + Sun)
      Tue–Fri → 1 day back
    latest is always the END date at 23:59:59 CST when explicit dates are given,
    or today 00:00 CST for the scheduled run.
    """
    if start:
        oldest = datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=CST)
        if end:
            latest = datetime.strptime(end, "%Y-%m-%d").replace(
                hour=23, minute=59, second=59, tzinfo=CST
            )
        else:
            latest = datetime.now(CST)
        return oldest.timestamp(), latest.timestamp()

    today = datetime.now(CST).replace(hour=0, minute=0, second=0, microsecond=0)
    days_back = 3 if today.weekday() == 0 else 1
    oldest = today - timedelta(days=days_back)
    return oldest.timestamp(), today.timestamp()


def ts_to_date(ts) -> str:
    return datetime.fromtimestamp(float(ts), tz=CST).strftime("%Y-%m-%d")


# ── Slack pagination helpers ──────────────────────────────────────────────────

def fetch_history(client: WebClient, channel_id: str, oldest: float, latest: float):
    msgs = []
    cursor = None
    while True:
        kwargs: dict = dict(
            channel=channel_id,
            oldest=str(oldest),
            latest=str(latest),
            limit=200,
            inclusive=False,
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


def fetch_replies(client: WebClient, channel_id: str, ts: str):
    """Returns thread replies (parent message excluded)."""
    replies = []
    cursor = None
    first_page = True
    while True:
        kwargs: dict = dict(channel=channel_id, ts=ts, limit=200)
        if cursor:
            kwargs["cursor"] = cursor
        try:
            resp = client.conversations_replies(**kwargs)
        except SlackApiError:
            return []
        msgs = resp.get("messages") or []
        # First page includes the parent at index 0 — skip it
        replies.extend(msgs[1:] if first_page else msgs)
        first_page = False
        if not resp.get("has_more"):
            break
        cursor = (resp.get("response_metadata") or {}).get("next_cursor")
    return replies


# ── Block-kit text extractor ──────────────────────────────────────────────────

def blocks_to_text(blocks: list) -> str:
    """Recursively flatten Slack Block Kit into a single string."""
    parts = []
    for b in blocks:
        if not isinstance(b, dict):
            continue
        btype = b.get("type", "")
        if btype in ("section", "context", "rich_text", "rich_text_section",
                     "rich_text_list", "rich_text_preformatted"):
            text_obj = b.get("text")
            if isinstance(text_obj, dict):
                parts.append(text_obj.get("text", ""))
            parts.append(blocks_to_text(b.get("fields") or []))
            parts.append(blocks_to_text(b.get("elements") or []))
        elif btype == "header":
            text_obj = b.get("text")
            if isinstance(text_obj, dict):
                parts.append(text_obj.get("text", ""))
        elif btype in ("plain_text", "mrkdwn"):
            parts.append(b.get("text", ""))
        elif btype == "text":
            parts.append(b.get("text", ""))
        elif btype == "link":
            parts.append(b.get("url", ""))
    return "\n".join(p for p in parts if p)


def full_text(msg: dict) -> str:
    return (msg.get("text") or "") + "\n" + blocks_to_text(msg.get("blocks") or [])


# ── Name extraction ───────────────────────────────────────────────────────────

def extract_name(msg: dict) -> str:
    """
    Attempts to find the lead's full name via three heuristics (in order):
    1. A 'header' block — short, no @ or URL
    2. A *bold* run in mrkdwn that looks like "First Last"
    3. An explicit "Name: …" or "Contact: …" field
    """
    for block in (msg.get("blocks") or []):
        if isinstance(block, dict) and block.get("type") == "header":
            t = (block.get("text") or {}).get("text", "").strip()
            if t and len(t.split()) <= 5 and "@" not in t and "http" not in t:
                return t

    text = full_text(msg)

    m = re.search(r"\*([A-Z][a-z]+(?:\s[A-Z][a-z]+){1,2})\*", text)
    if m:
        return m.group(1)

    m = re.search(r"(?i)(?:name|contact)[:\s]+([A-Z][a-z]+ [A-Z][a-z]+)", text)
    if m:
        return m.group(1)

    return ""


# ── Field extraction ──────────────────────────────────────────────────────────

FIELD_PATTERNS = {
    "Title":     r"(?i)(?:title|role|position)[:\s]+([^\n|•*]+)",
    "Company":   r"(?i)(?:company|org(?:anization)?|employer)[:\s]+([^\n|•*]+)",
    "Industry":  r"(?i)industry[:\s]+([^\n|•*]+)",
    "Employees": r"(?i)(?:employees?|headcount|team\s+size|size)[:\s]+([^\n|•*]+)",
    "Email":     r"[\w.+%-]+@[\w-]+\.[a-zA-Z]{2,}",
    "Phone":     r"(?:\+?1[\s.\-]?)?\(?\d{3}\)?[\s.\-]\d{3}[\s.\-]\d{4}",
    "LinkedIn":  r"(?:https?://)?(?:www\.)?linkedin\.com/in/[\w%-]+",
}


def extract_fields(text: str) -> dict:
    result = {}
    for field, pattern in FIELD_PATTERNS.items():
        m = re.search(pattern, text)
        if m:
            val = (m.group(1) if m.lastindex else m.group(0)).strip().rstrip(",;|•*")
            result[field] = val
    return result


# ── Assignment filter ─────────────────────────────────────────────────────────

def is_assigned_to(client: WebClient, channel_id: str, msg: dict, user_id: str) -> bool:
    """
    A lead is assigned to a rep when their Slack user ID appears
    in the message itself OR in any thread reply under that message.
    """
    mention = f"<@{user_id}>"
    if mention in full_text(msg):
        return True
    if (msg.get("reply_count") or 0) > 0:
        for reply in fetch_replies(client, channel_id, msg["ts"]):
            if mention in full_text(reply):
                return True
    return False


# ── Lead assembly ─────────────────────────────────────────────────────────────

def build_lead(msg: dict, source: str, extra_text: str = "") -> dict:
    lead = {h: "" for h in HEADERS}
    lead["Source"] = source
    lead["Date"]   = ts_to_date(msg["ts"])

    name = extract_name(msg)
    if name:
        parts = name.split(" ", 1)
        lead["First Name"] = parts[0]
        lead["Last Name"]  = parts[1] if len(parts) > 1 else ""

    text = full_text(msg) + ("\n" + extra_text if extra_text else "")
    lead.update(extract_fields(text))
    return lead


def is_empty(lead: dict) -> bool:
    return not any(lead.get(f) for f in ("First Name", "Company", "Email", "LinkedIn"))


# ── Per-channel logic ─────────────────────────────────────────────────────────

def process_channel(client: WebClient, ch: dict, oldest: float, latest: float, user_id: str):
    messages = fetch_history(client, ch["id"], oldest, latest)
    source   = ch["name"]
    leads = []

    for msg in messages:
        if msg.get("subtype"):          # skip app joins, message_changed, etc.
            continue

        if ch["filter"] == "andrea" and not is_assigned_to(client, ch["id"], msg, user_id):
            continue

        if ch["threads"]:
            # feed-website-visitors: parent = company visit notice
            #                        thread replies = individual contacts
            parent_text = full_text(msg)
            company_m   = re.search(r"(?i)(?:company|org(?:anization)?)[:\s]+([^\n|•*]+)", parent_text)
            company     = company_m.group(1).strip() if company_m else ""

            for reply in fetch_replies(client, ch["id"], msg["ts"]):
                lead = build_lead(reply, source, extra_text=parent_text)
                if not lead["Company"] and company:
                    lead["Company"] = company
                if not is_empty(lead):
                    leads.append(lead)
        else:
            lead = build_lead(msg, source)
            if not is_empty(lead):
                leads.append(lead)

    return leads


# ── CSV builder ───────────────────────────────────────────────────────────────

def build_csv(leads) -> str:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=HEADERS, extrasaction="ignore", lineterminator="\r\n")
    writer.writeheader()
    writer.writerows(leads)
    return buf.getvalue()


# ── Summary message ───────────────────────────────────────────────────────────

def classify_seniority(title: str) -> str:
    t = title.lower()
    for tier, keywords in SENIORITY_TIERS:
        if any(kw in t for kw in keywords):
            return tier
    return "individual"


def build_summary(leads, oldest: float, latest: float, rep: dict) -> str:
    fmt = "%b %-d"
    start = datetime.fromtimestamp(oldest, tz=CST).strftime(fmt)
    end   = datetime.fromtimestamp(latest - 1, tz=CST).strftime(fmt)
    date_range = start if start == end else f"{start}–{end}"

    by_source = {}
    by_seniority = {}
    for lead in leads:
        by_source[lead["Source"]] = by_source.get(lead["Source"], 0) + 1
        tier = classify_seniority(lead.get("Title", ""))
        by_seniority[tier] = by_seniority.get(tier, 0) + 1

    tier_order = [t for t, _ in SENIORITY_TIERS]
    src_lines = "\n".join(f"  • {s}: {n}" for s, n in sorted(by_source.items()))
    sen_lines = "\n".join(
        f"  • {tier}: {by_seniority[tier]}"
        for tier in tier_order
        if tier in by_seniority
    )

    # @mention the rep in their own summary if configured
    greeting = f"<@{rep['id']}> " if rep.get("mention") else ""

    return (
        f":wave: {greeting}Good morning, {rep['name'].split()[0]}! Here are your leads for {date_range}.\n\n"
        f"*{len(leads)} total leads*\n\n"
        f"*By source:*\n{src_lines}\n\n"
        f"*By seniority:*\n{sen_lines}"
    )


# ── Main ──────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Andrea daily leads job")
    p.add_argument("--start", metavar="YYYY-MM-DD", help="Inclusive start date (overrides schedule)")
    p.add_argument("--end",   metavar="YYYY-MM-DD", help="Inclusive end date   (overrides schedule)")
    return p.parse_args()


def write_input_json(args, oldest: float, latest: float) -> None:
    data = {
        "run_at":     datetime.now(CST).isoformat(),
        "start_date": args.start or datetime.fromtimestamp(oldest, tz=CST).strftime("%Y-%m-%d"),
        "end_date":   args.end   or datetime.fromtimestamp(latest - 1, tz=CST).strftime("%Y-%m-%d"),
        "reps":       [r["name"] for r in REPS],
        "channels":   [ch["name"] for ch in CHANNELS],
    }
    with open("input.json", "w") as f:
        json.dump(data, f, indent=2)


def write_output_json(results: list, oldest: float, latest: float) -> None:
    data = {
        "run_at":      datetime.now(CST).isoformat(),
        "date_range": {
            "start": datetime.fromtimestamp(oldest, tz=CST).strftime("%Y-%m-%d"),
            "end":   datetime.fromtimestamp(latest - 1, tz=CST).strftime("%Y-%m-%d"),
        },
        "total_leads": sum(r["lead_count"] for r in results),
        "by_rep": results,
    }
    with open("output.json", "w") as f:
        json.dump(data, f, indent=2)


def main() -> None:
    args           = parse_args()
    client         = WebClient(token=SLACK_BOT_TOKEN)
    oldest, latest = get_date_range(args.start, args.end)
    now_str        = datetime.now(CST).strftime("%Y-%m-%d")

    write_input_json(args, oldest, latest)

    results = []
    for rep in REPS:
        print(f"\n── {rep['name']} ──", file=sys.stderr)
        rep_leads = []
        by_channel = {}
        for ch in CHANNELS:
            print(f"  Processing #{ch['name']} …", file=sys.stderr)
            leads = process_channel(client, ch, oldest, latest, rep["id"])
            print(f"    → {len(leads)} lead(s)", file=sys.stderr)
            rep_leads.extend(leads)
            by_channel[ch["name"]] = len(leads)

        first = rep["name"].split()[0].lower()
        filename = f"{first}_leads_{now_str}.csv"
        summary  = build_summary(rep_leads, oldest, latest, rep)
        csv_text = build_csv(rep_leads)

        # Open DM (creates it if it doesn't exist)
        dm    = client.conversations_open(users=[rep["id"]])
        dm_id = dm["channel"]["id"]

        client.chat_postMessage(channel=dm_id, text=summary)
        client.files_upload_v2(
            channel=dm_id,
            content=csv_text,
            filename=filename,
            title=f"Leads {now_str}",
        )
        print(f"  Sent {len(rep_leads)} lead(s) → {filename}", file=sys.stderr)
        results.append({"rep": rep["name"], "lead_count": len(rep_leads), "by_channel": by_channel})

    write_output_json(results, oldest, latest)


if __name__ == "__main__":
    main()
