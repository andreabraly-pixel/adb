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

Output   : Per-rep CSV DM'd to each rep. All reps are @mentioned in their summary.

Idempotency
───────────
Slack message  → chat_update if a prior message_ts is stored in output.json for
                 today's run date; chat_postMessage otherwise.
Slack file     → NOT idempotent (Slack has no file-update API). On rerun the old
                 file is deleted and a new one uploaded. This is flagged in --dry-run.

Flags
─────
--dry-run      Print what would be sent/updated per rep without touching Slack.
--start / --end Override the automatic date range (YYYY-MM-DD).

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
OUTPUT_FILE     = "output.json"

REPS = [
    {"name": "Andrea Braly",    "id": "U09BU8CHL2W", "mention": True},
    {"name": "Daniel Mendez",   "id": "U0A0E2971HV", "mention": True},
    {"name": "Melissa Houston", "id": "U094BN3GBCG", "mention": True},
]

CHANNELS = [
    {"id": "C0ACUBVBNAZ", "name": "feed-hiring-alerts",               "filter": "rep", "threads": False},
    {"id": "C0AC2QKCZMJ", "name": "feed-job-postings",                "filter": "rep", "threads": False},
    {"id": "C0A11N5NQ3D", "name": "feed-funding-alerts",              "filter": "rep", "threads": False},
    {"id": "C0ADXCQ6CBC", "name": "feed-mergers-acquisitions-alerts", "filter": "rep", "threads": False},
    {"id": "C0AE86C0JUU", "name": "feed-ipo-alerts",                  "filter": "rep", "threads": False},
    {"id": "C0A1M3T271P", "name": "feed-website-visitors",            "filter": "rep", "threads": True},
    {"id": "C070FHEKWSV", "name": "feed-outbound-signals",            "filter": "all", "threads": False},
]

HEADERS = [
    "First Name", "Last Name", "Title", "Company", "Industry",
    "Employees",  "Email",     "Phone", "LinkedIn", "Source", "Date",
]

SENIORITY_TIERS = [
    ("c-suite",    ["ceo", "cto", "cfo", "coo", "cpo", "ciso", "chief"]),
    ("vp",         ["vp ", "vice president", "svp", "evp"]),
    ("director",   ["director", "dir "]),
    ("manager",    ["manager", "mgr"]),
    ("individual", []),
]

# ── Date helpers ──────────────────────────────────────────────────────────────

def get_date_range(start: Optional[str] = None, end: Optional[str] = None) -> Tuple[float, float]:
    if start:
        oldest = datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=CST)
        latest = (
            datetime.strptime(end, "%Y-%m-%d").replace(hour=23, minute=59, second=59, tzinfo=CST)
            if end else datetime.now(CST)
        )
        return oldest.timestamp(), latest.timestamp()
    today = datetime.now(CST).replace(hour=0, minute=0, second=0, microsecond=0)
    days_back = 3 if today.weekday() == 0 else 1
    return (today - timedelta(days=days_back)).timestamp(), today.timestamp()


def ts_to_date(ts) -> str:
    return datetime.fromtimestamp(float(ts), tz=CST).strftime("%Y-%m-%d")


# ── Slack pagination helpers ──────────────────────────────────────────────────

def fetch_history(client: WebClient, channel_id: str, oldest: float, latest: float):
    msgs, cursor = [], None
    while True:
        kwargs = dict(channel=channel_id, oldest=str(oldest), latest=str(latest),
                      limit=200, inclusive=False)
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
    replies, cursor, first_page = [], None, True
    while True:
        kwargs = dict(channel=channel_id, ts=ts, limit=200)
        if cursor:
            kwargs["cursor"] = cursor
        try:
            resp = client.conversations_replies(**kwargs)
        except SlackApiError:
            return []
        msgs = resp.get("messages") or []
        replies.extend(msgs[1:] if first_page else msgs)
        first_page = False
        if not resp.get("has_more"):
            break
        cursor = (resp.get("response_metadata") or {}).get("next_cursor")
    return replies


# ── Text / field extraction ───────────────────────────────────────────────────

def blocks_to_text(blocks: list) -> str:
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
        elif btype in ("plain_text", "mrkdwn", "text"):
            parts.append(b.get("text", ""))
        elif btype == "link":
            parts.append(b.get("url", ""))
    return "\n".join(p for p in parts if p)


def full_text(msg: dict) -> str:
    return (msg.get("text") or "") + "\n" + blocks_to_text(msg.get("blocks") or [])


def extract_name(msg: dict) -> str:
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
            result[field] = (m.group(1) if m.lastindex else m.group(0)).strip().rstrip(",;|•*")
    return result


# ── Assignment filter ─────────────────────────────────────────────────────────

def is_assigned_to(client: WebClient, channel_id: str, msg: dict, user_id: str) -> bool:
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
    lead.update(extract_fields(full_text(msg) + ("\n" + extra_text if extra_text else "")))
    return lead


def is_empty(lead: dict) -> bool:
    return not any(lead.get(f) for f in ("First Name", "Company", "Email", "LinkedIn"))


# ── Per-channel processing ────────────────────────────────────────────────────

def process_channel(client: WebClient, ch: dict, oldest: float, latest: float, user_id: str):
    messages = fetch_history(client, ch["id"], oldest, latest)
    leads = []
    for msg in messages:
        if msg.get("subtype"):
            continue
        if ch["filter"] == "rep" and not is_assigned_to(client, ch["id"], msg, user_id):
            continue
        if ch["threads"]:
            parent_text = full_text(msg)
            company_m   = re.search(r"(?i)(?:company|org(?:anization)?)[:\s]+([^\n|•*]+)", parent_text)
            company     = company_m.group(1).strip() if company_m else ""
            for reply in fetch_replies(client, ch["id"], msg["ts"]):
                lead = build_lead(reply, ch["name"], extra_text=parent_text)
                if not lead["Company"] and company:
                    lead["Company"] = company
                if not is_empty(lead):
                    leads.append(lead)
        else:
            lead = build_lead(msg, ch["name"])
            if not is_empty(lead):
                leads.append(lead)
    return leads


# ── CSV / summary builders ────────────────────────────────────────────────────

def build_csv(leads) -> str:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=HEADERS, extrasaction="ignore", lineterminator="\r\n")
    writer.writeheader()
    writer.writerows(leads)
    return buf.getvalue()


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

    by_source, by_seniority = {}, {}
    for lead in leads:
        by_source[lead["Source"]] = by_source.get(lead["Source"], 0) + 1
        tier = classify_seniority(lead.get("Title", ""))
        by_seniority[tier] = by_seniority.get(tier, 0) + 1

    tier_order = [t for t, _ in SENIORITY_TIERS]
    src_lines = "\n".join(f"  • {s}: {n}" for s, n in sorted(by_source.items()))
    sen_lines = "\n".join(
        f"  • {tier}: {by_seniority[tier]}" for tier in tier_order if tier in by_seniority
    )
    greeting = f"<@{rep['id']}> " if rep.get("mention") else ""
    return (
        f":wave: {greeting}Good morning, {rep['name'].split()[0]}! Here are your leads for {date_range}.\n\n"
        f"*{len(leads)} total leads*\n\n"
        f"*By source:*\n{src_lines}\n\n"
        f"*By seniority:*\n{sen_lines}"
    )


# ── Idempotent Slack delivery ─────────────────────────────────────────────────

def load_prior_output() -> dict:
    """Returns today's stored Slack IDs keyed by rep name, or {} if none."""
    if not os.path.exists(OUTPUT_FILE):
        return {}
    try:
        data = json.load(open(OUTPUT_FILE))
        now_str = datetime.now(CST).strftime("%Y-%m-%d")
        # Only reuse IDs from a run that targeted the same calendar date
        if data.get("run_date") == now_str:
            return {r["rep"]: r.get("slack_ids", {}) for r in data.get("by_rep", [])}
    except Exception:
        pass
    return {}


def deliver(client: WebClient, dm_id: str, summary: str, csv_text: str,
            filename: str, prior_ids: dict, dry_run: bool) -> dict:
    """
    Post or update the summary message and upload (replacing) the CSV file.
    Returns {"message_ts": ..., "file_id": ...} for storage in output.json.

    File replacement (delete + re-upload) is the only non-idempotent operation.
    It is clearly flagged in dry-run output.
    """
    prior_msg_ts  = prior_ids.get("message_ts")
    prior_file_id = prior_ids.get("file_id")

    if dry_run:
        action = "UPDATE message" if prior_msg_ts else "POST new message"
        print(f"    [dry-run] {action} → dm={dm_id}")
        if prior_file_id:
            print(f"    [dry-run] DELETE file {prior_file_id} + UPLOAD new {filename}  ⚠ not idempotent")
        else:
            print(f"    [dry-run] UPLOAD {filename}")
        return {}

    # ── Message: update if we have a prior ts, otherwise post ────────────────
    if prior_msg_ts:
        try:
            resp = client.chat_update(channel=dm_id, ts=prior_msg_ts, text=summary)
            message_ts = resp["ts"]
        except SlackApiError:
            # Message may have been deleted manually — fall back to new post
            resp = client.chat_postMessage(channel=dm_id, text=summary)
            message_ts = resp["ts"]
    else:
        resp = client.chat_postMessage(channel=dm_id, text=summary)
        message_ts = resp["ts"]

    # ── File: delete prior + upload new ──────────────────────────────────────
    if prior_file_id:
        try:
            client.files_delete(file=prior_file_id)
        except SlackApiError:
            pass  # already deleted or inaccessible — proceed

    upload = client.files_upload_v2(
        channel=dm_id,
        content=csv_text,
        filename=filename,
        title=filename,
    )
    file_id = (upload.get("files") or [{}])[0].get("id", "")

    return {"message_ts": message_ts, "file_id": file_id}


# ── input.json / output.json ──────────────────────────────────────────────────

def write_input_json(args, oldest: float, latest: float) -> None:
    data = {
        "run_at":     datetime.now(CST).isoformat(),
        "start_date": args.start or datetime.fromtimestamp(oldest, tz=CST).strftime("%Y-%m-%d"),
        "end_date":   args.end   or datetime.fromtimestamp(latest - 1, tz=CST).strftime("%Y-%m-%d"),
        "dry_run":    args.dry_run,
        "reps":       [r["name"] for r in REPS],
        "channels":   [ch["name"] for ch in CHANNELS],
    }
    with open("input.json", "w") as f:
        json.dump(data, f, indent=2)


def write_output_json(results: list, oldest: float, latest: float) -> None:
    data = {
        "run_at":   datetime.now(CST).isoformat(),
        "run_date": datetime.now(CST).strftime("%Y-%m-%d"),
        "date_range": {
            "start": datetime.fromtimestamp(oldest, tz=CST).strftime("%Y-%m-%d"),
            "end":   datetime.fromtimestamp(latest - 1, tz=CST).strftime("%Y-%m-%d"),
        },
        "total_leads": sum(r["lead_count"] for r in results),
        "by_rep": results,
    }
    with open(OUTPUT_FILE, "w") as f:
        json.dump(data, f, indent=2)


# ── Main ──────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Andrea daily leads job")
    p.add_argument("--start",   metavar="YYYY-MM-DD", help="Inclusive start date")
    p.add_argument("--end",     metavar="YYYY-MM-DD", help="Inclusive end date")
    p.add_argument("--dry-run", action="store_true",  help="Preview mutations without sending to Slack")
    return p.parse_args()


def main() -> None:
    args           = parse_args()
    client         = WebClient(token=SLACK_BOT_TOKEN)
    oldest, latest = get_date_range(args.start, args.end)
    now_str        = datetime.now(CST).strftime("%Y-%m-%d")
    prior_output   = load_prior_output()

    write_input_json(args, oldest, latest)

    if args.dry_run:
        print("\n[dry-run] No Slack messages will be sent.\n", file=sys.stderr)

    results = []
    for rep in REPS:
        print(f"\n── {rep['name']} ──", file=sys.stderr)

        # ── Phase 1: read (always runs) ───────────────────────────────────────
        rep_leads, by_channel = [], {}
        for ch in CHANNELS:
            print(f"  Processing #{ch['name']} …", file=sys.stderr)
            leads = process_channel(client, ch, oldest, latest, rep["id"])
            print(f"    → {len(leads)} lead(s)", file=sys.stderr)
            rep_leads.extend(leads)
            by_channel[ch["name"]] = len(leads)

        first    = rep["name"].split()[0].lower()
        filename = f"{first}_leads_{now_str}.csv"
        summary  = build_summary(rep_leads, oldest, latest, rep)
        csv_text = build_csv(rep_leads)

        # ── Phase 2: write (show plan, then execute unless --dry-run) ─────────
        prior_ids = prior_output.get(rep["name"], {})

        # Open DM with rep — works from bot side without user needing to message first.
        # If this fails the workspace admin needs to allow the bot to DM all users at:
        # admin.slack.com → Installed Apps → [bot] → Permissions → Allow DMs
        try:
            dm    = client.conversations_open(users=[rep["id"]])
            dm_id = dm["channel"]["id"]
        except SlackApiError as e:
            print(f"  [ERROR] Cannot open DM with {rep['name']}: {e.response['error']}", file=sys.stderr)
            print(f"  [ERROR] Fix: admin.slack.com → Installed Apps → your bot → Permissions → Allow DMs to all users", file=sys.stderr)
            results.append({"rep": rep["name"], "lead_count": len(rep_leads), "by_channel": by_channel, "slack_ids": {}, "error": e.response["error"]})
            continue

        print(f"  Mutations for {rep['name']}:", file=sys.stderr)
        slack_ids = deliver(client, dm_id, summary, csv_text, filename, prior_ids, args.dry_run)

        result = {
            "rep":        rep["name"],
            "lead_count": len(rep_leads),
            "by_channel": by_channel,
            "slack_ids":  slack_ids,
        }
        results.append(result)
        print(f"  {'[dry-run] ' if args.dry_run else ''}Done — {len(rep_leads)} lead(s)", file=sys.stderr)

    write_output_json(results, oldest, latest)


if __name__ == "__main__":
    main()
