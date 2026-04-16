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

Required env vars:
    SLACK_BOT_TOKEN    – bot token with the scopes listed in README
    ANTHROPIC_API_KEY  – (optional) enables AI-generated outreach hooks per lead
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

import anthropic

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# ── Config ────────────────────────────────────────────────────────────────────

SLACK_BOT_TOKEN   = os.environ["SLACK_BOT_TOKEN"]
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")  # optional
CST               = ZoneInfo("America/Chicago")
OUTPUT_FILE       = "output.json"

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
    "Employees",  "Email",     "Phone", "LinkedIn", "Source", "Date", "Hook",
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


def _link_display(slack_link: str) -> str:
    """Extract the display text from a Slack <url|display> token, or return raw."""
    m = re.match(r"<[^|>]+\|([^>]+)>", slack_link.strip())
    return m.group(1).strip() if m else slack_link.strip()


def _link_url(slack_link: str) -> str:
    """Extract the URL from a Slack <url|display> or <url> token."""
    m = re.match(r"<([^|>]+)(?:\|[^>]*)?>", slack_link.strip())
    return m.group(1).strip() if m else slack_link.strip()


def _field_value(text: str, label: str) -> str:
    """
    Return the value after a labelled field line.
    Handles both:
      - Name: <url|Display>   →  Display text
      - Name: plain text
    Strips surrounding Slack bold markers (*...*), leading dashes, and blockquote (>).
    """
    # match "- Label: value" or "> *Label*: value" or "*Label*: value"
    pattern = r"(?im)^[>\s\-]*\*?" + re.escape(label) + r"\*?:\s*(.+)$"
    m = re.search(pattern, text)
    if not m:
        return ""
    raw = m.group(1).strip().rstrip("*")
    # If the value looks like a Slack link token, extract the display text
    if raw.startswith("<"):
        return _link_display(raw)
    return raw


def _linkedin_url(text: str) -> str:
    """Extract a LinkedIn profile URL from any <url|...> or bare URL in the text."""
    # Prefer Slack link tokens that contain linkedin.com/in/
    m = re.search(r"<(https?://(?:www\.)?linkedin\.com/in/[^|>]+)(?:\|[^>]*)?>", text)
    if m:
        return m.group(1)
    # Bare URL fallback
    m = re.search(r"https?://(?:www\.)?linkedin\.com/in/[\w%-]+", text)
    return m.group(0) if m else ""


def _email(text: str) -> str:
    """Extract email address from <mailto:addr|addr> or bare address."""
    m = re.search(r"<mailto:([^|>]+)(?:\|[^>]*)?>", text)
    if m:
        return m.group(1)
    m = re.search(r"[\w.+%-]+@[\w-]+\.[a-zA-Z]{2,}", text)
    return m.group(0) if m else ""


def _phone(text: str) -> str:
    """Extract phone number from <tel:+1...|display> or bare number."""
    m = re.search(r"<tel:[^|>]+\|([^>]+)>", text)
    if m:
        return m.group(1).strip()
    m = re.search(r"(?:\+?1[\s.\-]?)?\(?\d{3}\)?[\s.\-]\d{3}[\s.\-]\d{4}", text)
    return m.group(0) if m else ""


# ── Channel-specific parsers ──────────────────────────────────────────────────

def parse_hiring_alert(text: str) -> dict:
    """
    Parses messages from feed-hiring-alerts, feed-job-postings,
    feed-funding-alerts, feed-mergers-acquisitions-alerts, feed-ipo-alerts.

    Expected format:
        *NEW HIRE: Full Name joined as Title*
        *Person Details*
        - Name: <linkedin_url|Full Name>
        - Title: Finance Director
        *Company: <url|Company Name>*
        - Industry: government administration
        - Size: 220 employees
        *Contact Information*
        - Email: <mailto:addr|addr>
        - Phone: <tel:+1...|+1 (617) 281-3368>
        :dart: *Assigned to:* <@USER_ID|Name>
    """
    result = {}

    # Name — from "- Name: <url|Full Name>" line
    name_raw = _field_value(text, "Name")
    if not name_raw:
        # fallback: headline "joined as" pattern
        m = re.search(r"(?:^|\n)\*[^:*\n]+:\s+([A-Z][a-z]+(?: [A-Z][a-z]+)+) joined", text)
        if m:
            name_raw = m.group(1)
    if name_raw:
        parts = name_raw.split(" ", 1)
        result["First Name"] = parts[0]
        result["Last Name"]  = parts[1] if len(parts) > 1 else ""

    result["Title"]     = _field_value(text, "Title")
    result["Industry"]  = _field_value(text, "Industry")
    result["Email"]     = _email(text)
    result["Phone"]     = _phone(text)
    result["LinkedIn"]  = _linkedin_url(text)

    # Company — from "*Company: <url|Name>*" line
    company_raw = _field_value(text, "Company")
    if not company_raw:
        # also try "Organization:" label used in some variants
        company_raw = _field_value(text, "Organization")
    result["Company"] = company_raw

    # Employees — from "- Size: 220 employees" or "- Employees: ..."
    size_val = _field_value(text, "Size")
    if not size_val:
        size_val = _field_value(text, "Employees")
    result["Employees"] = size_val

    return result


def parse_outbound_signal(text: str) -> dict:
    """
    Parses messages from feed-outbound-signals (Common Room bot).

    Expected format (blockquote lines):
        > *Contact*: <url|Full Name>
        > *Role*: Co-Founder/CEO
        > *Email*: <mailto:addr|addr>
        > *LinkedIn*: <url|linkedin.com/in/handle>
        > *Organization*: <url|Company Name>
        > *Industry*: SaaS
        > *Employees*: 12
    """
    result = {}

    name_raw = _field_value(text, "Contact")
    if name_raw:
        parts = name_raw.split(" ", 1)
        result["First Name"] = parts[0]
        result["Last Name"]  = parts[1] if len(parts) > 1 else ""

    result["Title"]     = _field_value(text, "Role")
    result["Company"]   = _field_value(text, "Organization")
    result["Industry"]  = _field_value(text, "Industry")
    result["Employees"] = _field_value(text, "Employees")
    result["Email"]     = _email(text)
    result["Phone"]     = _phone(text)
    result["LinkedIn"]  = _linkedin_url(text)

    return result


def parse_website_visitor(text: str) -> dict:
    """
    Parses reply messages from feed-website-visitors threads.
    Format tends to match the hiring-alert style; fall back to outbound if needed.
    """
    result = parse_hiring_alert(text)
    # If we got nothing, try outbound format
    if not result.get("First Name") and not result.get("Email"):
        result = parse_outbound_signal(text)
    return result


# Channel name → parser
_OUTBOUND_CHANNEL  = "feed-outbound-signals"
_VISITOR_CHANNEL   = "feed-website-visitors"


def extract_lead_fields(text: str, channel_name: str) -> dict:
    """Route to the correct parser based on channel."""
    if channel_name == _OUTBOUND_CHANNEL:
        return parse_outbound_signal(text)
    if channel_name == _VISITOR_CHANNEL:
        return parse_website_visitor(text)
    return parse_hiring_alert(text)


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
    combined = full_text(msg) + ("\n" + extra_text if extra_text else "")
    fields = extract_lead_fields(combined, source)
    lead.update({k: v for k, v in fields.items() if v})
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
            # Extract company from parent message using the channel's parser
            parent_fields = extract_lead_fields(parent_text, ch["name"])
            parent_company = parent_fields.get("Company", "")
            for reply in fetch_replies(client, ch["id"], msg["ts"]):
                lead = build_lead(reply, ch["name"], extra_text=parent_text)
                if not lead["Company"] and parent_company:
                    lead["Company"] = parent_company
                if not is_empty(lead):
                    leads.append(lead)
        else:
            lead = build_lead(msg, ch["name"])
            if not is_empty(lead):
                leads.append(lead)
    return leads


# ── AI hook generation ────────────────────────────────────────────────────────

_HOOK_SYSTEM = (
    "You are an expert SDR copywriter. Given a sales trigger alert and a lead's details, "
    "write a single 1-2 sentence LinkedIn message opener or cold email first line. "
    "Reference the specific trigger (funding round, new hire, acquisition, etc.) naturally. "
    "Be conversational and human — not salesy. Do NOT mention any product or company name. "
    "Output only the opener text, nothing else."
)

_TRIGGER_LABELS = {
    "feed-hiring-alerts":               "new hire",
    "feed-job-postings":                "job posting",
    "feed-funding-alerts":              "funding round",
    "feed-mergers-acquisitions-alerts": "acquisition",
    "feed-ipo-alerts":                  "IPO",
    "feed-website-visitors":            "website visit",
    "feed-outbound-signals":            "buying signal",
}


def generate_hooks(leads) -> None:
    """
    Populate lead["Hook"] in-place for all leads that have enough data.
    Uses a single Anthropic client with prompt caching on the system prompt.
    Silently skips if ANTHROPIC_API_KEY is not set.
    """
    if not ANTHROPIC_API_KEY:
        return

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    for lead in leads:
        first   = lead.get("First Name", "")
        last    = lead.get("Last Name", "")
        title   = lead.get("Title", "")
        company = lead.get("Company", "")
        source  = lead.get("Source", "")

        if not (first or company):
            continue

        trigger = _TRIGGER_LABELS.get(source, "signal")
        name_str = f"{first} {last}".strip()
        user_msg = (
            f"Trigger type: {trigger}\n"
            f"Lead: {name_str}, {title} at {company}\n"
            f"Write the opener."
        )

        try:
            resp = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=120,
                system=[
                    {
                        "type": "text",
                        "text": _HOOK_SYSTEM,
                        "cache_control": {"type": "ephemeral"},
                    }
                ],
                messages=[{"role": "user", "content": user_msg}],
            )
            lead["Hook"] = resp.content[0].text.strip()
        except Exception as e:
            print(f"  [WARN] Hook generation failed for {name_str}: {e}", file=sys.stderr)


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
    # Top hooks — senior leads that have a generated hook
    tier_rank = {t: i for i, (t, _) in enumerate(SENIORITY_TIERS)}
    hooked = [l for l in leads if l.get("Hook") and l.get("First Name")]
    hooked.sort(key=lambda l: tier_rank.get(classify_seniority(l.get("Title", "")), 99))
    top_hooks = hooked[:3]
    hooks_section = ""
    if top_hooks:
        hook_lines = []
        for l in top_hooks:
            name = f"{l.get('First Name','')} {l.get('Last Name','')}".strip()
            co   = l.get("Company", "")
            hook = l.get("Hook", "")
            hook_lines.append(f"  *{name}* ({co})\n  _{hook}_")
        hooks_section = "\n\n*:writing_hand: Top outreach hooks:*\n" + "\n\n".join(hook_lines)

    greeting = f"<@{rep['id']}> " if rep.get("mention") else ""
    return (
        f":wave: {greeting}Good morning, {rep['name'].split()[0]}! Here are your leads for {date_range}.\n\n"
        f"*{len(leads)} total leads*\n\n"
        f"*By source:*\n{src_lines}\n\n"
        f"*By seniority:*\n{sen_lines}"
        f"{hooks_section}"
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
    p.add_argument("--channel", metavar="CHANNEL_ID", help="Post to a shared channel instead of DMs")
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

        # Generate AI hooks (no-op if ANTHROPIC_API_KEY not set)
        if ANTHROPIC_API_KEY:
            print(f"  Generating outreach hooks …", file=sys.stderr)
        generate_hooks(rep_leads)

        first    = rep["name"].split()[0].lower()
        filename = f"{first}_leads_{now_str}.csv"
        summary  = build_summary(rep_leads, oldest, latest, rep)
        csv_text = build_csv(rep_leads)

        # ── Phase 2: write (show plan, then execute unless --dry-run) ─────────
        prior_ids = prior_output.get(rep["name"], {})

        if args.channel:
            # Post to a shared channel (e.g. #-team-sdr)
            target_id = args.channel
        else:
            # Open DM with rep — works from bot side without user needing to message first.
            # If this fails the workspace admin needs to allow the bot to DM all users at:
            # admin.slack.com → Installed Apps → [bot] → Permissions → Allow DMs
            try:
                dm    = client.conversations_open(users=[rep["id"]])
                target_id = dm["channel"]["id"]
            except SlackApiError as e:
                print(f"  [ERROR] Cannot open DM with {rep['name']}: {e.response['error']}", file=sys.stderr)
                print(f"  [ERROR] Fix: admin.slack.com → Installed Apps → your bot → Permissions → Allow DMs to all users", file=sys.stderr)
                results.append({"rep": rep["name"], "lead_count": len(rep_leads), "by_channel": by_channel, "slack_ids": {}, "error": e.response["error"]})
                continue

        print(f"  Mutations for {rep['name']}:", file=sys.stderr)
        slack_ids = deliver(client, target_id, summary, csv_text, filename, prior_ids, args.dry_run)

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
