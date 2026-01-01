# main.py — Daily Bible Year Groups Logger (reaction-driven)
import os
import json
import re
from datetime import datetime, date
from zoneinfo import ZoneInfo

import discord
from discord.ext import commands
import gspread
from google.oauth2.service_account import Credentials

# -------------------- ENV / CONFIG --------------------
TZ_LA = ZoneInfo("America/Los_Angeles")

DRY_RUN = os.getenv("DRY_RUN", "0") == "1"

SHEET_NAME = os.getenv("SHEET_NAME", "").strip()
if not SHEET_NAME:
    raise RuntimeError("SHEET_NAME env var is required")

TRACK_CHANNEL_ID = int(os.getenv("TRACK_CHANNEL_ID", "0") or "0")
if not TRACK_CHANNEL_ID:
    raise RuntimeError("TRACK_CHANNEL_ID env var is required")

REQUIRE_AUTHOR_ID = int(os.getenv("REQUIRE_AUTHOR_ID", "0") or "0")
if not REQUIRE_AUTHOR_ID:
    raise RuntimeError("REQUIRE_AUTHOR_ID env var is required")

TITLE_MATCH = (os.getenv("TITLE_MATCH", "") or "").strip().lower()
if not TITLE_MATCH:
    raise RuntimeError("TITLE_MATCH env var is required")

TRACK_EMOJI = os.getenv("TRACK_EMOJI", "✅")

TAB_INDIVIDUALS = os.getenv("TAB_INDIVIDUALS", "Individuals")
TAB_GROUPS = os.getenv("TAB_GROUPS", "Groups")
TAB_MAPPING = os.getenv("TAB_MAPPING", "Member Mapping")

# -------------------- DISCORD BOT ---------------------
intents = discord.Intents.default()
intents.reactions = True
intents.message_content = True
intents.members = True
bot = commands.Bot(command_prefix="!", intents=intents)

# -------------------- GOOGLE AUTH ---------------------
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDS_JSON")
if not GOOGLE_CREDS_JSON:
    raise RuntimeError("GOOGLE_CREDS_JSON secret is missing")

creds_dict = json.loads(GOOGLE_CREDS_JSON)
scopes = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
gc = gspread.authorize(credentials)

workbook = gc.open(SHEET_NAME)

# -------------------- SHEET HELPERS -------------------
DATE_RE = re.compile(r"\b(\d{1,2})/(\d{1,2})/(\d{2,4})\b")

def _ws(name: str) -> gspread.Worksheet:
    return workbook.worksheet(name)

def _today_la() -> date:
    return datetime.now(TZ_LA).date()

def _find_date_column(ws: gspread.Worksheet, target_date: date) -> int | None:
    """Find 1-based column index in row 1 matching target_date (supports multiple formats)."""
    headers = ws.row_values(1)
    targets = {
        target_date.strftime("%Y-%m-%d"),
        target_date.strftime("%-m/%-d/%Y"),
        target_date.strftime("%m/%d/%Y"),
        target_date.strftime("%-m/%-d/%y"),
        target_date.strftime("%m/%d/%y"),
    }
    for idx, raw in enumerate(headers, start=1):
        v = (raw or "").strip()
        if not v:
            continue
        if v in targets:
            return idx
        # last-resort ISO parse
        try:
            dt = datetime.fromisoformat(v.replace("Z", "").strip()).date()
            if dt == target_date:
                return idx
        except Exception:
            pass
    return None

def _set_checkbox(ws: gspread.Worksheet, row: int, col: int, value: bool):
    if DRY_RUN:
        print(f"[DRY_RUN] set {ws.title} R{row}C{col} = {value}")
        return
    ws.update_cell(row, col, "TRUE" if value else "FALSE")

def _load_mappings() -> dict[int, str]:
    """
    Member Mapping tab:
      Col A: Discord user id
      Col B: sheet name label (must match Individuals col A)
    """
    try:
        ws = workbook.worksheet(TAB_MAPPING)
    except gspread.WorksheetNotFound:
        print(f"[ERR] '{TAB_MAPPING}' tab not found; no one will be marked.")
        return {}
    rows = ws.get_all_values()
    mp: dict[int, str] = {}
    for r in rows[1:]:
        if len(r) < 2:
            continue
        uid_raw, label = (r[0] or "").strip(), (r[1] or "").strip()
        if not uid_raw or not label:
            continue
        try:
            mp[int(uid_raw)] = label
        except ValueError:
            print(f"[WARN] Skipping mapping with non-integer USER_ID: {uid_raw}")
    return mp

# Cache for column-A lookups per worksheet id
_ROW_MAP_CACHE: dict[int, dict[str, int]] = {}

def _normalize_label(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s*\(.*?\)\s*$", "", s)  # strip trailing "(...)" if any
    s = s.replace(".", "")
    s = re.sub(r"\s+", " ", s)
    return s.upper()

def _get_row_map(ws: gspread.Worksheet) -> dict[str, int]:
    """Build once per worksheet: map of exact + normalized labels -> row index."""
    key = ws.id
    if key in _ROW_MAP_CACHE:
        return _ROW_MAP_CACHE[key]
    colA = ws.col_values(1)
    m: dict[str, int] = {}
    for idx, val in enumerate(colA, start=1):
        v = (val or "").strip()
        if not v:
            continue
        m[v] = idx
        m.setdefault(_normalize_label(v), idx)
    _ROW_MAP_CACHE[key] = m
    return m

def _find_row_by_label(ws: gspread.Worksheet, sheet_label: str) -> int | None:
    m = _get_row_map(ws)
    target = (sheet_label or "").strip()
    return m.get(target) or m.get(_normalize_label(target))

def _split_roster(roster_cell: str) -> list[str]:
    if not roster_cell:
        return []
    parts = [p.strip() for p in roster_cell.split(",")]
    return [_normalize_label(p) for p in parts if p.strip()]

def _load_groups() -> list[dict]:
    """
    Groups tab:
      Col A: Group label
      Col B: roster (comma-separated names, matching Individuals col A labels)
    Returns: list of {row, group_label, members_norm}
    """
    ws = _ws(TAB_GROUPS)
    values = ws.get_all_values()
    out: list[dict] = []
    for i, r in enumerate(values[1:], start=2):  # skip header row
        group_label = (r[0] or "").strip() if len(r) > 0 else ""
        roster = (r[1] or "").strip() if len(r) > 1 else ""
        if not group_label:
            continue
        out.append({
            "row": i,
            "group_label": group_label,
            "members_norm": _split_roster(roster),
        })
    return out

def _find_group_for_member(groups: list[dict], member_sheet_name: str) -> dict | None:
    target = _normalize_label(member_sheet_name)
    for g in groups:
        if target in g["members_norm"]:
            return g
    return None

# -------------------- DISCORD HELPERS -----------------
_processed: set[tuple] = set()

def _emoji_matches(reaction_emoji) -> bool:
    try:
        name = reaction_emoji.name  # PartialEmoji
    except Exception:
        name = str(reaction_emoji)
    return str(name) == TRACK_EMOJI

def _message_matches_daily_post(msg: discord.Message, today: date) -> bool:
    # Author must be PC
    if not msg.author or msg.author.id != REQUIRE_AUTHOR_ID:
        return False

    # Must be posted today in LA
    created_la = msg.created_at.replace(tzinfo=ZoneInfo("UTC")).astimezone(TZ_LA).date()
    if created_la != today:
        return False

    # Must match TITLE_MATCH in content or embed title/description
    haystacks: list[str] = []
    if msg.content:
        haystacks.append(msg.content.lower())
    for emb in (msg.embeds or []):
        if emb.title:
            haystacks.append(emb.title.lower())
        if emb.description:
            haystacks.append(emb.description.lower())

    return any(TITLE_MATCH in h for h in haystacks)

@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    # Only watch the one channel
    if payload.channel_id != TRACK_CHANNEL_ID:
        return

    try:
        if payload.user_id is None or payload.user_id == bot.user.id:
            return
        if not _emoji_matches(payload.emoji):
            return

        today = _today_la()

        # Simple in-memory de-dupe (prevents repeats in the same process)
        dedupe_key = (payload.message_id, payload.user_id, str(payload.emoji), today.isoformat())
        if dedupe_key in _processed:
            return
        _processed.add(dedupe_key)

        channel = bot.get_channel(payload.channel_id) or await bot.fetch_channel(payload.channel_id)
        msg = await channel.fetch_message(payload.message_id)

        if not _message_matches_daily_post(msg, today):
            return

        # Resolve user -> sheet label
        mappings = _load_mappings()
        sheet_name = mappings.get(int(payload.user_id))
        if not sheet_name:
            print(f"[SKIP] user_id {payload.user_id} not in '{TAB_MAPPING}'")
            return

        ws_ind = _ws(TAB_INDIVIDUALS)
        ws_grp = _ws(TAB_GROUPS)

        col_ind = _find_date_column(ws_ind, today)
        col_grp = _find_date_column(ws_grp, today)
        if not col_ind or not col_grp:
            print(f"[ERR] date column not found for {today} (check header row formatting)")
            return

        row_ind = _find_row_by_label(ws_ind, sheet_name)
        if not row_ind:
            print(f"[ERR] could not find row for '{sheet_name}' in '{TAB_INDIVIDUALS}' col A")
            return

        # 1) Individuals TRUE
        _set_checkbox(ws_ind, row_ind, col_ind, True)

        # 2) Group recompute
        groups = _load_groups()
        g = _find_group_for_member(groups, sheet_name)
        if not g:
            print(f"[WARN] no group found containing '{sheet_name}' (Individuals updated only)")
            return

        all_true = True
        row_map = _get_row_map(ws_ind)
        for member_norm in g["members_norm"]:
            r = row_map.get(member_norm)
            if not r:
                all_true = False
                break
            val = ws_ind.cell(r, col_ind).value
            if str(val).strip().upper() != "TRUE":
                all_true = False
                break

        _set_checkbox(ws_grp, g["row"], col_grp, all_true)
        print(f"[OK] {sheet_name} marked TRUE; {g['group_label']} complete={all_true}")

    except Exception as e:
        print("[ERR] on_raw_reaction_add:", repr(e))

# -------------------- START --------------------------
token = os.getenv("DISCORD_TOKEN") or os.getenv("DISCORD_BOT_TOKEN")
if not token:
    raise RuntimeError("Discord token env missing (DISCORD_TOKEN or DISCORD_BOT_TOKEN).")
token = token.strip()

print("Starting… (token length:", len(token), ")")
bot.run(token)
