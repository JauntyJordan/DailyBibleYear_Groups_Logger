# main.py  — Daily Bread one-shot updater (GitHub Actions friendly)
import os, json, asyncio, re, unicodedata
from datetime import datetime, timedelta, date, time
from zoneinfo import ZoneInfo

import discord
from discord.ext import commands
import gspread
from google.oauth2.service_account import Credentials

# -------------------- ENV / CONFIG --------------------
DRY_RUN = os.getenv("DRY_RUN", "0") == "1"               # set to '1' in workflow while testing
RUN_ONCE = os.getenv("RUN_ONCE", "0") == "1"             # Actions default: run once then exit
SHEET_NAME = os.getenv("SHEET_NAME", "").strip()         # Google Sheet title

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

if not SHEET_NAME:
    raise RuntimeError("SHEET_NAME env var is required")

# Channel id env: supports either DBR_CHANNEL_ID or CHANNEL_ID
_env_ch = os.getenv("DBR_CHANNEL_ID") or os.getenv("CHANNEL_ID")
DAILY_BREAD_CHANNEL_ID = int(_env_ch) if _env_ch else None

DAILY_BREAD_MATCH = "daily bread"                        # phrase found in content or embed title
LOOKBACK_DAYS = 3                                        # channel history window
TZ_LA = ZoneInfo("America/Los_Angeles")

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

# Open workbook once
workbook = gc.open(SHEET_NAME)

# -------------------- SHEET HELPERS -------------------
def month_tab_for(d: date) -> str:
    return d.strftime("%B")  # "August"

DATE_RE = re.compile(r"\b(\d{1,2})/(\d{1,2})/(\d{2,4})\b")

def _title_matches_date(title: str, target: date) -> bool:
    m = DATE_RE.search(title or "")
    if not m:
        return False
    mm, dd, yy = map(int, m.groups())
    if yy < 100:
        yy += 2000
    try:
        return date(yy, mm, dd) == target
    except ValueError:
        return False

def _find_date_column(ws: gspread.Worksheet, target_date: date) -> int | None:
    """Find the 1-based column index in row 1 matching target_date."""
    headers = ws.row_values(1)
    targets = {
        target_date.strftime("%Y-%m-%d"),   # 2025-08-27
        target_date.strftime("%-m/%-d/%Y"), # 8/27/2025 (posix)
        target_date.strftime("%m/%d/%Y"),   # 08/27/2025
        target_date.strftime("%-m/%-d/%y"), # 8/27/25 (your sheet)
        target_date.strftime("%m/%d/%y"),   # 08/27/25
    }
    for idx, raw in enumerate(headers, start=1):
        v = (raw or "").strip()
        if not v:
            continue
        if v in targets:
            return idx
        # last-resort attempt: ISO parse
        try:
            dt = datetime.fromisoformat(v.replace("Z", "").strip()).date()
            if dt == target_date:
                return idx
        except Exception:
            pass
    return None

def _load_mappings() -> dict[int, str]:
    """
    Read 'Mappings' tab -> dict { user_id (int) : exact SHEET_NAME (str) }.
    Sheet format:
      A1: USER_ID   B1: SHEET_NAME
      rows below:   1645520343207 , JORDAN (RICKY)
    """
    try:
        ws = workbook.worksheet("Member Mapping")
    except gspread.WorksheetNotFound:
        print("Mappings tab not found; no one will be marked.")
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
            print(f"Skipping mapping with non-integer USER_ID: {uid_raw}")
    print(f"Loaded {len(mp)} mappings.")
    return mp

def _ws(name: str) -> gspread.Worksheet:
    return workbook.worksheet(name)

def _today_la() -> date:
    return datetime.now(TZ_LA).date()

def _set_checkbox(ws: gspread.Worksheet, row: int, col: int, value: bool):
    if DRY_RUN:
        print(f"[DRY_RUN] set {ws.title} R{row}C{col} = {value}")
        return
    ws.update_cell(row, col, "TRUE" if value else "FALSE")


def _find_row_by_exact_label(ws: gspread.Worksheet, sheet_label: str) -> int | None:
    """Find row in column A whose value equals sheet_label (trim, case-sensitive)."""
    colA = ws.col_values(1)
    target = (sheet_label or "").strip()
    # exact match first
    for idx, val in enumerate(colA, start=1):
        if (val or "").strip() == target:
            return idx
    # fallback case-insensitive match
    t_low = target.lower()
    for idx, val in enumerate(colA, start=1):
        if (val or "").strip().lower() == t_low:
            return idx
    return None
# Cache for column-A lookups per worksheet id
_ROW_MAP_CACHE: dict[int, dict[str, int]] = {}

def _normalize_label(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s*\(.*?\)\s*$", "", s)  # strip trailing "(...)" if any
    s = s.replace(".", "")
    s = re.sub(r"\s+", " ", s)
    return s.upper()

def _split_roster(roster_cell: str) -> list[str]:
    # "A, B, C" -> ["A", "B", "C"] normalized
    if not roster_cell:
        return []
    parts = [p.strip() for p in roster_cell.split(",")]
    return [_normalize_label(p) for p in parts if p.strip()]

def _load_groups() -> list[dict]:
    """
    Reads Groups tab:
      Col A: Group label (Group 1, Group 2...)
      Col B: roster (comma-separated names)
    Returns list of {row, group_label, members_norm}
    """
    ws = _ws(os.getenv("TAB_GROUPS", "Groups"))
    values = ws.get_all_values()
    out = []
    # row 1 is header; start at row 2
    for i, r in enumerate(values[1:], start=2):
        group_label = (r[0] or "").strip() if len(r) > 0 else ""
        roster = (r[1] or "").strip() if len(r) > 1 else ""
        if not group_label:
            continue
        members_norm = _split_roster(roster)
        out.append({"row": i, "group_label": group_label, "members_norm": members_norm})
    return out

def _find_group_for_member(groups: list[dict], member_sheet_name: str) -> dict | None:
    target = _normalize_label(member_sheet_name)
    for g in groups:
        if target in g["members_norm"]:
            return g
    return None

def _get_row_map(ws) -> dict[str, int]:
    """Build once per worksheet: map of both exact and normalized labels -> row index."""
    key = ws.id
    if key in _ROW_MAP_CACHE:
        return _ROW_MAP_CACHE[key]
    colA = ws.col_values(1)  # ONE READ ONLY
    m: dict[str, int] = {}
    for idx, val in enumerate(colA, start=1):
        v = (val or "").strip()
        if not v:
            continue
        # exact key
        m[v] = idx
        # normalized key (fallback)
        m.setdefault(_normalize_label(v), idx)
    _ROW_MAP_CACHE[key] = m
    return m

def _find_row_by_exact_label(ws, sheet_label: str) -> int | None:
    m = _get_row_map(ws)
    # try exact first, then normalized
    target = (sheet_label or "").strip()
    return m.get(target) or m.get(_normalize_label(target))


# -------------------- DISCORD HELPERS -----------------

def _is_bread_emoji(e) -> bool:
    """
    Treats the unicode 🍞 and any custom emoji whose name contains 'bread'
    as a 'bread' reaction.
    """
    s = str(e).lower()
    return s == "🍞" or "bread" in s  # handles <:bread:123...> etc.

def _bread_count(msg: discord.Message) -> int:
    """Total count of 'bread' reactions on a message."""
    return sum(r.count for r in msg.reactions if _is_bread_emoji(r.emoji))

def _total_reacts(msg: discord.Message) -> int:
    """Total reaction count (all emojis)."""
    return sum(r.count for r in msg.reactions)

# -------------------- DISCORD HELPERS -----------------
# Looks only at embedded posts with "Daily Bread" in the title
async def _find_db_message_for_date(channel: discord.TextChannel, target_local_date: date):
    """
    Find today's Daily Bread by:
      • requiring at least one embed,
      • requiring embed.title to contain 'daily bread' (case-insensitive),
      • requiring the message was created on target_local_date (LA),
      • preferring the candidate with most 🍞 reactions, then total reactions.
    """
    after_dt = datetime.now(ZoneInfo("UTC")) - timedelta(days=LOOKBACK_DAYS)
    candidates: list[discord.Message] = []

    async for m in channel.history(limit=200, oldest_first=False, after=after_dt):
        if not m.embeds:
            continue

        title = (m.embeds[0].title or "").lower()
        if "daily bread" not in title:
            continue

        created_local = m.created_at.replace(tzinfo=ZoneInfo("UTC")).astimezone(TZ_LA).date()
        if not (created_local == target_local_date or _title_matches_date(m.embeds[0].title, target_local_date)):
            continue


        candidates.append(m)

    if not candidates:
        print(f"[DEBUG] no embedded 'Daily Bread' messages found for {target_local_date}")
        return None

    # Prefer post with most 🍞, then most total reactions
    candidates.sort(key=lambda msg: (_bread_count(msg), _total_reacts(msg)), reverse=True)
    chosen = candidates[0]
    print(
        f"[DEBUG] chosen(embedded) id={chosen.id} "
        f"bread={_bread_count(chosen)} reacts={[(str(r.emoji), r.count) for r in chosen.reactions]}"
    )
    return chosen


async def _reactor_ids(message: discord.Message) -> set[int]:
    users: set[int] = set()
    if not message:
        return users

    # Debug: show the emoji + total count for each reaction
    print("[DEBUG] reaction counts:",
          [(str(r.emoji), r.count) for r in message.reactions])

    for rxn in message.reactions:
        async for u in rxn.users(limit=None):
            if not u.bot:
                users.add(u.id)
    return users


# -------------------- CORE UPDATE LOGIC ---------------
async def _update_checkmarks_for_message(channel: discord.TextChannel,
                                         msg: discord.Message,
                                         post_date: date,
                                         mappings: dict[int, str]):
    if not msg:
        return
    tab = month_tab_for(post_date)
    ws = workbook.worksheet(tab)

    col = _find_date_column(ws, post_date)
    if not col:
        print(f"Date column for {post_date} not found on tab '{tab}'.")
        return

    ids = await _reactor_ids(msg)
    if not ids:
        print(f"No reactors for {post_date} ({tab}).")
        return

    updated = 0
    ranges_to_true = []

    row_map = _get_row_map(ws)  # use cached column A
    for uid in ids:
        label = mappings.get(uid)
        if not label:
            print(f"No mapping for user_id {uid}; skipping.")
            continue

        row = _find_row_by_exact_label(ws, label)
        if not row:
            print(f"Mapping found but label '{label}' not present in column A of '{tab}'.")
            continue

        a1 = gspread.utils.rowcol_to_a1(row, col)
        sheet_range = f"'{tab}'!{a1}"
        if DRY_RUN:
            print(f"[DRY] Would mark '{label}' on {post_date} (tab {tab}, row {row}, col {col}).")
        else:
            ranges_to_true.append({"range": sheet_range, "values": [["TRUE"]]})
        updated += 1

    if not DRY_RUN and ranges_to_true:
        # ONE batch write instead of many updates
        workbook.values_batch_update({
            "valueInputOption": "USER_ENTERED",
            "data": ranges_to_true
        })

    verb = "Would mark" if DRY_RUN else "Marked"
    print(f"{verb} {updated} members for {post_date} on {tab}.")
    return updated

async def run_dbr_once():
    """Find DB posts for today & yesterday and update checkboxes based on Mappings only."""
    la_today = datetime.now(TZ_LA).date()
    la_yday = la_today - timedelta(days=1)

    mappings = _load_mappings()
    if not mappings:
        print("No mappings; nothing to do.")
        return

    channel = bot.get_channel(DAILY_BREAD_CHANNEL_ID)
    if not isinstance(channel, discord.TextChannel):
        print("Channel not found or not a text channel.")
        return

    today_msg = await _find_db_message_for_date(channel, la_today)
    yday_msg  = await _find_db_message_for_date(channel, la_yday)

    # Run once per day and capture counts
    t = await _update_checkmarks_for_message(channel, today_msg, la_today, mappings)
    y = await _update_checkmarks_for_message(channel, yday_msg,  la_yday,  mappings)

    # Machine-readable summary for the workflow parser
    print(f"SUMMARY: today={t or 0} yesterday={y or 0}")

    # Optional: post a human summary to a separate channel
    log_channel_id = int(os.getenv("LOG_CHANNEL_ID", "0") or 0)
    if log_channel_id:
        log_channel = bot.get_channel(log_channel_id)
        if isinstance(log_channel, discord.TextChannel):
            try:
                await log_channel.send(
                    f"✅ Daily Bread update summary\n"
                    f"• Today marked: {t or 0}\n"
                    f"• Yesterday marked: {y or 0}"
                )
            except Exception as e:
                print(f"Failed to send log message: {e}")

TRACK_MESSAGE_ID = int(os.getenv("TRACK_MESSAGE_ID", "0") or "0")
TRACK_EMOJI = os.getenv("TRACK_EMOJI", "✅")
REQUIRE_AUTHOR_ID = int(os.getenv("REQUIRE_AUTHOR_ID", "0") or "0")

TAB_INDIVIDUALS = os.getenv("TAB_INDIVIDUALS", "Individuals")
TAB_GROUPS = os.getenv("TAB_GROUPS", "Groups")
TAB_MAPPING = os.getenv("TAB_MAPPING", "Member Mapping")

_processed = set()  # simple in-memory de-dupe

def _emoji_matches(reaction_emoji) -> bool:
    # Handles unicode emoji and custom emoji names
    try:
        name = reaction_emoji.name  # discord.PartialEmoji
    except Exception:
        name = str(reaction_emoji)
    return str(name) == TRACK_EMOJI

@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    """
    Uses raw events so we don't miss events when messages aren't cached.
    """
    try:
        if payload.user_id is None:
            return
        if payload.user_id == bot.user.id:
            return
        if TRACK_MESSAGE_ID and payload.message_id != TRACK_MESSAGE_ID:
            return
        if not _emoji_matches(payload.emoji):
            return

        # de-dupe
        today = _today_la()
        dedupe_key = (payload.message_id, payload.user_id, str(payload.emoji), today.isoformat())
        if dedupe_key in _processed:
            return
        _processed.add(dedupe_key)

        channel = bot.get_channel(payload.channel_id)
        if channel is None:
            channel = await bot.fetch_channel(payload.channel_id)

        msg = await channel.fetch_message(payload.message_id)

        if REQUIRE_AUTHOR_ID and msg.author and msg.author.id != REQUIRE_AUTHOR_ID:
            return

        # Load mappings each event OR cache; start simple:
        mappings = _load_mappings()  # {user_id:int -> sheet_name:str}
        sheet_name = mappings.get(int(payload.user_id))
        if not sheet_name:
            print(f"[SKIP] user_id {payload.user_id} not in Member Mapping")
            return

        ws_ind = _ws(TAB_INDIVIDUALS)
        ws_grp = _ws(TAB_GROUPS)

        col_ind = _find_date_column(ws_ind, today)
        col_grp = _find_date_column(ws_grp, today)

        if not col_ind or not col_grp:
            print(f"[ERR] date column not found for {today} (check header row formatting)")
            return

        row_ind = _find_row_by_exact_label(ws_ind, sheet_name)
        if not row_ind:
            print(f"[ERR] could not find row for '{sheet_name}' in Individuals col A")
            return

        # 1) Set Individuals checkbox TRUE
        _set_checkbox(ws_ind, row_ind, col_ind, True)

        # 2) Group recompute
        groups = _load_groups()
        g = _find_group_for_member(groups, sheet_name)
        if not g:
            print(f"[WARN] no group found containing '{sheet_name}' (Individuals updated only)")
            return

        # Check every member in group for today's checkbox
        all_true = True
        row_map = _get_row_map(ws_ind)  # cached colA map
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




# -------------------- ONE-SHOT RUNNER -----------------
async def _run_and_quit():
    await bot.wait_until_ready()
    await run_dbr_once()
    await asyncio.sleep(1)
    await bot.close()

if RUN_ONCE:
    @bot.event
    async def on_ready():
        asyncio.create_task(_run_and_quit())

# -------------------- START --------------------------
token = os.getenv("DISCORD_TOKEN") or os.getenv("DISCORD_BOT_TOKEN")
if not token:
    raise RuntimeError("Discord token env missing (DISCORD_TOKEN or DISCORD_BOT_TOKEN).")
token = token.strip()
print("Starting… (token length:", len(token), ")")
bot.run(token)

