"""Bible In A Year – Groups Logger

Designed for scheduled runs (e.g., via GitHub Actions).

Per run:
  1) Find today's target post in TRACK_CHANNEL_ID authored by REQUIRE_AUTHOR_ID
     whose content/embed includes TITLE_MATCH.
  2) Collect users who reacted with TRACK_EMOJI.
  3) Update Google Sheets:
       - Individuals: mark TRUE/FALSE for each mapped member
       - Groups: mark TRUE only if every member in roster reacted
  4) Post a status summary message to Discord.

Notes:
  - This script runs once and exits (no long-running bot process).
  - It reads all reactions at the time of the run, so it is robust to downtime.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

import discord
import gspread
from google.oauth2.service_account import Credentials


# -------------------- CONFIG --------------------
TZ = ZoneInfo(os.getenv("TIMEZONE", "America/Los_Angeles"))

DRY_RUN = os.getenv("DRY_RUN", "0") == "1"

DISCORD_TOKEN = (os.getenv("DISCORD_TOKEN") or os.getenv("DISCORD_BOT_TOKEN") or "").strip()
if not DISCORD_TOKEN:
    raise RuntimeError("Discord token env missing (DISCORD_TOKEN or DISCORD_BOT_TOKEN).")

GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDS_JSON", "").strip()
if not GOOGLE_CREDS_JSON:
    raise RuntimeError("GOOGLE_CREDS_JSON secret is missing")

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

TRACK_EMOJI = os.getenv("TRACK_EMOJI", "✅").strip()

TAB_INDIVIDUALS = os.getenv("TAB_INDIVIDUALS", "Individuals")
TAB_GROUPS = os.getenv("TAB_GROUPS", "Groups")
TAB_MAPPING = os.getenv("TAB_MAPPING", "Member Mapping")

STATUS_CHANNEL_ID = int(os.getenv("STATUS_CHANNEL_ID", "0") or "0")
if not STATUS_CHANNEL_ID:
    raise RuntimeError("STATUS_CHANNEL_ID env var is required (where to post the run summary)")

CHECK_NAME = (os.getenv("CHECK_NAME", "Scheduled Check") or "Scheduled Check").strip()

LOOKBACK_MESSAGES = int(os.getenv("LOOKBACK_MESSAGES", "50") or "50")


# -------------------- GOOGLE SHEETS --------------------
creds_dict = json.loads(GOOGLE_CREDS_JSON)
scopes = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
gc = gspread.authorize(credentials)
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "").strip()
if not SPREADSHEET_ID:
    raise RuntimeError("SPREADSHEET_ID env var is required")

workbook = gc.open_by_key(SPREADSHEET_ID)

print("Opened workbook:", workbook.title)
print("Workbook ID:", workbook.id)
print("Workbook URL:", workbook.url)
print("Worksheets:", [ws.title for ws in workbook.worksheets()])

def _ws(name: str) -> gspread.Worksheet:
    return workbook.worksheet(name)


def _now_local() -> datetime:
    return datetime.now(TZ)


def _today_local() -> date:
    return _now_local().date()


def _yesterday_local() -> date:
    return _today_local() - timedelta(days=1)


def _normalize_label(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s*\(.*?\)\s*$", "", s)  # strip trailing "(...)" if any
    s = s.replace(".", "")
    s = re.sub(r"\s+", " ", s)
    return s.upper()

def find_date_col(sheet, target_date: date, header_rows=(1,)):
    """
    Returns 1-based column index where the header matches target_date.
    Assumes date headers are on row 1, starting at column C.
    """

    candidates = {
        target_date.isoformat(),                  # 2026-01-01
        target_date.strftime("%-m/%-d/%y"),        # 1/1/26 (mac/linux)
        target_date.strftime("%m/%d/%y"),          # 01/01/26
        target_date.strftime("%-m/%-d/%Y"),        # 1/1/2026
        target_date.strftime("%m/%d/%Y"),          # 01/01/2026
    }

    # Windows compatibility
    candidates.add(target_date.strftime("%#m/%#d/%y"))
    candidates.add(target_date.strftime("%#m/%#d/%Y"))

    row_vals = sheet.get("1:1")[0]
    

    for r in header_rows:
      row_vals = sheet.get(f"{r}:{r}")[0]
      print("HEADER ROW:", row_vals)
      
      for idx, v in enumerate(row_vals, start=1):
        if not v:
            continue

        text = str(v).replace("\xa0", " ").strip()

        if text.lower() in {
          "groups",
          "dates",
          "current streak 🔥",
          "longest streak",
          "false",
          "finished",
      }:
          continue


        if text in candidates:
            return idx

        # Try parsing common date formats
        for fmt in ("%m/%d/%y", "%m/%d/%Y"):
            try:
                if datetime.strptime(text, fmt).date() == target_date:
                    return idx
            except ValueError:
                pass

    raise RuntimeError(
        f"Date column not found for {target_date.isoformat()}. "
        f"Checked row 1 with formats: {sorted(candidates)}"
    )

def _set_checkbox(ws: gspread.Worksheet, row: int, col: int, value: bool):
    if DRY_RUN:
        print(f"[DRY_RUN] set {ws.title} R{row}C{col} = {value}")
        return
    ws.update_cell(row, col, "TRUE" if value else "FALSE")


def _count_true_in_column(ws: gspread.Worksheet, col: int, start_row: int = 2) -> int:
    """Count TRUE values in a given column, excluding header row."""
    # Fetch whole column once for speed
    vals = ws.col_values(col)
    n = 0
    for v in vals[start_row - 1 :]:
        if str(v).strip().upper() == "TRUE":
            n += 1
    return n


def _load_mappings() -> dict[int, str]:
    """Member Mapping tab:
    Col A: Discord user id
    Col B: sheet name label (must match Individuals col A)
    """
    try:
        ws = workbook.worksheet(TAB_MAPPING)
    except gspread.WorksheetNotFound:
        raise RuntimeError(f"'{TAB_MAPPING}' tab not found")

    rows = ws.get_all_values()
    mp: dict[int, str] = {}
    for r in rows[1:]:
        if len(r) < 2:
            continue
        uid_raw, label = (r[0] or "").strip(), (r[1] or "").strip()
        if not uid_raw or not label:
            continue
        try:
            mp[int(uid_raw)] = _normalize_label(label)
        except ValueError:
            print(f"[WARN] Skipping mapping with non-integer USER_ID: {uid_raw}")
    return mp

def _build_row_map(ws: gspread.Worksheet, name_col: int = 1) -> dict[str, int]:
    rows = ws.get_all_values()
    m: dict[str, int] = {}

    for idx, r in enumerate(rows, start=1):
        if len(r) < name_col:
            continue

        raw = r[name_col - 1].strip()
        if not raw:
            continue

        # Skip section headers
        if raw.isupper():
            continue

        key = _normalize_label(raw)
        m[key] = idx

    return m

def _split_roster(roster_cell: str) -> list[str]:
    if not roster_cell:
        return []
    parts = [p.strip() for p in roster_cell.split(",")]
    return [_normalize_label(p) for p in parts if p.strip()]


@dataclass(frozen=True)
class Group:
    row: int
    label: str
    members_norm: tuple[str, ...]


def _load_groups() -> list[Group]:
    """Groups tab:
    Col A: Group label
    Col B: roster (comma-separated names, matching Individuals col A labels)
    """
    ws = _ws(TAB_GROUPS)
    values = ws.get_all_values()
    out: list[Group] = []
    for i, r in enumerate(values[1:], start=2):  # skip header row
        group_label = (r[0] or "").strip() if len(r) > 0 else ""
        roster = (r[1] or "").strip() if len(r) > 1 else ""
        if not group_label:
            continue
        out.append(Group(row=i, label=group_label, members_norm=tuple(_split_roster(roster))))
    return out


def _message_matches_daily_post(msg: discord.Message, today: date) -> bool:
    # Author must match
    if not msg.author or msg.author.id != REQUIRE_AUTHOR_ID:
        return False

    # Must be posted today in configured timezone
    created_local = msg.created_at.replace(tzinfo=ZoneInfo("UTC")).astimezone(TZ).date()
    if created_local != today:
        return False

    haystacks: list[str] = []
    if msg.content:
        haystacks.append(msg.content.lower())
    for emb in (msg.embeds or []):
        if emb.title:
            haystacks.append(str(emb.title).lower())
        if emb.description:
            haystacks.append(str(emb.description).lower())

    return any(TITLE_MATCH in h for h in haystacks)


def _emoji_matches(emoji: discord.PartialEmoji | discord.Emoji | str) -> bool:
    try:
        name = emoji.name  # PartialEmoji
    except Exception:
        name = str(emoji)
    return str(name) == TRACK_EMOJI


async def _find_todays_post(channel: discord.TextChannel, today: date) -> discord.Message | None:
    """Search recent history for the target post."""
    async for msg in channel.history(limit=LOOKBACK_MESSAGES, oldest_first=False):
        if _message_matches_daily_post(msg, today):
            return msg
    return None


async def _get_reactors_for_emoji(msg: discord.Message) -> set[int]:
    """Return user IDs who reacted with TRACK_EMOJI."""
    reacted: set[int] = set()
    for reaction in msg.reactions:
        if not _emoji_matches(reaction.emoji):
            continue
        async for user in reaction.users(limit=None):
            if user.bot:
                continue
            reacted.add(int(user.id))
    return reacted


def _compute_group_completions(
    groups: list[Group],
    reacted_labels_norm: set[str],
) -> dict[int, bool]:
    """Return mapping: group_row -> completion bool."""
    out: dict[int, bool] = {}
    for g in groups:
        if not g.members_norm:
            out[g.row] = False
            continue
        out[g.row] = all(m in reacted_labels_norm for m in g.members_norm)
    return out


async def main():
    start = _now_local()
    today = _today_local()
    yesterday = _yesterday_local()

    # --- Discord client (single-run) ---
    intents = discord.Intents.default()
    intents.guilds = True
    intents.messages = True
    intents.message_content = True  # needed if matching TITLE_MATCH in msg.content
    client = discord.Client(intents=intents)

    run_url = ""
    if os.getenv("GITHUB_SERVER_URL") and os.getenv("GITHUB_REPOSITORY") and os.getenv("GITHUB_RUN_ID"):
        run_url = (
            f"{os.getenv('GITHUB_SERVER_URL')}/{os.getenv('GITHUB_REPOSITORY')}"
            f"/actions/runs/{os.getenv('GITHUB_RUN_ID')}"
        )

    @client.event
    async def on_ready():
        nonlocal run_url
        try:
            channel = await client.fetch_channel(TRACK_CHANNEL_ID)
            status_channel = await client.fetch_channel(STATUS_CHANNEL_ID)
            if not isinstance(channel, discord.TextChannel):
                raise RuntimeError("TRACK_CHANNEL_ID must be a text channel")
            if not isinstance(status_channel, discord.TextChannel):
                raise RuntimeError("STATUS_CHANNEL_ID must be a text channel")

            msg = await _find_todays_post(channel, today)
            if not msg:
                await status_channel.send(
                    "\n".join(
                        [
                            "❌ Bible In A Year update failed",
                            f"• Check: {CHECK_NAME}",
                            f"• When: {start.strftime('%-I:%M%p %Z')}",
                            f"• Reason: Could not find today's post in <#{TRACK_CHANNEL_ID}>",
                            f"• Repo: {os.getenv('GITHUB_REPOSITORY', 'local run')}",
                            f"• Run: #{os.getenv('GITHUB_RUN_NUMBER', 'local')}",
                            f"• Logs: {run_url}" if run_url else "• Logs: (local)",
                        ]
                    )
                )
                await client.close()
                return

            reactors = await _get_reactors_for_emoji(msg)

            # --- Sheets updates ---
            mappings = _load_mappings()  # user_id -> sheet label
            ws_ind = _ws(TAB_INDIVIDUALS)
            ws_grp = _ws(TAB_GROUPS)

            col_ind_today = find_date_col(ws_ind, today, header_rows=(1,))
            col_grp_today = find_date_col(ws_grp, today, header_rows=(2,))

            row_map_ind = _build_row_map(ws_ind, name_col=1)


            # Build a set of normalized sheet labels that reacted
            reacted_labels_norm: set[str] = set()
            updated_individuals = 0
            skipped_unmapped = 0
            skipped_no_row = 0

            for user_id, label_norm in mappings.items():
                has_reacted = user_id in reactors
                r = row_map_ind.get(label_norm)
                if not r:
                    skipped_no_row += 1
                    continue
                _set_checkbox(ws_ind, r, col_ind_today, has_reacted)
                updated_individuals += 1
                if has_reacted:
                    reacted_labels_norm.add(label_norm)

            # If there are reactors who aren't mapped, count them for visibility
            for uid in reactors:
                if uid not in mappings:
                    skipped_unmapped += 1

            # Groups recompute
            groups = _load_groups()
            group_completion = _compute_group_completions(groups, reacted_labels_norm)
            updated_groups = 0
            for g in groups:
                _set_checkbox(ws_grp, g.row, col_grp_today, group_completion.get(g.row, False))
                updated_groups += 1

            # Counts for summary (post-update)
            today_marked = _count_true_in_column(ws_ind, col_ind_today, start_row=2)
            col_ind_y = None
            try:
              col_ind_y = find_date_col(ws_ind, yesterday)
            except RuntimeError:
              col_ind_y = None

            yesterday_marked = None
            if col_ind_y is not None:
              yesterday_marked = _count_true_in_column(ws_ind, col_ind_y, start_row=2)


            end = _now_local()
            duration_s = int((end - start).total_seconds())

            # --- Discord status message ---
            lines = [
                "✅ Bible In A Year update completed",
                f"• Check: {CHECK_NAME}",
                f"• When: {start.strftime('%-I:%M%p %Z')}",
                f"• Today marked: {today_marked}",
                f"• Yesterday marked: {'N/A' if yesterday_marked is None else yesterday_marked}",
                f"• Reactors found: {len(reactors)}",
                f"• Individuals updated: {updated_individuals}",
                f"• Groups updated: {updated_groups}",
                f"• Unmapped reactors: {skipped_unmapped}",
                f"• Missing rows in Individuals: {skipped_no_row}",
                f"• Took: {duration_s}s",
                f"• Repo: {os.getenv('GITHUB_REPOSITORY', 'local run')}",
                f"• Run: #{os.getenv('GITHUB_RUN_NUMBER', 'local')}",
                "• Logs:",
                run_url if run_url else "(local)",
            ]
            await status_channel.send("\n".join(lines))

        except Exception as e:
            # Best effort: post error to status channel
            try:
                status_channel = await client.fetch_channel(STATUS_CHANNEL_ID)
                if isinstance(status_channel, discord.TextChannel):
                    await status_channel.send(
                        "\n".join(
                            [
                                "❌ Bible In A Year update failed",
                                f"• Check: {CHECK_NAME}",
                                f"• When: {start.strftime('%-I:%M%p %Z')}",
                                f"• Error: {repr(e)}",
                                f"• Repo: {os.getenv('GITHUB_REPOSITORY', 'local run')}",
                                f"• Run: #{os.getenv('GITHUB_RUN_NUMBER', 'local')}",
                                f"• Logs: {run_url}" if run_url else "• Logs: (local)",
                            ]
                        )
                    )
            except Exception:
                pass
        finally:
            await client.close()

    await client.start(DISCORD_TOKEN)


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
