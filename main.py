import os
import asyncio
from datetime import datetime, timedelta, timezone, date

import discord
import gspread
from google.oauth2.service_account import Credentials
from gspread import Cell

# ======================
# CONFIG
# ======================

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
GUILD_ID = int(os.getenv("GUILD_ID"))
CHANNEL_ID = int(os.getenv("CHANNEL_ID"))
TARGET_AUTHOR_ID = int(os.getenv("TARGET_AUTHOR_ID"))
TARGET_TITLE_KEYWORD = os.getenv("TARGET_TITLE_KEYWORD", "").lower()

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")

GROUPS_SHEET_NAME = "Groups"
MEMBER_MAPPING_SHEET_NAME = "Member Mapping"

# TEMPORARY: backfill past 11 days
BACKFILL_GROUPS_DAYS = 11

# ======================
# GOOGLE SHEETS SETUP
# ======================

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_file("credentials.json", scopes=SCOPES)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SPREADSHEET_ID)

ws_groups = sh.worksheet(GROUPS_SHEET_NAME)
ws_mapping = sh.worksheet(MEMBER_MAPPING_SHEET_NAME)

# ======================
# DISCORD CLIENT
# ======================

intents = discord.Intents.default()
intents.message_content = True
intents.reactions = True
intents.members = True

client = discord.Client(intents=intents)

# ======================
# HELPERS
# ======================

def normalize_name(s: str) -> str:
    return " ".join(s.lower().split())

def find_date_col(sheet, target_date: date, header_row=2, start_col=5):
    candidates = {
        target_date.strftime("%-m/%-d/%y"),
        target_date.strftime("%m/%d/%y"),
        target_date.strftime("%-m/%-d/%Y"),
        target_date.strftime("%m/%d/%Y"),
    }
    try:
        candidates.add(target_date.strftime("%#m/%#d/%y"))
        candidates.add(target_date.strftime("%#m/%#d/%Y"))
    except Exception:
        pass

    row_vals = sheet.row_values(header_row)
    for idx, val in enumerate(row_vals[start_col - 1:], start=start_col):
        if val.strip() in candidates:
            return idx

    raise RuntimeError(f"No date column found for {target_date}")

def load_group_mapping():
    rows = ws_mapping.get_all_values()[1:]
    groups = {}
    for r in rows:
        if len(r) < 2:
            continue
        group_name = r[0].strip()
        members = [normalize_name(x) for x in r[1].split(",") if x.strip()]
        groups[group_name] = members
    return groups

def compute_group_completion(groups, reacted_names):
    result = {}
    rows = ws_groups.get_all_values()

    for i, row in enumerate(rows[2:], start=3):
        group_name = row[0].strip()
        if not group_name or group_name not in groups:
            continue

        members = groups[group_name]
        completed = all(m in reacted_names for m in members)
        result[i] = completed

    return result

async def find_post_for_date(channel, target_date: date):
    async for msg in channel.history(limit=200):
        if msg.author.id != TARGET_AUTHOR_ID:
            continue
        if TARGET_TITLE_KEYWORD and TARGET_TITLE_KEYWORD not in (msg.content or "").lower():
            continue

        msg_date = msg.created_at.astimezone(timezone.utc).date()
        if msg_date == target_date:
            return msg

    return None

async def get_reacted_names(message):
    names = set()
    for reaction in message.reactions:
        async for user in reaction.users():
            if not user.bot:
                names.add(normalize_name(user.display_name))
    return names

# ======================
# MAIN LOGIC
# ======================

@client.event
async def on_ready():
    print(f"Logged in as {client.user}")

    guild = client.get_guild(GUILD_ID)
    channel = guild.get_channel(CHANNEL_ID)

    today = datetime.now(timezone.utc).date()
    groups = load_group_mapping()

    for offset in range(BACKFILL_GROUPS_DAYS):
        target_date = today - timedelta(days=offset)
        print(f"\nProcessing Groups for {target_date}")

        msg = await find_post_for_date(channel, target_date)
        if not msg:
            print("  No post found — skipping")
            continue

        reacted_names = await get_reacted_names(msg)
        print(f"  Reactors detected: {len(reacted_names)}")

        try:
            col = find_date_col(ws_groups, target_date)
        except RuntimeError as e:
            print(f"  {e}")
            continue

        group_completion = compute_group_completion(groups, reacted_names)

        cells = []
        for row_idx, completed in group_completion.items():
            cells.append(
                Cell(
                    row=row_idx,
                    col=col,
                    value="TRUE" if completed else "FALSE"
                )
            )

        if cells:
            ws_groups.update_cells(cells, value_input_option="USER_ENTERED")
            print(f"  Updated {len(cells)} group rows")

    print("\nBackfill complete. Shutting down.")
    await client.close()

# ======================
# RUN
# ======================

asyncio.run(client.start(DISCORD_TOKEN))

