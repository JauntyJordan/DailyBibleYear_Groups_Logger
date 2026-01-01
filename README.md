# Bible In A Year – Groups Logger

This bot is a **scheduled checker** (designed for GitHub Actions) that:

1. Finds **today's** Bible-in-a-Year post in a specific Discord channel (authored by a specific user).
2. Reads the users who reacted with a target emoji (default ✅).
3. Updates a Google Sheet:
   - **Individuals** tab: marks TRUE/FALSE per member for today's date column.
   - **Groups** tab: marks TRUE only when every member in that group's roster reacted.
4. Posts a status summary message to a Discord status channel.

## Required GitHub Secrets

- `DISCORD_TOKEN`
- `GOOGLE_CREDS_JSON` (service account JSON)
- `SHEET_NAME`
- `TRACK_CHANNEL_ID`
- `REQUIRE_AUTHOR_ID`
- `TITLE_MATCH`
- `TRACK_EMOJI` (optional; default ✅)
- `STATUS_CHANNEL_ID` (where the bot posts its summary)

## Sheet layout expectations

- Row 1 in **Individuals** and **Groups** contains date headers (e.g., `1/1/2026` or `2026-01-01`).
- **Individuals** tab: Column A is member labels.
- **Member Mapping** tab: Column A is Discord user IDs, Column B is the matching Individuals label.
- **Groups** tab: Column A is group name, Column B is comma-separated roster of Individuals labels.

## Running locally

Create a `.env` file (do not commit it), then:

```bash
python -m pip install -r requirements.txt
python main.py
```

Set `DRY_RUN=1` to validate without writing to the sheet.

