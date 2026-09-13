import json
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
MASTER_ID = int(os.getenv("MASTER_ID", "0"))
DATABASE_PATH = Path(os.getenv("DATABASE_PATH", str(ROOT / "data" / "bot.db")))
SETTINGS_PATH = Path(os.getenv("SETTINGS_PATH", str(ROOT / "settings" / "master.json")))


def load_settings() -> dict:
    with SETTINGS_PATH.open(encoding="utf-8") as stream:
        return json.load(stream)


SETTINGS = load_settings()
TIMEZONE = SETTINGS.get("timezone", "Europe/Minsk")
