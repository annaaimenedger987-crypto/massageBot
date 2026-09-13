import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from config import DATABASE_PATH

backup_dir = Path(__file__).resolve().parents[1] / "backups"
backup_dir.mkdir(exist_ok=True)
target = backup_dir / f"bot-{datetime.now(timezone.utc):%Y%m%d-%H%M%S}.db"

with sqlite3.connect(DATABASE_PATH) as source, sqlite3.connect(target) as destination:
    source.backup(destination)

print(target)
