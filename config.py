import os

BOT_TOKEN = os.getenv("BOT_TOKEN")
MASTER_ID = int(os.getenv("MASTER_ID", "0"))
TIMEZONE = os.getenv("TIMEZONE", "Europe/Minsk")