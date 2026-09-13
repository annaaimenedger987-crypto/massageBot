from datetime import datetime

from .ui import money


def appointment_text(row, owner=False):
    start = datetime.fromisoformat(row["start_at"])
    text = (
        f"<b>Запись {row['public_code']}</b>\n\n"
        f"<b>Услуга:</b> {row['service_name']}\n"
        f"<b>Дата:</b> {start:%d.%m.%Y}\n"
        f"<b>Время:</b> {start:%H:%M}\n"
        f"<b>Длительность:</b> {row['duration_minutes']} мин\n"
        f"<b>Стоимость:</b> {money(row['price'])}"
    )
    if owner:
        username = (
            f"@{row['client_username']}" if row["client_username"] else "не указан"
        )
        source = "внесена мастером" if row["created_by_master"] else "Telegram"
        text += (
            f"\n<b>Клиент:</b> {row['client_name']}\n"
            f"<b>Телефон:</b> {row['client_phone']}\n"
            f"<b>Telegram:</b> {username}\n"
            f"<b>Источник:</b> {source}"
        )
    return text
