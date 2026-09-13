import logging
from contextlib import suppress
from datetime import timedelta

from aiogram.exceptions import TelegramAPIError

from .texts import appointment_text


async def notify_master(bot, master_id, title, appointment):
    with suppress(Exception):
        await bot.send_message(
            master_id,
            f"<b>{title}</b>\n\n{appointment_text(appointment, owner=True)}",
            parse_mode="HTML",
        )


async def reminder_loop(bot, database, booking_service, settings):
    import asyncio

    while True:
        current = booking_service.now()
        thresholds = sorted(
            {int(value) for value in settings.get("reminder_hours", [24])},
            reverse=True,
        )
        for index, hours in enumerate(thresholds):
            lower_hours = thresholds[index + 1] if index + 1 < len(thresholds) else 0
            notification_type = f"client_{hours}h"
            due = database.due_reminders(
                (current + timedelta(hours=lower_hours)).isoformat(),
                (current + timedelta(hours=int(hours))).isoformat(),
                notification_type,
            )
            for appointment in due:
                try:
                    await bot.send_message(
                        appointment["client_telegram_id"],
                        f"<b>Напоминание о записи</b>\n\n"
                        f"До визита осталось около {hours} ч.\n\n"
                        f"{appointment_text(appointment)}",
                        parse_mode="HTML",
                    )
                except TelegramAPIError:
                    logging.getLogger("softflow.reminders").exception(
                        "Не удалось отправить напоминание для записи %s",
                        appointment["id"],
                    )
                    continue
                database.mark_notification(
                    appointment["id"], notification_type, current.isoformat()
                )
        await asyncio.sleep(300)
