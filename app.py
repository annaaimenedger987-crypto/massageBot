import asyncio
import json
import logging
from contextlib import suppress

from aiogram import Bot, Dispatcher
from aiogram.types import BotCommand, ErrorEvent

from bot.booking import BookingService
from bot.client import build_client_router
from bot.master import build_master_router
from bot.notifications import reminder_loop
from bot.storage import Database
from config import BOT_TOKEN, DATABASE_PATH, MASTER_ID, ROOT, SETTINGS


def validate_configuration():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN не задан в переменных окружения")
    if not MASTER_ID:
        raise RuntimeError("MASTER_ID не задан в переменных окружения")
    if SETTINGS.get("slot_step_minutes", 0) <= 0:
        raise RuntimeError("slot_step_minutes должен быть больше нуля")


async def main():
    validate_configuration()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    database = Database(DATABASE_PATH)
    with (ROOT / "settings" / "services.json").open(encoding="utf-8") as stream:
        database.seed_services(json.load(stream))
    booking_service = BookingService(database, SETTINGS)
    bot = Bot(BOT_TOKEN)
    dispatcher = Dispatcher()
    dispatcher.include_router(
        build_master_router(database, booking_service, SETTINGS, MASTER_ID)
    )
    dispatcher.include_router(
        build_client_router(database, booking_service, SETTINGS, MASTER_ID)
    )

    @dispatcher.errors()
    async def handle_error(event: ErrorEvent):
        logging.getLogger("softflow").error(
            "Необработанная ошибка обновления",
            exc_info=(
                type(event.exception),
                event.exception,
                event.exception.__traceback__,
            ),
        )
        update = event.update
        target = (
            update.callback_query.message if update.callback_query else update.message
        )
        if target:
            with suppress(Exception):
                await target.answer(
                    "Не удалось выполнить действие. Данные не потеряны — вернитесь в меню и попробуйте ещё раз."
                )
        with suppress(Exception):
            await bot.send_message(
                MASTER_ID,
                "В работе бота возникла техническая ошибка. Подробности сохранены в журнале сервера.",
            )
        return True

    identity = await bot.get_me()
    await bot.set_my_commands(
        [
            BotCommand(command="start", description="Открыть главное меню"),
            BotCommand(command="menu", description="Вернуться в главное меню"),
            BotCommand(command="cancel", description="Отменить текущее действие"),
        ]
    )
    logging.getLogger("softflow").info("Запущен бот @%s", identity.username)
    reminders = asyncio.create_task(
        reminder_loop(bot, database, booking_service, SETTINGS)
    )
    try:
        await dispatcher.start_polling(bot, close_bot_session=False)
    finally:
        reminders.cancel()
        with suppress(asyncio.CancelledError):
            await reminders
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
