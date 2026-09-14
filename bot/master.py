import re
from contextlib import suppress
from datetime import date

from aiogram import Bot, F, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message, ReplyKeyboardRemove

from .booking import callback_time, parse_breaks, parse_hours
from .states import DaySettings, ManualBooking
from .storage import SlotBusyError
from .texts import appointment_text
from .ui import (
    button,
    dates_keyboard,
    inline,
    reply_menu,
    services_keyboard,
    slots_keyboard,
)


def build_master_router(database, booking_service, settings, master_id):
    router = Router(name="master")
    icons = settings.get("icons", {})

    def allowed(event):
        return event.from_user and event.from_user.id == master_id

    async def deny(event):
        if isinstance(event, CallbackQuery):
            await event.answer("Недоступно.", show_alert=True)

    def panel():
        return inline(
            [
                [button("Предстоящие записи", "master:list", "primary")],
                [button("Добавить запись вручную", "manual:start", "success")],
                [button("Настроить день", "day:start", "primary")],
                [button("Режим клиента", "master:client")],
            ]
        )

    @router.message(F.from_user.id == master_id, Command("start", "menu", "cancel"))
    async def master_menu(message: Message, state: FSMContext):
        await state.clear()
        await message.answer(
            settings["texts"]["welcome"], reply_markup=reply_menu(True)
        )

    @router.message(Command("admin"))
    @router.message(F.text == "Панель мастера")
    async def show_panel(message: Message, state: FSMContext):
        if not allowed(message):
            return
        await state.clear()
        await message.answer(
            "<b>Панель мастера</b>", parse_mode="HTML", reply_markup=panel()
        )

    @router.callback_query(F.data == "master:panel")
    async def callback_panel(callback: CallbackQuery, state: FSMContext):
        if not allowed(callback):
            return await deny(callback)
        await state.clear()
        await callback.message.edit_text(
            "<b>Панель мастера</b>", parse_mode="HTML", reply_markup=panel()
        )
        await callback.answer()

    @router.callback_query(F.data == "master:client")
    async def client_mode(callback: CallbackQuery, state: FSMContext):
        if not allowed(callback):
            return await deny(callback)
        await state.clear()
        await callback.message.answer(
            settings["texts"]["welcome"], reply_markup=reply_menu(True)
        )
        await callback.answer()

    @router.callback_query(F.data == "master:list")
    async def list_appointments(callback: CallbackQuery):
        if not allowed(callback):
            return await deny(callback)
        items = database.upcoming(booking_service.now().isoformat())
        if not items:
            await callback.message.edit_text(
                "Предстоящих записей нет.", reply_markup=panel()
            )
        else:
            await callback.message.edit_text(
                "<b>Предстоящие записи</b>",
                parse_mode="HTML",
                reply_markup=inline([[button("Назад", "master:panel")]]),
            )
            for item in items:
                await callback.message.answer(
                    appointment_text(item, owner=True),
                    parse_mode="HTML",
                    reply_markup=inline(
                        [
                            [
                                button(
                                    "Отменить запись",
                                    f"master_cancel:{item['id']}",
                                    "danger",
                                )
                            ]
                        ]
                    ),
                )
        await callback.answer()

    @router.callback_query(F.data.startswith("master_cancel:"))
    async def cancel_appointment(callback: CallbackQuery, bot: Bot):
        if not allowed(callback):
            return await deny(callback)
        appointment_id = int(callback.data.split(":")[1])
        item = database.appointment(appointment_id)
        if not item or item["status"] != "active":
            return await callback.answer("Запись уже недоступна.", show_alert=True)
        database.cancel(appointment_id, "master")
        await callback.message.edit_text(
            "<b>Запись отменена мастером</b>\n\n" + appointment_text(item, owner=True),
            parse_mode="HTML",
        )
        if item["client_telegram_id"]:
            with suppress(Exception):
                await bot.send_message(
                    item["client_telegram_id"],
                    "<b>Мастер отменил запись</b>\n\n" + appointment_text(item),
                    parse_mode="HTML",
                )
        await callback.answer("Время освобождено.")

    @router.callback_query(F.data == "manual:start")
    async def manual_start(callback: CallbackQuery, state: FSMContext):
        if not allowed(callback):
            return await deny(callback)
        await state.clear()
        await state.set_state(ManualBooking.service)
        await callback.message.edit_text(
            "Выберите услугу для ручной записи:",
            reply_markup=services_keyboard(database.services(), icons),
        )
        await callback.answer()

    @router.callback_query(ManualBooking.service, F.data.startswith("book:service:"))
    async def manual_service(callback: CallbackQuery, state: FSMContext):
        if not allowed(callback):
            return await deny(callback)
        service_id = int(callback.data.rsplit(":", 1)[1])
        if not database.service(service_id):
            return await callback.answer("Услуга недоступна.", show_alert=True)
        await state.update_data(service_id=service_id)
        await state.set_state(ManualBooking.date)
        await callback.message.edit_text(
            "Выберите дату:",
            reply_markup=dates_keyboard(
                booking_service.booking_dates(), "manual:date", icons
            ),
        )
        await callback.answer()

    @router.callback_query(ManualBooking.date, F.data.startswith("manual:date:"))
    async def manual_date(callback: CallbackQuery, state: FSMContext):
        if not allowed(callback):
            return await deny(callback)
        day = date.fromisoformat(callback.data.rsplit(":", 1)[1])
        service = database.service((await state.get_data())["service_id"])
        slots = booking_service.available_slots(service, day)
        if not slots:
            return await callback.answer("Свободных окон нет.", show_alert=True)
        await state.update_data(date=day.isoformat())
        await state.set_state(ManualBooking.slot)
        await callback.message.edit_text(
            "Выберите время:", reply_markup=slots_keyboard(slots, "manual:slot")
        )
        await callback.answer()

    @router.callback_query(ManualBooking.slot, F.data.startswith("manual:slot:"))
    async def manual_slot(callback: CallbackQuery, state: FSMContext):
        if not allowed(callback):
            return await deny(callback)
        await state.update_data(slot=callback_time(callback.data, "manual:slot:"))
        await state.set_state(ManualBooking.name)
        await callback.message.edit_text("Введите имя клиента:")
        await callback.message.answer("Ожидаю имя.", reply_markup=ReplyKeyboardRemove())
        await callback.answer()

    @router.message(ManualBooking.name)
    async def manual_name(message: Message, state: FSMContext):
        if not allowed(message):
            return
        name = (message.text or "").strip()
        if len(name) < 2 or len(name) > 80:
            return await message.answer("Введите имя длиной от 2 до 80 символов.")
        await state.update_data(name=name)
        await state.set_state(ManualBooking.phone)
        await message.answer("Введите телефон клиента:")

    @router.message(ManualBooking.phone)
    async def manual_phone(message: Message, state: FSMContext):
        if not allowed(message):
            return
        phone = (message.text or "").strip()
        if len(re.sub(r"\D", "", phone)) < 7 or len(phone) > 40:
            return await message.answer("Проверьте номер телефона.")
        await state.update_data(phone=phone)
        data = await state.get_data()
        service = database.service(data["service_id"])
        await state.set_state(ManualBooking.confirm)
        await message.answer(
            f"<b>Добавить запись?</b>\n\n"
            f"{service['name']}\n"
            f"{date.fromisoformat(data['date']):%d.%m.%Y} в {data['slot']}\n"
            f"{data['name']} · {phone}",
            parse_mode="HTML",
            reply_markup=inline(
                [
                    [button("Добавить запись", "manual:confirm", "success")],
                    [button("Отмена", "master:panel", "danger")],
                ]
            ),
        )

    @router.callback_query(ManualBooking.confirm, F.data == "manual:confirm")
    async def manual_confirm(callback: CallbackQuery, state: FSMContext):
        if not allowed(callback):
            return await deny(callback)
        data = await state.get_data()
        service = database.service(data["service_id"])
        day = date.fromisoformat(data["date"])
        start_at, end_at = booking_service.make_interval(
            day, data["slot"], service["duration_minutes"]
        )
        try:
            appointment = database.create_appointment(
                service,
                start_at.isoformat(),
                end_at.isoformat(),
                data["name"],
                data["phone"],
                source="manual",
                manual=True,
            )
        except SlotBusyError:
            return await callback.answer("Время уже занято.", show_alert=True)
        await state.clear()
        await callback.message.edit_text(
            "<b>Запись добавлена</b>\n\n" + appointment_text(appointment, owner=True),
            parse_mode="HTML",
            reply_markup=inline([[button("Панель мастера", "master:panel")]]),
        )
        await callback.answer("Слот закрыт для онлайн-записи.")

    @router.callback_query(F.data == "day:start")
    async def day_start(callback: CallbackQuery, state: FSMContext):
        if not allowed(callback):
            return await deny(callback)
        await state.clear()
        await state.set_state(DaySettings.date)
        await callback.message.edit_text(
            "Выберите дату:",
            reply_markup=dates_keyboard(
                booking_service.booking_dates(), "day:date", icons
            ),
        )
        await callback.answer()

    @router.callback_query(DaySettings.date, F.data.startswith("day:date:"))
    async def day_date(callback: CallbackQuery, state: FSMContext):
        if not allowed(callback):
            return await deny(callback)
        value = callback.data.rsplit(":", 1)[1]
        await state.update_data(date=value)
        await state.set_state(DaySettings.action)
        await callback.message.edit_text(
            f"Настройка {date.fromisoformat(value):%d.%m.%Y}:",
            reply_markup=inline(
                [
                    [button("Изменить рабочие часы", "day:hours", "primary")],
                    [button("Добавить перерывы", "day:breaks", "primary")],
                    [button("Сделать выходным", "day:off", "danger")],
                    [button("Вернуть обычное расписание", "day:default")],
                    [button("Назад", "master:panel")],
                ]
            ),
        )
        await callback.answer()

    @router.callback_query(DaySettings.action, F.data == "day:off")
    async def day_off(callback: CallbackQuery, state: FSMContext):
        value = (await state.get_data())["date"]
        if database.active_on_date(value):
            return await callback.answer(
                "На этот день уже есть записи. Сначала перенесите или отмените их.",
                show_alert=True,
            )
        database.set_override(value, False)
        await state.clear()
        await callback.message.edit_text("День отмечен выходным.", reply_markup=panel())
        await callback.answer()

    @router.callback_query(DaySettings.action, F.data == "day:default")
    async def day_default(callback: CallbackQuery, state: FSMContext):
        value = (await state.get_data())["date"]
        database.clear_override(value)
        await state.clear()
        await callback.message.edit_text(
            "Обычное расписание восстановлено.", reply_markup=panel()
        )
        await callback.answer()

    @router.callback_query(DaySettings.action, F.data == "day:hours")
    async def day_hours_start(callback: CallbackQuery, state: FSMContext):
        await state.set_state(DaySettings.hours)
        await callback.message.edit_text("Введите рабочие часы, например: 10:00-18:30")
        await callback.answer()

    @router.message(DaySettings.hours)
    async def day_hours_save(message: Message, state: FSMContext):
        if not allowed(message):
            return
        try:
            start, end = parse_hours(message.text or "")
        except (ValueError, TypeError):
            return await message.answer("Формат не распознан. Пример: 10:00-18:30")
        value = (await state.get_data())["date"]
        if database.active_on_date(value):
            return await message.answer(
                "На этот день уже есть записи. Сначала перенесите или отмените их."
            )
        database.set_override(value, True, start, end, [])
        await state.clear()
        await message.answer("Рабочие часы сохранены.", reply_markup=reply_menu(True))
        await message.answer(
            "<b>Панель мастера</b>", parse_mode="HTML", reply_markup=panel()
        )

    @router.callback_query(DaySettings.action, F.data == "day:breaks")
    async def day_breaks_start(callback: CallbackQuery, state: FSMContext):
        value = (await state.get_data())["date"]
        rules = booking_service.day_rules(date.fromisoformat(value))
        if not rules:
            return await callback.answer(
                "Сначала задайте рабочие часы для этого дня.", show_alert=True
            )
        await state.update_data(start=rules["start"], end=rules["end"])
        await state.set_state(DaySettings.breaks)
        await callback.message.edit_text(
            "Введите перерывы: 13:00-14:00 или несколько через запятую.\n"
            "Чтобы убрать перерывы, отправьте: нет"
        )
        await callback.answer()

    @router.message(DaySettings.breaks)
    async def day_breaks_save(message: Message, state: FSMContext):
        if not allowed(message):
            return
        try:
            breaks = parse_breaks(message.text or "")
        except (ValueError, TypeError):
            return await message.answer(
                "Формат не распознан. Пример: 13:00-14:00, 16:00-16:30"
            )
        data = await state.get_data()
        if database.active_on_date(data["date"]):
            return await message.answer(
                "На этот день уже есть записи. Сначала перенесите или отмените их."
            )
        if any(item[0] < data["start"] or item[1] > data["end"] for item in breaks):
            return await message.answer(
                "Перерыв должен находиться внутри рабочих часов."
            )
        database.set_override(data["date"], True, data["start"], data["end"], breaks)
        await state.clear()
        await message.answer("Перерывы сохранены.", reply_markup=reply_menu(True))
        await message.answer(
            "<b>Панель мастера</b>", parse_mode="HTML", reply_markup=panel()
        )

    return router
