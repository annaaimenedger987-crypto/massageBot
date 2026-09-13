import re
from contextlib import suppress
from datetime import date

from aiogram import Bot, F, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.types import (
    CallbackQuery,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)

from .notifications import notify_master
from .states import ClientBooking, ClientCancel, ClientMove
from .storage import SlotBusyError
from .texts import appointment_text
from .ui import (
    button,
    dates_keyboard,
    inline,
    money,
    reply_menu,
    services_keyboard,
    slots_keyboard,
)


def build_client_router(database, booking_service, settings, master_id):
    router = Router(name="client")
    icons = settings.get("icons", {})

    async def send_menu(target, state=None):
        if state:
            await state.clear()
        text = settings["texts"]["welcome"]
        markup = reply_menu(getattr(target.from_user, "id", 0) == master_id)
        await target.answer(text, reply_markup=markup)

    @router.message(Command("start"))
    async def start(message: Message, state: FSMContext):
        await send_menu(message, state)

    @router.message(F.text == "Записаться")
    async def start_booking(message: Message, state: FSMContext):
        await state.clear()
        services = database.services()
        if not services:
            return await message.answer(
                "Запись временно недоступна: услуги ещё не настроены."
            )
        await state.set_state(ClientBooking.service)
        await message.answer(
            "Выберите услугу:", reply_markup=services_keyboard(services, icons)
        )

    @router.callback_query(ClientBooking.service, F.data.startswith("book:service:"))
    async def choose_service(callback: CallbackQuery, state: FSMContext):
        service_id = int(callback.data.rsplit(":", 1)[1])
        service = database.service(service_id)
        if not service:
            await callback.answer("Услуга больше недоступна.", show_alert=True)
            return
        await state.update_data(service_id=service_id)
        await state.set_state(ClientBooking.date)
        await callback.message.edit_text(
            "Выберите дату:",
            reply_markup=dates_keyboard(
                booking_service.booking_dates(), "book:date", icons
            ),
        )
        await callback.answer()

    @router.callback_query(ClientBooking.date, F.data.startswith("book:date:"))
    async def choose_date(callback: CallbackQuery, state: FSMContext):
        day = date.fromisoformat(callback.data.rsplit(":", 1)[1])
        service = database.service((await state.get_data())["service_id"])
        slots = booking_service.available_slots(service, day)
        if not slots:
            await callback.answer("На эту дату свободных окон нет.", show_alert=True)
            return
        await state.update_data(date=day.isoformat())
        await state.set_state(ClientBooking.slot)
        await callback.message.edit_text(
            f"Выберите время на {day:%d.%m.%Y}:",
            reply_markup=slots_keyboard(slots, "book:slot"),
        )
        await callback.answer()

    @router.callback_query(ClientBooking.slot, F.data.startswith("book:slot:"))
    async def choose_slot(callback: CallbackQuery, state: FSMContext):
        value = callback.data.rsplit(":", 1)[1]
        data = await state.get_data()
        service = database.service(data["service_id"])
        day = date.fromisoformat(data["date"])
        if value not in [
            x.strftime("%H:%M") for x in booking_service.available_slots(service, day)
        ]:
            await callback.answer(
                "Это время уже занято. Выберите другое.", show_alert=True
            )
            return
        await state.update_data(slot=value)
        await state.set_state(ClientBooking.name)
        await callback.message.edit_text("Введите ваше имя:")
        await callback.message.answer("Ожидаю имя.", reply_markup=ReplyKeyboardRemove())
        await callback.answer()

    @router.message(ClientBooking.name)
    async def enter_name(message: Message, state: FSMContext):
        name = (message.text or "").strip()
        if len(name) < 2 or len(name) > 80:
            return await message.answer("Введите имя длиной от 2 до 80 символов.")
        await state.update_data(name=name)
        await state.set_state(ClientBooking.phone)
        await message.answer(
            "Отправьте номер телефона или введите его вручную:",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="Отправить мой номер", request_contact=True)]
                ],
                resize_keyboard=True,
                one_time_keyboard=True,
            ),
        )

    @router.message(ClientBooking.phone)
    async def enter_phone(message: Message, state: FSMContext):
        phone = (
            message.contact.phone_number
            if message.contact
            else (message.text or "").strip()
        )
        if len(re.sub(r"\D", "", phone)) < 7 or len(phone) > 40:
            return await message.answer(
                "Проверьте номер телефона и отправьте его ещё раз."
            )
        await state.update_data(phone=phone)
        data = await state.get_data()
        service = database.service(data["service_id"])
        await state.set_state(ClientBooking.confirm)
        await message.answer(
            "<b>Проверьте запись</b>\n\n"
            f"<b>Услуга:</b> {service['name']}\n"
            f"<b>Дата:</b> {date.fromisoformat(data['date']):%d.%m.%Y}\n"
            f"<b>Время:</b> {data['slot']}\n"
            f"<b>Длительность:</b> {service['duration_minutes']} мин\n"
            f"<b>Стоимость:</b> {money(service['price'])}\n"
            f"<b>Клиент:</b> {data['name']}\n"
            f"<b>Телефон:</b> {phone}\n\n"
            f"{settings['texts']['privacy']}",
            parse_mode="HTML",
            reply_markup=inline(
                [
                    [
                        button(
                            "Подтвердить запись",
                            "book:confirm",
                            "success",
                            icons.get("confirm"),
                        )
                    ],
                    [button("Отмена", "nav:menu", "danger", icons.get("cancel"))],
                ]
            ),
        )

    @router.callback_query(ClientBooking.confirm, F.data == "book:confirm")
    async def confirm_booking(callback: CallbackQuery, state: FSMContext, bot: Bot):
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
                callback.from_user.id,
                callback.from_user.username,
            )
        except SlotBusyError:
            await state.set_state(ClientBooking.date)
            await callback.message.edit_text(
                "Это время только что заняли. Выберите другую дату:",
                reply_markup=dates_keyboard(
                    booking_service.booking_dates(), "book:date", icons
                ),
            )
            return await callback.answer("Время уже занято.", show_alert=True)
        await state.clear()
        await callback.message.edit_text(
            "<b>Запись подтверждена</b>\n\n"
            f"{appointment_text(appointment)}\n\n"
            "Посмотреть, перенести или отменить её можно в разделе «Мои записи».",
            parse_mode="HTML",
        )
        await callback.message.answer(
            "Главное меню", reply_markup=reply_menu(callback.from_user.id == master_id)
        )
        await notify_master(bot, master_id, "Новая запись", appointment)
        await callback.answer()

    @router.message(F.text == "Мои записи")
    async def my_bookings(message: Message):
        items = database.client_appointments(
            message.from_user.id, booking_service.now().isoformat()
        )
        if not items:
            return await message.answer("У вас нет предстоящих записей.")
        for item in items:
            await message.answer(
                appointment_text(item),
                parse_mode="HTML",
                reply_markup=inline(
                    [
                        [button("Перенести", f"move:{item['id']}", "primary")],
                        [
                            button(
                                "Отменить",
                                f"cancel:{item['id']}",
                                "danger",
                                icons.get("cancel"),
                            )
                        ],
                    ]
                ),
            )

    @router.callback_query(F.data.startswith("cancel:"))
    async def cancel_start(callback: CallbackQuery, state: FSMContext):
        appointment_id = int(callback.data.split(":")[1])
        item = database.appointment(appointment_id)
        if (
            not item
            or item["client_telegram_id"] != callback.from_user.id
            or item["status"] != "active"
        ):
            return await callback.answer("Запись уже недоступна.", show_alert=True)
        await state.update_data(appointment_id=appointment_id)
        await state.set_state(ClientCancel.confirm)
        await callback.message.edit_reply_markup(
            reply_markup=inline(
                [
                    [
                        button(
                            "Да, отменить запись",
                            f"cancel_confirm:{appointment_id}",
                            "danger",
                        )
                    ],
                    [button("Не отменять", "nav:menu")],
                ]
            )
        )
        await callback.answer()

    @router.callback_query(ClientCancel.confirm, F.data.startswith("cancel_confirm:"))
    async def cancel_confirm(callback: CallbackQuery, state: FSMContext, bot: Bot):
        appointment_id = int(callback.data.split(":")[1])
        item = database.appointment(appointment_id)
        state_data = await state.get_data()
        if (
            not item
            or item["client_telegram_id"] != callback.from_user.id
            or state_data.get("appointment_id") != appointment_id
        ):
            return await callback.answer("Запись недоступна.", show_alert=True)
        if database.cancel(appointment_id, "client"):
            await notify_master(bot, master_id, "Клиент отменил запись", item)
        await state.clear()
        await callback.message.edit_text("Запись отменена. Время снова доступно.")
        await callback.message.answer("Главное меню", reply_markup=reply_menu(False))
        await callback.answer()

    @router.callback_query(F.data.startswith("move:"))
    async def move_start(callback: CallbackQuery, state: FSMContext):
        appointment_id = int(callback.data.split(":")[1])
        item = database.appointment(appointment_id)
        if (
            not item
            or item["client_telegram_id"] != callback.from_user.id
            or item["status"] != "active"
        ):
            return await callback.answer("Запись недоступна.", show_alert=True)
        await state.update_data(appointment_id=appointment_id)
        await state.set_state(ClientMove.date)
        await callback.message.edit_text(
            "Выберите новую дату:",
            reply_markup=dates_keyboard(
                booking_service.booking_dates(), "move_date", icons
            ),
        )
        await callback.answer()

    @router.callback_query(ClientMove.date, F.data.startswith("move_date:"))
    async def move_date(callback: CallbackQuery, state: FSMContext):
        day = date.fromisoformat(callback.data.rsplit(":", 1)[1])
        item = database.appointment((await state.get_data())["appointment_id"])
        service = database.appointment_service(item["id"])
        slots = booking_service.available_slots(service, day, item["id"])
        if not slots:
            return await callback.answer(
                "На эту дату свободных окон нет.", show_alert=True
            )
        await state.update_data(date=day.isoformat())
        await state.set_state(ClientMove.slot)
        await callback.message.edit_text(
            "Выберите новое время:", reply_markup=slots_keyboard(slots, "move_slot")
        )
        await callback.answer()

    @router.callback_query(ClientMove.slot, F.data.startswith("move_slot:"))
    async def move_slot(callback: CallbackQuery, state: FSMContext, bot: Bot):
        data = await state.get_data()
        item = database.appointment(data["appointment_id"])
        day = date.fromisoformat(data["date"])
        start_at, end_at = booking_service.make_interval(
            day, callback.data.rsplit(":", 1)[1], item["duration_minutes"]
        )
        try:
            updated = database.move(
                item["id"], start_at.isoformat(), end_at.isoformat()
            )
        except SlotBusyError:
            return await callback.answer("Время уже занято.", show_alert=True)
        await state.clear()
        await callback.message.edit_text(
            "<b>Запись перенесена</b>\n\n" + appointment_text(updated),
            parse_mode="HTML",
        )
        await callback.message.answer("Главное меню", reply_markup=reply_menu(False))
        await notify_master(bot, master_id, "Клиент перенёс запись", updated)
        await callback.answer()

    @router.message(F.text == "Услуги")
    async def show_services(message: Message):
        items = database.services()
        text = "<b>Услуги и стоимость</b>\n\n" + "\n\n".join(
            f"<b>{item['name']}</b>\n"
            f"{item['duration_minutes']} мин · {money(item['price'])}"
            for item in items
        )
        await message.answer(text, parse_mode="HTML")

    @router.message(F.text == "Контакты")
    async def show_contacts(message: Message):
        contacts = settings["contacts"]
        await message.answer(
            f"<b>{settings['business_name']}</b>\n\n"
            f"<b>Телефон:</b> {contacts['phone']}\n"
            f"<b>Адрес:</b> {contacts['address']}\n"
            f"{contacts.get('note', '')}",
            parse_mode="HTML",
        )

    @router.callback_query(F.data == "nav:menu")
    async def callback_menu(callback: CallbackQuery, state: FSMContext):
        await state.clear()
        with suppress(Exception):
            await callback.message.edit_reply_markup(reply_markup=None)
        await callback.message.answer(
            settings["texts"]["welcome"],
            reply_markup=reply_menu(callback.from_user.id == master_id),
        )
        await callback.answer()

    @router.callback_query(F.data == "nav:close")
    async def close(callback: CallbackQuery, state: FSMContext):
        await state.clear()
        with suppress(Exception):
            await callback.message.edit_reply_markup(reply_markup=None)
        await callback.answer()

    return router
