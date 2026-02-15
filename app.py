import os
import sys
import json
import asyncio
from datetime import datetime, timedelta, time

from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message,
    ReplyKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardRemove,
)
from aiogram.filters import Command
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage

from config import BOT_TOKEN, MASTER_ID


# =========================
# 0) Защита от второго запуска
# =========================
LOCK_FILE = "bot.lock"
if os.path.exists(LOCK_FILE):
    print("❌ Бот уже запущен. Закрой прошлый запуск (терминал) и попробуй снова.")
    sys.exit(1)

with open(LOCK_FILE, "w", encoding="utf-8") as f:
    f.write("locked")


# =========================
# 1) База расписания
# =========================
BASE_START = time(8, 0)
BASE_END = time(20, 0)
STEP_MIN = 30  # шаг слотов (30 минут)

# Данные, которые будут сохраняться
services = []       # [{"name":"Массаж","price":80,"duration":60}, ...]
overrides = {}      # {"2026-02-15": None | ["10:00","10:30"...]}
appointments = {}   # {"2026-02-15": [ {booking}, {booking} ]}
contacts = {"phone": "", "address": ""}
import os
DATA_FILE = os.path.join(os.getcwd(), "data.json")


# =========================
# 2) Сохранение/загрузка данных
# =========================
def save_data():
    data = {
        "services": services,
        "overrides": overrides,
        "appointments": appointments,
        "contacts": contacts,
    }
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def load_data():
    global services, overrides, appointments, contacts

    if not os.path.exists(DATA_FILE):
        # первый запуск — создаём пустой файл
        save_data()
        return

    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        services = data.get("services", [])
        overrides = data.get("overrides", {})
        appointments = data.get("appointments", {})
        contacts = data.get("contacts", {"phone": "", "address": ""})
    except Exception:
        # если файл сломан — не падаем
        services = []
        overrides = {}
        appointments = {}
        contacts = {"phone": "", "address": ""}


# =========================
# 3) Кнопки
# =========================
BACK_TO_MENU = "⬅️ В меню"
BACK_TO_DATES = "⬅️ К датам"
CANCEL = "❌ Отмена"

ADMIN_RECORDS_TODAY = "📋 Записи: сегодня"
ADMIN_RECORDS_TOM = "📋 Записи: завтра"
ADMIN_RECORDS_ALL = "📋 Записи: все"
ADMIN_DELETE = "🗑 Удалить запись"
ADMIN_FREE = "🕒 Свободные окна"
ADMIN_CONTACTS = "📍 Контакты (настроить)"

client_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📅 Записаться")],
        [KeyboardButton(text="💆‍♀️ Услуги и цены")],
        [KeyboardButton(text="📍 Контакты")],
    ],
    resize_keyboard=True,
)

admin_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📅 Управление расписанием")],
        [KeyboardButton(text=ADMIN_RECORDS_TODAY), KeyboardButton(text=ADMIN_RECORDS_TOM)],
        [KeyboardButton(text=ADMIN_RECORDS_ALL)],
        [KeyboardButton(text=ADMIN_DELETE), KeyboardButton(text=ADMIN_FREE)],
        [KeyboardButton(text=ADMIN_CONTACTS)],
        [KeyboardButton(text="💆‍♀️ Услуги и цены")],
    ],
    resize_keyboard=True,
)


# =========================
# 4) FSM состояния
# =========================
class AdminSchedule(StatesGroup):
    pick_date = State()
    pick_action = State()
    manual_hours = State()

class Booking(StatesGroup):
    pick_service = State()
    pick_date = State()
    pick_time = State()
    enter_name = State()
    enter_phone = State()

class AdminDelete(StatesGroup):
    pick_date = State()
    pick_booking = State()

class AdminFree(StatesGroup):
    pick_service = State()
    pick_date = State()

class AdminContacts(StatesGroup):
    phone = State()
    address = State()


# =========================
# 5) Утилиты времени и блокировок
# =========================
def gen_times(start_t: time, end_t: time, step_min: int = STEP_MIN):
    res = []
    cur = datetime.combine(datetime.today(), start_t)
    end = datetime.combine(datetime.today(), end_t)
    while cur < end:
        res.append(cur.strftime("%H:%M"))
        cur += timedelta(minutes=step_min)
    return res

def next_14_days():
    today = datetime.today().date()
    return [(today + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(14)]

def day_times(date_str: str):
    if date_str in overrides:
        return overrides[date_str]  # None или список
    return gen_times(BASE_START, BASE_END, STEP_MIN)

def parse_ranges(text: str):
    # "10-12" или "10-12, 16-18"
    result = []
    for part in text.split(","):
        part = part.strip()
        start_h, end_h = part.split("-")
        start = time(int(start_h), 0)
        end = time(int(end_h), 0)
        result.extend(gen_times(start, end, STEP_MIN))
    return sorted(list(set(result)))

def duration_to_slots(duration_min: int):
    return duration_min // STEP_MIN

def build_block(start_time: str, duration_min: int):
    slots_needed = duration_to_slots(duration_min)
    h, m = map(int, start_time.split(":"))
    cur = datetime.combine(datetime.today(), time(h, m))
    block = []
    for _ in range(slots_needed):
        block.append(cur.strftime("%H:%M"))
        cur += timedelta(minutes=STEP_MIN)
    return block

def get_busy_slots(date_str: str):
    # занятые слоты считаем из записей
    busy = set()
    for b in appointments.get(date_str, []):
        for t in b.get("block", []):
            busy.add(t)
    return busy

def available_start_times_for_service(date_str: str, duration_min: int):
    times = day_times(date_str)
    if times is None:
        return []

    times_set = set(times)
    busy = get_busy_slots(date_str)

    res = []
    for t in times:
        block = build_block(t, duration_min)
        # блок должен полностью существовать в расписании дня
        if not all(x in times_set for x in block):
            continue
        # блок должен быть свободен
        if any(x in busy for x in block):
            continue
        res.append(t)

    return res

def fmt_date(date_str: str):
    # для красоты: 2026-02-15 -> 15.02.2026
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d").date()
        return d.strftime("%d.%m.%Y")
    except Exception:
        return date_str


# =========================
# 6) Dispatcher
# =========================
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# загрузка данных при старте файла
load_data()


# =========================
# 7) /start
# =========================
@dp.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    if message.from_user.id == MASTER_ID:
        await message.answer("Админ-режим ⚙️", reply_markup=admin_kb)
    else:
        await message.answer("Я бот онлайн-записи 💫", reply_markup=client_kb)


# =========================
# 8) Клиент: контакты / услуги
# =========================
@dp.message(F.text == "📍 Контакты")
async def client_contacts(message: Message):
    phone = contacts.get("phone", "")
    address = contacts.get("address", "")
    text = "📍 Контакты мастера:\n"
    text += f"📞 Телефон: {phone if phone else 'не указан'}\n"
    text += f"🏠 Адрес: {address if address else 'не указан'}\n"
    await message.answer(text)

@dp.message(F.text == "💆‍♀️ Услуги и цены")
async def show_services(message: Message):
    if not services:
        await message.answer("Пока нет добавленных услуг.")
        return
    text = "💆‍♀️ Услуги и цены:\n\n"
    for i, s in enumerate(services, 1):
        text += f"{i}) {s['name']} — {s['price']} BYN — {s['duration']} мин\n"
    await message.answer(text)


# =========================
# 9) Админ: настройка контактов
# =========================
@dp.message(F.text == ADMIN_CONTACTS)
async def admin_contacts_start(message: Message, state: FSMContext):
    if message.from_user.id != MASTER_ID:
        return
    await message.answer(
        "Введите телефон (как хочешь показывать клиенту), например: +375 29 ...\n\n"
        f"Текущий: {contacts.get('phone','') or 'не указан'}",
        reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state(AdminContacts.phone)

@dp.message(AdminContacts.phone)
async def admin_contacts_phone(message: Message, state: FSMContext):
    contacts["phone"] = message.text.strip()
    save_data()
    await message.answer(
        "Теперь введи адрес (или просто город/район), как будет удобно клиенту.\n\n"
        f"Текущий: {contacts.get('address','') or 'не указан'}"
    )
    await state.set_state(AdminContacts.address)

@dp.message(AdminContacts.address)
async def admin_contacts_address(message: Message, state: FSMContext):
    contacts["address"] = message.text.strip()
    save_data()
    await state.clear()
    await message.answer("✅ Контакты сохранены.", reply_markup=admin_kb)


# =========================
# 10) Админ: расписание
# =========================
@dp.message(F.text == "📅 Управление расписанием")
async def admin_schedule_start(message: Message, state: FSMContext):
    if message.from_user.id != MASTER_ID:
        return

    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=d)] for d in next_14_days()] + [[KeyboardButton(text=BACK_TO_MENU)]],
        resize_keyboard=True
    )
    await message.answer("📅 Выберите дату (14 дней вперёд):", reply_markup=kb)
    await state.set_state(AdminSchedule.pick_date)

@dp.message(AdminSchedule.pick_date)
async def admin_pick_date(message: Message, state: FSMContext):
    if message.text == BACK_TO_MENU:
        await state.clear()
        await message.answer("Админ-меню ⚙️", reply_markup=admin_kb)
        return

    date_str = message.text.strip()
    if date_str not in next_14_days():
        await message.answer("Выберите дату кнопкой.")
        return

    await state.update_data(date=date_str)

    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="⏰ Задать часы вручную")],
            [KeyboardButton(text="🚫 Сделать выходным")],
            [KeyboardButton(text="🔄 Вернуть стандарт (08–20)")],
            [KeyboardButton(text=BACK_TO_DATES)],
            [KeyboardButton(text=BACK_TO_MENU)],
        ],
        resize_keyboard=True
    )
    await message.answer(f"Дата: {fmt_date(date_str)}\nЧто сделать?", reply_markup=kb)
    await state.set_state(AdminSchedule.pick_action)

@dp.message(AdminSchedule.pick_action, F.text == BACK_TO_DATES)
async def admin_back_to_dates(message: Message, state: FSMContext):
    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=d)] for d in next_14_days()] + [[KeyboardButton(text=BACK_TO_MENU)]],
        resize_keyboard=True
    )
    await message.answer("📅 Выберите дату:", reply_markup=kb)
    await state.set_state(AdminSchedule.pick_date)

@dp.message(AdminSchedule.pick_action, F.text == BACK_TO_MENU)
async def admin_back_to_menu(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Админ-меню ⚙️", reply_markup=admin_kb)

@dp.message(AdminSchedule.pick_action, F.text == "🚫 Сделать выходным")
async def admin_make_day_off(message: Message, state: FSMContext):
    data = await state.get_data()
    date_str = data["date"]
    overrides[date_str] = None
    save_data()
    await message.answer(f"✅ {fmt_date(date_str)} — выходной.", reply_markup=ReplyKeyboardRemove())
    await admin_back_to_dates(message, state)

@dp.message(AdminSchedule.pick_action, F.text == "🔄 Вернуть стандарт (08–20)")
async def admin_restore_default(message: Message, state: FSMContext):
    data = await state.get_data()
    date_str = data["date"]
    overrides.pop(date_str, None)
    save_data()
    await message.answer(f"✅ {fmt_date(date_str)} — вернули стандарт 08:00–20:00.", reply_markup=ReplyKeyboardRemove())
    await admin_back_to_dates(message, state)

@dp.message(AdminSchedule.pick_action, F.text == "⏰ Задать часы вручную")
async def admin_manual_hours_start(message: Message, state: FSMContext):
    await message.answer(
        "Введите часы диапазонами:\n"
        "10-12\n"
        "или\n"
        "10-12, 16-18\n\n"
        f"Шаг {STEP_MIN} минут.",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text=BACK_TO_DATES)], [KeyboardButton(text=BACK_TO_MENU)]],
            resize_keyboard=True
        )
    )
    await state.set_state(AdminSchedule.manual_hours)

@dp.message(AdminSchedule.manual_hours)
async def admin_manual_hours_save(message: Message, state: FSMContext):
    if message.text == BACK_TO_DATES:
        await admin_back_to_dates(message, state)
        return
    if message.text == BACK_TO_MENU:
        await admin_back_to_menu(message, state)
        return

    data = await state.get_data()
    date_str = data["date"]

    try:
        times = parse_ranges(message.text)
    except Exception:
        await message.answer("❌ Формат неверный. Пример: 10-12, 16-18")
        return

    overrides[date_str] = times
    save_data()

    await message.answer(f"✅ Часы на {fmt_date(date_str)} сохранены.", reply_markup=ReplyKeyboardRemove())
    await admin_back_to_dates(message, state)


# =========================
# 11) Клиент: запись (услуга -> дата -> время -> имя -> телефон)
# =========================
@dp.message(F.text == "📅 Записаться")
async def booking_start(message: Message, state: FSMContext):
    if not services:
        await message.answer("Пока нет услуг. Мастер ещё не добавил услуги.")
        return

    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=f"{i}) {s['name']} ({s['duration']} мин)")] for i, s in enumerate(services, 1)]
                + [[KeyboardButton(text=CANCEL)]],
        resize_keyboard=True
    )
    await message.answer("Выберите услугу:", reply_markup=kb)
    await state.set_state(Booking.pick_service)

@dp.message(Booking.pick_service)
async def booking_pick_service(message: Message, state: FSMContext):
    if message.text == CANCEL:
        await state.clear()
        await message.answer("Ок 🙂", reply_markup=client_kb)
        return

    try:
        idx = int(message.text.split(")")[0]) - 1
        service = services[idx]
    except Exception:
        await message.answer("Выберите услугу кнопкой.")
        return

    await state.update_data(service=service)

    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=d)] for d in next_14_days()] + [[KeyboardButton(text=CANCEL)]],
        resize_keyboard=True
    )
    await message.answer("Выберите дату:", reply_markup=kb)
    await state.set_state(Booking.pick_date)

@dp.message(Booking.pick_date)
async def booking_pick_date(message: Message, state: FSMContext):
    if message.text == CANCEL:
        await state.clear()
        await message.answer("Ок 🙂", reply_markup=client_kb)
        return

    date_str = message.text.strip()
    if date_str not in next_14_days():
        await message.answer("Выберите дату кнопкой.")
        return

    data = await state.get_data()
    service = data["service"]
    duration = service["duration"]

    # выходной
    if day_times(date_str) is None:
        await message.answer("🚫 В этот день мастер не работает. Выберите другую дату.")
        return

    starts = available_start_times_for_service(date_str, duration)
    if not starts:
        await message.answer("На этот день нет свободных окон под выбранную услугу. Выберите другую дату.")
        return

    await state.update_data(date=date_str)

    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=t)] for t in starts] + [[KeyboardButton(text=CANCEL)]],
        resize_keyboard=True
    )
    await message.answer("Выберите время:", reply_markup=kb)
    await state.set_state(Booking.pick_time)

@dp.message(Booking.pick_time)
async def booking_pick_time(message: Message, state: FSMContext):
    if message.text == CANCEL:
        await state.clear()
        await message.answer("Ок 🙂", reply_markup=client_kb)
        return

    data = await state.get_data()
    service = data["service"]
    date_str = data["date"]
    duration = service["duration"]
    start_time = message.text.strip()

    # финальная проверка (на случай, если кто-то занял время секунду назад)
    starts = available_start_times_for_service(date_str, duration)
    if start_time not in starts:
        await message.answer("Это время уже заняли 😿 Выберите другое время.")
        return

    await state.update_data(time=start_time)
    await message.answer("Введите ваше имя:", reply_markup=ReplyKeyboardRemove())
    await state.set_state(Booking.enter_name)

@dp.message(Booking.enter_name)
async def booking_enter_name(message: Message, state: FSMContext):
    name = message.text.strip()
    if len(name) < 2:
        await message.answer("Имя слишком короткое. Введите ещё раз:")
        return
    await state.update_data(name=name)
    await message.answer("Введите телефон (например +375...):")
    await state.set_state(Booking.enter_phone)

@dp.message(Booking.enter_phone)
async def booking_enter_phone(message: Message, state: FSMContext):
    phone = message.text.strip()
    if len(phone) < 6:
        await message.answer("Телефон выглядит странно. Введите ещё раз:")
        return

    data = await state.get_data()
    service = data["service"]
    date_str = data["date"]
    start_time = data["time"]
    name = data["name"]

    block = build_block(start_time, service["duration"])

    # записываем
    booking = {
        "id": int(datetime.now().timestamp() * 1000),  # уникальный id
        "time": start_time,
        "name": name,
        "phone": phone,
        "service": service["name"],
        "duration": service["duration"],
        "price": service["price"],
        "block": block,
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    appointments.setdefault(date_str, []).append(booking)
    # сортируем записи по времени внутри дня
    appointments[date_str] = sorted(appointments[date_str], key=lambda x: x["time"])
    save_data()

    # клиент
    await message.answer(
        "✅ Вы записаны!\n"
        f"Услуга: {booking['service']}\n"
        f"Дата: {fmt_date(date_str)}\n"
        f"Время: {booking['time']}\n"
        f"Длительность: {booking['duration']} мин\n"
        f"Цена: {booking['price']} BYN\n\n"
        "Если нужно — мастер свяжется 💛",
        reply_markup=client_kb
    )

    # мастер
    try:
        bot = Bot(BOT_TOKEN)
        await bot.send_message(
            MASTER_ID,
            "📌 Новая запись!\n"
            f"Дата: {fmt_date(date_str)}\n"
            f"Время: {booking['time']}\n"
            f"Услуга: {booking['service']} ({booking['duration']} мин)\n"
            f"Цена: {booking['price']} BYN\n"
            f"Клиент: {booking['name']}\n"
            f"Телефон: {booking['phone']}"
        )
        await bot.session.close()
    except Exception:
        pass

    await state.clear()


# =========================
# 12) Админ: красивые записи (сегодня/завтра/все)
# =========================
def render_records_for_dates(dates: list[str]):
    lines = []

    for d in dates:
        all_times = day_times(d)

        if all_times is None:
            lines.append(f"📅 {fmt_date(d)} — выходной")
            lines.append("")
            continue

        busy = get_busy_slots(d)
        free = [t for t in all_times if t not in busy]

        lines.append(f"📅 {fmt_date(d)}")

        if busy:
            lines.append("🔴 Занято:")
            lines.append(", ".join(sorted(busy)))
        else:
            lines.append("🔴 Занято: нет")

        lines.append("")

        if free:
            lines.append("🟢 Свободно:")
            lines.append(", ".join(sorted(free)))
        else:
            lines.append("🟢 Свободно: нет")

        lines.append("")

    if not lines:
        return "Записей нет."

    return "\n".join(lines).strip()


@dp.message(F.text == ADMIN_RECORDS_TODAY)
async def admin_records_today(message: Message):
    if message.from_user.id != MASTER_ID:
        return
    today = datetime.today().date().strftime("%Y-%m-%d")
    text = render_records_for_dates([today])
    await message.answer(text)

@dp.message(F.text == ADMIN_RECORDS_TOM)
async def admin_records_tom(message: Message):
    if message.from_user.id != MASTER_ID:
        return
    tom = (datetime.today().date() + timedelta(days=1)).strftime("%Y-%m-%d")
    text = render_records_for_dates([tom])
    await message.answer(text)

@dp.message(F.text == ADMIN_RECORDS_ALL)
async def admin_records_all(message: Message):
    if message.from_user.id != MASTER_ID:
        return
    dates = [d for d in sorted(appointments.keys()) if d in next_14_days()]
    text = render_records_for_dates(dates)
    await message.answer(text)


# =========================
# 13) Админ: удалить запись (освобождает время)
# =========================
@dp.message(F.text == ADMIN_DELETE)
async def admin_delete_start(message: Message, state: FSMContext):
    if message.from_user.id != MASTER_ID:
        return

    # показываем только даты, где есть записи
    dates_with = [d for d in sorted(appointments.keys()) if appointments.get(d)]
    if not dates_with:
        await message.answer("Записей нет — удалять нечего.")
        return

    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=d)] for d in dates_with] + [[KeyboardButton(text=CANCEL)]],
        resize_keyboard=True
    )
    await message.answer("Выберите дату, где удалить запись:", reply_markup=kb)
    await state.set_state(AdminDelete.pick_date)

@dp.message(AdminDelete.pick_date)
async def admin_delete_pick_date(message: Message, state: FSMContext):
    if message.text == CANCEL:
        await state.clear()
        await message.answer("Ок.", reply_markup=admin_kb)
        return

    date_str = message.text.strip()
    day_list = appointments.get(date_str, [])
    if not day_list:
        await message.answer("На этой дате нет записей. Выберите другую.")
        return

    await state.update_data(date=date_str)

    text = f"📅 {fmt_date(date_str)}\nВыберите номер записи для удаления:\n\n"
    for i, b in enumerate(sorted(day_list, key=lambda x: x["time"]), 1):
        text += f"{i}) {b['time']} — {b['service']} — {b['name']} ({b['phone']})\n"

    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=str(i))] for i in range(1, len(day_list) + 1)] + [[KeyboardButton(text=CANCEL)]],
        resize_keyboard=True
    )
    await message.answer(text, reply_markup=kb)
    await state.set_state(AdminDelete.pick_booking)

@dp.message(AdminDelete.pick_booking)
async def admin_delete_pick_booking(message: Message, state: FSMContext):
    if message.text == CANCEL:
        await state.clear()
        await message.answer("Ок.", reply_markup=admin_kb)
        return

    data = await state.get_data()
    date_str = data["date"]
    day_list = sorted(appointments.get(date_str, []), key=lambda x: x["time"])

    if not message.text.isdigit():
        await message.answer("Нужно нажать номер кнопкой.")
        return

    idx = int(message.text) - 1
    if idx < 0 or idx >= len(day_list):
        await message.answer("Неверный номер.")
        return

    deleted = day_list[idx]

    # удаляем конкретный объект из оригинального списка по id
    original = appointments.get(date_str, [])
    appointments[date_str] = [b for b in original if b.get("id") != deleted.get("id")]

    # если день пустой — можно удалить ключ
    if not appointments[date_str]:
        appointments.pop(date_str, None)

    save_data()
    await state.clear()

    await message.answer(
        "✅ Запись удалена, время освобождено:\n"
        f"{fmt_date(date_str)} {deleted['time']} — {deleted['service']} — {deleted['name']}",
        reply_markup=admin_kb
    )


# =========================
# 14) Админ: свободные окна (все 14 дней)
# =========================
@dp.message(F.text == ADMIN_FREE)
async def admin_free_all(message: Message):
    if message.from_user.id != MASTER_ID:
        return

    lines = []

    for d in next_14_days():
        times = day_times(d)

        if times is None:
            lines.append(f"📅 {fmt_date(d)} — выходной")
            lines.append("")
            continue

        busy = get_busy_slots(d)
        free = [t for t in times if t not in busy]

        lines.append(f"📅 {fmt_date(d)}")

        if free:
            lines.append("🟢 Свободно:")
            lines.append(", ".join(sorted(free)))
        else:
            lines.append("🟢 Свободно: нет")

        lines.append("")

    await message.answer("\n".join(lines).strip(), reply_markup=admin_kb)

# =========================
# 15) RUN
# =========================
async def main():
    bot = Bot(BOT_TOKEN)
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    finally:
        if os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)

