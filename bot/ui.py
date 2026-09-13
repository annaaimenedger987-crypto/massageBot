from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
)


def reply_menu(is_master=False):
    rows = [
        [KeyboardButton(text="Записаться")],
        [KeyboardButton(text="Мои записи"), KeyboardButton(text="Услуги")],
        [KeyboardButton(text="Контакты")],
    ]
    if is_master:
        rows.append([KeyboardButton(text="Панель мастера")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def inline(rows):
    return InlineKeyboardMarkup(inline_keyboard=rows)


def button(text, data, style=None, icon=None):
    kwargs = {"text": text, "callback_data": data}
    if style:
        kwargs["style"] = style
    if icon:
        kwargs["icon_custom_emoji_id"] = icon
    return InlineKeyboardButton(**kwargs)


def services_keyboard(services, icons):
    return inline(
        [
            [
                button(
                    f"{row['name']} · {row['duration_minutes']} мин · {money(row['price'])}",
                    f"book:service:{row['id']}",
                    "primary",
                    icons.get("services"),
                )
            ]
            for row in services
        ]
        + [[button("Закрыть", "nav:close", icon=icons.get("cancel"))]]
    )


def dates_keyboard(days, prefix, icons):
    rows = []
    for index in range(0, len(days), 2):
        rows.append(
            [
                button(
                    day.strftime("%d.%m · %a"),
                    f"{prefix}:{day.isoformat()}",
                    icon=icons.get("calendar"),
                )
                for day in days[index : index + 2]
            ]
        )
    rows.append([button("Назад", "nav:menu", icon=icons.get("back"))])
    return inline(rows)


def slots_keyboard(slots, prefix):
    rows = []
    values = [slot.strftime("%H:%M") for slot in slots]
    for index in range(0, len(values), 3):
        rows.append(
            [
                button(item, f"{prefix}:{item}", "primary")
                for item in values[index : index + 3]
            ]
        )
    rows.append([button("Назад", "nav:menu")])
    return inline(rows)


def money(value):
    number = float(value)
    return f"{int(number) if number.is_integer() else number:g} BYN"
