from aiogram.fsm.state import State, StatesGroup


class ClientBooking(StatesGroup):
    service = State()
    date = State()
    slot = State()
    name = State()
    phone = State()
    confirm = State()


class ClientMove(StatesGroup):
    date = State()
    slot = State()


class ClientCancel(StatesGroup):
    confirm = State()


class ManualBooking(StatesGroup):
    service = State()
    date = State()
    slot = State()
    name = State()
    phone = State()
    confirm = State()


class DaySettings(StatesGroup):
    date = State()
    action = State()
    hours = State()
    breaks = State()
