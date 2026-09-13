from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo


def parse_local(value, day, tz):
    parsed = time.fromisoformat(value)
    return datetime.combine(day, parsed, tzinfo=tz)


class BookingService:
    def __init__(self, database, settings):
        self.db = database
        self.settings = settings
        self.tz = ZoneInfo(settings["timezone"])

    def now(self):
        return datetime.now(self.tz)

    def booking_dates(self):
        start = self.now().date()
        days = int(self.settings["booking_window_days"])
        return [start + timedelta(days=i) for i in range(days)]

    def day_rules(self, day: date):
        override = self.db.override(day.isoformat())
        if override:
            if not override["working"]:
                return None
            import json

            return {
                "working": True,
                "start": override["start_time"],
                "end": override["end_time"],
                "breaks": json.loads(override["breaks_json"]),
            }
        rules = self.settings["weekly_schedule"][str(day.isoweekday())]
        return rules if rules.get("working") else None

    def available_slots(self, service, day: date, ignore_appointment_id=None):
        rules = self.day_rules(day)
        if not rules:
            return []
        step = int(self.settings["slot_step_minutes"])
        duration = int(service["duration_minutes"])
        minimum = self.now() + timedelta(
            hours=int(self.settings["minimum_notice_hours"])
        )
        cursor = parse_local(rules["start"], day, self.tz)
        finish = parse_local(rules["end"], day, self.tz)
        breaks = [
            (parse_local(item[0], day, self.tz), parse_local(item[1], day, self.tz))
            for item in rules.get("breaks", [])
        ]
        result = []
        while cursor + timedelta(minutes=duration) <= finish:
            end = cursor + timedelta(minutes=duration)
            crosses_break = any(
                cursor < break_end and end > break_start
                for break_start, break_end in breaks
            )
            busy = self.db.overlapping(
                cursor.isoformat(), end.isoformat(), ignore_id=ignore_appointment_id
            )
            if cursor >= minimum and not crosses_break and not busy:
                result.append(cursor)
            cursor += timedelta(minutes=step)
        return result

    def make_interval(self, day: date, start_value: str, duration_minutes: int):
        start = parse_local(start_value, day, self.tz)
        return start, start + timedelta(minutes=duration_minutes)


def parse_hours(value: str):
    left, right = [part.strip() for part in value.split("-", 1)]
    start, end = time.fromisoformat(left), time.fromisoformat(right)
    if start >= end:
        raise ValueError
    return start.strftime("%H:%M"), end.strftime("%H:%M")


def parse_breaks(value: str):
    if value.strip().lower() in {"нет", "-", "без перерыва"}:
        return []
    result = [parse_hours(item.strip()) for item in value.split(",")]
    for index, current in enumerate(result):
        for other in result[index + 1 :]:
            if current[0] < other[1] and current[1] > other[0]:
                raise ValueError
    return result
