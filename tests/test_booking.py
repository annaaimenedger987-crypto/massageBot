import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path

from bot.booking import BookingService, callback_time, parse_breaks, parse_hours
from bot.storage import Database, SlotBusyError

SETTINGS = {
    "timezone": "Europe/Minsk",
    "booking_window_days": 30,
    "minimum_notice_hours": 0,
    "slot_step_minutes": 30,
    "weekly_schedule": {
        str(day): {"working": False, "start": None, "end": None, "breaks": []}
        for day in range(1, 8)
    },
}


class BookingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.temp.name) / "bot.db")
        self.db.seed_services(
            [
                {"name": "60 минут", "price": 80, "duration_minutes": 60},
                {"name": "45 минут", "price": 65, "duration_minutes": 45},
            ]
        )
        self.service = self.db.services()[0]
        self.logic = BookingService(self.db, SETTINGS)
        self.day = self.logic.now().date() + timedelta(days=1)
        self.db.set_override(
            self.day.isoformat(), True, "09:00", "18:00", [["13:00", "14:00"]]
        )

    def tearDown(self):
        self.temp.cleanup()

    def interval(self, value, duration=60):
        return self.logic.make_interval(self.day, value, duration)

    def create(self, value, duration=60, client_id=1, manual=False):
        service = dict(self.service)
        service["duration_minutes"] = duration
        start, end = self.interval(value, duration)
        return self.db.create_appointment(
            service,
            start.isoformat(),
            end.isoformat(),
            "Анна",
            "+375290000000",
            client_id=None if manual else client_id,
            manual=manual,
            source="manual" if manual else "telegram",
        )

    def test_break_removes_crossing_slots(self):
        slots = [
            item.strftime("%H:%M")
            for item in self.logic.available_slots(self.service, self.day)
        ]
        self.assertIn("12:00", slots)
        self.assertNotIn("12:30", slots)
        self.assertNotIn("13:00", slots)
        self.assertNotIn("13:30", slots)
        self.assertIn("14:00", slots)

    def test_overlapping_booking_is_rejected(self):
        self.create("10:00", 60)
        with self.assertRaises(SlotBusyError):
            self.create("10:30", 45, client_id=2)

    def test_two_simultaneous_attempts_create_only_one_booking(self):
        def attempt(client_id):
            try:
                return self.create("16:00", client_id=client_id)["id"]
            except SlotBusyError:
                return None

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(attempt, [1, 2]))
        self.assertEqual(sum(result is not None for result in results), 1)

    def test_cancel_releases_interval(self):
        appointment = self.create("10:00")
        self.assertTrue(self.db.cancel(appointment["id"], "client"))
        replacement = self.create("10:00", client_id=2)
        self.assertEqual(replacement["client_telegram_id"], 2)

    def test_move_rechecks_conflict(self):
        first = self.create("10:00", client_id=1)
        second = self.create("11:00", client_id=2)
        start, end = self.interval("10:30")
        with self.assertRaises(SlotBusyError):
            self.db.move(second["id"], start.isoformat(), end.isoformat())
        start, end = self.interval("15:00")
        moved = self.db.move(first["id"], start.isoformat(), end.isoformat())
        self.assertIn("T15:00:00", moved["start_at"])

    def test_manual_booking_blocks_online_slot(self):
        manual = self.create("15:00", manual=True)
        self.assertEqual(manual["created_by_master"], 1)
        slots = [
            item.strftime("%H:%M")
            for item in self.logic.available_slots(self.service, self.day)
        ]
        self.assertNotIn("15:00", slots)
        self.assertNotIn("15:30", slots)

    def test_callback_time_preserves_hours_and_minutes(self):
        self.assertEqual(callback_time("book:slot:09:30", "book:slot:"), "09:30")
        self.assertEqual(callback_time("manual:slot:18:00", "manual:slot:"), "18:00")

    def test_client_sees_only_own_active_bookings(self):
        own = self.create("09:00", client_id=10)
        self.create("10:00", client_id=20)
        rows = self.db.client_appointments(10, self.logic.now().isoformat())
        self.assertEqual([row["id"] for row in rows], [own["id"]])

    def test_future_tables_exist(self):
        with self.db.connect() as conn:
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
        self.assertIn("waitlist", tables)
        self.assertIn("appointment_services", tables)

    def test_reminder_query_uses_separate_threshold_windows(self):
        appointment = self.create("15:00")
        start = self.logic.make_interval(self.day, "14:00", 60)[0]
        end = self.logic.make_interval(self.day, "16:00", 60)[0]
        rows = self.db.due_reminders(start.isoformat(), end.isoformat(), "client_2h")
        self.assertEqual([row["id"] for row in rows], [appointment["id"]])
        self.db.mark_notification(
            appointment["id"], "client_2h", self.logic.now().isoformat()
        )
        self.assertEqual(
            self.db.due_reminders(start.isoformat(), end.isoformat(), "client_2h"),
            [],
        )

    def test_parsers(self):
        self.assertEqual(parse_hours("10:00-18:30"), ("10:00", "18:30"))
        self.assertEqual(
            parse_breaks("13:00-14:00, 16:00-16:30"),
            [("13:00", "14:00"), ("16:00", "16:30")],
        )
        self.assertEqual(parse_breaks("нет"), [])


if __name__ == "__main__":
    unittest.main()
