import unittest

from tg_agent.gateway.formatters import live_message_lines


class TestLiveMessageLines(unittest.TestCase):
    def test_text_only(self):
        data = {
            "messages": [
                {
                    "message_id": 1,
                    "date": "2026-11-07 12:00:00",
                    "sender_name": "Alice",
                    "text": "Hello world",
                }
            ],
            "reaction_users": {},
        }
        lines = live_message_lines(data)
        self.assertIn("  Hello world", lines)
        self.assertFalse(any("[media:" in line for line in lines))

    def test_media_only_without_caption(self):
        data = {
            "messages": [
                {
                    "message_id": 2,
                    "date": "2026-11-07 12:01:00",
                    "sender_name": "Bob",
                    "media_type": "document",
                }
            ],
            "reaction_users": {},
        }
        lines = live_message_lines(data)
        self.assertIn("  [media: document]", lines)

    def test_media_with_caption_shows_both(self):
        # Reproduces issue #1776
        data = {
            "messages": [
                {
                    "message_id": 17829,
                    "date": "2026-11-03 09:18:29",
                    "sender_name": "user_001",
                    "text": "@user_002 registration database starts on January 1",
                    "media_type": "document",
                }
            ],
            "reaction_users": {},
        }
        lines = live_message_lines(data)
        self.assertIn("  @user_002 registration database starts on January 1", lines)
        self.assertIn("  [media: document]", lines)

    def test_budget_exceeded(self):
        data = {
            "messages": [
                {
                    "message_id": i,
                    "date": "2026-11-07 12:00:00",
                    "sender_name": "User",
                    "text": "X" * 100,
                }
                for i in range(10)
            ],
            "reaction_users": {},
        }
        lines = live_message_lines(data, budget=250)
        self.assertIn("[message text budget reached]", lines)


if __name__ == "__main__":
    unittest.main()
