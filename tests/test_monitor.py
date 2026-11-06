"""The Discord monitor: when it alerts, when it stays quiet, and what it sends."""
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from unittest.mock import patch

from tracker import monitor
from tracker.monitor import REMIND_EVERY, check, run_check, send

URL = "http://unused"


def status(active="active", enabled="enabled", restarts=0):
    return {"ActiveState": active, "UnitFileState": enabled, "NRestarts": str(restarts)}


def test_healthy_service_is_quiet():
    alerts, state = check(status(), {}, 1000)
    assert alerts == [] and state["down_checks"] == 0


def test_one_failed_check_is_tolerated_and_the_second_alerts():
    alerts, state = check(status("failed"), {}, 1000)
    assert alerts == []
    alerts, state = check(status("failed"), state, 1060)
    assert len(alerts) == 1 and "is down" in alerts[0] and "failed" in alerts[0]


def test_down_alert_repeats_only_after_the_reminder_interval():
    state = {"down_checks": 2, "down_alerted_at": 1000}
    assert check(status("failed"), state, 1060)[0] == []
    alerts, state = check(status("failed"), state, 1000 + REMIND_EVERY)
    assert "still down" in alerts[0] and state["down_alerted_at"] == 1000 + REMIND_EVERY


def test_recovery_is_reported_once():
    alerts, state = check(status(), {"down_checks": 5, "down_alerted_at": 1000}, 2000)
    assert "running again" in alerts[0]
    assert check(status(), state, 2060)[0] == []


def test_recovery_before_any_alert_is_quiet():
    assert check(status(), {"down_checks": 1, "down_alerted_at": None}, 2000)[0] == []


def test_deliberately_disabled_bot_never_alerts():
    state = {}
    for now in range(0, 600, 60):
        alerts, state = check(status("inactive", "disabled"), state, now)
        assert alerts == []


def test_disabling_during_an_outage_clears_it_without_a_false_recovery():
    _, state = check(status("inactive", "disabled"), {"down_checks": 3, "down_alerted_at": 1000}, 2000)
    assert state["down_alerted_at"] is None
    assert check(status(), state, 2060)[0] == []


def test_crash_and_restart_between_checks_is_reported_and_rate_limited():
    _, state = check(status(restarts=0), {}, 0)
    alerts, state = check(status(restarts=2), state, 5000)
    assert "crashed and restarted" in alerts[0] and "(2 time(s))" in alerts[0]
    assert check(status(restarts=9), state, 5060)[0] == []  # a crash loop doesn't alert every minute
    alerts, _ = check(status(restarts=9), state, 5000 + REMIND_EVERY)
    assert "(7 time(s))" in alerts[0]


def test_starting_by_hand_resets_the_counter_without_an_alert():
    _, state = check(status(restarts=5), {}, 0)
    alerts, state = check(status(restarts=0), state, 60)
    assert alerts == [] and state["restarts"] == 0


def test_mention_is_on_problems_but_not_recovery():
    down = {"down_checks": 1}
    assert check(status("failed"), down, 1000, "<@123>")[0][0].startswith("🔴 <@123> ")
    assert "<@123>" not in check(status(), {"down_alerted_at": 1}, 1000, "<@123>")[0][0]


def test_failed_delivery_is_retried_on_the_next_check(tmp_path):
    path = str(tmp_path / "state.json")
    monitor.save_state(path, {"down_checks": 1})
    with patch.object(monitor, "read_status", return_value=status("failed")):
        with patch.object(monitor, "send", side_effect=OSError("no network")):
            assert run_check(URL, "", path) == 1
        with patch.object(monitor, "send") as sent:
            assert run_check(URL, "", path) == 0
    sent.assert_called_once()
    assert monitor.load_state(path)["down_alerted_at"] is not None


def test_unreadable_status_counts_as_down(tmp_path):
    path = str(tmp_path / "state.json")
    monitor.save_state(path, {"down_checks": 1})
    with patch.object(monitor, "read_status", side_effect=OSError("no systemctl")), patch.object(monitor, "send") as sent:
        run_check(URL, "", path)
    assert "unknown" in sent.call_args.args[1]


def test_corrupt_state_file_starts_fresh(tmp_path):
    path = tmp_path / "state.json"
    path.write_text("{not json")
    assert monitor.load_state(str(path)) == {}
    assert monitor.load_state(str(tmp_path / "missing.json")) == {}


def test_send_posts_json_with_a_custom_user_agent():
    received = {}

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            received["agent"] = self.headers["User-Agent"]
            received["body"] = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            self.send_response(204)
            self.end_headers()

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.handle_request)
    thread.start()
    send(f"http://127.0.0.1:{server.server_port}/webhook", "hello")
    thread.join(5)
    server.server_close()
    assert received["body"] == {"content": "hello"}
    assert "Python-urllib" not in received["agent"]
