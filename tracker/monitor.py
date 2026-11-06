"""Checks the bot's systemd service and alerts a Discord webhook. A systemd timer runs this every minute.

    python -m tracker.monitor         run one check
    python -m tracker.monitor --test  send a test message to the webhook

It runs on the same VM as the bot, so it can't report the whole VM being down. It checks that the
process is running, not that it's doing useful work.
"""
import json
import logging
import os
import subprocess
import sys
import time
import urllib.request

from dotenv import load_dotenv

log = logging.getLogger("tracker.monitor")

SERVICE = "hl-tracker"
STATE_PATH = "data/monitor_state.json"
DOWN_CHECKS_BEFORE_ALERT = 2  # one failed check can just be a deploy or a reboot
REMIND_EVERY = 3600  # seconds between repeat alerts while something is still wrong
LOGS_HINT = f"Logs: `sudo journalctl -u {SERVICE} -n 30 --no-pager`"


def read_status() -> dict:
    out = subprocess.run(
        ["systemctl", "show", SERVICE, "--property=ActiveState,UnitFileState,NRestarts"],
        capture_output=True, text=True, check=True, timeout=15,
    ).stdout
    return dict(line.split("=", 1) for line in out.splitlines() if "=" in line)


def check(status: dict, state: dict, now: float, mention: str = "") -> tuple[list[str], dict]:
    """Work out which alerts to send. Returns them with the state to keep if they're delivered."""
    state = dict(state)
    alerts = []
    prefix = f"{mention} " if mention else ""
    restarts = int(status.get("NRestarts") or 0)

    # `systemctl disable --now` is how the bot is deliberately turned off, so that's not an outage.
    if status.get("UnitFileState") != "enabled":
        state.update(down_checks=0, down_alerted_at=None, restarts=restarts)
        return alerts, state

    if status.get("ActiveState") == "active":
        if state.get("down_alerted_at") is not None:
            alerts.append(f"🟢 **{SERVICE} is running again.**")
        state.update(down_checks=0, down_alerted_at=None)
        reported = state.get("restarts", restarts)
        if restarts < reported:  # the counter resets when the service is started by hand
            state["restarts"] = reported = restarts
        if restarts > reported and now - state.get("restart_alerted_at", 0) >= REMIND_EVERY:
            alerts.append(f"🟠 {prefix}**{SERVICE} crashed and restarted** "
                          f"({restarts - reported} time(s)). {LOGS_HINT}")
            state.update(restarts=restarts, restart_alerted_at=now)
        else:
            state.setdefault("restarts", restarts)
        return alerts, state

    state["down_checks"] = state.get("down_checks", 0) + 1
    last_alert = state.get("down_alerted_at")
    if state["down_checks"] >= DOWN_CHECKS_BEFORE_ALERT and (last_alert is None or now - last_alert >= REMIND_EVERY):
        verb = "is down" if last_alert is None else "is still down"
        alerts.append(f"🔴 {prefix}**{SERVICE} {verb}** (state: {status.get('ActiveState', 'unknown')}). {LOGS_HINT}")
        state["down_alerted_at"] = now
    return alerts, state


def send(url: str, text: str) -> None:
    request = urllib.request.Request(
        url, json.dumps({"content": text}).encode(),
        # Discord refuses Python's default User-Agent.
        {"Content-Type": "application/json", "User-Agent": "hl-wallet-tracker-monitor"},
    )
    with urllib.request.urlopen(request, timeout=15):
        pass


def load_state(path: str) -> dict:
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def save_state(path: str, state: dict) -> None:
    temp = f"{path}.tmp"
    with open(temp, "w") as f:
        json.dump(state, f)
    os.replace(temp, path)


def run_check(url: str, mention: str, path: str = STATE_PATH) -> int:
    old = load_state(path)
    try:
        status = read_status()
    except (OSError, subprocess.SubprocessError):
        log.exception("Could not read the status of %s", SERVICE)
        status = {"UnitFileState": "enabled", "ActiveState": "unknown"}
    alerts, new = check(status, old, time.time(), mention)
    try:
        for text in alerts:
            send(url, text)
    except OSError as e:
        # Keep the old alert times so the next check tries again. The error can't contain the URL.
        log.error("Could not send the alert to Discord: %s", type(e).__name__)
        save_state(path, {**old, "down_checks": new.get("down_checks", 0)})
        return 1
    save_state(path, new)
    return 0


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    load_dotenv()
    url = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
    if not url:
        log.error("DISCORD_WEBHOOK_URL is not set in .env")
        return 1
    if "--test" in sys.argv[1:]:
        send(url, f"✅ Test message from the {SERVICE} monitor.")
        log.info("Test message sent")
        return 0
    return run_check(url, os.getenv("DISCORD_MENTION", "").strip())


if __name__ == "__main__":
    sys.exit(main())
