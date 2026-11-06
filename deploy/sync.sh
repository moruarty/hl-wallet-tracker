#!/usr/bin/env bash
# Runs on your machine (Git Bash on Windows): push main to GitHub, then update the VM and restart.
#   bash deploy/sync.sh
# Override the defaults with: VM=other-name ZONE=us-east1-b bash deploy/sync.sh
set -euo pipefail
VM="${VM:-hl-tracker}"
ZONE="${ZONE:-us-central1-a}"

if [ -n "$(git status --porcelain)" ]; then
  echo "You have uncommitted changes, and only committed code gets deployed:" >&2
  git status --short >&2
  echo "Commit them first, then run this again." >&2
  exit 1
fi

git push origin main

# Kept on one line so it survives being passed through gcloud and PuTTY on Windows.
gcloud compute ssh "$VM" --zone "$ZONE" --command "cd /opt/hl-wallet-tracker && sudo -u hltracker git pull --ff-only && sudo -u hltracker .venv/bin/pip install --quiet --no-cache-dir -r requirements.txt && sudo cp deploy/hl-tracker.service deploy/hl-tracker-monitor.service deploy/hl-tracker-monitor.timer /etc/systemd/system/ && sudo systemctl daemon-reload && sudo systemctl enable --now hl-tracker-monitor.timer && sudo systemctl restart hl-tracker && sleep 5 && sudo systemctl is-active hl-tracker || { sudo journalctl -u hl-tracker -n 30 --no-pager; exit 1; }"
