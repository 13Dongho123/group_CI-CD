#!/usr/bin/env bash
set -euo pipefail
if [[ -f /etc/systemd/system/group-agent.service ]]; then
  systemctl daemon-reload
  systemctl enable group-agent.service || true
  systemctl restart group-agent.service || true
else
  nohup /usr/bin/python3 /opt/group-agent/active_sender.py >> /var/log/group-agent.log 2>&1 &
fi
