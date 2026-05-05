#!/usr/bin/env bash
set -euo pipefail
if [[ -f /etc/systemd/system/group-agent.service ]]; then
  systemctl daemon-reload
  systemctl enable group-agent.service
  systemctl restart group-agent.service
  systemctl is-active --quiet group-agent.service
else
  echo "FAIL: /etc/systemd/system/group-agent.service not found"
  exit 1
fi
