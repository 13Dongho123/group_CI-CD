#!/usr/bin/env bash
set -euo pipefail
AUTO_START="false"
if [[ -f /opt/group-agent/.env ]]; then
  # shellcheck disable=SC1091
  source /opt/group-agent/.env || true
  AUTO_START="${AUTO_START_GROUP_SENDER:-false}"
fi

AUTO_START="${AUTO_START//$'\r'/}"
AUTO_START="${AUTO_START// /}"
if [[ ! "${AUTO_START}" =~ ^(1|true|yes)$ ]]; then
  echo "[INFO] AUTO_START_GROUP_SENDER=false -> keep sender stopped"
  pkill -f /opt/group-agent/active_sender.py 2>/dev/null || true
  if [[ -f /etc/systemd/system/group-agent.service ]]; then
    systemctl stop group-agent.service 2>/dev/null || true
    systemctl disable group-agent.service 2>/dev/null || true
  fi
  exit 0
fi

if [[ -f /etc/systemd/system/group-agent.service ]]; then
  systemctl daemon-reload
  systemctl enable group-agent.service
  systemctl restart group-agent.service
  systemctl is-active --quiet group-agent.service
else
  echo "FAIL: /etc/systemd/system/group-agent.service not found"
  exit 1
fi
