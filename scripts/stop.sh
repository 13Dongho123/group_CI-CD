#!/usr/bin/env bash
set -euo pipefail
systemctl stop group-agent.service 2>/dev/null || pkill -f group_sender.py || true
