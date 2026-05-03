#!/usr/bin/env bash
set -euo pipefail
if id ec2-user &>/dev/null; then
  chown -R ec2-user:ec2-user /opt/group-agent || true
elif id ubuntu &>/dev/null; then
  chown -R ubuntu:ubuntu /opt/group-agent || true
fi
chmod +x /opt/group-agent/scripts/*.sh 2>/dev/null || true
bash /opt/group-agent/scripts/select_active_sender.sh /opt/group-agent
