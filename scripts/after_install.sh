#!/usr/bin/env bash
set -euo pipefail
chown -R ec2-user:ec2-user /opt/group-agent || true
chmod +x /opt/group-agent/scripts/*.sh 2>/dev/null || true
bash /opt/group-agent/scripts/select_active_sender.sh /opt/group-agent
