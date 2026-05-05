#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/opt/group-agent"
SERVICE_NAME="group-agent"

if [[ ! -f "${APP_DIR}/group_sender.py" ]] || [[ ! -f "${APP_DIR}/requirements.txt" ]]; then
  echo "FAIL: required files missing in ${APP_DIR}"
  ls -la "${APP_DIR}" || true
  exit 1
fi

mkdir -p "${APP_DIR}/batches"
chmod +x "${APP_DIR}/scripts/"*.sh

if [[ -f "/etc/group-agent/env" ]]; then
  cp -f "/etc/group-agent/env" "${APP_DIR}/.env"
elif [[ -f "${APP_DIR}/.env.sample" ]]; then
  cp -n "${APP_DIR}/.env.sample" "${APP_DIR}/.env"
elif [[ -f "${APP_DIR}/.env.example" ]]; then
  cp -n "${APP_DIR}/.env.example" "${APP_DIR}/.env"
fi

if [[ ! -f "${APP_DIR}/group-agent.service" ]]; then
  echo "FAIL: ${APP_DIR}/group-agent.service not found"
  exit 1
fi
cp -f "${APP_DIR}/group-agent.service" "/etc/systemd/system/${SERVICE_NAME}.service"

if id ec2-user &>/dev/null; then
  chown -R ec2-user:ec2-user "${APP_DIR}" || true
elif id ubuntu &>/dev/null; then
  chown -R ubuntu:ubuntu "${APP_DIR}" || true
fi

if command -v python3 >/dev/null 2>&1; then
  python3 -m pip install -r "${APP_DIR}/requirements.txt" || python3 -m pip install -r "${APP_DIR}/requirements.txt" --break-system-packages
elif command -v pip3 >/dev/null 2>&1; then
  pip3 install -r "${APP_DIR}/requirements.txt"
else
  echo "FAIL: python3/pip3 not found"
  exit 1
fi

bash "${APP_DIR}/scripts/select_active_sender.sh" "${APP_DIR}"
systemctl daemon-reload
