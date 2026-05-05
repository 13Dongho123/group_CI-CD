#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/opt/group-agent"
REV_DIR="/opt/group-agent/revision"
# CodeBuild Deploy 단계: 아티팩트는 CODEBUILD_SRC_DIR 루트.
if [[ -n "${CODEBUILD_SRC_DIR:-}" ]] && [[ -f "${CODEBUILD_SRC_DIR}/group_sender.py" ]]; then
  REV_DIR="${CODEBUILD_SRC_DIR}"
fi
SERVICE_NAME="group-agent"
SUDO=""
if [[ "$(id -u)" -ne 0 ]] && command -v sudo >/dev/null 2>&1; then
  SUDO="sudo"
fi

echo "[INFO] group codedeploy hook start"
mkdir -p "${APP_DIR}"
mkdir -p "${REV_DIR}"

if [[ ! -f "${REV_DIR}/group_sender.py" ]] || [[ ! -f "${REV_DIR}/requirements.txt" ]]; then
  echo "FAIL: expected files missing under ${REV_DIR}"
  ls -la "${APP_DIR}" 2>/dev/null || true
  ls -la "${REV_DIR}" 2>/dev/null || true
  exit 1
fi

cp -f "${REV_DIR}/group_sender.py" "${APP_DIR}/group_sender.py"
cp -f "${REV_DIR}/group_sender.py" "${APP_DIR}/active_sender.py"
cp -f "${REV_DIR}/requirements.txt" "${APP_DIR}/requirements.txt"
for f in bank_sender.py card_sender.py insurance_sender.py insurance_online_sender.py securities_sender.py hospital_sender.py healthcare_sender.py batch_export.py api_collector.py; do
  [[ -f "${REV_DIR}/${f}" ]] && cp -f "${REV_DIR}/${f}" "${APP_DIR}/${f}"
done

if [[ -f "${REV_DIR}/.env.sample" ]]; then
  cp -n "${REV_DIR}/.env.sample" "${APP_DIR}/.env"
elif [[ -f "${REV_DIR}/.env.example" ]]; then
  cp -n "${REV_DIR}/.env.example" "${APP_DIR}/.env"
fi

if [[ -f "/etc/group-agent/env" ]]; then
  cp -f "/etc/group-agent/env" "${APP_DIR}/.env"
  echo "[INFO] loaded runtime env from /etc/group-agent/env"
fi

if [[ -f "${REV_DIR}/group-agent.service" ]]; then
  ${SUDO} cp -f "${REV_DIR}/group-agent.service" "/etc/systemd/system/${SERVICE_NAME}.service"
else
  echo "FAIL: ${REV_DIR}/group-agent.service not found"
  exit 1
fi

if command -v python3 >/dev/null 2>&1; then
  python3 -m pip install -r "${APP_DIR}/requirements.txt" || python3 -m pip install -r "${APP_DIR}/requirements.txt" --break-system-packages
elif command -v pip3 >/dev/null 2>&1; then
  pip3 install -r "${APP_DIR}/requirements.txt"
else
  echo "FAIL: python3/pip3 not found"
  exit 1
fi

${SUDO} systemctl daemon-reload
echo "[INFO] service start is handled in scripts/start.sh (AUTO_START_GROUP_SENDER gate)"
echo "[DONE] group codedeploy hook finished"
