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
  cp -n "${REV_DIR}/.env.sample" "${APP_DIR}/.env" || true
elif [[ -f "${REV_DIR}/.env.example" ]]; then
  cp -n "${REV_DIR}/.env.example" "${APP_DIR}/.env" || true
fi

if [[ -f "${REV_DIR}/group-agent.service" ]]; then
  ${SUDO} cp -f "${REV_DIR}/group-agent.service" "/etc/systemd/system/${SERVICE_NAME}.service"
fi

if command -v python3 >/dev/null 2>&1; then
  python3 -m pip install -r "${APP_DIR}/requirements.txt" \
    || python3 -m pip install -r "${APP_DIR}/requirements.txt" --break-system-packages \
    || true
elif command -v pip3 >/dev/null 2>&1; then
  pip3 install -r "${APP_DIR}/requirements.txt" || true
fi

${SUDO} systemctl daemon-reload || true
${SUDO} systemctl enable "${SERVICE_NAME}" || true
${SUDO} systemctl restart "${SERVICE_NAME}" || true
${SUDO} systemctl status "${SERVICE_NAME}" --no-pager || true
echo "[DONE] group codedeploy hook finished"
