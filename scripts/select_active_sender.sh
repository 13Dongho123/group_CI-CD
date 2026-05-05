#!/usr/bin/env bash
# EC2 Name 예: lifesync-dev-group-bank-ec2 → bank_sender.py 를 group_sender.py 로 복사
# 매핑: group-{segment}-ec2 의 segment → *_sender.py
set -euo pipefail

DEPLOY_ROOT="${1:-/opt/group-agent}"

if [[ ! -d "${DEPLOY_ROOT}" ]]; then
  echo "FAIL: ${DEPLOY_ROOT} not found"
  exit 1
fi

TOKEN="$(curl -s -f -X PUT "http://169.254.169.254/latest/api/token" \
  -H "X-aws-ec2-metadata-token-ttl-seconds: 21600" || true)"
if [[ -z "${TOKEN}" ]]; then
  echo "[WARN] no IMDSv2 token; trying IMDSv1"
  IID="$(curl -s -f http://169.254.169.254/latest/meta-data/instance-id || true)"
  REGION="$(curl -s -f http://169.254.169.254/latest/meta-data/placement/region || true)"
else
  IID="$(curl -s -f -H "X-aws-ec2-metadata-token: ${TOKEN}" \
    http://169.254.169.254/latest/meta-data/instance-id)"
  REGION="$(curl -s -f -H "X-aws-ec2-metadata-token: ${TOKEN}" \
    http://169.254.169.254/latest/meta-data/placement/region)"
fi

if [[ -z "${IID}" || -z "${REGION}" ]]; then
  echo "FAIL: could not read instance id / region from instance metadata"
  exit 1
fi

# Prefer IMDS (no IAM / no aws-cli) when InstanceMetadataTags is enabled on the instance.
IMDS_TAGS="http://169.254.169.254/latest/meta-data/tags/instance"
if [[ -n "${TOKEN}" ]]; then
  NAME_TAG="$(curl -sS -m 10 -f -H "X-aws-ec2-metadata-token: ${TOKEN}" "${IMDS_TAGS}/Name" 2>/dev/null || true)"
else
  NAME_TAG="$(curl -sS -m 10 -f "${IMDS_TAGS}/Name" 2>/dev/null || true)"
fi
NAME_TAG="${NAME_TAG//$'\r'/}"

if [[ -z "${NAME_TAG}" || "${NAME_TAG}" == "None" ]]; then
  if command -v aws >/dev/null 2>&1; then
    AWS_ERR="$(mktemp)"
    trap 'rm -f "${AWS_ERR}"' EXIT
    set +e
    NAME_TAG="$(aws ec2 describe-tags --region "${REGION}" \
      --filters "Name=resource-id,Values=${IID}" "Name=key,Values=Name" \
      --query 'Tags[0].Value' --output text 2>"${AWS_ERR}")"
    DESC_RC=$?
    set -e
    if [[ "${DESC_RC}" -ne 0 ]]; then
      echo "FAIL: aws ec2 describe-tags failed (exit ${DESC_RC}). Check IAM ec2:DescribeTags, or rely on InstanceMetadataTags (see 12-ec2)." >&2
      if [[ -s "${AWS_ERR}" ]]; then sed 's/^/[aws stderr] /' "${AWS_ERR}" >&2; fi
      exit 1
    fi
    NAME_TAG="${NAME_TAG//$'\r'/}"
  fi
fi

if [[ -z "${NAME_TAG}" || "${NAME_TAG}" == "None" ]]; then
  echo "FAIL: could not read EC2 Name tag (want e.g. lifesync-dev-group-bank-ec2). Enable InstanceMetadataTags on EC2 updated from 12-ec2, or install aws-cli on the AMI." >&2
  exit 1
fi

if [[ "${NAME_TAG}" =~ group-(.+)-ec2$ ]]; then
  segment="${BASH_REMATCH[1]}"
else
  echo "FAIL: Name tag '${NAME_TAG}' does not match *group-<segment>-ec2"
  exit 1
fi

case "${segment}" in
  bank) src="bank_sender.py" ;;
  card) src="card_sender.py" ;;
  insurance) src="insurance_sender.py" ;;
  online-insurance) src="insurance_online_sender.py" ;;
  securities) src="securities_sender.py" ;;
  hospital) src="hospital_sender.py" ;;
  healthcare) src="healthcare_sender.py" ;;
  *)
    echo "FAIL: unknown group segment '${segment}' in Name tag (expected bank|card|insurance|online-insurance|securities|hospital|healthcare)"
    exit 1
    ;;
esac

if [[ ! -f "${DEPLOY_ROOT}/${src}" ]]; then
  echo "FAIL: ${DEPLOY_ROOT}/${src} missing"
  exit 1
fi

cp -f "${DEPLOY_ROOT}/${src}" "${DEPLOY_ROOT}/active_sender.py"
chmod 0644 "${DEPLOY_ROOT}/active_sender.py"
echo "[OK] active sender: ${src} (segment=${segment}, Name=${NAME_TAG})"
