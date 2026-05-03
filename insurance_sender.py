#!/usr/bin/env python3
"""lifesync-dev-group-insurance-ec2 — insurance 계열 sender (샘플)."""
import os
import time


def main():
    interval = int(os.environ.get("GROUP_SEND_INTERVAL_SEC", "120"))
    while True:
        print("group-agent[insurance]: heartbeat", flush=True)
        time.sleep(interval)


if __name__ == "__main__":
    main()
