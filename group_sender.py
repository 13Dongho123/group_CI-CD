#!/usr/bin/env python3
"""Group VM 시뮬레이터 샘플 — 금융/병원 등 배치·API 연동은 확장."""
import os
import time


def main():
    interval = int(os.environ.get("GROUP_SEND_INTERVAL_SEC", "120"))
    while True:
        print("group-agent: heartbeat", flush=True)
        time.sleep(interval)


if __name__ == "__main__":
    main()
