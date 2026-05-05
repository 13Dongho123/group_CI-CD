#!/usr/bin/env python3
"""Manual one-shot batch export trigger using group_sender logic."""
import os

from group_sender import send_once


if __name__ == "__main__":
    raise SystemExit(send_once(os.environ.get("SOURCE_NAME", "batch-export")))
