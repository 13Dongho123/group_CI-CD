#!/usr/bin/env python3
"""API collector one-shot bridge to BatchLoaderLambda."""
import os

from group_sender import send_once


if __name__ == "__main__":
    raise SystemExit(send_once(os.environ.get("SOURCE_NAME", "api-collector")))
