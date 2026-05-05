#!/usr/bin/env python3
"""online-insurance batch sender -> BatchLoaderLambda."""
from group_sender import run_sender


if __name__ == "__main__":
    raise SystemExit(run_sender("online-insurance"))
