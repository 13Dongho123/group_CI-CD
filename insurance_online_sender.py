#!/usr/bin/env python3
"""online-insurance EC2 batch sender -> BatchLoaderLambda.

EC2 Name 태그는 `...-group-online-insurance-ec2` (하이픈)이지만,
09 BatchLoaderLambda 의 허용 source_name 은 `online_insurance` (언더스코어)이다.
"""
from group_sender import run_sender


if __name__ == "__main__":
    raise SystemExit(run_sender("online_insurance"))
