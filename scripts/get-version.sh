#!/usr/bin/env bash
#
# Copyright (c) 2019 - 2024 StreamNative, Inc.. All Rights Reserved.
#

mvn -q \
  -Dexec.executable=echo \
  -Dexec.args='${project.version}' \
  --non-recursive \
  exec:exec
