dist := "dist/"

ci-build:
  #! /bin/bash
  set -e
  go build -o {{dist}}/pulses

build:
  #! /bin/bash
  set -e
  devbox run go build -o {{dist}}/pulses

docs:
  #! /bin/bash
  set -e
  devbox run gomarkdoc -u -o README.md -e .
  echo "README.md has been updated"
