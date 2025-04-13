dist := "dist/"

ci-build:
  #! /bin/bash
  set -e
  go build -o dist/ingestor ./cmd/ingestor

ci-test:
  #! /bin/bash
  set -e
  go test ./... -v

build:
  #! /bin/bash
  set -e
  devbox run go build -o dist/ingestor ./cmd/ingestor

docs:
  #! /bin/bash
  set -e
  devbox run gomarkdoc -u -o README.md -e ./cmd/ingestor
  echo "README.md has been updated"
