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

source:
  #! /bin/bash
  set -e

  echo "Extracting unique tenant_ids..."
  jq -r '.tenant_id' ./.data/source.pulses | sort -u

  echo
  echo "Extracting unique product_skus..."
  jq -r '.product_sku' ./.data/source.pulses | sort -u

sink tenant sku:
  #! /bin/bash
  set -e
  echo "listing aggregated amount by tenant and sku"
  cat ./.data/tenants.{{tenant}}.aggregated.pulses.amount | grep {{sku}}

  echo "listing grouped amount by tenant"
  cat ./.data/tenants.{{tenant}}.grouped.pulses | grep {{sku}}

sink-before tenant sku epoch:
  jq 'select(.timestamp < {{epoch}} and .product_sku == "{{sku}}")' ./.data/tenants.{{tenant}}.grouped.pulses

sink-aggr tenant sku epoch:
  jq -s 'map(select(.timestamp <= {{epoch}} and .product_sku == "{{sku}}")) | map(.used_ammount) | add' ./.data/tenants.{{tenant}}.grouped.pulses

