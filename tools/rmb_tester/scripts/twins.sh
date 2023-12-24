#!/bin/bash

if [[ $# -gt 3 ]]; then
    echo 'Too many arguments, expecting max of 3' >&2
    exit 1
fi

case $1 in
    --likely-up|--likely-down|--standby|all )  # Ok
        ;;
    *)
        # The wrong first argument.
        echo 'Expected "--likely-up", "--likely-down", "--standby", "all" as first arg' >&2
        exit 1
esac

case $2 in
    main|dev|qa|test )  # Ok
        ;;
    *)
        # The wrong first argument.
        echo 'Expected "dev", "qa", "test", or "main" as second arg' >&2
        exit 1
esac

case $3 in
    *[!0-9,]*)
        echo 'Expected number or comma seprated list of farm numbers as third arg' >&2
        exit 1
        ;;
    *)
esac

if [[ "$2" == "main" ]]; then
  gridproxy_url="https://gridproxy.grid.tf"
else
  gridproxy_url="https://gridproxy.$2.grid.tf"
fi
if [[ "$1" == "--likely-up" ]]; then
  url="$gridproxy_url/nodes?status=up&size=20000&farm_ids=$3"
elif [[ "$1" == "--likely-down" ]]; then
  url="$gridproxy_url/nodes?status=down&size=20000&farm_ids=$3"
elif [[ "$1" == "--standby" ]]; then
  url="$gridproxy_url/nodes?status=standby&size=20000&farm_ids=$3"
else
  url="$gridproxy_url/nodes?size=20000&farm_ids=$3"
fi

# query gridproxy for the registred nodes and retrun a list of nodes' twin IDs
response=$(curl -s -X 'GET' \
  "$url" \
  -H 'accept: application/json')

twinIds=$(echo "$response" | jq -r '.[] | .twinId')

echo "${twinIds[*]}" | tr '\n' ' '
