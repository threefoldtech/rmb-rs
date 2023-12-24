#!/bin/bash

# Check if the script receives a list of numbers as arguments
if [ $# -lt 2 ]; then
  echo "Too few arguments, expecting at least 2"
  exit 1
fi

case $1 in
    main|dev|qa|test|"" )  # Ok
        ;;
    *)
        # The wrong first argument.
        echo 'Expected "dev", "qa", "test", or "main" as second arg' >&2
        exit 1
esac

if [[ "$1" == "main" ]]; then
  gridproxy_url="https://gridproxy.grid.tf"
else
  gridproxy_url="https://gridproxy.$2.grid.tf"
fi


# Store the arguments but the firts one in an array
numbers=("${@:2}")

# Query the api and store the json response in a variable
response=$(curl -s "${gridproxy_url}"/nodes?size=20000)

# Loop through the json objects and find the ones that match the twinid
for number in "${numbers[@]}"; do
  # Use jq to filter the objects by twinid and print the twinid and nodeid values
  # echo "Objects with twinid = ${number}:"
  jq -r --arg number "${number}" '.[] | select(.twinId == ($number | tonumber)) | "twin ID: \(.twinId) node ID: \(.nodeId) status: \(.status)"' <<< "${response}"
done
