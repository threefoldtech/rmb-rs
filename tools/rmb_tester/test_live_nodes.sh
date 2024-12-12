#!/usr/bin/env bash

case $1 in
  main|dev|qa|test )  # Ok
    ;;
  *)
  # The wrong first argument.
  echo 'Expected "dev", "qa", "test", or "main" as second arg' >&2
  exit 1
esac


if [[ "$1" == "main" ]]; then
  SUBSTRATE_URL="wss://tfchain.grid.tf:443"
  RELAY_URL="wss://relay.grid.tf"
else
  SUBSTRATE_URL="wss://tfchain.$1.grid.tf:443"
  RELAY_URL="wss://relay.$1.grid.tf"
fi
RMB_LOG_FILE="./rmb-peer.log"
TIMEOUT="${TIMEOUT:-60}"
RMB_BIN="${RMB_BIN:-../../target/x86_64-unknown-linux-musl/release/rmb-peer}"
VERBOSE="${VERBOSE:-false}"
cleanup() {
  set +e
  if command -v deactivate &> /dev/null; then
    deactivate
  fi
  jlist=$(jobs -p)
  plist=$(ps --ppid $$ | awk '/[0-9]/{print $1}' | grep -v -E "^$$|^$(pgrep -f 'ps')|^$(pgrep -f 'awk')|^$(pgrep -f 'grep')$")
  pids=${jlist:-$plist}
  if [ -n "$pids" ]; then
    echo "stop all bash managed jobs"
    kill $pids
  else
    echo "No processes to stop."
  fi
  exit
}

trap cleanup SIGHUP	SIGINT SIGQUIT SIGABRT SIGTERM


# start redis in backgroud and skip errors in case alreday running
set +e
echo "redis-server starting .."

redis-server --port 6379 > /dev/null 2>&1 &
sleep 3
# clear all databases
echo "Removes all keys in Redis"
redis-cli -p 6379 FLUSHALL
set -e

# ensure that RMB is not already running
if pgrep -x $(basename "$RMB_BIN") > /dev/null; then
    echo "Another instance of rmb-peer is already running. Killing..."
    pkill -x $(basename "$RMB_BIN")
fi

# ensure the MNEMONIC has no leading or trailing spaces
MNEMONIC="${MNEMONIC#"${MNEMONIC%%[![:space:]]*}"}"; MNEMONIC="${MNEMONIC%"${MNEMONIC##*[![:space:]]}"}"

# start rmb in background
echo "rmb-peer starting ("$1"net).."
$RMB_BIN -m "$MNEMONIC" --substrate "$SUBSTRATE_URL" --relay "$RELAY_URL" --redis "redis://localhost:6379" --debug &> $RMB_LOG_FILE &

# wait till peer establish connection to a relay
timeout --preserve-status 20 tail -f -n0 $RMB_LOG_FILE | grep -qe 'now connected' || (echo "rmb-peer taking too much time to start! check the log at $RMB_LOG_FILE for more info." && cleanup)

# start rmb_tester
source venv/bin/activate
echo "rmb_tester starting .."
python3 ./rmb_tester.py -d $(./scripts/twins.sh --likely-up $1) -c "zos.system.version" -t "$TIMEOUT" -e "$TIMEOUT" $(if [[ "$VERBOSE" == "false" ]]; then echo "--short"; fi)

cleanup
