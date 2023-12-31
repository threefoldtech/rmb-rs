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

cleanup() {
    echo "stop all bash managed jobs"
    jlist=$(jobs -p)
    plist=$(ps --ppid $$ | awk '/[0-9]/{print $1}')
    
    kill ${jlist:-$plist}
}

trap cleanup SIGHUP	SIGINT SIGQUIT SIGABRT SIGTERM


# start redis in backgroud and skip errors in case alreday running
set +e
echo "redis-server starting .."

redis-server --port 6379 2>&1 > /dev/null&
sleep 3
set -e

# start rmb in background
echo "rmb-peer starting .."
$RMB_BIN -m "$MNEMONIC" --substrate "$SUBSTRATE_URL" --relay "$RELAY_URL" --redis "redis://localhost:6379" --debug &> $RMB_LOG_FILE &

# wait till peer establish connection to a relay
timeout --preserve-status 10 tail -f -n0 $RMB_LOG_FILE | grep -qe 'now connected' || (echo "rmb-peer taking too much time to start! check the log at $RMB_LOG_FILE for more info." && cleanup)

# start rmb_tester
source venv/bin/activate
echo "rmb_tester starting .."
python3 ./rmb_tester.py -d $(./scripts/twins.sh --likely-up $1) -c "rmb.version" -t $TIMEOUT -e $TIMEOUT --short
deactivate

cleanup
