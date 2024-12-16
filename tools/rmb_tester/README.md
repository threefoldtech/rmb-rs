# RMB tools (CLI tools/scripts)

You can find here CLI tools and scripts that can be used for testing and benchmarking [RMB](https://github.com/threefoldtech/rmb-rs). You can use either RMB_Tester, RMB_echo, or both to quickly test the communications over RMB.

## Installation

- clone the repo
- create a new env

```py
python3 -m venv venv
```

- activate the new env

```py
source ./venv/bin/activate
```

- install dependencies

```py
pip install -r requirements.txt
```

## Usage

RMB tools comprise two Python programs that can be used independently or in conjunction with each other.

### RMB_Tester

RMB_Tester is a CLI tool that serves as an RMB client to automate the process of crafting a specified number of test messages to be sent to one or more destinations. The number of messages, command, data, destination list, and other parameters can be configured through the command line. The tool will wait for the correct number of responses and report some statistics.

Please ensure that there is a process running on the destination side that can handle this command and respond back or use RMB_echo for this purpose.

Also, note that the rmb.version built-in command mentioned in this document is specific to the Rust rmb-peer implementation and is not guaranteed to be available in other RMB implementations. ZOS nodes no longer use the Rust rmb-peer. If you run this tool against a ZOS node, you must use a registered command, such as zos.system.version.

example:

```sh
# We sending to two destinations
# The default test command will be used and can be handled by RMB_echo process
python3 ./rmb_tester.py --dest 41 55
```

to just print the summary use `--short` option

to override default command use the `--command`

```sh
# The `rmb.version` command will be handled by RMB process itself
python3 ./rmb_tester.py --dest 41 --command rmb.version
```

for all optional args see

```sh
python3 ./rmb_tester.py -h
```

### RMB_Echo (message handler)

This tool will automate handling the messages coming to $queue and respond with same message back to the source and display the count of processed messages.

example:

```sh
python3 ./msg_handler.py
```

or specify the redis queue (command) to handle the messages from

```sh
python3 ./msg_handler.py --queue helloworld
```

for all optional args see

```sh
python3 ./msg_handler.py -h
```

## Recipes

### Simple method for testing live nodes

- For simplicity, you can install this tool's dependencies by running the ``install.sh` script:

```sh
./install.sh
```

you can start testing live nodes if it is reachable over rmb by running `test-live-nodes.sh` script. it takes only one argument, the network name (one of `dev`, `qa`, `test`, `main`) and required to pass set you mnemonic as env var `MNEMONIC`. for testing dev network nodes:

```sh
MNEMONIC="[YOUR MNEMONIC]" ./test_live_nodes.sh dev
```

optionally, set `TIMEOUT` and/or `RMB_BIN`.
`TIMEOUT` : set message ttl and client timeout. default to 60 (for large number of destinations use appropriate value)
`RMB_BIN` : set the path of the rmb_peer binary file. default to `../../target/x86_64-unknown-linux-musl/release/rmb-peer`

Additionally, you can set `VERBOSE` to true (or any non-empty value) to display detailed response and error messages and/or `DEBUG` can be configured to enable debug output.

```sh
MNEMONIC="[YOUR MNEMONIC]" TIMEOUT=500 ./test_live_nodes.sh main
```

### More Customized method

- Test all dest twins to ensure that they are reachable over RMB

```sh
# The nodes.sh script when used with `--likely-up` option will output the IDs of the online nodes in the network using the gridproxy API.
python3 ./rmb_tester.py -d $(./scripts/twins.sh --likely-up main) -c "rmb.version" -t 600 -e 600
```

Note: this tool is for testing purposes and not optimized for speed, for large number of destinations use appropriate expiration and timeout values.

you can copy and paste all non responsive twins and run `./twinid_to_nodeid.sh` with the list of twins ids for easy lookup node id and verfiying the status (like know if node in standby mode).

```sh
./scripts/twinid_to_nodeid.sh main 2562 5666 2086 2092
```

First arg is network (one of `dev`, `qa`, `test`, `main`)
Then you follow it with space separated list of twin ids

the output would be like

```sh
twin ID: 2562 node ID: 1419 status: up
twin ID: 5666 node ID: 3568 status: up
twin ID: 2086 node ID: 943 status: up
twin ID: 2092 node ID: 949 status: up
```
