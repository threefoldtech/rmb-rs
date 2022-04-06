# RMB
RMB is (reliable message bus) is a set of tools (client and daemon) that aims to abstract inter-process communication between multiple processes running over multiple nodes.

The point behind using RMB is to allow the clients to not know much about the other process, or where it lives (client doesn't know network addresses, or identity). Unlike HTTP(S) where the caller must know exact address (or dns-name) and endpoints of the calls. Instead RMB requires you to only know about
- Twin ID (numeric ID) of where the service can be found
- Command (string) is simply the function to call

A single node can run multiple services behind a single RMB instance (on one node). Which means multiple commands can be handled by multiple processes running on the same node. But reachable only over a single RMB (with single node id) instance.

> It's illegal to run multiple RMBs on a single node, unless each using it's own separate redis instance otherwise they will conflict on redis queue.

## Overview of the operation of RMB
![rmb](png/RMB.png)

The process can be summarized as follows:
### Client
- A local process wants to call a function on remote/local process. It only know which twin-id, and the function (command) to run. The client also need to know what data the remote function expect.
- The local process creates a message. In it's minimal form, a message is
  - id: unique id of the message, it's the client responsibility to create a unique ID (uuid4 is recommended)
  - dst: Twin ID to handle the request (can be multiple)
  - cmd: the remote command name
  - data: payload of the command data (request body)
  - ret: return queue where the client expect the command response to arrive. Can be anything (unique to the process), but ideally can be the same `id` as the message itself. A client implementation can choose to have one queue for all responses depends on the implementation.
- Once the client construct the full message, it pushes it to it's local redis on specific queue (msgbus.system.local)
- The client then just wait on response on the `ret` queue.

### RMB (local)
Note that Local and Remote RMB are just one service. a single instance of RMB can run on any node and handle both local, remote, forwarding traffic. This list here only shows the operation when RMB receive a request from local client.


- RMB waits on messages coming over the `msgbus.system.local` queue. One a message arrives a message processing happens as follows:
  - verify message body is valid
  - verify dst twins are valid, for each twin in the list the identity and the IP of the dst twin is retrieved and cached if not already in cache.
  - if one or more twins are invalid, the message can't be processed, and an error message is pushed directly tot the `$ret` queue. with details of the error.
  - if $dst == $local  (where $local is local twin id) the msg is immediately forwarded to `msgbus.$cmd` for local processing by a local process. This takes RMB out of the picture for local inter process communication. Because the local receiver will just reply to the msg $ret which is waited on by the client process.
  - if $dst != local, this message is intended for remote process The message is then stored in redis on key (backlog.$id) AND a TTL is set on that with $exp. This means a message that never receive a response will eventually be flushed out of redis memory. Then the message id ($id) is pushed to `msgbus.system.forward`.
- The ids pushed to the `msgbus.system.forward` are handled in another routine of the system as follows:
  - The RMB maintain a set of workers, those workers are waiting for jobs pushed to them on a queue
  - When an ID is received on `msgbus.system.forward` queue the message is retrieved from `backlog.$id` then **signed**, and pushed to a free worker
  - The worker then can start (trying) to push this to remote RMB over the `/rmb-remote` url. The IP has already been resolved earlier from the cache (or re-retrieved if needed)
  - The worker can try up to `$try` times before giving up and report an error for that `$dst` id.

### RMB (remote)
- On receiving a message on the `/rmb-remote` entrypoint. This entry-point will verify message body, and signature. A 202 Accepted is returned if all is okay, 400 BadRequest, or 401 Unauthorized if signature verifications fails.
- If message $dst == $local, $ret is set to `msgbus.system.reply` and pushed to `msgbus.$cmd`

### RMB (remote) again
- Once a process handles messages on `msgbus.$cmd` and push response to `msgbus.system.reply`
- Messages are poped from this queue, and signed before pushing to the http workers
- The http workers can pick this message as well, but then now push the message to `/rmb-reply`. failure to do this is logged only, after all retries are exhausted (not retires are only done on connection errors, not HTTP errors)

### RMB (local) again
- If a message is received, if $dst != $local an http 403 Forbidden is returned.
- Original message is GET from the backlog. If message does not exist anymore (timed-out) return 408 Request Timeout (?) may be not the best code we can change later
- If message still available, an 202 Accepted is returned instead.
- Response is pushed to $ret (from the original message from the backlog)


# Components
This is a rough guide of the separate entities that can be developed in parallel. Of course this can be modified and tweaked during development to make sure it operates as intended.

## The message
```json
{
  "ver": 1,                                # version identifier (always 1 for now)
  "uid": "uuid4",                          # unique id (filled by server)
  "cmd": "wallet.stellar.balance.tft",     # command to call (aka function name)
  "exp": 3600,                             # expiration in seconds (relative to 'now')
  "try": 4,                                # amount of retry if remote cannot be joined
  "dat": "R0E3...2WUwzTzdOQzNQUlkN=",      # data base64 encoded
  "src": 1001,                             # source twin id (filled by server)
  "dst": [1002],                           # list of twin destination id (filled by client)
  "ret": "5bf6bc...0c7-e87d799fbc73",      # return queue expected (please use uuid4)
  "shm": "",                               # schema definition (not used now)
  "now": 1621944461,                       # sent timestamp (filled by client)
  "err": ""                                # optional error (would be set by server)
}
```

## Client
to be defined

## Identity
Identity is the RMB twin identity. It's created with the private key of the twin (or mnemonics). It uses this identity to retrieve it's twin id from substrate.

This must be built before any of other rmb operation proceed.

```rust
trait Identity {
    // returns signed message
    id() -> u64
    sign(msg: Message) -> Message
}
```

## Twin
A Twin represents a remote twin, A remote twin is identified by it's ID and must hold it's public key as well. A twin can be retrieved from substrate with a given ID. then cached in a local cache (in memory)

```rust
trait Twin {
    id() -> u64
    verify(msg: &Message) -> Result<()>
    address() -> String // we use string not IP because the twin address can be a dns name
}
```

## Redis abstraction layer
One component that need to be built is the Redis wrapper. This hides the calls to local redis and instead expose a concrete and statically typed trait to set, get, pop, and push data to separate "queues" as follows
```rust

enum QueuedMessage {
    Forward(Message)
    Reply(Message)
}

trait Storage {
    // operation against backlog
    // sets backlog.$uid and set ttl to $exp
    set(msg: Message) -> Result<()>
    // gets message with ID.
    get(id: String) -> Result<Option<Message>>

    // pushes the message to local process (msgbus.$cmd) queue
    run(msg: Message) -> Result<()>

    // pushes message to `msgbus.system.forward` queue
    forward(msg: Message) -> Result<()>

    // pushes message to `msg.$ret` queue
    reply(msg: Message) -> Result<()>

    // gets a message from local queue waits
    // until a message is available
    local() -> Result<Message>

    // find a better name
    // process will wait on both msgbus.system.forward AND msgbus.system.reply
    // and return the first message available with the correct Queue type
    process() -> Result<QueuedMessage>
}
```

Not implementation for storage should be implement connection pooling, also a Storage object can be passed around and cloned to be used by other parts of the system in async/io fashion

## HTTP Server
this server implements only 2 end-points as explained above
- `/rmb-remote`
- `/rmb-reply`

The server holds an instance to Storage. If you followed the diagram the endpoints will need to be able to:
- Has instance of Identity
- Able to retrieve a twin object. A twin implementation can support a get twin operation which will either access the cache or retrieve the twin from the chain and cache it.
- Use storage object either to do `run`, `forward`, or `reply` as per the sequence diagram

## Dispatcher no.1 (local dispatcher)
This one is simple, it calls Storage.local() and then decide what to do with the message based on the sequence diagram

## Dispatcher no.2 (http workers)
This is a little bit complex. It needs to do the following:
- Wait until an http worker is free (the worker should ask for a new job)
- call Storage.process() and block until a message is available.
- When a message is available, the message is signed with twin identity
- Message is sent to the worker, along side the twin object
- worker can then try to call either `/rmb-remote` or `/rmb-reply` based on the Queue type.
- if **FORWARD** the worker can report the caller immediately with an error if message was not able to deliver (calling Storage.reply() with right error filled in)
