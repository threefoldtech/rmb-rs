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
  - When an ID is received on `msbugs.system.forward` queue the message is retrieved from `backlog.$id` then **signed**, and pushed to a free worker
  - The worker then can start (trying) to push this to remove RMB over the `/rmb-remote` url. The IP has already been resolved earlier from the cache (or re-retrieved if needed)
  - The worker can try up to $try times before giving up and report an error for that $dst id.

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
