# Plugins

A plugin is a built in service that can intercept user local requests, modify them before it send them to remote peer, and
also handle incoming requests (and optionally send back responses).

## Interception points

Once a plugin is started, a plugin can then intercept at the following interface points:

- `local` this method will receive outgoing user requests to a remote peer. If the command is prefix with `${plugin.name}.`
the plugin will receive the request, and then it can decide either to `hijack` the request or forward it as is.
  - NOTE: if the plugin choose to forward the request as is, any received responses to that request will be then directly
    channeled to the client (based on request `reply-to`). If the plugin choose to hijack the request it then can choose
    to reroute the returning response.
- `remote` this method will receiving incoming requests that that has a command that is prefixed with `${plugin.name}.`. The plugin
then can decide to either drop the request or send a response(s) back to caller. If receiving a remote request from a remote peer
the backlog (tracker) provided to the method will always be None.
If a remote message is only routed to the plugin because a tracker existed for that message ID that routed the message back to the plugin
hence the tracker will also be provided to the method.

In other words `remote` will be called if a message is:

- a request with a command that is prefixed with `${plugin.name}.`, tracker will be None
- a response message that has an associated tracker that specifically routed the message to plugin, in that case tracker will be set

Note: the only way a response message will have a tracker set is by the plugin itself set the tracker before sending a request message by the means of the postman, hence any related responses will be routed back with the tracker information

## Postman

The plugin is granted to receive a `start` call before any of the `local` or `remote` methods are called. The start method will
receive a `Send` (stream) channel that accepts a `Bag` object. The send channel is the plugin only way to send message to remote peers
by constructing an `Envelope` with all fields that is needed for message delivery, and an optional `Backlog` object that can be used
to route expected responses (if there are any) back to the plugin.

## Available Plugins

There are two plugins already included in this implementation one of them is very simple and was mainly to be used as a simple example `rmb` implemented in the [rmb.rs](rmb.rs) file. It handles a request to ask the peer for its version and send a simple response. Can be extended later with more functionality.

The other more complex plugin is `upload` plugin implemented in [upload.rs](upload.rs) file. Please read the plugin doc for explanation of its operation
