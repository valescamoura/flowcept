# Academy Redis Message Stream Adapter

This adapter explores how FlowCept can observe Academy workflows by watching
Academy's Redis-based message exchange and translating message traffic into
FlowCept task records.

## Context

Academy's Redis exchange does not use Redis Pub/Sub or Redis Streams for its
main agent messages. It implements each entity mailbox as a Redis list:

```text
queue:<entity_uuid>
```

Messages are sent with `RPUSH queue:<dest_uuid> <pickled academy.message.Message>`
and received by Academy with `BLPOP queue:<mailbox_uuid>`.

Academy also uses supporting keys:

```text
active:<entity_uuid>
agent:<agent_uuid>
request:<dest_uuid>:<message_tag>
heartbeat:<entity_uuid>
```

This means a passive observer must be careful: consuming from `queue:*` would
remove messages that Academy itself needs to process.

## Design Options

### 1. Patch or wrap `RedisExchangeTransport.send`

This approach intercepts Academy messages at the point where Academy sends them
to Redis. It can duplicate every message into FlowCept without racing Academy's
own consumers.

Advantages:

- Reliable access to the original `academy.message.Message` object.
- No need to reconstruct pickled payloads from Redis command text.
- Minimal risk of losing events once the wrapper is active.

Disadvantages:

- It modifies or monkey-patches the Academy runtime.
- It is closer to runtime instrumentation than external observability.
- It may be perceived as stronger coupling to Academy internals.

### 2. Observe Redis with `MONITOR`

This approach attaches an observer to Redis using the `MONITOR` command and
listens for Academy `RPUSH queue:*` commands. It does not consume from Academy's
mailboxes and therefore does not interfere with normal message delivery.

Advantages:

- Lowest interference with Academy.
- Does not require code changes in the Academy workflow.
- Matches the goal of observing the message stream externally.

Disadvantages:

- Redis `MONITOR` output is command-oriented and may expose binary pickle
  payloads in an escaped textual form.
- Payload reconstruction can be fragile across Redis clients, Redis versions,
  and binary content.
- `MONITOR` observes all Redis commands, so production use would need filtering
  and care around overhead and sensitive data.

### 3. Add an explicit Academy message mirror

This approach changes Academy or an Academy integration layer so that every
message sent to `queue:*` is also mirrored to a durable stream such as
`XADD academy:messages ...` or to Pub/Sub.

Advantages:

- Cleanest event source for downstream observers.
- Can preserve message metadata in a stable, documented format.
- Easier to replay and test than Redis `MONITOR`.

Disadvantages:

- Requires changing Academy behavior or adding an Academy-side integration.
- No longer observes the unmodified Academy Redis exchange.
- Adds a second message path whose semantics must be maintained.

## Chosen Approach for This Experiment

For the `message_stream_observability` experiment, we start with option 2:
Redis `MONITOR`.

The goal of this approach is to interfere as little as possible with Academy.
Because Academy mailboxes are Redis lists, a direct consumer using `BLPOP` would
steal messages and break the workflow. `MONITOR` lets us observe `RPUSH`
commands without consuming from the queues.

This adapter is intentionally experimental. Its first job is to determine
whether the Academy message payload can be reconstructed reliably from the
Redis command stream. If reconstruction proves too fragile, the next best
design is option 3: an explicit Academy message mirror.

## Current Implementation

The current code provides:

- `AcademyRedisMonitorInterceptor`: a FlowCept `BaseInterceptor` subclass that
  watches Redis `MONITOR` output.
- `AcademyRedisMonitorParser`: parser utilities for detecting `RPUSH queue:*`
  events and attempting to deserialize Academy messages.

When the Academy message can be deserialized, the adapter correlates request
and response messages by their Academy message tag and emits one FlowCept task
for the whole exchange. When deserialization fails, it still emits an observed
task containing raw Redis command metadata so the experiment can measure what
was observable.

## Data Selection Decisions

Academy's Redis exchange contains both message data and operational runtime
bookkeeping. This adapter treats them differently.

### Captured as FlowCept tasks

The adapter captures `RPUSH queue:*` commands whose payload is an
`academy.message.Message`.

These messages are the core provenance signal for agentic workflows because
they contain:

- source and destination entity identifiers;
- message tag and label for request/response correlation;
- action names such as `calc_fibs` and `next_item`;
- action arguments;
- action results or errors;
- Redis queue metadata and timestamps.

The adapter does not emit one FlowCept task per Redis message. Instead, it
collapses request/response pairs with the same Academy message tag into a
single `TaskObject` with subtype `academy_exchange_interaction`.

This is the current modeling decision because a Redis message is a transport
event, while a FlowCept task is a better fit for the semantic interaction: one
agent asks another entity to perform an action and receives a result or an
error. Collapsing the pair avoids over-counting transport packets as workflow
tasks and makes this approach easier to compare with action-level Academy
instrumentation.

For action calls:

- `activity_id` is the requested Academy action name when the request is
  available.
- `source_agent_id` is the requester.
- `agent_id` is the destination/executor mailbox.
- `group_id` is the Academy message tag.
- `used` contains the semantic action inputs (`args` and `kwargs`).
- `generated` contains the semantic action result, or exception metadata for
  errors.
- `custom_metadata.communication.request` stores the raw request message and
  Redis command metadata.
- `custom_metadata.communication.response` stores the raw response message and
  Redis command metadata.

Incomplete or asymmetric exchanges are represented explicitly:

- `pairing_status = "complete"`: request and response were both observed.
- `pairing_status = "request_without_response"`: the adapter observed a
  request but did not observe a response before shutdown. The emitted task is
  left in `SUBMITTED` status.
- `pairing_status = "response_without_request"`: the adapter observed a
  response whose request was not seen, usually because the observer started too
  late or the request was not deserializable. The emitted task uses the response
  status.
- `pairing_status = "unpaired_message"`: the payload was an Academy message but
  did not follow the request/response shape.

This keeps the original message fragments queryable without treating the
transport fragments themselves as independent domain tasks.

### Used to enrich captured tasks

The adapter observes and stores selected operational metadata in memory so it
can enrich later message tasks:

- `agent:<agent_uuid>` maps an Academy `AgentId` to the registered agent class
  and MRO. This helps interpret opaque agent identifiers in provenance records.
- `heartbeat:<entity_uuid>` stores the latest heartbeat timestamp when Academy
  publishes one. This is a lightweight liveness/telemetry signal.

These keys are not emitted as standalone FlowCept tasks in the current
experiment because they do not represent domain actions. They are contextual
runtime metadata.

### Currently ignored as standalone records

The adapter intentionally does not convert the following Redis keys/events into
FlowCept tasks:

- `active:<entity_uuid>`: mailbox lifecycle state. Useful for debugging, but it
  does not describe a domain action or data transformation.
- `request:<entity_uuid>:<message_tag>`: Academy's internal request tracking.
  This is redundant with the message header fields already captured from
  `academy.message.Message`.
- close sentinels written to `queue:*`: transport-level shutdown markers, not
  Academy messages.

These decisions keep the experiment focused on provenance-bearing messages
while still retaining useful agent/runtime context where it adds explanatory
value.
