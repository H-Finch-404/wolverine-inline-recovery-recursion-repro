# Wolverine: inbox recovery into parallel inline listeners recurses forever

Minimal reproduction of two Wolverine bugs found in production-like environments (Azure Service Bus sessions,
PostgreSQL message storage). Both reproduce on WolverineFx **6.51.3** and the latest **6.61.0**.

## Run

Requires Docker (Testcontainers starts PostgreSQL and RabbitMQ).

```bash
dotnet test                                   # WolverineFx 6.51.3
dotnet test -p:WolverineVersion=6.61.0        # any other version
dotnet test -- --explicit only                # the stack overflow on host stop (crashes the test host)
```

| Test | Result |
|---|---|
| `recovery_into_a_single_inline_listener_completes` | passes (control) |
| `recovery_into_parallel_inline_listeners_completes` | **fails**: `Recovery never returned from EnqueueDirectlyAsync; the inbox is hit ~4000 times/s` |
| `recovered_cloud_event_with_a_non_guid_id_is_marked_as_handled` | **fails**: `Inbox row … is still Incoming after it was handled` |
| `stopping_the_host_after_recovery_into_parallel_inline_listeners` (explicit) | test host dies with `Stack overflow.` |

## Bug 1: infinite recursion when recovering into an inline endpoint with `ListenerCount > 1`

`ProcessInline()` with `ListenerCount(n > 1)` — which is also what Azure Service Bus `RequireSessions(n)` builds —
makes `ListeningAgent.Listener` a `ParallelListener`. When the durability agent recovers a dormant inbox row for that
endpoint (`RecoverIncomingMessagesCommand` → `ListeningAgent.EnqueueDirectlyAsync`):

1. `EnqueueDirectlyAsync` hands the envelopes to the `InlineReceiver` with `new RetryOnInlineChannelCallback(Listener, runtime)`.
2. `InlineReceiver` sets that callback as `envelope.Listener` (`MarkReceived`) and runs the handler.
3. On success, `RetryOnInlineChannelCallback.CompleteAsync` marks the inbox row handled, then calls `_inner.CompleteAsync`.
4. `_inner` is the `ParallelListener`, whose `CompleteAsync` is `envelope.Listener!.CompleteAsync(envelope)` — the
   `RetryOnInlineChannelCallback` again. Back to 3, forever.

Consequences:

* `EnqueueDirectlyAsync` never returns ("Successfully recovered …" is never logged).
* One `update … set status = 'Handled' …` per iteration, as fast as the database answers (~4,000/s locally, ~30/s
  against Azure PostgreSQL), burning CPU on the app and the database.
* Every iteration awaits the next, so the chain of pending async state machines grows without bound: a managed memory
  leak (~140 MB/h in our environment) until the host runs out of memory.
* On shutdown the message store is disposed, `MarkIncomingEnvelopeAsHandledAsync` returns synchronously, and the
  recursion overflows the stack, crashing the process.
* Because the recovery never finishes, the same row is recovered again by the next process after every restart.

Possible fixes: don't forward completion to `envelope.Listener` when that is the wrapping callback (e.g. let
`RetryOnInlineChannelCallback` skip `_inner` for a `ParallelListener`, or have `EnqueueDirectlyAsync` not wrap the
`ParallelListener`); an envelope recovered from storage has no broker delivery to settle anyway.

Workaround (Azure Service Bus sessions): use a single listener and let the session processor run the sessions:

```csharp
opts.ListenToAzureServiceBusQueue("queue")
    .RequireSessions()                     // ListenerCount 1: no ParallelListener
    .ConfigureSessionProcessor(p => p.MaxConcurrentSessions = 16)
    .ProcessInline();
```

## Bug 2: `CloudEventsMapper` replaces the envelope id when the CloudEvent id is not a Guid

`CloudEventsMapper.mapIncoming` sets `envelope.Id = Envelope.IdGenerator()` whenever the CloudEvent `id` does not
parse as a Guid (common for non-Wolverine producers; ours sends Auth0 log ids). It does this on every read, including
`ReadFromData` for an envelope loaded from the inbox that already has an id. The recovered message is then marked as
handled under the new random id, the stored row stays `Incoming`, and it is recovered and handled again after every
restart. A random id per read also defeats inbox de-duplication of broker redeliveries.

Possible fix: keep an existing envelope id, and for a new message derive a stable name-based Guid from the CloudEvent
`source` + `id` (the pair is unique per the CloudEvents spec), as #5268 / #5273 already do for Kafka, NATS, Pulsar,
SQS and others.
