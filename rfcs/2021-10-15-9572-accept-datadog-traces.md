# RFC 9572 - 2021-10-15 - Accept Datadog Traces

This RFC describes a change that would:

* Bring initial support for traces
* Ingest traces from the Datadog trace agent (a.k.a. the client side of the APM product)
* Relay ingested traces to Datadog

## Context

This RFC is part of the global effort to enable Vector to ingest & process traffic coming out of Datadog Agents. Vector
internal tracing has its own [RFC].

[RFC]: https://github.com/vectordotdev/vector/blob/97f8eb7/rfcs/2021-08-13-8025-internal-tracing.md

### Some details about traces handling by the Datadog Agent

Offical "Datadog Agent" bundles (rpm/deb/msi/container image) actually ship multiple binaries, collectively named
"agents". Each of these "agents" is tasked to collect some data. For example "the core agent" (often shortened to "the
agent", because it was the first of those agents to be released) is the one collecting metrics, logs and running checks.
There are other agents like the process-agent, the security-agent or the trace-agent. But all of those are part of the
official "Datadog Agent" distribution logic, and they all come out of the [datadog-agent] codebase. So we are focusing
on the `trace-agent` which is one of the several binaries shipped along with others agents.

Traces are collected by this specific agent, that comes with a lot of dedicated configuration settings (usually under
the `apm_config` prefix), but it also shares some global option like `site` with other agents to select the Datadog
region where to send data. It exposes a [local API], that is used by [tracing libraries] to submit traces & profiling
data.

It has several communication channels to Datadog:

* Processed traces are relayed to `trace.<SITE>` (can be overridden by the `apm_config.apm_dd_url` config key)
* Additional profiling data are relayed to `intake.profile.<SITE>` (can be overridden by the
  `apm_config.profiling_dd_url` config key), they are not processed by the trace-agent and [relayed directly to Datadog]
* Some debug log are simply [proxified] to the log endpoint `http-intake.logs.<SITE>` (can be overridden by the
  `apm_config.debugger_dd_url` config key), it is fairly [recent] and unused as of October 2021.
* It emits [metrics], it does so for observability purpose, those metrics are sent to the core agent using the
  [dogstatsd] protocol, usually running on the same host (it could be in a different container if using the official
  Helm chart). The core agent then forward all those metrics with additional enrichment (hostname, tags) to Datadog.

Profiling and Tracing are enabled independently on traced applications. But they can be correlated once ingested at
Datadog, mainly to refine a span with profiling data.

The trace-agent encodes data using protobuf, .proto are located in the [datadog-agent repository]. Trace-agent requests
to the trace endpoint contain two major kind of data:

* standard traces that consist of an aggregate of spans (cf.[[1]] & [[2]])
* it also sends "selected" spans (a.k.a. APM events) (cf.[[3]], they are extracted by the [trace-agent], once ingested
  by Datadog those selected spans are used under the hood to better identify & contextualize traces, they can be fully
  indexed as well ([short description of APM events])

[datadog-agent]: https://github.com/DataDog/datadog-agent/
[official container image]: gcr.io/datadoghq/agent
[local API]: https://github.com/DataDog/datadog-agent/blob/main/pkg/trace/api/endpoints.go
[tracing libraries]: https://docs.datadoghq.com/developers/community/libraries/#apm--continuous-profiler-client-libraries
[relayed directly to Datadog]: https://github.com/DataDog/datadog-agent/blob/44eec15/pkg/trace/api/endpoints.go#L83-L86
[proxified]: https://github.com/DataDog/datadog-agent/blob/44eec15/pkg/trace/api/endpoints.go#L96-L98
[recent]: https://github.com/DataDog/datadog-agent/blob/7.31.x/CHANGELOG.rst?plain=1#L60
[metrics]: https://docs.datadoghq.com/tracing/troubleshooting/agent_apm_metrics/
[dogstatsd]: https://github.com/DataDog/datadog-agent/tree/44eec15/pkg/trace/metrics
[datadog-agent repository]: https://github.com/DataDog/datadog-agent/blob/0a19a75/pkg/trace/pb/trace_payload.proto
[1]: https://github.com/DataDog/datadog-agent/blob/0a19a75/pkg/trace/pb/trace_payload.proto#L11
[2]: https://github.com/DataDog/datadog-agent/blob/0a19a75/pkg/trace/pb/trace.proto#L7-L12
[3]: https://github.com/DataDog/datadog-agent/blob/0a19a75/pkg/trace/pb/trace_payload.proto#L12
[trace-agent]: https://github.com/DataDog/datadog-agent/blob/0a19a75/pkg/trace/event/processor.go#L47-L91
[short description of APM events]: https://github.com/DataDog/datadog-agent/blob/e081bed/pkg/trace/event/doc.go

## Cross cutting concerns

N/A

## Scope

### In scope

* Ingest traces from the trace-agent in the `datadog_agent` source
* Send traces to the Datadog trace endpoint through a new `datadog_trace` sink
* Basic operation on traces: filtering, routing
* Pave the way for OpenTelemetry traces

### Out of scope

* Profiling data, but as the trace-agent only proxies profiling data, the same behaviour can be implemented rather
  quickly in Vector.
* Debugger logs can be already diverted to Vector (untested but it should work as Vector supports datadog logs and there
  is an config option to explicitly configure the debugger log destination)
* Metrics emitted by the trace-agent (they could theoretically be received by Vector by a statsd source, but the host
  used by the trace-agent is derived from the local config to programmatically discover the main agent, thus there is no
  existing knob to force the trace agent to send metrics to a custom dogstatsd host)
* Span extraction, filtering
* Other sources & sinks for traces than `datadog_agent` source & `datadog_trace` sink

## Pain

* Vector does not support any traces (full json representation may be ingested as log event) at the moment and it is a
  key part of observability

## Proposal

### User Experience

* User will be able to ingest traces from the trace agent
  * Vector config would then consist of: `datadog_agent` source -> some filtering/enrichment transform ->
    `datadog_trace` sink
  * Datadog trace agent can be configured to send traces to any arbitrary endpoint using `apm_config.apm_dd_url` [config
    key]
* This change is a pure addition to Vector, there will be no impact on existing feature

[config key]: https://github.com/DataDog/datadog-agent/blob/34a5589/pkg/config/apm.go#L61-L87

### Implementation

To keep vector-core as generic as possible, the first implementation will decode datadog traces as `LogEvent`, the
resulting event will be deeper than usual but this should not be a problem. To keep track that those `LogEvent`s are
traces, we could add a simple `Option<bool>` to the `EventMetadata` struct. However this may not be immediately
required. While being naive this does not predates future vector evolution towards event traits by not adding a new
concrete type into the `Event` enum.

Upcoming [work][schema-rfc] on having the ability to validate a `LogEvent` against a schema would provide a
nice way (with the performance question) of ensuring that a `datadog-traces` sinks would receive a properly structured
`LogEvent`.

Based on the aforementioned work the following source & sink addition would have to be done:

* A `datadog_agent` addition that decodes incoming [gzip'ed protobuf over http] to a `LogEvent` .proto files are located
  in the [datadog-agent repository].
* A new `datadog_trace` sink that does the opposite conversion and sends the trace to Datadog to the relevant region
  according to the sink config.

The `datadog_agent` agent addition would materialize as new filter (like the [one dedicated to receive
logs][event-filter]), ideally colocated the trace decoding logic in its own source file
(./src/sources/datadog/traces.rs). The filter would be attached to the  warp server upon a new configuration flags. This
way the traces related code would be isolated. New configuration flags would be three booleans, for logs, metrics and
traces enabling/disabling each datatype. This way the user can multiplex all three datatype over a single socket, or a
socket per one or more datatype at users convenience.

Datadog API key management would be the same as it is for Datadog logs & metrics.

[schema-rfc]: https://github.com/vectordotdev/vector/pull/9388
[gzip'ed protobuf over http]: https://github.com/DataDog/datadog-agent/blob/8b63d85/pkg/trace/writer/trace.go#L230-L269
[datadog-agent repository]: https://github.com/DataDog/datadog-agent/blob/0a19a75/pkg/trace/pb/trace_payload.proto
[event-filter]: https://github.com/vectordotdev/vector/blob/a0ca04c/src/sources/datadog/agent.rs#L206-L233

## Rationale

* Traces support is expected by users
* Local sampling is an interesting feature to lessen the amount of data sent to Datadog

## Drawbacks

* Adding a brand new datatype has a large impact and although it will be mostly a code addition, it will impact
  vector-core

## Prior Art

* Internal Rust traces can be converted into [log event], but this is not reversible. This is still a good way of
  getting a text-based representation

[log event]: https://github.com/vectordotdev/vector/blob/bd3d58c/lib/vector-core/src/event/log_event.rs#L402-L432

## Alternatives

* Regarding internal traces representation, instead of reusing the `LogEvent` type, a new `Trace` concrete type could be
  added to the `Event` enum:
  * Either specific implementation per vendor, allowing almost direct mapping from protocol definition into Rust
    struct(s).
  * Or generic enough struct, most likely based on the [OpenTelemetry trace format], possibly with additional fields to
    cover corner cases and/or metadata that may not be properly mapped into the OTLP trace structure. Overall, there is
    no huge discrepancy between Datadog traces and OpenTelemetry traces (The trace-agent already offers [OTLP->Datadog]
    conversion).

[OpenTelemetry trace format]:
https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/trace/v1/trace.proto
[OLTP->Datadog]: https://github.com/DataDog/datadog-agent/blob/637b43e/pkg/trace/api/otlp.go#L305-L377

## Outstanding Questions

* Confirm that this RFC only addresses traces.
* Confirm wether we rely on `LogEvent` or if we introduce a new type to represent traces inside Vector.

## Plan Of Attack

* [ ] Submit a PR introducing traces support in the `datadog_agent` source emitting a `LogEvent` for each trace and each APM event.
* [ ] Submit a PR introducing the `datadog_trace` sink that would

## Future Improvements

* Support for additional trace formats, probably OpenTelemetry first
* Profile support
* Ingest traces from Datadog tracing libraries directly
* Opentelemetry exporter support (the Datadog export would probably be easily supported once this RFC has been
  implemented as it's using the same [Datadog endpoint] as the trace-agent
* Traces helpers in VRL
* Trace-agent configuration with a `vector.traces.url` & `vector.traces.enabled`

[Datadog endpoint]: https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/04f97ec/exporter/datadogexporter/config/config.go#L288-L290