# Scope and acceptance criteria

## User-visible problem

- [HTTPX discussion #3391](https://github.com/encode/httpx/discussions/3391)
  describes body aggregation that prevents observing progressive delivery in
  an in-process transport. This is motivation, not a claim about every current
  HTTPX version or transport.
- [FastMCP issue #4192](https://github.com/PrefectHQ/fastmcp/issues/4192)
  reports orphan SSE tasks and eventual hangs after repeated sessions on
  Windows. We have not independently reproduced that report. Our tests must
  not imply that fixing one framework bug validates all disconnect behavior.

## Existing building blocks

- [httpx-sse](https://github.com/florimondmanca/httpx-sse): SSE parsing/client API.
- [async-asgi-testclient](https://github.com/vinissimus/async-asgi-testclient):
  in-process ASGI tests, including streaming.
- [SSE Agent Bench](https://github.com/AlexJ1ang/sse-agent-bench): black-box
  functional evaluation, replay, regression comparison, and load testing.
- [aimock](https://github.com/CopilotKit/aimock): mock AI application dependencies.
- [pyleak](https://github.com/deepankarm/pyleak): task/thread leak detection.

This project is an experiment in composing real-network tests with explicit
application lifecycle probes. It makes no claim to be the first or only tool
in this space. Use existing parsers and transports rather than reimplementing
protocols. Avoid a general testing platform.

## Milestones

1. Installable pytest entry point; bounded loopback ASGI server; startup,
   shutdown, failure and socket-release tests; CI and packaging checks.
2. Streaming and cleanup contracts, backed by intentionally broken examples:
   - observe an event before a gated producer finishes;
   - disconnect while production is still gated; require explicit cleanup
     observation, and separately require upstream cleanup when instrumented;
   - repeat independently instrumented sessions without hiding outstanding work;
   - report timeouts as failures, never silently classify unknown cleanup as OK.
3. Comparative examples showing what a body-only assertion misses, plus
   integration and repeatability checks. No performance or adoption claims
   without measurement.

## Implementation constraints

Retain the port-zero socket until the server owns it. Bind only 127.0.0.1. Do
not install signal handlers or change application/global logging configuration.
Only stop server-owned tasks; never cancel every task in the test event loop.
Use synchronization barriers for test ordering, with wall-clock deadlines only
as bounded failure detection. Do not equate TCP writes with client consumption,
client closure with server cancellation, or a Python cleanup hook with a remote
provider's acknowledgement.
