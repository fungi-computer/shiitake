# Audited Shiitake proof disposition

| Audited file                         | Tier and reason                                                                                                                                               |
| ------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cloudflare-durable-wake.native.mjs` | Merge: actual Worker restart and native alarm rollback/recovery must supplement the unit laws; its fixture owns its ports, SQL store and deterministic model. |

`test:unit` retains the Vitest suite for batch. `test:merge` owns the native
proof; ordinary `test` joins both for nightly and direct full verification.
Build prerequisites belong to Turbo; neither command rebuilds shared output.

The failure witness observes the rejected storage alarm write. The maintained
owner catches wake errors and re-arms itself, so requiring a thrown outer
`alarm()` call would never observe this refusal. The proof still requires a
later successful alarm with a finite recovery deadline and only one live model
execution; it also asserts exactly one injected write rejection.

The October 2 Agent-turn recovery contract supersedes the old interrupted-turn
expectation: both the recovered running turn and the queued command must
complete once, in order, without target traffic after restart or a cancellation.
The fixture supplies exactly those two model responses in the new incarnation.
