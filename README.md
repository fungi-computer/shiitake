# Shiitake by Fungi (`@fungi.computer/shiitake`)

Fungi is an open-source workspace for AI agents: Apps that people and agents
both use, Computers that keep their files, and permissions the system checks.
Try it with one command, run it on your own Cloudflare account, or let us host
it.

Shiitake by Fungi runs durable Agent conversations. A Session holds accepted
work, messages and observations, while the host supplies storage, models, tools
and the wake that runs queued work.

There are three ways to run it:

- **In your code**: install this package and host Agents yourself, on Node for
  tests or in a
  [Cloudflare Durable Object](https://docs.fungi.computer/shiitake/run/cloudflare/).
- **On your machine**: the [`fungi` CLI](https://docs.fungi.computer/install/)
  is a Shiitake Agent with a terminal interface.
- **Hosted**: [Fungi](https://docs.fungi.computer/shiitake/run/fungi/) runs
  Agents for your Team, and its models work from your own host too.

To add your own behaviour, start the extension guide at
[Your first extension](https://docs.fungi.computer/shiitake/extensions/first-extension/).

## Choose how to start

**Use the terminal agent.** Run Shiitake in your project folder, on your Claude
or ChatGPT subscription, a provider key or Fungi hosted models. Needs Node 22.13
or newer on Linux x64/arm64 with glibc 2.35+, or macOS 13.5+ on Apple Silicon or
Intel. Windows and Alpine/musl are not supported. On Apple Silicon, use arm64
Node rather than an x64 installation under Rosetta.

```sh
npm i -g @fungi.computer/spore
cd your-project
fungi
```

**Embed the library.** Host durable Agent Sessions in your own code. This runs
one Session with a test model, so it needs no provider account or key.

```sh
npm init -y && npm pkg set type=module
npm install @fungi.computer/shiitake @earendil-works/pi-ai@1.3.0
npm install --save-dev tsx@4.23.12
```

```ts title="hello.ts"
import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux";
import { memoryHost, fauxModelSource } from "@fungi.computer/shiitake/testing";

const host = await memoryHost({
  model: fauxModelSource([fauxAssistantMessage("Ready.")]),
});
try {
  const session = host.agent.session("first-session");
  await session.send("Say hello.", { requestId: "first-session-prompt" });
  const snapshot = await host.settle("first-session");
  console.log(JSON.stringify(snapshot.messages.at(-1)?.value, null, 2));
} finally {
  await host.close();
}
```

Run it with `node --import tsx hello.ts`; the last message is the reply
`Ready.`.

## Set up

```sh
npm install @fungi.computer/shiitake
```

The package ships ESM only. Its runtime dependencies include Effect, Zod, Pi and
the Fungi packages [Cairn](../cairn/README.md), [Mule](../mule/README.md),
[Mycelium](../mycelium/README.md), [Watchdog](../watchdog/README.md),
[Bird Dog](../bird-dog/README.md) and Knapsack. See the [manifest](package.json)
for the complete list. Hono `^4.13.5` is a peer dependency for the HTTP
adapters.

`Shiitake.make(options)` is the Promise constructor. The Effect 4 constructor is
`make` from `@fungi.computer/shiitake/effect`. Both use the same owner. The host
supplies:

| Input          | Responsibility                                                                                         |
| -------------- | ------------------------------------------------------------------------------------------------------ |
| `store`        | A native SQLite owner with scalar rows and synchronous transactions.                                   |
| `runtime`      | Bind reconciliation, commit mutations with a wake guarantee, schedule work and close the registration. |
| `model`        | Acquire a Pi model and its stream, then release them. The host owns selection and credentials.         |
| `instructions` | Load an instruction snapshot and revision for the run.                                                 |
| `extensions`   | An explicit roster and capability grants. Use an empty roster and grants when none are needed.         |
| `tools`        | Optional Mycelium modules, sources, the host's sandbox and `access` grants passed down as data.        |
| `meta`         | Optional schema for the opaque metadata the host attaches to prompts and operator lines.               |

Construction needs an owner and a runtime host that meet their transaction and
wake contracts. Existing host fixtures show deterministic and native adapters.

## Quick start without a provider account

Node's built-in SQLite (Node 22.13 or newer) backs the public testing helper.
This runs the real durable Agent with a deterministic faux model; no credentials
or background timer are needed. Drive settlement explicitly.

```ts
import { memoryHost } from "@fungi.computer/shiitake/testing";

const host = await memoryHost();
try {
  const receipt = await host.agent.session("example-session").send("Hello", {
    requestId: "example-first-prompt",
  });
  const snapshot = await host.settle("example-session");
  console.log(receipt.status, snapshot.terminals.at(-1)?.outcome);
} finally {
  await host.close();
}
```

`memoryHost({ owner, model, extensions, tools })` accepts canonical inputs and
borrows a supplied owner without closing its database. `extensions` may be a
plain list; the test host then grants each extension exactly what it requests
(`Shiitake.make` always takes explicit grants). `script` replaces `model` and
`tools`: strings are replies, `callTool("bookmarks.add", input)` is a tool call
that a scripted sandbox runs through the admitted operation, and
`echoSystemPrompt()` replies with the system prompt the model received.
`fauxModelSource(replies)` supplies deterministic responses for each model
acquisition. `reconcile()` drives one canonical tick;
`settle(sessionId, maxTicks)` accepts a 1–1,000 tick budget (default 1,000) and
refuses when it is exhausted. This manually driven helper does not assert host
wake delivery; the native runtime-law tests qualify that scheduling contract.
The helper is **not isolation**: trusted callbacks retain the process's
privileges, and submitted JavaScript is never evaluated.

Production callers use `Shiitake.make(options)` with their storage and wake
owner. Keep that host open while it owns active work. Closing it stops runtime
resources; it does not erase SQLite data or turn acceptance into a completed
reply.

## Accept, execute and observe

An accepted prompt returns `status: "accepted"` and an owner-issued `commandId`
after the durable commit. Repeating the same request identity and input replays
the receipt. Changing the input under that identity is a conflict.

The runtime wakes reconciliation, acquires run resources, executes the model and
tools, releases resources and records settlement. The host owns provider
configuration and retry policy. Shiitake owns durable Session facts and work.

`session.read()` returns a bounded baseline. `session.watch()` owns a baseline
and live changes resource. Close the watch when its consumer leaves. Addressing
a Session performs no I/O. A client uses `createShiitakeClient` from the
`/client` entry with the host's authorized transport. Releasing that client does
not cancel host work.

Live `progress` frames carry `SessionProgressDelta`: independent text and
thinking `{ offset, append }` values, with offsets in UTF-16 string units.
`appendSessionProgress` from `/client` folds appends idempotently; a gap waits
for the owner's reset. A read/watch/reset baseline carries only the latest 64
KiB per channel, its start offsets, and accurate truncation flags. A watch
continues after that tail; the eventual durable message is always complete.
Progress stays process-local, in bounded chunks, and never writes SQLite. Tiny
provider deltas are coalesced (first append immediately, then each 4,096 string
units or within 100 ms); durable completion carries any remaining partial chunk.
Tool tails travel only in `tool_output`, not in message progress.

`session.readSettlement(commandId, { timeoutMs, signal })` returns that exact
command's `CommandSettlement` (terminal, answer, usage and tool-call count),
independent of the selected transcript branch. `timeoutMs` defaults to zero and
is bounded to 30,000 ms. A pending command returns `undefined` at the deadline;
an unknown command fails. Aborting the read never cancels the command.
`agents.wait` uses this read and settlement hints, not a watch or a transcript
read. In-process and HTTP clients share the contract; the HTTP encoding is
`GET /sessions/:sessionId/settlement?commandId=...&timeoutMs=...`, with `null`
for a pending command.

Cancellation targets an accepted `commandId`. `too_late` is an ordinary outcome
when that work can no longer be withdrawn. `session.abort()` atomically requests
cancellation of the running turn and every queued prompt in that Session.
Cancelled prompts retain their text for resubmission. Neither a cancelled
observation nor a closed browser proves a tool has stopped.

## Extensions and authority

Use `defineExtension("id")` from `/extensions`. The version defaults to `0.0.0`;
pass `{ id, version }` when a behaviour change is not visible in the schemas,
config or requested capabilities (for example a fixed hook). The version joins
those in the extension's revision; a recorded stop persists until the revision
changes or the host resumes the extension. Configuration, collections and
documents are declared before callbacks.
`.state({ name: defineCollection(...) })` declares state version 1; pass
`{ version, collections, migrations }` when a row schema changes. The host's
grants are separate from the definition's requests. Product feature names belong
to the host roster.

`context.state.read((s) => ...)` reads rows synchronously outside a transaction,
for example in a prompt section; `context.state.update` writes. A projection's
`read` returns the value itself (or an `Option`; `undefined` publishes none).
The `tools` builder's third argument exposes a declared command to the model:
`commands.tool("add", { description })` is a Mycelium operation that runs the
same handler in the same transaction, scoped to the run's Session.

Commands, settled hooks and timer hooks run synchronously within the owner
transaction. A command may instead declare `{ prepare: (call) => ... }`, and a
timer `{ prepare: (context, timer) => ... }`. Preparation is an Effect with read
access only: it may await host evidence, then returns the synchronous Effect to
commit. Re-read mutable rows in that body before applying captured evidence.
Timer consumption still commits or rolls back with its body. Model tools use
`context.transact({ prepare: ... })` for the same two phases; plain
`context.transact(effect)` needs no preparation. Asynchronous work in an atomic
body still stops the extension and rolls back. Prompt and tool builders run
outside that write transaction. Activation resources use the Effect scope. An
extension failure records a stop reason. Resuming it replays missed settled work
through its cursor.

`context.send({ sessionId, requireExistingSession: true, ... })` opts into an
existing target: the admission transaction rejects an absent Session with
`AdmissionError` reason `closed`, without creating it. Leave the condition out
when creating the addressed Session is intended. Automated continuations use the
condition to prevent recreating a Session after deletion.

An extension declaring `attention: true` can request the `attention` grant.
`context.attention.set(sessionId, item)` and `clear(sessionId, id)` share its
owner transaction and publish after commit. Items have `id`, `kind`, `summary`
(at most 200 characters), `since` and optional `expiresAt`; each Session carries
at most 64. An owner may retain at most 4,096 outstanding items across Sessions;
clearing releases capacity. Public observations qualify ids with `extensionId`.
Open attention makes the inbox `needs_you`, ahead of running or settled work.
Stopped owners are excluded until resumed, and owners without a current grant
are excluded until the grant returns. Expiry is display metadata; the
extension's timer owns clearing, and restarting does not discard pending
attention. Clearing an absent item, including after Session deletion, is
idempotent; setting an item requires an existing Session.

Clients bind an extension contract with `bindSessionExtension` or
`bindAgentExtension` from `/client`. Projection slots describe observed state,
not assumed completion. See the
[prompt/tool test](test/extension-prompt-sections.test.ts),
[state binding test](test/extension-state-binding.test.ts) and
[close test](test/c2-extension-close-orchestrator.test.ts).

An extension may request the exclusive `files` grant and return Knapsack-minted
`FileTool`s from `files(context, run)`. Core captures the tools once per run,
passes them to Mycelium beside `execute`, and records their names in the
execution protocol digest. A second granted holder is refused. This contribution
adds no shared Files module to `execute`; explicit Computer capabilities remain
independent.

Shiitake holds no permission logic. Tool authors label each operation `read` or
`write` (and optionally `confirm`); the host owns every grant decision and
delivers it down as versioned data, and Mycelium decides each call.
`tools.access.grants` is the Agent-wide default. The in-process owner returned
by `Shiitake.make` delivers one Session's grants with
`agent.grants.deliver(sessionId, { version, grants })`, synchronously; code
inside a Cloudflare owner also calls `deliverSessionGrantsToStorage` while its
runtime is cold. Versions never move back: an older version is refused, and the
same version with other grants replaces the stored ones, so a host that
recomputes its decision never leaves the old grant in place. Delivered grants
override the default key by key, and Mycelium reads them at every call, so a
change applies to the very next call, including in queued, recovered and
extension-started runs. Grants are keyed by module id; a module an extension
built is granted by its roster grant's `grantKey` (the extension id by default),
and Files direct tools by `files`. Keys listed in `access.required` fail closed:
a Session with no delivered or default grant for them is refused. With no access
data at all, every tool keeps full access, so a first-time host writes no
permission code; a `confirm` operation is still refused unless the host supplies
`access.confirm`. A Files extension still requires a host tool runtime.
Extension tool authority and final external access remain host decisions.

## Agent fleet

`@fungi.computer/shiitake/fleet` owns Agent directory reservations, replay
receipts, erasure addresses, granted Session operations and hosted BirdDog
delegation. Hosts supply grants containing Agent scope, actions, expiry and
provenance. Fleet operations do not read Team, billing or broker policy. Usage
has no fleet cap; the host's inference admission remains the credit authority.

`createAgentDirectory(sql)` runs mutations inside the caller's transaction and
returns lifecycle facts so host projections commit with the directory change.
`createFleetSessions(open)` lists, creates, reads and deletes Sessions, and
`send(grant, agentId, sessionId, message)` admits replayable work. An omitted
Session is created with the message's request identity.
`createFleetDelegation({storage, deliver, open, readSettlement})` records the
grant's parent Session and freezes BirdDog mail before delivery.
`start(grant, task)` returns the admission receipt; `delegate(grant, task)`
returns Agar Pending until the exact command settles, then its terminal, final
text, native usage facts and tool-call count. The host-only
`readSettlement(sessionId, commandId)` reads exact work rows, independently of
transcript pagination and branch selection. Poll with the same request identity
and task. Closing a client never cancels work.

Host metadata stays opaque. An in-process `open` adapter applies it with
`agent.withMeta(meta)`; a remote host attaches it at its authenticated transport
edge. Cancellation returns the cancellation entry's metadata, allowing the host
to name the canceller in the reply text without making them its author.

### Delegated replies

A `followUp` delegation returns `{status: "accepted", commandId}` at once. The
child's result arrives later as one new user-role line in the parent Session. It
is ordinary follow-up work: an idle parent wakes a new run, and an active parent
queues it behind the running work, which it never interrupts, so it runs next.
Each fact on that line has one setter:

| Fact                                | Setter                                                                               | Rule                                                                                                                                                                                                                                                                                                                         |
| ----------------------------------- | ------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Content                             | BirdDog `settlementReplies`                                                          | The child's last assistant text, or the host's cancellation text such as `Work cancelled by Ada.`, verbatim; with neither, one sentence naming the outcome.                                                                                                                                                                  |
| `reply.commandId`                   | BirdDog, from the child's accepted command                                           | Settled work replies carry the parent's `agents.send` receipt `commandId`; undeliverable replies have no command ID.                                                                                                                                                                                                         |
| `reply.outcome`, `reply.diagnostic` | BirdDog, from the exact Watchdog outcome and the child Session's terminal diagnostic | `completed`, `failed`, `interrupted`, `cancelled` and `outcome_unknown` stay distinct. Only `failed`, `interrupted` and `outcome_unknown` carry a diagnostic, bounded by `boundedDiagnostic`. `undeliverable` instead carries `recipient_offline` or `recipient_left`.                                                       |
| Authentication                      | `fleet.authenticateReply(mail)` at the parent's host                                 | The key is the frozen send's `<key>:reply:0`, `replyTo` is empty, and `from`/`to` mirror the frozen send exactly. Settled replies correlate with the receipt's `commandId`; undeliverable replies correlate only by the frozen mail key and typed reason. Otherwise the result is `undefined` and the host refuses the mail. |
| Author and `meta.delegation.reply`  | The parent's host, only after authentication                                         | The author is the replying child Agent. The settling actor, a human canceller and the parent never author a reply.                                                                                                                                                                                                           |
| Model-visible provenance            | Shiitake, from the `reply` the host passes to `acceptMail(mail, meta, reply)`        | The model sees the line inside `<fungi_agent_reply agent_id=… session_id=… [command_id=…] outcome=… [diagnostic=…] [reason=…]>`.                                                                                                                                                                                             |

The provider role stays `user`. Shiitake renders the tag from the verified
facts, never from the text, and neutralizes the tag in every other user-role
line (typed prompts, unverified mail, operator lines) and inside the reply body.
Typed or mailed text that copies the tag therefore never reads as a verified
reply. The stored line keeps the child's text verbatim; clients join it to the
send on `meta.delegation.reply.commandId` through agar's
`messageDelegationReply`, never on its text. `acceptMail` refuses a `reply` that
does not name its mail's sender or that arrives on mail with a `replyTo`.

Apps exposing an Agent extension import `agentExtensionMethods` from
`@fungi.computer/shiitake/agar-extension` and spread the result into Agar's
`defineAppBackend({methods})`. Agar itself has no Shiitake dependency.

## Run it on Node

`@fungi.computer/shiitake/node` keeps one durable Agent in a SQLite file on
disk, on the Node event loop. `openNodeShiitake({ path, ... })` takes the same
model, instructions, extensions and tools as `Shiitake.make`, holds an exclusive
lease on the file until `close`, and returns the in-process `agent` plus its
Session `routes` as a Hono app to serve behind your own middleware. Reopening
the file resumes accepted work. `nodeRuntimeHost` and `acquireNodeStoreLease`
are the same pieces for a host that composes its own store. See
[Run it on Node](docs/site/run/node.mdx) and the
[Node host tests](test/node-host.test.ts).

The bound runtime's pending-work answer includes BirdDog's Carrier-owned reply
retries. Its next wake is the earliest extension timer or owed reply delivery,
so an otherwise idle reconciliation cannot erase a mail retry timer. Reopening
before a retained reply's due time schedules its future delivery without a
client request.

The agent-control source offers
`agents.send({agentId, sessionId, requestId, text, delivery})` and
`agents.recall({agentId, sessionId, requestId})` only when the host supplies the
corresponding request-owning callback. Choose a nonempty caller request ID of at
most 256 UTF-8 bytes before sending; reuse it for retries or recovery after a
lost acceptance response. The host scopes it to the sending Agent and invoking
Session and the target Agent and Session. Model input cannot supply the invoking
Session or override the pending-only recall guard. Local replay uses that caller
identity, not the current tool ID or model program bytes. Recall cancels only
pending work: `accepted` names the known command, `too_late` names active or
settled work without aborting it, and `unknown` carries no invented command ID.
An unknown recall still retains the target owner's late-admission fence.

The `/agent-control` source exposes
`agents.create({requestId, name?, model?, reasoning?})`,
`agents.setModel({agentId, model, reasoning?})` and `agents.models({})` when its
host authorizes those operations. `name` labels the new Agent and `reasoning`
(`minimal` to `max`, one of `modelReasoningLevels`) needs a `model`.
`agents.create` creates the Agent and its first Session through the same
idempotent Session-creation door as `agents.createSession`. It returns `agentId`
and a usable `sessionId`: send work immediately with `agents.send` and
`delivery: "steer"`, without a separate `createSession` call. Retrying the same
creation request reuses that first Session.

It also returns the Agent's `name`, the requested `model` and `reasoning` (the
stored default; absent when it inherits), and the `workingDirectory` its tools
use; `agents.list` reports the same facts for existing Agents. A host that can
archive also accepts `archived: true` there to list only the archived ones
(other hosts refuse that input). A host can offer `agents.archive({agentId})`
and `agents.unarchive`: an Agent archiving an Agent it created hides it from
lists and delegation without deleting anything, and the host refuses any other
target, the caller itself, or an Agent with a running command or delegated work
still waiting. Each refusal, and a send to an archived Agent, reaches the caller
as a `CapabilityFailed` diagnostic such as `archive:not_creator: …`,
`archive:self`, `archive:running`, `archive:pending`, `archive:unreadable` (its
store predates the schema) or `archive:archived`. `agents.wait` accepts
`timeoutMs` from 1 to 30000. Models use `provider/model` spelling and must be
offered by that host; `agents.models` returns at most 200 offered addresses from
signed-in or configured providers. A host model refusal reaches the caller as a
`CapabilityFailed` diagnostic such as `model:rejected: …` with up to ten
addresses from the host's offered catalog. If catalog diagnostics are
unavailable, the original refusal remains; other failures propagate unchanged.
The `/model-preference` resolver orders explicit Session, explicit Agent, the
person's last manual switch, Team default, then available login. Hosts own those
stored layers and credential admission: choosing a model never bypasses an
owner's Team or Agent key-source restriction. A host can refuse an unrunnable
send before acceptance; `agents.wait` includes the failed run's bounded
diagnostic so callers can explain login or model-selection problems. For a
completed command, `agents.wait` also returns the command's final assistant
`text`, at most 16,384 characters, with `truncated` set when it was cut; failed,
cancelled and interrupted commands return no text, and `agents.read` stays the
full bounded Session read. Hosted ACP sessions use the Agent and Team layers;
they do not select a person's saved model preference.

## API reference and examples

The [generated API reference](docs/api/README.md) covers the public exports in
[package.json](package.json):

| Entry                                 | Use                                                                              |
| ------------------------------------- | -------------------------------------------------------------------------------- |
| Root and `/effect`                    | Promise and Effect host construction.                                            |
| `/client`, `/http`                    | Neutral Session clients and HTTP route composition. The host authorizes ingress. |
| `/cloudflare`                         | Durable Object storage and alarm composition, including owner-local handles.     |
| `/extensions`, `/client-entry`        | Host authoring and the independent `shiitake.client/v1` presentation contract.   |
| `/agent-control`, `/model-preference` | Authorized Agent operations and model selection contracts.                       |

[Bound transport tests](test/bound-fetch-client.test.ts) exercise a client with
a host-supplied fetch.
[Restart tests](test/resume-after-restart-tool-outcomes.test.ts) exercise
interrupted tools against the existing replay rules. They are local proofs, not
a guarantee about another deployment's storage or sandbox.

## Limits and errors

Use `ShiitakeError`'s boundary and cause to distinguish invalid input, conflict,
storage failure and unavailable execution. Preserve request identity when an
acknowledgement is uncertain. Do not manufacture an accepted result from an
unavailable owner.

Safe recovery depends on durable facts and the tool's replay contract. An
operation with an uncertain external effect is not made safe by retrying it.
Schema-incompatible extension state fails rather than using another shape.
Credentials, product permissions, browser delivery and deployment remain with
the host.

## Contribute and license

Add a regression to the nearest Session, extension or host test. From the root:

```sh
pnpm --dir packages/shiitake run test:unit
pnpm --dir packages/shiitake run test:merge
pnpm --dir packages/shiitake run docs
```

The native merge proof needs the configured Workers-compatible runtime. Unit
tests alone do not qualify that host. MIT. See [LICENSE](LICENSE).
