# @fungi.computer/shiitake

## 0.7.0

### Minor Changes

- Changes since 0.6.0:

  - fix(shiitake): settle hung runs and route ancestor reports one-way
  - fix(shiitake): release unbound Node host wake timers
  - fix(shiitake): bound far-future open Agent wake timers
  - fix(shiitake): keep watch context atomic with its replay page
  - fix(shiitake): keep direct snapshot projections atomic
  - feat(local): expose private retained schedule controls
  - fix(shiitake): bound and share Session read ancestry
  - feat(agents): adopt delegated goals on recipient admission
  - fix(shiitake): retain private catalog diagnostic failures
  - fix(shiitake): correct refusal test and Session guidance
  - fix(agents): open first Sessions and admit bounded exec environments
  - fix(shiitake): clarify queued-title invalidation effect
  - fix(shiitake): name work blocking queued conversations
  - fix(shiitake): keep progress through CLI watch updates
  - fix(shiitake): classify unreadable delegation settlement
  - feat(oyster): archive agents without deleting their work
  - fix(shiitake): preserve visitor append continuity across activity frames
  - fix(shiitake): bound progress flush latency and strengthen delta proofs
  - fix(shiitake): stream bounded offset append progress across all clients

### Patch Changes

- Updated dependencies
- Updated dependencies
- Updated dependencies
  - @fungi.computer/agar@0.6.0
  - @fungi.computer/bird-dog@0.3.1
  - @fungi.computer/mycelium@0.3.0
  - @fungi.computer/knapsack@0.4.1

## 0.6.0

### Minor Changes

- Changes since 0.5.0:

  - fix(shiitake): drop the visitor line alias the integration removed
  - fix(shiitake): probe terminal facts before projecting settlement
  - fix(subagents): retain quiet watched rows in the bounded cache
  - fix(subagents): poll the footer only for live observers
  - fix(shiitake): keep the waiting-on-you cap private
  - feat(tui,truffle): the Board ships as a program extension, read from stores
  - feat(shiitake): route a child's waiting items to the Agent that sent the
    work
  - fix(delegation): report a delegated Session once it rests, never another
    sender's mail
  - feat(parakeet): answer questions in the composer on the terminal and the web
  - fix(delegation): every idle Session reports to whoever sent it work
  - feat(goal): goal.wait, polling turns wait on children, agents.send puts new
    Sessions under a goal
  - fix(shiitake): keep usage limits out of repeated-model-failure summaries
  - feat(shiitake): support Local parent answers to child Asks
  - fix(tui): checkpoint bounded live client recovery
  - feat(oyster): compose visitor channel through Shiitake client

### Patch Changes

- Updated dependencies
- Updated dependencies
- Updated dependencies
- Updated dependencies
  - @fungi.computer/agar@0.5.0
  - @fungi.computer/bird-dog@0.3.0
  - @fungi.computer/knapsack@0.4.0
  - @fungi.computer/mule@0.2.1
  - @fungi.computer/mycelium@0.2.3

## 0.5.0

### Minor Changes

- Changes since 0.4.0:

  - fix(local): bind create receipts to creator session
  - fix(mesh): admit authenticated frozen offline replies
  - fix(mesh): admit Hub offline dispositions
  - fix(mesh): reply after carrier dead letters
  - feat(oyster): reply to expired machine mail

### Patch Changes

- Updated dependencies
- Updated dependencies
- Updated dependencies
  - @fungi.computer/agar@0.4.0
  - @fungi.computer/bird-dog@0.2.1
  - @fungi.computer/knapsack@0.3.1

## 0.4.0

### Minor Changes

- Changes since 0.3.1:

  - fix(shiitake): restore generated API docs
  - fix(live-updates): consume activity frames across clients
  - fix(shiitake): prove in-flight settlement waits over HTTP
  - fix(shiitake): target queued session activity wakes
  - fix(shiitake): await exact command settlement without session watches
  - fix(shiitake): guard context cache writes by cursor
  - fix(shiitake): keep scrollback figures out of cursor cache
  - fix: isolate provider prose to recorded failed turns
  - fix: assert safe retry reasons without untyped matchers
  - fix: distinguish absent provider text from safe diagnostics
  - fix: retain provider error text on failed turns
  - fix: bound hung model attempts and retain failed partials
  - fix(mesh): stage sessionless Hub grants atomically
  - fix(mesh): stage standalone acceptance notices
  - fix(shiitake): read context use only when an update moves history
  - fix(mesh): unify sessionless delegation acceptance
  - fix(bird-dog): remove unreachable undeliverable builder
  - fix(mesh): type atomic sessionless admission
  - fix(mesh): checkpoint atomic local notice staging
  - feat(mesh): wip sessionless acceptance checkpoint
  - fix(mesh): preserve held custody separately
  - feat(mesh): distinguish held delegation mail
  - fix(mesh): clear lint failures in mail contract
  - fix(mesh): accept frozen undeliverable replies after leave
  - feat(mesh): report terminal undeliverable mail replies

### Patch Changes

- Updated dependencies
- Updated dependencies
- Updated dependencies
- Updated dependencies
- Updated dependencies
  - @fungi.computer/bird-dog@0.2.0
  - @fungi.computer/carrier@0.2.0
  - @fungi.computer/knapsack@0.3.0
  - @fungi.computer/mule@0.2.0
  - @fungi.computer/seam@0.2.0
  - @fungi.computer/agar@0.3.1
  - @fungi.computer/mycelium@0.2.2

## 0.3.1

### Patch Changes

- Changes since 0.3.0:

  - fix(loop): keep retry diagnostics local
  - fix(loop): retry failed ticks with backoff
  - fix(mule): own provider data types at the Pi boundary

- Updated dependencies
- Updated dependencies
  - @fungi.computer/knapsack@0.2.1
  - @fungi.computer/mule@0.1.1
  - @fungi.computer/mycelium@0.2.1

## 0.3.0

### Minor Changes

- Changes since 0.2.0:

  - feat(stranger): typecheck each public entry in its own environment
  - fix(stranger): align Mycelium, Knapsack and Shiitake inventories and fixture
  - fix(sites): add subtle SHIIT safety surfaces
  - fix(sprite): support React 19 and share Shiitake Hono
  - docs: three-step terminal quick start for Shiitake
  - docs: plain Oyster lead and one Node story for Shiitake
  - feat(docs): consolidate package documentation under one site
  - docs(fungi-landing): add community funding page and token address
  - fix(docs): repair Oyster setup and name terminal entry points
  - refactor(oyster)!: lend Shiitake's Agent grammar to the machine link as host
    ports
  - fix(mesh): expose transport types and return reservation cases to B1
  - fix(shiitake): bound Hub reset guidance to authorized Team erasure
  - test(shiitake): reopen conformance storage after transactions
  - feat(mesh)!: preserve caller-owned intents across Local reconnects
  - feat(mule,shiitake,parakeet)!: separate tool data from model values
  - fix(shiitake): make repeated session titles a no-op
  - test(shiitake): reproduce title replay inbox revision churn
  - fix(test): remove shared-host races and repeated qualification work
  - fix(shiitake): close every staged scope before reporting cleanup failures
  - fix(shiitake): discard staged extensions in awaited reverse order
  - test(mesh): pin hosted-to-local task reconnect e2e red
  - docs(shiitake): refresh composed public API contracts
  - test(shiitake): expose staged roster cleanup order
  - test(shiitake): prove addressed helper ownership across reopen
  - test(shiitake): close timer and activation proof gaps
  - test: align retry consumers with typed provider ownership
  - fix(shiitake): admit ancestor peers within delegation depth
  - fix(shiitake): retain compaction usage after handoff failure
  - feat(agents): scope send and recall to caller requests
  - feat(shiitake): expose unknown request outcomes and fleet receipts
  - docs(shiitake): document returned fleet correspondence methods
  - fix(extensions): isolate required state resets from session runs
  - fix(goal): pin cleared events to committed session positions
  - refactor(files): split search tools and align hosted ignore visibility
  - refactor(mule): require typed retry contracts across model owners
  - docs(shiitake): document remaining public API symbols and refresh docs/api
  - fix(shiitake): retain Carrier reply retries in the runtime wake answer
  - fix(oyster): defer Agent runtime capabilities and use built bindings
  - feat(goal): expose paused attention without promoting the inbox
  - test: cut redundant fork and connection fixture work
  - fix(agents): deliver recipient-owned delegation evidence through Post
  - test(reload): defer authored-startup rollback characterizations
  - fix(shiitake): retain delivered control failure diagnostics
  - chore(agents): preserve Option A work before the 02 ancestry merge
  - fix(shiitake): resolve footer window from current model
  - fix(agents): disclose command settlement only to its accepted sender
  - fix(reload): transact core migration and document preparation together
  - fix(shiitake): retain Local-owned failure provenance
  - fix(shiitake): export diagnostic owner types
  - test(shiitake): expect owned diagnostics in Session settlement state
  - fix(shiitake): publish one safe diagnostic-bearing failure terminal
  - fix(shiitake): preserve safe Mule model failure reasons
  - fix(shiitake): retain returned run failure diagnostics
  - fix(hub): preserve caller identity through request withdrawal
  - chore(reload): preserve WIP publication fix and startup characterizations
  - fix(recall): preserve image cargo through withdrawn admission retry
  - feat(shiitake): atomically resolve stalled admission requests
  - fix(recall): respect Woodstock import boundary and test target
  - test(shiitake): join snapshot and isolation fixtures to required tool output
  - fix(shiitake): publish durable tool call identity for live output
  - fix(recall): preserve authorization errors across Session transport
  - feat(shiitake): scope pending lookup and cancellation to host identity
  - fix(shiitake): refuse damaged Session work reads before selecting rows
  - test(shiitake): assert REPLACE refusal without capturing errors
  - test(shiitake): prove REPLACE position conflict loses parent and orphans
    child
  - fix(shiitake): share the run progress owner for tool tails
  - fix(shiitake): refuse a damaged Session at its rows; close the REPLACE
    position hole
  - feat(shiitake): record immutable Agent createdBy separately from credential
    owner
  - feat(shiitake): carry process-local tool output snapshots
  - fix(reload): verify live snapshots and release removed timers
  - fix(shiitake): refuse a deleted leaf in the bounded preference walk
  - test(shiitake): prove bounded opens at 300 and 1,200 turns; say why the name
    is stored
  - fix(shiitake): name the store through an explicit storeName option
  - fix(reload): name the existing stop command in restart refusal
  - chore: checkpoint fungi reload WIP, not ready
  - test(shiitake): count the bounded settled read as slice 5's context read
  - fix(shiitake): settle work queued for a refused Session once, then move on
  - fix(shiitake): isolate and release failed activation scopes
  - test(shiitake): specify next-turn roster reload (red until reload lands)
  - fix(shiitake): name the store and its real reset in a Session refusal
  - Revert "fix(shiitake): refuse a broken store whole, naming it and its reset"
  - fix(shiitake): refuse a broken store whole, naming it and its reset
  - fix(shiitake): refuse short lineage windows; guard REPLACE; name the damage
  - fix(shiitake): refuse only the damaged Session, never the whole store
  - perf(shiitake): step bounded walks only to older parents; return only their
    ends
  - fix(shiitake): import the receipt schema through the Woodstock index
  - fix(shiitake): decode the handoff result and its receipt where it arrives
  - test(shiitake): the receipt is stored only by the entry that commits
  - feat(shiitake): store the archive receipt in the committing compaction entry
  - perf(shiitake): bound a prepared run's context and the compaction head check
  - perf(shiitake): guard lineages where they are written; open from the newest
    rows
  - fix(shiitake): read the declaration files without a URL path
  - test(shiitake): prove bounded context reads at 500 and 2,000 turns
  - fix(knapsack): review fixes for Knapsack.ui
  - fix(shiitake): refuse broken bounded context and preference walks
  - perf(shiitake): read the settled context and footer from the kept window
  - test(shiitake): a blocking join with an insufficient summary compacts
    ordinarily
  - fix(zookeeper): encode Session ids once for both writers and prove the real
    paths
  - docs(shiitake): drop the removed SessionModules type from the API docs
  - feat(shiitake): drop an insufficient background summary before appending
  - perf(shiitake): stop re-walking lineage for in-page compaction heads
  - feat(knapsack): the live-App bridge becomes Knapsack.ui
  - docs(shiitake): label run-scoped, single-flight background compaction as
    Shiitake policy
  - test(shiitake): keepRecentTokens gates an unclamped background threshold
  - fix(shiitake): atomic compaction slot, in-transaction head check, real
    release
  - feat(zookeeper): compaction handoff extension and view sessionId
  - feat(agent-host): live-App bridge leaves Shiitake
  - test(shiitake): assert background failure diagnostics exactly
  - test(shiitake): an overflow mid-summary joins it and resumes from it once
  - test(shiitake): a run ending mid-summary waits for it and places it after
    the terminal
  - test(shiitake): background summary failures; parity mid-run law stays
    blocking-only
  - feat(shiitake): background compaction fiber, one slot per owner, passive
    placement and join
  - fix(shiitake): label stale notices and type usage as stale-only
  - docs(shiitake): show backgroundTokens in the API docs; name the serializer
    test group
  - feat(shiitake): add compaction head check and stale notice
  - feat(shiitake): summary input leaves thinking out; backgroundTokens setting
    and predicate
  - fix(shiitake): type the compaction fixture through exported types
  - test(shiitake): prove absent compaction facts render as null in clients
  - feat(shiitake): keep absent compaction facts absent and prove the store
    rewrite
  - feat(shiitake): store compactions as head, model and data, and rewrite old
    rows
  - fix(zookeeper): drop silent catch, rename admitWithoutCheck, prove Hub
    prompt refusal
  - feat(zookeeper): move storage-pressure numbers out of Shiitake core
  - feat(shiitake): serve the cold-session page from the owner
  - feat(shiitake): read one bounded page of cold sessions from woodstock
  - fix(agents): gate open-pane commands behind archive, name unreadable pending
    work
  - fix(agents): serialize archive with admission, refuse pending work and old
    stores
  - feat(shiitake): one pi-ai estimator and Pi Durable's compaction cut
  - test(shiitake): make the long conformance cases cheap
  - feat(agents): archive agents; an agent tidies the ones it made, a person any
  - test(shiitake): run one Woodstock storage conformance suite on every host
  - feat(shiitake): run on pi-ai 1.3.0
  - fix(shiitake): take the stored-bytes context cap out of core
  - fix(shiitake): keep stored context bytes from bricking a Session
  - feat(files): one find tool replaces list, glob and grep
  - feat(local): an Agent creating an Agent cannot give it more than it holds
  - feat(session): show another Agent's message as a compact block
  - refactor(agents): keep the stored default as the one meaning of model
  - feat(agents): name, model echo, thinking level and wait bound for native
    subagents
  - feat(corral): herdr shows an open agent question as blocked
  - feat(session): show goal and loop automation in the footer, not the timeline
  - feat(oyster): public release 0.2.0

### Patch Changes

- Updated dependencies
- Updated dependencies
- Updated dependencies
- Updated dependencies
- Updated dependencies
- Updated dependencies
- Updated dependencies
- Updated dependencies
- Updated dependencies
- Updated dependencies
  - @fungi.computer/agar@0.3.0
  - @fungi.computer/bird-dog@0.1.0
  - @fungi.computer/cairn@0.1.0
  - @fungi.computer/carrier@0.1.0
  - @fungi.computer/gills@0.2.0
  - @fungi.computer/knapsack@0.2.0
  - @fungi.computer/mule@0.1.0
  - @fungi.computer/mycelium@0.2.0
  - @fungi.computer/seam@0.1.0
  - @fungi.computer/watchdog@0.1.0

## 0.2.0

### Minor Changes

- Changes since 0.1.0:

  - perf(shiitake): build schemas on first use, not at import
  - feat(oyster): public release 0.1.0
  - refactor(subagents): record sub-agents in the hosts, not Shiitake core

### Patch Changes

- Updated dependencies
- Updated dependencies
- Updated dependencies
- Updated dependencies
  - @fungi.computer/agar@0.2.0
  - @fungi.computer/gills@0.1.0
  - @fungi.computer/knapsack@0.1.0
  - @fungi.computer/mycelium@0.1.0

## 0.1.0

### Minor Changes

- Changes since 0.0.1:

  - perf(oyster): defer heavy SDKs out of Worker startup
  - docs(shiitake): regenerate API docs for recipients and invoker
  - feat(subagents): show a conversation's sub-agents and open them beside it
  - fix(shiitake): steer authenticated delegated replies

### Patch Changes

- Updated dependencies
- Updated dependencies
- Updated dependencies
  - @fungi.computer/agar@0.1.0
  - @fungi.computer/knapsack@0.0.2
  - @fungi.computer/mule@0.0.2
  - @fungi.computer/mycelium@0.0.2

## 0.0.1

### Patch Changes

- Inaugural public release of @fungi.computer/shiitake.
- Updated dependencies
- Updated dependencies
- Updated dependencies
- Updated dependencies
- Updated dependencies
- Updated dependencies
- Updated dependencies
- Updated dependencies
  - @fungi.computer/agar@0.0.1
  - @fungi.computer/bird-dog@0.0.1
  - @fungi.computer/cairn@0.0.1
  - @fungi.computer/gills@0.0.1
  - @fungi.computer/knapsack@0.0.1
  - @fungi.computer/mule@0.0.1
  - @fungi.computer/mycelium@0.0.1
  - @fungi.computer/watchdog@0.0.1
