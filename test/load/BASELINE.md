# Local live-updates load baseline

`HEAVY=/nonexistent/load-proof pnpm --dir packages/shiitake test:load` on
`test/live-updates-load-harness`, 2026-10-25. Three independent invocations,
each with three fresh zero-watcher/watched pairs. All exit 1. Under the
superseded absolute-control rule, invocation 1 stopped at a zero-watcher p99
above the S5 bar; that is not a failure under the current relative-control rule.
This is not a PASS or a production timing claim. The future-slice S2–S6 bars are
red where evaluated; S3 and the draining S4 CPU probe are red in all three
invocations. `LIVE_UPDATES_S0` reports all nine pairs and
`LIVE_UPDATES_S0_MEDIAN` selects each invocation's median slowdown pair. These
numbers predate the CTO's control-rule correction below; they remain historical
measurements, not an acceptance verdict under the new rule.

Both phases used **fresh**, separate memoryHost groups: one parent and eight
sub-agent Agents, with 900 real Woodstock rows and about 4.18 MB of history per
Session. Five sub-agents streamed 200 KB thinking and 50 KB text in
eight-character deltas, yielding a real event-loop turn between chunks. Each
phase injected 20 Mule-shaped tool-tail updates per streaming reply into the
real progress producer. The watched phase ran eight real `agents.wait` calls
with 15 s timeouts, three TUI-style watches (each taking one `limit=200`
snapshot), two additional SSE watches and one reader throttled to 1 KiB/s. The
full 8 × 20-cycle S6 case is **not** exercised here.

The timed turn ends as soon as all eight terminals arrive, **before** the
harness's eight `settle` snapshot/context reads. Histograms are created after
setup, primed on an on-time 10 ms timer, and required to cover at least 90% of
each timed turn (`count × mean ≥ 0.9 × turn duration`). Whole-turn lag includes
run-start product work; S5 lag instead covers the STREAMING window from all five
replies' first deltas to their last delta. The original run required the control
to meet S5's streaming bar as a hard S0 assertion. Decision `sm:f1f2f970073e`
replaced that rule: record zero-watcher streaming p99, then require watched
streaming p99 ≤ control p99 + 10 ms on the median pair. The fixed 10 ms
allowance tests added watcher overhead without treating this host's unloaded
timer latency as a product failure. Both streaming histograms must contain
samples covering ≥90% of their streaming windows; the control's reported p99
must be >0 and <50 ms. This <50 ms ceiling is a **harness-health bound**, not
the S5 <20 ms control bar: a stalled or empty control would conceal watcher
overhead in the relative check. The bound is 50 ms because that is S5's own
max-lag bar for the watched run and about twice the highest control p99 recorded
on this machine (27.1 ms, below): a control whose worst 1% of ticks already lag
that much is too noisy to measure watcher overhead against. S5's design-set <50
ms max and <20 ms p99 absolute bars remain on the watched run, as soft
future-slice assertions. The reported histogram lag subtracts its 10 ms tick.

| Invocation | Pair slowdown ratios | Median zero / watched turn (ms) | Whole-turn histogram max zero / watched (ms) | Streaming max zero / watched (ms) | Streaming p99 zero / watched (ms) | Histogram samples zero / watched |
| ---------- | -------------------- | ------------------------------: | -------------------------------------------: | --------------------------------: | --------------------------------: | -------------------------------: |
| 1          | 48.18, 52.83, 52.10  |                  1,581 / 82,355 |                                  232 / 1,127 |                      41.3 / 366.5 |                   **27.1** / 28.3 |                      123 / 6,605 |
| 2          | 54.25, 52.09, 53.53  |                  1,546 / 82,743 |                                  222 / 1,104 |                      34.2 / 377.4 |                       19.3 / 28.5 |                      122 / 6,646 |
| 3          | 54.01, 54.63, 52.29  |                  1,523 / 82,231 |                                  231 / 1,101 |                      28.6 / 381.7 |                        7.6 / 27.3 |                      123 / 6,618 |

All nine pairs exceeded the ≥10× S0 slowdown. The zero-watcher streaming
control's max stayed under 50 ms in all three median pairs, but invocation 1's
p99 (27.1 ms) violated the <20 ms S5 reachability bar. The other two median
controls met both bars; the individual zero-watcher p99 values range from 7.6 to
27.1 ms. The watched streaming max and p99 bars are red. All whole-turn
histograms covered 99–100% of their timed windows. Streaming-window sample
counts and coverage were not recorded in these historical runs.

| Invocation | Server SSE bytes after snapshots | Snapshot bytes | Context reads zero / watched | SQLite statements zero / watched | Watched RSS growth (MiB) | Wait progress frames | Slow reader consumed bytes / reset | S4 CPU ratio to 2× |
| ---------- | -------------------------------: | -------------: | ---------------------------: | -------------------------------: | -----------------------: | -------------------: | ---------------------------------: | -----------------: |
| 1          |                       36,104,685 |      2,681,033 |                     16 / 145 |                1,560 / 1,412,785 |                      9.9 |                  188 |                        81,108 / no |               1.66 |
| 2          |                       36,104,009 |      2,681,033 |                     16 / 145 |                1,560 / 1,412,783 |                     19.3 |                  188 |                        83,702 / no |               1.65 |
| 3          |                       36,104,242 |      2,680,999 |                     16 / 145 |                1,560 / 1,412,780 |                     60.2 |                  188 |                        80,764 / no |               1.65 |

The fast clients count consumed SSE wire bytes. The slow client's
`mode=subscribe` iterator is wrapped at the server's frame boundary, **not**
just `watch`: its produced frames now count toward server bytes even if it has
not pulled them yet. The separately reported 1 KiB/s client's _consumed_ bytes
are not substituted for server bytes. Snapshot allowance is one bounded snapshot
per TUI reader. S2 compares watched-minus-zero context reads per Session; the
unchanged parent still gets 2 extra reads. A focused S3 sibling job-transition
check finds 2 replay reads (bar 0).

The S4 CPU probe measures 250 KB versus 500 KB **with one real SSE reader
draining the whole-text progress frames**. Its ratio to linear is 1.65–1.66 (bar
≤1.2), so S4 is red; the earlier no-reader green result measured missing
serialization work. The S4 byte bar is also red. S5's slow-client reset and
slowdown bars are red; within-phase RSS happened to pass. The partial S6
wait-progress bar is red. The future-slice assertions use `expect.soft` so a
genuine S0 proof can expose all slice costs. In the first historical invocation,
the **retired** absolute-control assertion stopped further slice assertions for
its median pair. Now missing/undercovered streaming samples or a control p99
outside (0, 50) ms are hard S0 failures, not soft S5 verdicts.

**Unowned product cost / design-owner decision needed:** at run start, eight
sub-agent runs each synchronously read their entire kept context window in one
reconcile tick (`readRunContextRows`), about 25 ms each. This burst occurs with
zero watchers and is not a watcher-stack regression. S2 caches the context
figure for read/watch/reset but does not remove these reads; none of S1–S7
currently owns spreading or avoiding the burst. **Which slice owns this cost?**
The sporadic zero-watcher streaming p99 >20 ms meant that S5 reachability was
not a stable proof under the original absolute control bar. The CTO subsequently
ruled that the zero-watcher phase is a measured baseline, not an absolute <20 ms
gate; watcher overhead uses baseline + 10 ms while the watched absolute S5 bars
stay in place. This does not make the future-slice assertions green.

One fresh default-size zero-watcher/watched pair with `LOAD_TRIALS=1` measured
streaming p99 at 17.607 / 47.115 ms: the watched value exceeds its relative
limit of 27.607 ms by 19.508 ms. Its watched absolute p99 (<20 ms) and max (<50
ms; observed 447.577 ms) also fail. This is an expected red future-slice
verdict, not an S0 control failure. The pair reproduced 66.82× turn slowdown;
the S3 and S4 probes and other future-slice assertions also remained red.

S1 hung-provider deadlines, the full S6 8 × 20 no-snapshot/no-context form, and
S7 TUI behavior belong to their separate slices and are **not** claimed by this
S0 load case. Retained Woodstock laws 30, 32 and 36 run as separate focused
tests. No product migration, deployment or publication was performed.
