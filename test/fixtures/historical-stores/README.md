# Historical Agent stores

One real Agent store per Woodstock schema marker, written by Fungi's own Local
host at the last commit that used that marker. They prove that a person's
Agents, Sessions and history survive the in-place upgrade.

| File                    | Marker | Written by commit                          |
| ----------------------- | ------ | ------------------------------------------ |
| `marker-1.agent.sqlite` | 1      | `1ea10e44f1b4b9fdeea88b9366c7d4a5b2451918` |
| `marker-2.agent.sqlite` | 2      | `961550ce677077f7d1d66ce66fba5ed723ca91a5` |
| `marker-3.agent.sqlite` | 3      | `1e7e73d94c3ee4191cc357227c60a46a8fe9ea3d` |
| `marker-4.agent.sqlite` | 4      | `20a27d9c67fc4d245a42b790845756990afd2fd7` |
| `marker-5.agent.sqlite` | 5      | `cbaff484459e1d5c1907d8e8042bf54fa583a5da` |
| `marker-6.agent.sqlite` | 6      | `12e3e02165cf9f31b219f2d6f0bff29b8b75fc1a` |

## Which markers shipped

The 266 npm tarballs examined for PR #963 contain markers 1, 3 and 6, not 2, 4
or 5. This is a bounded publication survey, not a claim about future releases.
The marker constant in `package/dist/internal/woodstock/session-store.js`:
`@fungi.computer/shiitake` 0.0.1, 0.1.0 and 0.2.0 write 1 (published 15 to 17
October 2026), 0.3.0 to 0.11.1 write 3 and 0.12.0 to 0.18.3 write 6.
`@fungi.computer/spore` 0.1.1 bundles 1 (17 October), 0.2.0 to 0.8.3 bundle 3
and 0.9.0 to 0.15.1 bundle 6. `@fungi.computer/truffle` 0.1.1 depends on
shiitake 0.2.0 (marker 1), 0.2.0 to 0.8.2 on 0.3.0 and later (marker 3), and
0.8.3 is the last that writes 3; 0.9.0 to 0.13.1 depend on marker-6 builds.
`@fungi.computer/oyster` 0.1.0 and 0.2.0 depend on shiitake 0.1.0 and 0.2.0
(marker 1), 0.3.0 to 0.13.1 on marker-3 builds, and 0.14.0 to 0.33.0 on marker-6
builds. The marker 2, 4 and 5 stores come from unpublished builds and are kept
because the upgrade must not depend on which builds were published.

The Spore evidence is its actual `package/dist/cli.js` bundle, following the
variable passed to
`INSERT INTO woodstock_schema (version, protocol) VALUES (?, ?)`: `dwn = 1` in
0.1.1, `pTn = 3` in 0.2.0 and `uRn = 6` in 0.9.0. Oyster and Truffle use the
exact Shiitake dependency in their tarball's `package/package.json`; that
dependency's tarball supplies `currentWoodstockSchemaVersion`. The survey
includes each package's `0.0.0-stage` placeholder and Oyster 0.0.1, where no
marker was found; absence is not assigned a guessed version. PR #963 records the
complete version list.

## Fixture contents and privacy

Nothing in the stores is anyone's own data. Each has an Agent named "Fixture
Agent", three Sessions of scripted prompts and replies from a stub model, `list`
and `read` tool calls over a note, an `execute` call that reads a one-pixel
image (so its result carries image details), two long prompts and a compaction
over them, and one loop timer. Markers 3 to 6 also hold one automatic retry.
They were never copied from a person's `.fungi` folder, and must not be: a real
store holds prompts, code and secrets.

## Regenerating

Check out the commit in a temporary `git worktree` under `.botanical/`, install
and build `@fungi.computer/truffle` there, copy
`runtimes/local/test/fixtures/historical-store.generator.mts` into the same path
of that checkout, and run it from `runtimes/local` with an empty scratch folder
outside any repository (a repository's own skills would otherwise enter the
stored prompts):

```sh
O=$(mktemp -d /tmp/botanical-test.XXXXXX); mkdir -p "$O/home"
HOME="$O/home" node --import tsx test/fixtures/historical-store.generator.mts "$O"
```

Markers 1 and 2 predate automatic retries in Local, so they are generated with
`FIXTURE_RETRY=0`. The generator prints the store's path; copy that one file. In
a current checkout the same script is
`pnpm --dir runtimes/local fixture:historical-store <folder>`.
