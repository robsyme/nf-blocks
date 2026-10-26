# Identity

Every block that records an assertion carries an `asserted_by` string, and the explorer shows it as the avatar next to a claim. This page says where that string comes from.

An explicit `cas.asserted_by` in the config always wins, even over a verified Seqera Platform identity: when it is set, Platform is never asked and the blocks carry the configured name.

## Resolution order

nf-blocks resolves the name once per session, the first time something needs to sign, and uses the first source that gives a non-blank answer:

1. `cas.asserted_by` in the Nextflow config.
2. The owner of your Seqera Platform access token, read from Platform.
3. The `NXF_BLOCKS_USER` environment variable.
4. `git config user.email`.
5. `anonymous`, with a warning.

If the config says `asserted_by = 'gate'` and a Platform token is also set, the blocks say `gate`. This keeps CI and the Gate stable, but it also means a config value can name someone other than the token's owner. Leave it unset if you want the verified name.

Blank values (empty or only whitespace) count as unset at every step. So does a `cas.asserted_by` or `NXF_BLOCKS_USER` of `anonymous` in any case (`Anonymous`, ` ANONYMOUS `): resolution moves on to the next source, because `anonymous` means nobody was named and typing it never opts in. The name is trimmed. It is not lowercased, so a block keeps the spelling the user typed.

## When it is resolved

Nothing is asked until a block needs a name. The answer is then kept for the rest of the session, and threads that ask at the same moment share one resolution.

| Who | When |
|---|---|
| A Nextflow run | at the start of the run (`onFlowBegin`), before any task runs, when it writes its RunManifest |
| `nf-blocks:put` | when it builds the Selection, Claim or Project, dry runs included, since the name is part of the block's address. A write refused for its shape, or for being too large (a request over 2 MiB, or a block over 1 MiB once encoded), is refused whatever name would sign it, and the signer is never resolved for it: the size is measured on a copy signed with a one-character name, and no name is shorter. The one exception is a block that crosses the 1 MiB limit only by the bytes the signer's name adds: it passes that check and is refused as too large after signing. On a real write (not on a dry run, which only builds the block), a block of the wrong kind, a missing block, an item its collection does not list or a member that is not writable (`requireWritable()`) is also refused before signing, and never asks. `POST /api/handoff` keeps the same order. A clock-skew or already-superseded refusal comes after the name is resolved, because a retried request is first matched against the block it already wrote, by an address that holds the name. So does a write the store turns down only when the files are written, such as a permission error |
| `nf-blocks:sweep --apply` | when a trash ledger past its deadline holds metadata blocks, since deleting those writes a Forget block. It asks before taking `sweep.lock`, even when the pass is then refused (a running pipeline's Live Writer, or another sweep holding the lock), so a refused sweep can wait on identity first. An apply with no metadata past its deadline never asks |
| `nf-blocks:prune --apply` | when it writes its first release Claim |
| `nf-blocks:explore` | right after the server starts listening, on a background thread; `members.json` and a write that passes the checks above wait for it, and so does the page, which loads `members.json` before it shows anything. The store files do not wait |
| `items`, `snapshot`, `reindex`, `untrash`, and `sweep` or `prune` without `--apply` | never; they sign nothing, so they never call Platform or git and never print an identity line |

So a read-only verb with `TOWER_ACCESS_TOKEN` set and Platform unreachable costs nothing extra. `explore` listens straight away, and the wait (5 seconds for Platform at most, 2 for git, and on macOS with `/usr/bin/git` up to 2 more for the `xcode-select -p` check) falls on the first `members.json`. The page waits on that answer, so with Platform unreachable its first load can stay blank for up to about 7 seconds, or 9 on macOS with `/usr/bin/git`.

## What each source reads

| Source | Where it looks |
|---|---|
| Config | `cas.asserted_by` |
| Platform | token from `tower.accessToken`, else the `TOWER_ACCESS_TOKEN` environment variable; endpoint from `tower.endpoint`, else `TOWER_API_ENDPOINT`, else `https://api.cloud.seqera.io` |
| Environment | `NXF_BLOCKS_USER` |
| Git | the output of `git config --get user.email` in the launch directory |
| Anonymous | nothing; the name is `anonymous` |

The Platform step is only tried when a token is present. It sends `GET <endpoint>/user-info` with the token as a bearer credential and takes `user.email` from the JSON answer. A trailing `/` on the endpoint is dropped.

## What is recorded

Only the name goes into blocks, as `asserted_by`. Which source supplied it is not written into any block. When the name is resolved, nf-blocks prints one line, `nf-blocks: signing as <name> (from <source>)`. The source reads `cas.asserted_by`, `Seqera Platform`, `NXF_BLOCKS_USER` or `git config user.email`. When the principal is anonymous (the last step of the order, since a typed `anonymous` falls through) that line is replaced by the anonymous warning described below. In addition, `nf-blocks:explore` passes it to the page in `members.json` so the page can say "signing as ada@example.org (from git config user.email)".

Where the line goes depends on what is running. The identity warnings, the storage-class warning, the clock-skew warning and the cache index's lines (a member with no usable Index Snapshot, a cache rebuilt because a member was replaced) go to the same place, each once.

| Session | Where its notices go |
|---|---|
| A Nextflow run | the plugin's `nextflow.cas` logger. Nextflow's console shows only loggers under `nextflow`, so this reaches the terminal as well as `.nextflow.log`. |
| A `nextflow plugin nf-blocks:<verb>` command | stderr. These commands write no `.nextflow.log`, and stdout is kept for their output, so `--format json` and scripts see only that. |
| `nf-blocks:explore` | stderr, like any other verb. Its stdout is the `nf-blocks explorer: <url>` line and nothing else. |

## Timeouts and fallbacks

A slow or broken source never stops a run.

- Platform has a 5 second limit for connecting and 5 seconds for the whole answer, body included. A timeout, a non-2xx answer (such as `HTTP 401` for a bad token), an answer without an email, or a network error logs one warning, `nf-blocks: could not read your Seqera Platform identity (<reason>); using <next source> instead`, and resolution moves to the next source. The token is never written to the log or the message. Each lookup builds its own HTTP client and shuts it down before returning (on Java 21 and later, where the client can be shut down).
- `git` is given 2 seconds. If it is not installed, exits non-zero, times out or prints nothing, resolution moves on to `anonymous`. Its streams are closed whatever it answers.
- On macOS, `/usr/bin/git` is a stub that opens the "install the Command Line Tools" dialog when they are missing. When `os.name` starts with `Mac` and the first `git` on `PATH` is `/usr/bin/git` or a symlink whose real path is `/usr/bin/git`, or no `git` is found there (as when `PATH` is unset and the lookup falls back to `/usr/bin`), nf-blocks first runs `/usr/bin/xcode-select -p` (2 seconds). If that exits non-zero, git is skipped, as if it had printed nothing, and the reason is logged at debug. A git from Homebrew or elsewhere earlier on `PATH` runs without the check. Other systems never run `xcode-select`.
- `anonymous` logs one warning. A run says `nf-blocks: no identity is configured, so this run's blocks and claims will say 'anonymous'. Set NXF_BLOCKS_USER or git config user.email (see docs/identity.md)`. A command says `what this command writes` in place of `this run's blocks and claims`. Runs still work. Their claims show the dashed `?` avatar in the explorer, and the Mine filter is switched off because it would match every anonymous row.
- When Platform fails and no later source answers either, the two warnings become one that says what failed: `nf-blocks: could not read your Seqera Platform identity (<reason>) and no other identity is configured, so <what> will say 'anonymous'. Set NXF_BLOCKS_USER or git config user.email (see docs/identity.md)`, where `<what>` is the run or command wording above.

## Setting each source

Config, in `nextflow.config`:

```groovy
cas {
    asserted_by = 'ada@example.org'
}
```

Platform, with a token from your Platform user menu (this is the same token the `tower` scope uses):

```bash
export TOWER_ACCESS_TOKEN=...
```

Or in `nextflow.config`, `tower { accessToken = '...' }`. For a Platform install other than Seqera Cloud, also set `TOWER_API_ENDPOINT` or `tower.endpoint`, for example `https://platform.example.com/api`.

Environment:

```bash
export NXF_BLOCKS_USER=ada@example.org
```

Git:

```bash
git config --global user.email ada@example.org
```

## What "Mine" compares

The explorer's Mine filter matches runs whose RunCompletion `asserted_by` equals your name, and Selections assembled by you. `Ada.Lovelace@Example.org ` in a block and `ada.lovelace@example.org` from git are the same person. A name that is not an email still matches itself in the same way, and `anonymous` never counts as yours.

Two principals are compared after trimming ASCII whitespace (space, `\t`, `\n`, `\r`, `\f`, `\v`) from both ends and lowercasing ASCII letters only. In SQL that is `lower(trim(x, char(32,9,10,13,12,11)))`. In the page's JavaScript it is the same set of characters and ASCII-only lowercasing. Other letters keep their case, so two spellings that differ in the case of a letter such as `É` do not match.
