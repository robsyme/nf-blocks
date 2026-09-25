# VFS spike results

Measured 2026-09-25 on an Apple M3 Pro, macOS 27.0, with `web/bench/run.sh`.

- Chromium 153.0.8010.12 (Playwright 1.63.0, `gate/browser`), headless
- SQLite 3.53.4 (`@sqlite.org/sqlite-wasm` 3.53.4-build1, from the Worker)
- Year file: `gate/gen_year.py` over a Gate schema-2 index, 591,814,656 bytes,
  sha256 `94f0e83d57a7ca2cec0c207c8a0572f2af83cbe7ef1c7879a76d8aab0e414081`
- Page: `dist/bench.html`, 1,381,186 bytes, one file (Worker and wasm inlined)

## Range mode, year file

Parameters from `gate/year_params.py` offsets 0 (cold, warm) and 3 (other).
Requests and bytes are Playwright's network events; the counting server's
`/__stats` gave the same numbers in every step. Milliseconds are one run on
loopback and vary between runs (cold:itemsWhere took 39 ms and 94 ms in two runs).

| step | requests | bytes | ms | limit | reference (sql.js-httpvfs) |
|---|---|---|---|---|---|
| open (probe + header) | 3 | 12,288 | | | |
| cold:producersOf | 7 | 28,672 | 17 | 9 / 65,536 | 7 / 28 KB |
| cold:latestSuccessfulRun | 5 | 20,480 | 13 | 7 / 65,536 | 5 / 20 KB |
| cold:itemsWhere | 43 | 176,128 | 94 | 44 / 524,288 | 42 / 180 KB |
| warm:producersOf | 0 | 0 | 0 | 0 / 0 | 0 |
| warm:latestSuccessfulRun | 0 | 0 | 0 | 0 / 0 | 0 |
| warm:itemsWhere | 0 | 0 | 15 | 0 / 0 | 0 |
| other:producersOf | 6 | 24,576 | 13 | | |
| other:latestSuccessfulRun | 1 | 4,096 | 2 | | |
| other:itemsWhere | 26 | 106,496 | 53 | | |

Every criterion passes. The request counts were identical across two runs.
`rows` and the first column of the first row equal what Python's `sqlite3`
returns for the same SQL and parameters in all nine query steps.

## Whole-file mode, no Range

`serve.py --no-range` over a `VACUUM INTO` copy of the Gate's index (page size
4096, rollback journal, 180,224 bytes), parameters from that file.

| step | requests | bytes |
|---|---|---|
| open | 1 | 180,224 |
| every query step | 0 | 0 |

Mode `whole`; browser and server counts agree; `rows` and `first` equal
Python's `sqlite3` in all nine query steps.

## Reproduce

```bash
GATE_ROOT=$SCRATCH/g make gate
SCHEMA=$(ls $SCRATCH/g/cache/nf-blocks/*.sqlite | head -1)
BENCH_DIR=$SCRATCH/bench web/bench/run.sh "$SCHEMA" 8811
sqlite3 "$SCHEMA" "PRAGMA page_size=4096; VACUUM INTO '$SCRATCH/bench/small.sqlite'"
SNAPSHOT=$SCRATCH/bench/small.sqlite NO_RANGE=1 BENCH_DIR=$SCRATCH/bench-small web/bench/run.sh "$SCHEMA" 8812
```
