// Constants the page shares with the plugin. Each names the spec section or
// DESIGN.md section it comes from; a change here without one there is a bug.

/** Index schema version (DESIGN.md §12, Index.SCHEMA_VERSION). Task 5 pins them equal. */
export const SCHEMA_VERSION = 6
/** Where a member keeps its Index Snapshot (spec section 4), the only one the page opens. */
export const SNAPSHOT_PATH = `index/v${SCHEMA_VERSION}.sqlite`
/** Snapshots an older plugin wrote. The page never reads them: a member with one and no SNAPSHOT_PATH is told to rewrite it. */
export const OLDER_SNAPSHOT_PATHS = ['index/v5.sqlite', 'index/v4.sqlite', 'index/v3.sqlite']
/** The whole-file fallback's cap, cas.snapshot.maxBytes' default (spec section 4). */
export const DEFAULT_CAP_BYTES = 64 * 1024 * 1024
/** The snapshot's page size, and the unit the VFS fetches in (spec section 4). */
export const CHUNK_BYTES = 4096
/** How far before the watermark the tail re-reads (spec section 3). */
export const OVERLAP_MILLIS = 10 * 60 * 1000
/** Past this many stale runs the page shows the command that rewrites the snapshot (spec section 5.4). */
export const STALE_RUNS_NOTICE = 20
/** Past this many block fetches for one query, likewise (spec section 5.4). */
export const CLOSURE_FETCH_NOTICE = 2000
/** The page size for the Selections list (spec section 5.6). */
export const SELECTIONS_PAGE = 50
/** Item rows whose previews load without a click (DESIGN.md §16 milestone 3 decision 10, ticket 03 Q3). */
export const PREVIEW_CAP = 100
/** Items the run page's Samples tab reads and joins (P1a); a run with more points to the Outputs tab. */
export const SAMPLES_ITEM_CAP = 2000
/** Items a project's Samples tab reads over all the whole runs it joins (P4); a run past it is left out, and named. */
export const PROJECT_SAMPLES_ITEM_BUDGET = 6000
/** Runs a project's Samples tab reads at once (P4). */
export const PROJECT_RUNS_AT_ONCE = 2
/** OutputItem fetches for previews in flight at once (DESIGN.md §16 milestone 3 decision 10). */
export const PREVIEW_CONCURRENCY = 6
/** Item blocks fetched at once while rewriting an Output Index File to addresses (outputindex.js). */
export const STABLE_INDEX_CONCURRENCY = 8
/** The largest raw block the block page reads whole and hash-checks (visual language spec §4.3). */
export const BLOCK_PAGE_RAW_CAP = 1048576
/** The bytes of a raw block the block page previews. */
export const BLOCK_PAGE_PREVIEW = 4096
/** Block fetches the size walks run at once, shared by every size method (model.js; only tail runs and lean snapshots walk). */
export const SIZE_CONCURRENCY = 8
/** The newest runs a pipeline's Stored figure sums when it walks (model.js pipelineSize on a lean snapshot); with more it reads "at least". */
export const PIPELINE_SIZE_RUNS = 10
