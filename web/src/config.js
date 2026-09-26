// Constants the page shares with the plugin. Each names the spec section or
// DESIGN.md section it comes from; a change here without one there is a bug.

/** Index schema version (DESIGN.md §12, Index.SCHEMA_VERSION). Task 5 pins them equal. */
export const SCHEMA_VERSION = 3
/** Where a member keeps its Index Snapshot (spec section 4). */
export const SNAPSHOT_PATH = `index/v${SCHEMA_VERSION}.sqlite`
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
