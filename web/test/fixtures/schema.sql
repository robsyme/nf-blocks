CREATE TABLE schema_version(version INTEGER NOT NULL);
CREATE TABLE run(
                 completion_cid TEXT PRIMARY KEY, manifest_cid TEXT, pipeline TEXT, revision TEXT,
                 commit_id TEXT, nf_run_hash TEXT, session_id TEXT, run_name TEXT, asserted_by TEXT,
                 status TEXT, possibly_incomplete INTEGER, finished_at TEXT, member TEXT);
CREATE INDEX run_pipeline_status_finished_at ON run(pipeline, status, finished_at DESC);
CREATE INDEX run_nf_run_hash ON run(nf_run_hash);
CREATE INDEX run_manifest_cid ON run(manifest_cid);
CREATE TABLE collection(collection_cid TEXT PRIMARY KEY, completion_cid TEXT, output_name TEXT);
CREATE TABLE item(item_cid TEXT PRIMARY KEY);
CREATE TABLE collection_item(collection_cid TEXT, item_cid TEXT);
CREATE INDEX collection_completion_output ON collection(completion_cid, output_name);
CREATE INDEX collection_item_collection ON collection_item(collection_cid);
CREATE TABLE producer(
                 content_cid TEXT, item_cid TEXT, collection_cid TEXT, completion_cid TEXT, filename TEXT);
CREATE INDEX producer_content_cid ON producer(content_cid);
CREATE TABLE consumer(content_cid TEXT, completion_cid TEXT, name TEXT, how TEXT);
CREATE TABLE item_attr(item_cid TEXT, path TEXT, type TEXT, value TEXT, truncated INTEGER);
CREATE INDEX item_attr_path_type_value ON item_attr(path, type, value);
CREATE TABLE claim_current(
                 subject_cid TEXT, attribute TEXT, value TEXT, claim_cid TEXT, conflicted INTEGER);
CREATE TABLE missing(have_cid TEXT, needed_cid TEXT);
CREATE TABLE nf_record(
                 key TEXT PRIMARY KEY, kind TEXT, workflow_run TEXT, task_run TEXT,
                 labels_json TEXT, block_cid TEXT);
CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT);
