CREATE TABLE schema_version(version INTEGER NOT NULL);
CREATE TABLE run(
                 completion_cid TEXT PRIMARY KEY, manifest_cid TEXT, pipeline TEXT, revision TEXT,
                 commit_id TEXT, nf_run_hash TEXT, session_id TEXT, run_name TEXT, asserted_by TEXT,
                 status TEXT, possibly_incomplete INTEGER, finished_at TEXT, member TEXT);
CREATE INDEX run_pipeline_status_finished_at ON run(pipeline, status, finished_at DESC);
CREATE INDEX run_nf_run_hash ON run(nf_run_hash);
CREATE INDEX run_manifest_cid ON run(manifest_cid);
CREATE TABLE collection(
                 collection_cid TEXT PRIMARY KEY, kind TEXT, completion_cid TEXT, output_name TEXT, asserted_by TEXT);
CREATE TABLE item(item_cid TEXT PRIMARY KEY);
CREATE TABLE collection_item(collection_cid TEXT, item_cid TEXT, via_cid TEXT);
CREATE INDEX collection_completion_output ON collection(completion_cid, output_name);
CREATE INDEX collection_item_collection ON collection_item(collection_cid);
CREATE INDEX collection_item_item ON collection_item(item_cid);
CREATE TABLE selection_child(parent_cid TEXT, child_cid TEXT);
CREATE INDEX selection_child_child ON selection_child(child_cid);
CREATE TABLE selection_derived(selection_cid TEXT, derived_from_cid TEXT);
CREATE TABLE producer(
                 content_cid TEXT, item_cid TEXT, collection_cid TEXT, completion_cid TEXT, filename TEXT);
CREATE INDEX producer_content_cid ON producer(content_cid);
CREATE TABLE consumer(content_cid TEXT, completion_cid TEXT, name TEXT, how TEXT);
CREATE TABLE item_attr(item_cid TEXT, path TEXT, type TEXT, value TEXT, truncated INTEGER);
CREATE INDEX item_attr_path_type_value ON item_attr(path, type, value);
CREATE TABLE log_entry(cid TEXT, kind TEXT, member TEXT, written_at TEXT);
CREATE UNIQUE INDEX log_entry_cid_member ON log_entry(cid, member);
CREATE INDEX log_entry_kind_written_at ON log_entry(kind, written_at DESC);
CREATE TABLE claim(
                 claim_cid TEXT PRIMARY KEY, subject_cid TEXT, verb TEXT, attribute TEXT, value TEXT,
                 timestamp TEXT, asserted_by TEXT);
CREATE INDEX claim_subject ON claim(subject_cid);
CREATE TABLE claim_supersedes(claim_cid TEXT, superseded_cid TEXT);
CREATE INDEX claim_supersedes_superseded ON claim_supersedes(superseded_cid);
CREATE TABLE claim_current(
                 subject_cid TEXT, attribute TEXT, value TEXT, claim_cid TEXT, conflicted INTEGER);
CREATE INDEX claim_current_subject ON claim_current(subject_cid, attribute);
CREATE TABLE missing(have_cid TEXT, needed_cid TEXT);
CREATE TABLE nf_record(
                 key TEXT PRIMARY KEY, kind TEXT, workflow_run TEXT, task_run TEXT,
                 labels_json TEXT, block_cid TEXT);
CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE block(cid TEXT PRIMARY KEY, size INTEGER, codec TEXT);
CREATE TABLE block_ref(owner_cid TEXT, cid TEXT, name TEXT);
CREATE INDEX block_ref_owner ON block_ref(owner_cid);
CREATE TABLE root(id INTEGER PRIMARY KEY, cid TEXT, kind TEXT, asserted_by TEXT, state TEXT, since TEXT);
CREATE INDEX root_cid ON root(cid);
CREATE INDEX root_kind ON root(kind);
CREATE TABLE root_stats(root_id INTEGER PRIMARY KEY, reachable_bytes INTEGER, reachable_blocks INTEGER,
                 exclusive_bytes INTEGER, exclusive_blocks INTEGER, pinned_bytes INTEGER, partial INTEGER);
CREATE TABLE root_shared(root_id INTEGER, other_root_id INTEGER, bytes INTEGER);
CREATE INDEX root_shared_root ON root_shared(root_id);
CREATE TABLE store_stats(stored_bytes INTEGER, stored_blocks INTEGER, referenced_bytes INTEGER,
                 pinned_bytes INTEGER, held_bytes INTEGER, releasing_bytes INTEGER, shared_bytes INTEGER, partial INTEGER);
CREATE TABLE asserter_stats(asserted_by TEXT PRIMARY KEY, reachable_bytes INTEGER, exclusive_bytes INTEGER,
                 pinned_bytes INTEGER, releasing_bytes INTEGER);
CREATE TABLE pipeline_stats(pipeline TEXT PRIMARY KEY, runs INTEGER, bytes INTEGER, blocks INTEGER, releasing_bytes INTEGER);
CREATE TABLE collection_size(collection_cid TEXT PRIMARY KEY, bytes INTEGER, blocks INTEGER);
CREATE TABLE item_size(item_cid TEXT PRIMARY KEY, bytes INTEGER, blocks INTEGER);
CREATE TABLE kind_stats(kind TEXT PRIMARY KEY, files INTEGER, bytes INTEGER);
CREATE TABLE pinned_overlap(subject_cid TEXT, asserted_by TEXT, bytes INTEGER);
CREATE INDEX pinned_overlap_subject ON pinned_overlap(subject_cid);
CREATE TABLE transfer(
             cid TEXT PRIMARY KEY, from_member TEXT, to_member TEXT, asserted_by TEXT, depth TEXT, claims TEXT,
             blocks INTEGER, copied INTEGER, bytes INTEGER, at TEXT);
CREATE TABLE transfer_root(transfer_cid TEXT, root_cid TEXT, role TEXT);
CREATE INDEX transfer_root_root ON transfer_root(root_cid);
CREATE TABLE project(project_cid TEXT PRIMARY KEY, name TEXT, description TEXT, created_by TEXT, created_at TEXT);
CREATE TABLE project_stats(project_cid TEXT PRIMARY KEY, reachable_bytes INTEGER, pinned_bytes INTEGER,
             held_bytes INTEGER, releasing_bytes INTEGER, roots INTEGER);
CREATE TABLE run_input(completion_cid TEXT, role TEXT, name TEXT, address TEXT, size INTEGER, sample TEXT, position INTEGER);
CREATE INDEX run_input_address ON run_input(address, role, completion_cid);
CREATE INDEX run_input_completion ON run_input(completion_cid);
CREATE TABLE handoff(
             cid TEXT PRIMARY KEY, template TEXT, selection_cid TEXT, project_cid TEXT, asserted_by TEXT, at TEXT);
CREATE INDEX claim_current_attribute_value ON claim_current(attribute, value);
CREATE TABLE input_origin(completion_cid TEXT, position INTEGER, item_cid TEXT, collection_cid TEXT, selection_cid TEXT);
CREATE INDEX input_origin_item ON input_origin(item_cid);
CREATE INDEX input_origin_completion ON input_origin(completion_cid, position);
CREATE TABLE item_input(completion_cid TEXT, item_cid TEXT, positions TEXT);
CREATE INDEX item_input_item ON item_input(item_cid);
CREATE INDEX item_input_completion ON item_input(completion_cid);
CREATE TABLE item_task(completion_cid TEXT, item_cid TEXT, task_hash TEXT, process TEXT, tag TEXT);
CREATE INDEX item_task_item ON item_task(item_cid);
CREATE INDEX item_task_completion ON item_task(completion_cid);
