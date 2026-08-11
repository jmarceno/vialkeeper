PRAGMA application_id = 1163412546;
PRAGMA user_version = 1;
PRAGMA journal_mode = DELETE;
PRAGMA foreign_keys = ON;
PRAGMA synchronous = EXTRA;
PRAGMA locking_mode = NORMAL;
PRAGMA trusted_schema = OFF;

CREATE TABLE IF NOT EXISTS db_meta (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  database_uuid TEXT NOT NULL UNIQUE,
  database_kind TEXT NOT NULL CHECK (database_kind IN ('ordinary', 'derived')),
  history_epoch TEXT NOT NULL,
  file_format_version INTEGER NOT NULL CHECK (file_format_version = 1),
  logical_schema_version INTEGER NOT NULL CHECK (logical_schema_version = 1),
  revision_algorithm_version INTEGER NOT NULL CHECK (revision_algorithm_version = 1),
  canonicalization_version INTEGER NOT NULL CHECK (canonicalization_version = 1),
  replication_protocol_major INTEGER NOT NULL CHECK (replication_protocol_major = 1),
  current_sequence INTEGER NOT NULL CHECK (current_sequence >= 0),
  retention_floor_sequence INTEGER NOT NULL CHECK (retention_floor_sequence >= 0),
  compaction_epoch INTEGER NOT NULL CHECK (compaction_epoch >= 0),
  retention_boundary_digest TEXT,
  created_at TEXT NOT NULL,
  config_json TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS documents (
  doc_key INTEGER PRIMARY KEY,
  document_id TEXT UNIQUE NOT NULL,
  winning_revision TEXT,
  winning_body_json TEXT,
  winning_body_term BLOB,
  winning_deleted INTEGER NOT NULL CHECK (winning_deleted IN (0, 1)),
  update_sequence INTEGER NOT NULL CHECK (update_sequence >= 0),
  CHECK (
    (winning_revision IS NULL AND winning_body_json IS NULL AND winning_body_term IS NULL AND winning_deleted = 1 AND update_sequence = 0)
    OR
    (winning_revision IS NOT NULL AND ((winning_deleted = 1 AND winning_body_json IS NULL AND winning_body_term IS NULL) OR (winning_deleted = 0 AND winning_body_json IS NOT NULL AND winning_body_term IS NOT NULL)))
  )
) STRICT;

CREATE TABLE IF NOT EXISTS revisions (
  doc_key INTEGER NOT NULL REFERENCES documents(doc_key) ON DELETE RESTRICT,
  revision_id TEXT NOT NULL,
  generation INTEGER NOT NULL CHECK (generation > 0),
  parent_revision TEXT,
  history_id TEXT NOT NULL,
  digest TEXT NOT NULL,
  deleted INTEGER NOT NULL CHECK (deleted IN (0, 1)),
  body_json TEXT,
  body_term BLOB,
  insertion_sequence INTEGER NOT NULL CHECK (insertion_sequence >= 0),
  is_leaf INTEGER NOT NULL CHECK (is_leaf IN (0, 1)),
  PRIMARY KEY (doc_key, revision_id),
  CHECK ((deleted = 1 AND body_json IS NULL AND body_term IS NULL) OR (deleted = 0 AND body_json IS NOT NULL AND body_term IS NOT NULL)),
  UNIQUE (doc_key, parent_revision, revision_id)
) STRICT;

CREATE INDEX IF NOT EXISTS revisions_leaves ON revisions(doc_key, is_leaf);
CREATE INDEX IF NOT EXISTS revisions_leaf_winner ON revisions(doc_key, is_leaf, deleted, generation, revision_id);
CREATE INDEX IF NOT EXISTS revisions_parents ON revisions(doc_key, parent_revision);

CREATE TABLE IF NOT EXISTS changes (
  sequence INTEGER PRIMARY KEY,
  doc_key INTEGER NOT NULL REFERENCES documents(doc_key) ON DELETE RESTRICT,
  document_id TEXT NOT NULL,
  winning_revision TEXT NOT NULL,
  winning_deleted INTEGER NOT NULL CHECK (winning_deleted IN (0, 1)),
  leaf_set_json TEXT NOT NULL,
  leaf_set_term BLOB NOT NULL,
  origin TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS local_records (
  namespace TEXT NOT NULL,
  record_key TEXT NOT NULL,
  record_version INTEGER NOT NULL CHECK (record_version >= 0),
  value_json TEXT NOT NULL,
  PRIMARY KEY (namespace, record_key)
) STRICT;

CREATE TABLE IF NOT EXISTS replication_jobs (
  job_id TEXT PRIMARY KEY,
  definition_json TEXT NOT NULL,
  enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
  last_diagnostic_json TEXT
) STRICT;

CREATE TABLE IF NOT EXISTS index_definitions (
  index_id TEXT PRIMARY KEY,
  name TEXT UNIQUE NOT NULL,
  index_type TEXT NOT NULL CHECK (index_type IN ('structured', 'full_text')),
  definition_digest TEXT NOT NULL,
  definition_json TEXT NOT NULL,
  lifecycle_state TEXT NOT NULL,
  adapter_metadata_json TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS revision_attachments (
  doc_key INTEGER NOT NULL,
  revision_id TEXT NOT NULL,
  attachment_name TEXT NOT NULL,
  blob_digest TEXT NOT NULL,
  logical_size INTEGER NOT NULL CHECK (logical_size >= 0),
  content_type TEXT NOT NULL,
  PRIMARY KEY (doc_key, revision_id, attachment_name),
  FOREIGN KEY (doc_key, revision_id) REFERENCES revisions(doc_key, revision_id) ON DELETE CASCADE
) STRICT;

CREATE INDEX IF NOT EXISTS revision_attachments_blob_digest ON revision_attachments(blob_digest);

CREATE TABLE IF NOT EXISTS pending_blobs (
  blob_digest TEXT PRIMARY KEY,
  logical_size INTEGER NOT NULL CHECK (logical_size >= 0),
  expires_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS view_definitions (
  view_id TEXT PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  definition_json TEXT NOT NULL,
  definition_digest TEXT NOT NULL,
  created_at TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS view_state (
  view_id TEXT PRIMARY KEY REFERENCES view_definitions(view_id) ON DELETE RESTRICT,
  active_generation INTEGER NOT NULL CHECK (active_generation > 0),
  building_generation INTEGER NULL CHECK (building_generation IS NULL OR building_generation > 0),
  indexed_through INTEGER NOT NULL CHECK (indexed_through >= 0),
  status TEXT NOT NULL,
  last_error_code TEXT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS view_rows (
  view_id TEXT NOT NULL REFERENCES view_definitions(view_id) ON DELETE RESTRICT,
  generation INTEGER NOT NULL CHECK (generation > 0),
  document_id TEXT NOT NULL,
  revision_id TEXT NOT NULL,
  key_json TEXT NOT NULL,
  key_sort BLOB NOT NULL,
  value_json TEXT NULL,
  PRIMARY KEY (view_id, generation, document_id)
) STRICT;

CREATE INDEX IF NOT EXISTS view_rows_sort ON view_rows(view_id, generation, key_sort, document_id);

CREATE TABLE IF NOT EXISTS derived_view (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  materialization_id TEXT NOT NULL UNIQUE,
  name TEXT NOT NULL,
  definition_json TEXT NOT NULL,
  definition_digest TEXT NOT NULL,
  enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
  status TEXT NOT NULL,
  options_json TEXT NOT NULL,
  last_error_code TEXT
) STRICT;

CREATE TABLE IF NOT EXISTS derived_sources (
  source_ordinal INTEGER NOT NULL UNIQUE CHECK (source_ordinal >= 0),
  source_database_uuid TEXT PRIMARY KEY,
  source_history_epoch TEXT,
  checkpoint_sequence INTEGER NOT NULL DEFAULT 0 CHECK (checkpoint_sequence >= 0),
  state TEXT NOT NULL,
  rebuild_generation INTEGER NOT NULL DEFAULT 0 CHECK (rebuild_generation >= 0),
  rebuild_start_sequence INTEGER CHECK (rebuild_start_sequence IS NULL OR rebuild_start_sequence >= 0),
  rebuild_after_document_id TEXT,
  rebuild_catchup_sequence INTEGER CHECK (rebuild_catchup_sequence IS NULL OR rebuild_catchup_sequence >= 0),
  last_error_code TEXT
) STRICT;

CREATE TABLE IF NOT EXISTS derived_rows (
  source_database_uuid TEXT NOT NULL,
  source_document_id TEXT NOT NULL,
  source_revision_id TEXT NOT NULL,
  rebuild_generation INTEGER NOT NULL DEFAULT 0 CHECK (rebuild_generation >= 0),
  key_json TEXT NOT NULL,
  key_sort BLOB NOT NULL,
  group_key_json TEXT,
  group_key_sort BLOB,
  value_json TEXT,
  value_sort BLOB,
  PRIMARY KEY (source_database_uuid, source_document_id),
  CHECK ((group_key_json IS NULL AND group_key_sort IS NULL) OR (group_key_json IS NOT NULL AND group_key_sort IS NOT NULL)),
  CHECK (value_json IS NOT NULL OR value_sort IS NULL)
) STRICT;

CREATE INDEX IF NOT EXISTS derived_rows_source_rebuild
  ON derived_rows(source_database_uuid, rebuild_generation, source_document_id);
CREATE INDEX IF NOT EXISTS derived_rows_group
  ON derived_rows(group_key_sort, value_sort, source_document_id);

CREATE TABLE IF NOT EXISTS derived_groups (
  group_key_sort BLOB PRIMARY KEY,
  group_key_json TEXT NOT NULL,
  count INTEGER NOT NULL CHECK (count >= 0),
  sum_units TEXT NOT NULL,
  sumsqr_units TEXT NOT NULL,
  min_value_json TEXT,
  min_value_sort BLOB,
  max_value_json TEXT,
  max_value_sort BLOB,
  output_document_id TEXT NOT NULL UNIQUE,
  CHECK ((min_value_json IS NULL AND min_value_sort IS NULL) OR (min_value_json IS NOT NULL AND min_value_sort IS NOT NULL)),
  CHECK ((max_value_json IS NULL AND max_value_sort IS NULL) OR (max_value_json IS NOT NULL AND max_value_sort IS NOT NULL))
) STRICT;

CREATE INDEX IF NOT EXISTS derived_groups_output ON derived_groups(output_document_id);
