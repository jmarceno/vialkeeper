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
  file_format_version INTEGER NOT NULL CHECK (file_format_version = 1),
  logical_schema_version INTEGER NOT NULL CHECK (logical_schema_version = 1),
  revision_algorithm_version INTEGER NOT NULL CHECK (revision_algorithm_version = 1),
  canonicalization_version INTEGER NOT NULL CHECK (canonicalization_version = 1),
  replication_protocol_major INTEGER NOT NULL CHECK (replication_protocol_major = 1),
  current_sequence INTEGER NOT NULL CHECK (current_sequence >= 0),
  created_at TEXT NOT NULL,
  config_json TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS documents (
  doc_key INTEGER PRIMARY KEY,
  document_id TEXT UNIQUE NOT NULL,
  winning_revision TEXT,
  winning_body_json TEXT,
  winning_deleted INTEGER NOT NULL CHECK (winning_deleted IN (0, 1)),
  update_sequence INTEGER NOT NULL CHECK (update_sequence >= 0),
  CHECK (
    (winning_revision IS NULL AND winning_body_json IS NULL AND winning_deleted = 1 AND update_sequence = 0)
    OR
    (winning_revision IS NOT NULL AND ((winning_deleted = 1 AND winning_body_json IS NULL) OR (winning_deleted = 0 AND winning_body_json IS NOT NULL)))
  )
) STRICT;

CREATE TABLE IF NOT EXISTS revisions (
  doc_key INTEGER NOT NULL REFERENCES documents(doc_key) ON DELETE RESTRICT,
  revision_id TEXT NOT NULL,
  generation INTEGER NOT NULL CHECK (generation > 0),
  parent_revision TEXT,
  digest TEXT NOT NULL,
  deleted INTEGER NOT NULL CHECK (deleted IN (0, 1)),
  body_json TEXT,
  insertion_sequence INTEGER NOT NULL CHECK (insertion_sequence >= 0),
  is_leaf INTEGER NOT NULL CHECK (is_leaf IN (0, 1)),
  PRIMARY KEY (doc_key, revision_id),
  CHECK ((deleted = 1 AND body_json IS NULL) OR (deleted = 0 AND body_json IS NOT NULL)),
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
