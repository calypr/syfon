PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS drs_object (
  id TEXT PRIMARY KEY,
  size INTEGER,
  created_time TIMESTAMP,
  updated_time TIMESTAMP,
  name TEXT,
  version TEXT,
  description TEXT
);

CREATE TABLE IF NOT EXISTS drs_object_access_method (
  object_id TEXT,
  url TEXT,
  type TEXT,
  org TEXT NOT NULL DEFAULT '',
  project TEXT NOT NULL DEFAULT '',
  FOREIGN KEY(object_id) REFERENCES drs_object(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS drs_object_checksum (
  object_id TEXT,
  type TEXT,
  checksum TEXT,
  FOREIGN KEY(object_id) REFERENCES drs_object(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS s3_credential (
  bucket TEXT PRIMARY KEY,
  provider TEXT NOT NULL DEFAULT 's3',
  region TEXT,
  access_key TEXT,
  secret_key TEXT,
  endpoint TEXT,
  billing_log_bucket TEXT,
  billing_log_prefix TEXT
);

CREATE TABLE IF NOT EXISTS bucket_scope (
  organization TEXT NOT NULL,
  project_id TEXT NOT NULL,
  bucket TEXT NOT NULL,
  path_prefix TEXT,
  PRIMARY KEY (organization, project_id)
);

CREATE INDEX IF NOT EXISTS idx_bucket_scope_bucket ON bucket_scope(bucket);

CREATE TABLE IF NOT EXISTS access_grant (
  access_grant_id TEXT PRIMARY KEY,
  first_issued_at TIMESTAMP NOT NULL,
  last_issued_at TIMESTAMP NOT NULL,
  issue_count INTEGER NOT NULL DEFAULT 0,
  object_id TEXT NOT NULL DEFAULT '',
  sha256 TEXT NOT NULL DEFAULT '',
  object_size INTEGER NOT NULL DEFAULT 0,
  organization TEXT NOT NULL DEFAULT '',
  project TEXT NOT NULL DEFAULT '',
  access_id TEXT NOT NULL DEFAULT '',
  provider TEXT NOT NULL DEFAULT '',
  bucket TEXT NOT NULL DEFAULT '',
  storage_url TEXT NOT NULL DEFAULT '',
  actor_email TEXT NOT NULL DEFAULT '',
  actor_subject TEXT NOT NULL DEFAULT '',
  auth_mode TEXT NOT NULL DEFAULT ''
);
