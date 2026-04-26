CREATE TABLE IF NOT EXISTS drs_object (
  id TEXT PRIMARY KEY,
  size BIGINT,
  created_time TIMESTAMPTZ,
  updated_time TIMESTAMPTZ,
  name TEXT,
  version TEXT,
  description TEXT
);

CREATE TABLE IF NOT EXISTS drs_object_access_method (
  object_id TEXT NOT NULL,
  url TEXT NOT NULL,
  type TEXT NOT NULL,
  org TEXT NOT NULL DEFAULT '',
  project TEXT NOT NULL DEFAULT '',
  FOREIGN KEY(object_id) REFERENCES drs_object(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS drs_object_checksum (
  object_id TEXT NOT NULL,
  type TEXT NOT NULL,
  checksum TEXT NOT NULL,
  FOREIGN KEY(object_id) REFERENCES drs_object(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS drs_object_alias (
  alias_id TEXT PRIMARY KEY,
  object_id TEXT NOT NULL,
  FOREIGN KEY(object_id) REFERENCES drs_object(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS drs_object_access_method_object_id_idx
  ON drs_object_access_method(object_id);

CREATE INDEX IF NOT EXISTS drs_object_checksum_object_id_idx
  ON drs_object_checksum(object_id);

CREATE INDEX IF NOT EXISTS drs_object_checksum_checksum_idx
  ON drs_object_checksum(checksum);

CREATE INDEX IF NOT EXISTS drs_object_access_method_scope_idx
  ON drs_object_access_method(org, project);

CREATE INDEX IF NOT EXISTS drs_object_alias_object_id_idx
  ON drs_object_alias(object_id);
