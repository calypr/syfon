CREATE TABLE IF NOT EXISTS drs_object (
  id TEXT PRIMARY KEY,
  size BIGINT,
  created_time TIMESTAMPTZ,
  updated_time TIMESTAMPTZ,
  name TEXT,
  file_name TEXT,
  version TEXT,
  description TEXT
);

ALTER TABLE drs_object
  ADD COLUMN IF NOT EXISTS file_name TEXT;

CREATE TABLE IF NOT EXISTS drs_object_access_method (
  object_id TEXT NOT NULL,
  url TEXT NOT NULL,
  type TEXT NOT NULL,
  FOREIGN KEY(object_id) REFERENCES drs_object(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS drs_object_controlled_access (
  object_id TEXT NOT NULL,
  resource TEXT NOT NULL,
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

CREATE TABLE IF NOT EXISTS drs_object_browse_index (
  object_id TEXT NOT NULL,
  resource TEXT NOT NULL,
  normalized_path TEXT NOT NULL,
  parent_path TEXT NOT NULL,
  entry_name TEXT NOT NULL,
  PRIMARY KEY (resource, object_id),
  FOREIGN KEY(object_id) REFERENCES drs_object(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS drs_object_access_method_object_id_idx
  ON drs_object_access_method(object_id);

CREATE INDEX IF NOT EXISTS drs_object_checksum_object_id_idx
  ON drs_object_checksum(object_id);

CREATE INDEX IF NOT EXISTS drs_object_checksum_checksum_idx
  ON drs_object_checksum(checksum);

CREATE INDEX IF NOT EXISTS drs_object_checksum_checksum_type_object_id_idx
  ON drs_object_checksum(checksum, type, object_id);

CREATE INDEX IF NOT EXISTS drs_object_controlled_access_object_id_idx
  ON drs_object_controlled_access(object_id);

CREATE INDEX IF NOT EXISTS drs_object_controlled_access_resource_idx
  ON drs_object_controlled_access(resource);

CREATE INDEX IF NOT EXISTS drs_object_controlled_access_resource_object_id_idx
  ON drs_object_controlled_access(resource, object_id);

CREATE INDEX IF NOT EXISTS drs_object_controlled_access_object_id_resource_idx
  ON drs_object_controlled_access(object_id, resource);

CREATE INDEX IF NOT EXISTS drs_object_alias_object_id_idx
  ON drs_object_alias(object_id);

CREATE INDEX IF NOT EXISTS drs_object_browse_index_resource_parent_object_id_idx
  ON drs_object_browse_index(resource, parent_path, object_id);

CREATE INDEX IF NOT EXISTS drs_object_browse_index_resource_parent_entry_name_idx
  ON drs_object_browse_index(resource, parent_path, entry_name);

CREATE INDEX IF NOT EXISTS drs_object_browse_index_resource_normalized_path_idx
  ON drs_object_browse_index(resource, normalized_path);
