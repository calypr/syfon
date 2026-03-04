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
  FOREIGN KEY(object_id) REFERENCES drs_object(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS drs_object_checksum (
  object_id TEXT,
  type TEXT,
  checksum TEXT,
  FOREIGN KEY(object_id) REFERENCES drs_object(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS drs_object_authz (
  object_id TEXT,
  resource TEXT,
  FOREIGN KEY(object_id) REFERENCES drs_object(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS s3_credential (
  bucket TEXT PRIMARY KEY,
  region TEXT,
  access_key TEXT,
  secret_key TEXT,
  endpoint TEXT
);

