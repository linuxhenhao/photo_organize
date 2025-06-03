CREATE TABLE IF NOT EXISTS photos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_path TEXT NOT NULL UNIQUE,
    size INTEGER NOT NULL,
    create_time DATETIME NOT NULL,
    mmh3_hash TEXT DEFAULT '',
    group_id INTEGER DEFAULT 0
);

CREATE INDEX idx_size ON photos(size);
CREATE INDEX idx_group ON photos(group_id);