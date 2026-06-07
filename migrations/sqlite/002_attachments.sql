CREATE TABLE IF NOT EXISTS attachments (
    bulletin_id INTEGER NOT NULL REFERENCES bulletins(id),
    url TEXT NOT NULL,
    msg_id INTEGER NOT NULL DEFAULT 0,
    file_name TEXT,
    mime_type TEXT,
    PRIMARY KEY (bulletin_id, msg_id)
);
