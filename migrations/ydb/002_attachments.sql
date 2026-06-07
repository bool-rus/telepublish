create table attachments (
    bulletin_id Int32,
    url Utf8,
    msg_id Int32,
    file_name Utf8,
    mime_type Utf8,
    primary key (bulletin_id, msg_id)
);
