create table bulletin_photos (
    bulletin_id Int32,
    url Utf8,
    sort_order Int32,
    primary key (bulletin_id, sort_order)
);
