create table bulletins (
    id Int32,
    ts Datetime,
    content Utf8,
    index idx_bltn_ts global on (ts),
    primary key (id)
);