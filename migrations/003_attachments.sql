create table attachments (
    bulletin_id Int32,
    url Utf8,
    sort_order Int32,
    file_name Text,
    mime_type Text,
    primary key (bulletin_id, sort_order)
);
insert into attachments select * from bulletin_photos;
drop table bulletin_photos;
