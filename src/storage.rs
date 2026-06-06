use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;
use ydb_unofficial::error::YdbError;
use ydb_unofficial::sqlx::prelude::*;

pub struct BulletinRow {
    pub id: i32,
    pub ts: u32,
    pub content: String,
}

pub struct FileInfo {
    pub url: String,
    pub file_name: String,
    pub mime_type: String,
}

#[async_trait]
pub trait Storage: Send + Sync {
    async fn migrate(&self) -> anyhow::Result<()>;
    async fn upsert_bulletin(&self, id: i32, ts: u32, content: &str) -> anyhow::Result<()>;
    async fn delete_bulletin(&self, id: i32) -> anyhow::Result<()>;
    async fn get_bulletins(&self, offset: u32) -> anyhow::Result<Vec<BulletinRow>>;
    async fn insert_photo(&self, bulletin_id: i32, url: &str, sort_order: i32) -> anyhow::Result<()>;
    async fn insert_file(&self, bulletin_id: i32, url: &str, sort_order: i32, file_name: &str, mime_type: &str) -> anyhow::Result<()>;
    async fn get_photo_paths(&self, bulletin_id: i32) -> anyhow::Result<Vec<String>>;
    async fn get_file_info(&self, bulletin_id: i32) -> anyhow::Result<Vec<FileInfo>>;
    async fn get_attachment_count(&self, bulletin_id: i32) -> anyhow::Result<i32>;
    async fn get_attachment_keys(&self, bulletin_id: i32) -> anyhow::Result<Vec<(i32, i32, Option<String>)>>;
    async fn delete_attachments_for_bulletin(&self, bulletin_id: i32) -> anyhow::Result<()>;
}

pub struct YdbStorage {
    conn: Arc<Mutex<YdbConnection>>,
}

impl YdbStorage {
    pub async fn new(db_url: &str) -> anyhow::Result<Self> {
        let conn = YdbConnectOptions::from_str(db_url)?.connect().await?;
        Ok(Self { conn: Arc::new(Mutex::new(conn)) })
    }
}

macro_rules! executor {
    ($conn:expr) => {{
        let conn = &mut *$conn;
        let e = match conn.executor() {
            Ok(e) => e,
            Err(YdbError::NoSession) => {
                log::warn!("received no session error, reconnecting...");
                conn.reconnect().await?;
                conn.executor()?
            },
            Err(e) => Err(e)?
        };
        e.retry()
    }}
}

fn into_ts(dt: Datetime) -> u32 {
    dt.into()
}

#[async_trait]
impl Storage for YdbStorage {
    async fn migrate(&self) -> anyhow::Result<()> {
        let mut conn = self.conn.lock().await;
        let migrator = sqlx_macros::migrate!("./migrations");
        migrator.run_direct(&mut *conn).await?;
        Ok(())
    }

    async fn upsert_bulletin(&self, id: i32, ts: u32, content: &str) -> anyhow::Result<()> {
        let mut conn = self.conn.lock().await;
        executor!(&mut conn).
            execute(
                query("declare $id as Int32; declare $ts as Uint32; declare $content as Utf8; upsert into bulletins (id, ts, content) values ($id, cast($ts as Datetime), $content);")
                    .bind(("$id", id))
                    .bind(("$ts", ts))
                    .bind(("$content", content.to_owned()))
            ).await?;
        Ok(())
    }

    async fn delete_bulletin(&self, id: i32) -> anyhow::Result<()> {
        let mut conn = self.conn.lock().await;
        executor!(&mut conn).
            execute(
                query("declare $id as Int32; delete from bulletins where id = $id;")
                    .bind(("$id", id))
            ).await?;
        Ok(())
    }

    async fn get_bulletins(&self, offset: u32) -> anyhow::Result<Vec<BulletinRow>> {
        let mut conn = self.conn.lock().await;
        let rows = query_as::<_, (i32, Datetime, String)>("
            declare $offset as Uint32;
            select id, ts, content from bulletins order by ts desc limit 10 offset $offset;
        ").bind(("$offset", offset))
            .fetch_all(executor!(&mut conn)).await?;

        Ok(rows.into_iter().map(|(id, ts, content)| BulletinRow {
            id,
            ts: into_ts(ts),
            content,
        }).collect())
    }

    async fn insert_photo(&self, bulletin_id: i32, url: &str, sort_order: i32) -> anyhow::Result<()> {
        let mut conn = self.conn.lock().await;
        executor!(&mut conn).
            execute(
                query("declare $bid as Int32; declare $url as Utf8; declare $sort as Int32; upsert into attachments (bulletin_id, url, sort_order, file_name, mime_type) values ($bid, $url, $sort, Null, 'image/jpeg');")
                    .bind(("$bid", bulletin_id))
                    .bind(("$url", url.to_owned()))
                    .bind(("$sort", sort_order))
            ).await?;
        Ok(())
    }

    async fn insert_file(&self, bulletin_id: i32, url: &str, sort_order: i32, file_name: &str, mime_type: &str) -> anyhow::Result<()> {
        let mut conn = self.conn.lock().await;
        executor!(&mut conn).
            execute(
                query("declare $bid as Int32; declare $url as Utf8; declare $sort as Int32; declare $name as Utf8; declare $mime as Utf8; upsert into attachments (bulletin_id, url, sort_order, file_name, mime_type) values ($bid, $url, $sort, $name, $mime);")
                    .bind(("$bid", bulletin_id))
                    .bind(("$url", url.to_owned()))
                    .bind(("$sort", sort_order))
                    .bind(("$name", file_name.to_owned()))
                    .bind(("$mime", mime_type.to_owned()))
            ).await?;
        Ok(())
    }

    async fn get_photo_paths(&self, bulletin_id: i32) -> anyhow::Result<Vec<String>> {
        let mut conn = self.conn.lock().await;
        let rows = query_as::<_, (String,)>("
            declare $bid as Int32;
            select url from attachments where bulletin_id = $bid and url like '/photo/%' order by sort_order;
        ").bind(("$bid", bulletin_id))
            .fetch_all(executor!(&mut conn)).await?;

        Ok(rows.into_iter().map(|(u,)| u).collect())
    }

    async fn get_file_info(&self, bulletin_id: i32) -> anyhow::Result<Vec<FileInfo>> {
        let mut conn = self.conn.lock().await;
        let rows = query_as::<_, (String, String, String)>("
            declare $bid as Int32;
            select url, file_name, mime_type from attachments where bulletin_id = $bid and file_name is not null order by sort_order;
        ").bind(("$bid", bulletin_id))
            .fetch_all(executor!(&mut conn)).await?;

        Ok(rows.into_iter().map(|(url, file_name, mime_type)| FileInfo { url, file_name, mime_type }).collect())
    }

    async fn get_attachment_count(&self, bulletin_id: i32) -> anyhow::Result<i32> {
        let mut conn = self.conn.lock().await;
        let rows = query_as::<_, (i64,)>("
            declare $bid as Int32;
            select count(*) from attachments where bulletin_id = $bid;
        ").bind(("$bid", bulletin_id))
            .fetch_all(executor!(&mut conn)).await?;

        Ok(rows.first().map(|(c,)| *c as i32).unwrap_or(0))
    }

    async fn get_attachment_keys(&self, bulletin_id: i32) -> anyhow::Result<Vec<(i32, i32, Option<String>)>> {
        let mut conn = self.conn.lock().await;
        let rows = query_as::<_, (i32, i32, Option<String>)>("
            declare $id as Int32;
            select bulletin_id, sort_order, file_name from attachments where bulletin_id = $id;
        ").bind(("$id", bulletin_id))
            .fetch_all(executor!(&mut conn)).await?;

        Ok(rows)
    }

    async fn delete_attachments_for_bulletin(&self, bulletin_id: i32) -> anyhow::Result<()> {
        let mut conn = self.conn.lock().await;
        executor!(&mut conn).
            execute(
                query("declare $id as Int32; delete from attachments where bulletin_id = $id;")
                    .bind(("$id", bulletin_id))
            ).await?;
        Ok(())
    }
}
