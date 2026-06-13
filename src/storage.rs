use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;
use ydb_unofficial::error::YdbError;
use ydb_unofficial::sqlx::prelude::*;

pub struct BulletinRow {
    pub id: i32,
    pub ts: u32,
    pub content: String,
    pub photos: Vec<String>,
    pub files: Vec<FileInfo>,
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
    async fn insert_photo(&self, bulletin_id: i32, url: &str, msg_id: i32) -> anyhow::Result<()>;
    async fn insert_file(&self, bulletin_id: i32, url: &str, msg_id: i32, file_name: &str, mime_type: &str) -> anyhow::Result<()>;
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
        let migrator = sqlx_macros::migrate!("./migrations/ydb");
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
        let rows = query_as::<_, (i32, Datetime, String, Option<String>, Option<String>, Option<String>)>("
            declare $offset as Uint32;
            select b.id, b.ts, b.content, a.url, a.file_name, a.mime_type
            from bulletins b
            left join attachments a on b.id = a.bulletin_id
            order by b.ts desc, a.msg_id asc
            limit 10 offset $offset;
        ").bind(("$offset", offset))
            .fetch_all(executor!(&mut conn)).await?;

        let mut result: Vec<BulletinRow> = Vec::new();
        let mut current: Option<(i32, u32, String)> = None;
        let mut photos: Vec<String> = Vec::new();
        let mut files: Vec<FileInfo> = Vec::new();

        for (id, ts, content, url, file_name, mime_type) in rows {
            let key = (id, into_ts(ts), content);
            match &current {
                Some(c) if c != &key => {
                    result.push(BulletinRow { id: c.0, ts: c.1, content: c.2.clone(), photos: std::mem::take(&mut photos), files: std::mem::take(&mut files) });
                    current = Some(key);
                }
                None => { current = Some(key); }
                _ => {}
            }
            if let Some(u) = url {
                if u.starts_with("/file/") {
                    files.push(FileInfo { url: u, file_name: file_name.unwrap_or_default(), mime_type: mime_type.unwrap_or_default() });
                } else {
                    photos.push(u);
                }
            }
        }
        if let Some(c) = current {
            result.push(BulletinRow { id: c.0, ts: c.1, content: c.2, photos, files });
        }

        Ok(result)
    }

    async fn insert_photo(&self, bulletin_id: i32, url: &str, msg_id: i32) -> anyhow::Result<()> {
        let mut conn = self.conn.lock().await;
        executor!(&mut conn).
            execute(
                query("declare $bid as Int32; declare $url as Utf8; declare $msg_id as Int32; upsert into attachments (bulletin_id, url, msg_id, file_name, mime_type) values ($bid, $url, $msg_id, Null, 'image/jpeg');")
                    .bind(("$bid", bulletin_id))
                    .bind(("$url", url.to_owned()))
                    .bind(("$msg_id", msg_id))
            ).await?;
        Ok(())
    }

    async fn insert_file(&self, bulletin_id: i32, url: &str, msg_id: i32, file_name: &str, mime_type: &str) -> anyhow::Result<()> {
        let mut conn = self.conn.lock().await;
        executor!(&mut conn).
            execute(
                query("declare $bid as Int32; declare $url as Utf8; declare $msg_id as Int32; declare $name as Utf8; declare $mime as Utf8; upsert into attachments (bulletin_id, url, msg_id, file_name, mime_type) values ($bid, $url, $msg_id, $name, $mime);")
                    .bind(("$bid", bulletin_id))
                    .bind(("$url", url.to_owned()))
                    .bind(("$msg_id", msg_id))
                    .bind(("$name", file_name.to_owned()))
                    .bind(("$mime", mime_type.to_owned()))
            ).await?;
        Ok(())
    }

    async fn get_attachment_keys(&self, bulletin_id: i32) -> anyhow::Result<Vec<(i32, i32, Option<String>)>> {
        let mut conn = self.conn.lock().await;
        let rows = query_as::<_, (i32, i32, Option<String>)>("
            declare $id as Int32;
            select bulletin_id, msg_id, file_name from attachments where bulletin_id = $id;
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
