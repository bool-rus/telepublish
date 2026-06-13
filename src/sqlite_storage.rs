use async_trait::async_trait;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::SqlitePool;

use crate::storage::{BulletinRow, FileInfo, Storage};

pub struct SqliteStorage {
    pool: SqlitePool,
}

impl SqliteStorage {
    pub async fn new(db_url: &str) -> anyhow::Result<Self> {
        let pool = SqlitePoolOptions::new().connect(db_url).await?;
        Ok(Self { pool })
    }
}

#[async_trait]
impl Storage for SqliteStorage {
    async fn migrate(&self) -> anyhow::Result<()> {
        sqlx::migrate!("./migrations/sqlite").run(&self.pool).await?;
        Ok(())
    }

    async fn upsert_bulletin(&self, id: i32, ts: u32, content: &str) -> anyhow::Result<()> {
        sqlx::query(
            "INSERT OR REPLACE INTO bulletins (id, ts, content) VALUES (?, ?, ?)"
        )
        .bind(id)
        .bind(ts as i64)
        .bind(content)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn delete_bulletin(&self, id: i32) -> anyhow::Result<()> {
        sqlx::query("DELETE FROM bulletins WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn get_bulletins(&self, offset: u32) -> anyhow::Result<Vec<BulletinRow>> {
        let rows = sqlx::query_as::<_, (i32, i64, String, Option<String>, Option<String>, Option<String>)>(
            "SELECT b.id, b.ts, b.content, a.url, a.file_name, a.mime_type
             FROM bulletins b LEFT JOIN attachments a ON b.id = a.bulletin_id
             ORDER BY b.ts DESC, a.msg_id ASC LIMIT 10 OFFSET ?"
        )
        .bind(offset as i64)
        .fetch_all(&self.pool)
        .await?;

        let mut result: Vec<BulletinRow> = Vec::new();
        let mut current: Option<(i32, u32, String)> = None;
        let mut photos: Vec<String> = Vec::new();
        let mut files: Vec<FileInfo> = Vec::new();

        for (id, ts, content, url, file_name, mime_type) in rows {
            let key = (id, ts as u32, content);
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
        sqlx::query(
            "INSERT OR REPLACE INTO attachments (bulletin_id, url, msg_id, file_name, mime_type) VALUES (?, ?, ?, NULL, 'image/jpeg')"
        )
        .bind(bulletin_id)
        .bind(url)
        .bind(msg_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn insert_file(&self, bulletin_id: i32, url: &str, msg_id: i32, file_name: &str, mime_type: &str) -> anyhow::Result<()> {
        sqlx::query(
            "INSERT OR REPLACE INTO attachments (bulletin_id, url, msg_id, file_name, mime_type) VALUES (?, ?, ?, ?, ?)"
        )
        .bind(bulletin_id)
        .bind(url)
        .bind(msg_id)
        .bind(file_name)
        .bind(mime_type)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_attachment_keys(&self, bulletin_id: i32) -> anyhow::Result<Vec<(i32, i32, Option<String>)>> {
        let rows = sqlx::query_as::<_, (i32, i32, Option<String>)>(
            "SELECT bulletin_id, msg_id, file_name FROM attachments WHERE bulletin_id = ?"
        )
        .bind(bulletin_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }

    async fn delete_attachments_for_bulletin(&self, bulletin_id: i32) -> anyhow::Result<()> {
        sqlx::query("DELETE FROM attachments WHERE bulletin_id = ?")
            .bind(bulletin_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}
