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
        let rows = sqlx::query_as::<_, (i32, i64, String)>(
            "SELECT id, ts, content FROM bulletins ORDER BY ts DESC LIMIT 10 OFFSET ?"
        )
        .bind(offset as i64)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(|(id, ts, content)| BulletinRow {
            id,
            ts: ts as u32,
            content,
        }).collect())
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

    async fn get_photo_paths(&self, bulletin_id: i32) -> anyhow::Result<Vec<String>> {
        let rows = sqlx::query_as::<_, (String,)>(
            "SELECT url FROM attachments WHERE bulletin_id = ? AND url LIKE '/photo/%' ORDER BY msg_id"
        )
        .bind(bulletin_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(|(u,)| u).collect())
    }

    async fn get_file_info(&self, bulletin_id: i32) -> anyhow::Result<Vec<FileInfo>> {
        let rows = sqlx::query_as::<_, (String, String, String)>(
            "SELECT url, file_name, mime_type FROM attachments WHERE bulletin_id = ? AND file_name IS NOT NULL ORDER BY msg_id"
        )
        .bind(bulletin_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(|(url, file_name, mime_type)| FileInfo { url, file_name, mime_type }).collect())
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
