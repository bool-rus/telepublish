use async_trait::async_trait;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::SqlitePool;

use crate::storage::{BulletinRow, Storage};

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
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS bulletins (
                id INTEGER PRIMARY KEY,
                ts INTEGER NOT NULL,
                content TEXT NOT NULL DEFAULT ''
            )"
        ).execute(&self.pool).await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS bulletin_photos (
                bulletin_id INTEGER NOT NULL REFERENCES bulletins(id),
                url TEXT NOT NULL,
                sort_order INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (bulletin_id, sort_order)
            )"
        ).execute(&self.pool).await?;
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

    async fn insert_photo(&self, bulletin_id: i32, url: &str, sort_order: i32) -> anyhow::Result<()> {
        sqlx::query(
            "INSERT OR REPLACE INTO bulletin_photos (bulletin_id, url, sort_order) VALUES (?, ?, ?)"
        )
        .bind(bulletin_id)
        .bind(url)
        .bind(sort_order)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_photo_paths(&self, bulletin_id: i32) -> anyhow::Result<Vec<String>> {
        let rows = sqlx::query_as::<_, (String,)>(
            "SELECT url FROM bulletin_photos WHERE bulletin_id = ? ORDER BY sort_order"
        )
        .bind(bulletin_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(|(u,)| u).collect())
    }

    async fn get_bulletin_photo_keys(&self, bulletin_id: i32) -> anyhow::Result<Vec<(i32, i32)>> {
        let rows = sqlx::query_as::<_, (i32, i32)>(
            "SELECT bulletin_id, sort_order FROM bulletin_photos WHERE bulletin_id = ?"
        )
        .bind(bulletin_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }

    async fn delete_photos_for_bulletin(&self, bulletin_id: i32) -> anyhow::Result<()> {
        sqlx::query("DELETE FROM bulletin_photos WHERE bulletin_id = ?")
            .bind(bulletin_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}
