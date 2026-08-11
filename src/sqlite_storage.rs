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
             FROM (SELECT id, ts, content FROM bulletins ORDER BY ts DESC LIMIT 10 OFFSET ?) b
             LEFT JOIN attachments a ON b.id = a.bulletin_id
             ORDER BY b.ts DESC, a.msg_id ASC"
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn tmp_db() -> std::path::PathBuf {
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        std::env::temp_dir().join(format!("telepublish_test_{}_{}.db", std::process::id(), n))
    }

    async fn storage(tmp: &std::path::Path) -> SqliteStorage {
        let s = SqliteStorage::new(&format!("sqlite://{}?mode=rwc", tmp.display())).await.unwrap();
        s.migrate().await.unwrap();
        s
    }

    fn cleanup(tmp: &std::path::Path) {
        for p in [
            tmp.to_path_buf(),
            tmp.with_extension("db-wal"),
            tmp.with_extension("db-shm"),
        ] {
            let _ = std::fs::remove_file(p);
        }
    }

    #[tokio::test]
    async fn pagination_with_albums() {
        let tmp = tmp_db();
        let s = storage(&tmp).await;

        // newest bulletin id=15 has an album with 5 photos,
        // the rest (1..=14) are text-only, ts == id
        for id in 1..=15 {
            s.upsert_bulletin(id, id as u32, &format!("text {}", id)).await.unwrap();
        }
        for msg in 1..=5 {
            s.insert_photo(15, &format!("/photo/15/{}", msg), msg).await.unwrap();
        }

        let page1 = s.get_bulletins(0).await.unwrap();
        assert_eq!(page1.len(), 10, "page 1 must contain exactly 10 bulletins");
        assert_eq!(
            page1.iter().map(|b| b.id).collect::<Vec<_>>(),
            vec![15, 14, 13, 12, 11, 10, 9, 8, 7, 6]
        );
        assert_eq!(page1[0].photos.len(), 5, "album must carry all 5 photos");

        let page2 = s.get_bulletins(10).await.unwrap();
        assert_eq!(page2.len(), 5, "page 2 must contain the remaining 5 bulletins");
        assert_eq!(page2.iter().map(|b| b.id).collect::<Vec<_>>(), vec![5, 4, 3, 2, 1]);

        let page3 = s.get_bulletins(15).await.unwrap();
        assert!(page3.is_empty(), "offset beyond the end must be empty");

        let mut seen = std::collections::HashSet::new();
        for b in page1.iter().chain(page2.iter()) {
            assert!(seen.insert(b.id), "duplicate bulletin id={} in pagination", b.id);
        }
        assert_eq!(seen.len(), 15);

        drop(s);
        cleanup(&tmp);
    }
}
