use std::time::Duration;
use tokio::time::sleep;

const TG: &str = "https://api.telegram.org/bot";
const LOCAL: &str = "http://127.0.0.1:3000";
const RETRIES: usize = 15;

fn unique() -> String {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos().to_string()
}

fn tg_url(method: &str) -> String {
    let token = std::env::var("TEST_BOT_TOKEN").expect("TEST_BOT_TOKEN not set (bot that sends messages)");
    format!("{TG}{token}/{method}")
}

fn channel() -> i64 {
    std::env::var("CHANNEL").expect("CHANNEL not set").parse().expect("invalid CHANNEL")
}

fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build().unwrap()
}

fn cli() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build().unwrap()
}

async fn tg_post(met: &str, form: &[(&str, &str)]) -> serde_json::Value {
    let mut retries = 5u32;
    loop {
        let resp: serde_json::Value = client().post(tg_url(met)).form(form).send().await.unwrap().json().await.unwrap();
        if resp["ok"].as_bool().unwrap_or(false) {
            return resp;
        }
        let desc = resp["description"].as_str().unwrap_or("");
        if desc.starts_with("Too Many Requests") && retries > 0 {
            let rest = desc.rsplit(' ').next().and_then(|s| s.parse::<u64>().ok()).unwrap_or(5);
            sleep(Duration::from_secs(rest.min(10))).await;
            retries -= 1;
            continue;
        }
        panic!("tg {met} failed: {desc}");
    }
}

async fn tg_multipart(met: &str, form: reqwest::multipart::Form) -> serde_json::Value {
    // Form не Clone, поэтому на 429 не retry, просто подождём
    let resp: serde_json::Value = client().post(tg_url(met)).multipart(form).send().await.unwrap().json().await.unwrap();
    if let Some(desc) = resp["description"].as_str() {
        if desc.starts_with("Too Many Requests") {
            let rest = desc.rsplit(' ').next().and_then(|s| s.parse::<u64>().ok()).unwrap_or(5);
            eprintln!("rate-limited on {met}, sleeping {rest}s");
            sleep(Duration::from_secs(rest.min(15))).await;
            // пересоздавать form нельзя, просто паникуем
        }
    }
    if !resp["ok"].as_bool().unwrap() {
        panic!("tg {met} failed: {}", resp["description"]);
    }
    resp
}

#[derive(serde::Deserialize, Debug)]
struct Bulletins { bulletins: Vec<B> }
#[derive(serde::Deserialize, Debug)]
struct B { text: String, photos: Vec<String>, files: Vec<F> }
#[derive(serde::Deserialize, Debug)]
struct F { url: String, name: String, mime: String }

async fn get_bulletins() -> Vec<B> {
    cli().get(format!("{LOCAL}/bulletins")).send().await.unwrap().json::<Bulletins>().await.unwrap().bulletins
}

/// Ждём bulletin с текстом (только текст, без проверки фото/файлов)
async fn wait_text(contains: &str) -> B {
    for i in 0..RETRIES {
        let bs = get_bulletins().await;
        if let Some(b) = bs.into_iter().find(|b| b.text.contains(contains)) {
            return b;
        }
        if i < RETRIES - 1 { sleep(Duration::from_secs(1)).await; }
    }
    panic!("text bulletin with '{contains}' not found after {RETRIES}s")
}

/// Ждём bulletin с текстом, содержащим `contains`, и хотя бы одним фото или файлом.
async fn wait_b(contains: &str) -> B {
    for i in 0..RETRIES {
        let bs = get_bulletins().await;
        if let Some(b) = bs.into_iter().find(|b| b.text.contains(contains) && (!b.photos.is_empty() || !b.files.is_empty())) {
            return b;
        }
        if i < RETRIES - 1 { sleep(Duration::from_secs(1)).await; }
    }
    // fallback: print what we found
    let bs = get_bulletins().await;
    for b in &bs {
        if b.text.contains(contains) {
            panic!("bulletin with '{contains}' found but photos={:?} files={:?} (waited {RETRIES}s)", b.photos, b.files);
        }
    }
    panic!("bulletin with '{contains}' not found after {RETRIES}s")
}

/// Ждём, что bulletin с текстом, содержащим `contains`, исчез.
async fn wait_gone(contains: &str) {
    for i in 0..RETRIES {
        let bs = get_bulletins().await;
        if !bs.iter().any(|b| b.text.contains(contains)) {
            return;
        }
        if i < RETRIES - 1 { sleep(Duration::from_secs(1)).await; }
    }
    panic!("bulletin with '{contains}' still present after {RETRIES}s")
}

#[tokio::test]
async fn full_flow() {
    // ---- 1. text-only ----
    let t1 = format!("t1-{}", unique());
    let id1 = send_text(&t1).await;
    let b = wait_text(&t1).await;
    assert!(b.photos.is_empty(), "t1: no photos expected");
    assert!(b.files.is_empty(), "t1: no files expected");
    assert_eq!(b.text, t1);
    del_and_wait(id1, &t1).await;

    // ---- 2. photo with caption ----
    let t2 = format!("t2-{}", unique());
    let id2 = send_photo(&t2).await;
    let b = wait_b(&t2).await;
    assert!(b.photos[0].starts_with("/photo/"));
    assert_eq!(b.text, t2);
    del_and_wait(id2, &t2).await;

    // ---- 3. document with caption (standalone) ----
    let t3 = format!("t3-{}", unique());
    let id3 = send_doc_with_caption(&t3).await;
    let b = wait_b(&t3).await;
    assert_eq!(b.files.len(), 1, "t3: expected 1 file");
    assert!(b.files[0].url.starts_with("/file/"));
    assert_eq!(b.files[0].name, "test.txt");
    assert_eq!(b.text, t3);
    del_and_wait(id3, &t3).await;

    // ---- 4. file reply to existing bulletin ----
    let t4 = format!("t4-{}", unique());
    let id4 = send_text(&t4).await;
    let _b = wait_text(&t4).await;

    send_doc_reply(id4).await;
    let b = wait_b(&t4).await;
    assert_eq!(b.files.len(), 1, "t4: expected 1 file attached by reply");
    assert_eq!(b.files[0].name, "test.txt");
    assert!(b.files[0].url.starts_with("/file/"));
    del_and_wait(id4, &t4).await;
}

// helpers

async fn send_text(text: &str) -> i64 {
    let r = tg_post("sendMessage", &[("chat_id", &channel().to_string()), ("text", text)]).await;
    r["result"]["message_id"].as_i64().unwrap()
}

async fn send_photo(caption: &str) -> i64 {
    let data = tokio::fs::read("test_data/photo.jpg").await.unwrap();
    let part = reqwest::multipart::Part::bytes(data).file_name("photo.jpg");
    let form = reqwest::multipart::Form::new()
        .text("chat_id", channel().to_string())
        .text("caption", caption.to_string())
        .part("photo", part);
    let r = tg_multipart("sendPhoto", form).await;
    r["result"]["message_id"].as_i64().unwrap()
}

async fn send_doc_with_caption(caption: &str) -> i64 {
    let data = tokio::fs::read("test_data/test.txt").await.unwrap();
    let part = reqwest::multipart::Part::bytes(data).file_name("test.txt");
    let form = reqwest::multipart::Form::new()
        .text("chat_id", channel().to_string())
        .text("caption", caption.to_string())
        .part("document", part);
    let r = tg_multipart("sendDocument", form).await;
    r["result"]["message_id"].as_i64().unwrap()
}

async fn send_doc_reply(reply_to: i64) -> i64 {
    let data = tokio::fs::read("test_data/test.txt").await.unwrap();
    let part = reqwest::multipart::Part::bytes(data).file_name("test.txt");
    let form = reqwest::multipart::Form::new()
        .text("chat_id", channel().to_string())
        .text("reply_to_message_id", reply_to.to_string())
        .part("document", part);
    let r = tg_multipart("sendDocument", form).await;
    r["result"]["message_id"].as_i64().unwrap()
}

async fn del_and_wait(reply_to: i64, text_contains: &str) {
    tg_post("sendMessage", &[
        ("chat_id", &channel().to_string()),
        ("text", "del"),
        ("reply_to_message_id", &reply_to.to_string()),
    ]).await;
    sleep(Duration::from_secs(2)).await;
    wait_gone(text_contains).await;
}

// ---- new helpers for all_scenarios ----

async fn edit_caption(msg_id: i64, caption: &str) {
    tg_post("editMessageCaption", &[
        ("chat_id", &channel().to_string()),
        ("message_id", &msg_id.to_string()),
        ("caption", caption),
    ]).await;
}

async fn edit_text(msg_id: i64, text: &str) {
    tg_post("editMessageText", &[
        ("chat_id", &channel().to_string()),
        ("message_id", &msg_id.to_string()),
        ("text", text),
    ]).await;
}

async fn send_media_group_photos(count: u32, caption: &str) -> Vec<i64> {
    let data = tokio::fs::read("test_data/photo.jpg").await.unwrap();
    let mut form = reqwest::multipart::Form::new()
        .text("chat_id", channel().to_string());
    let mut media = Vec::new();
    for i in 0..count {
        let key = format!("p{i}");
        let mut item = serde_json::Map::new();
        item.insert("type".into(), serde_json::json!("photo"));
        item.insert("media".into(), serde_json::json!(format!("attach://{key}")));
        if i == 0 {
            item.insert("caption".into(), serde_json::json!(caption));
        }
        media.push(serde_json::Value::Object(item));
        form = form.part(key.clone(), reqwest::multipart::Part::bytes(data.clone()).file_name(format!("{key}.jpg")));
    }
    form = form.text("media", serde_json::json!(media).to_string());
    let r = tg_multipart("sendMediaGroup", form).await;
    r["result"].as_array().unwrap().iter().map(|m| m["message_id"].as_i64().unwrap()).collect()
}

async fn send_media_group_docs(count: u32, caption: &str) -> Vec<i64> {
    let data = tokio::fs::read("test_data/test.txt").await.unwrap();
    let mut form = reqwest::multipart::Form::new()
        .text("chat_id", channel().to_string());
    let mut media = Vec::new();
    for i in 0..count {
        let key = format!("d{i}");
        let mut item = serde_json::Map::new();
        item.insert("type".into(), serde_json::json!("document"));
        item.insert("media".into(), serde_json::json!(format!("attach://{key}")));
        if i == 0 {
            item.insert("caption".into(), serde_json::json!(caption));
        }
        media.push(serde_json::Value::Object(item));
        form = form.part(key, reqwest::multipart::Part::bytes(data.clone()).file_name("test.txt"));
    }
    form = form.text("media", serde_json::json!(media).to_string());
    let r = tg_multipart("sendMediaGroup", form).await;
    r["result"].as_array().unwrap().iter().map(|m| m["message_id"].as_i64().unwrap()).collect()
}

async fn send_doc_reply_to(reply_to: i64) -> i64 {
    let data = tokio::fs::read("test_data/test.txt").await.unwrap();
    let part = reqwest::multipart::Part::bytes(data).file_name("test.txt");
    let form = reqwest::multipart::Form::new()
        .text("chat_id", channel().to_string())
        .text("reply_to_message_id", reply_to.to_string())
        .part("document", part);
    let r = tg_multipart("sendDocument", form).await;
    r["result"]["message_id"].as_i64().unwrap()
}

/// Ждём bulletin с текстом, содержащим `contains`, и хотя бы одним файлом.
async fn wait_bf(contains: &str) -> B {
    for i in 0..RETRIES {
        let bs = get_bulletins().await;
        if let Some(b) = bs.into_iter().find(|b| b.text.contains(contains) && !b.files.is_empty()) {
            return b;
        }
        if i < RETRIES - 1 { sleep(Duration::from_secs(1)).await; }
    }
    let bs = get_bulletins().await;
    for b in &bs {
        if b.text.contains(contains) {
            panic!("bulletin with '{contains}' found but files={:?} (waited {RETRIES}s)", b.files);
        }
    }
    panic!("bulletin with '{contains}' not found after {RETRIES}s")
}

#[tokio::test]
async fn all_scenarios() {
    sleep(Duration::from_secs(3)).await; // rate-limit gap from previous test

    // ---- 1. text-only ----
    let t1 = format!("sc1-{}", unique());
    let id1 = send_text(&t1).await;
    let b = wait_text(&t1).await;
    assert!(b.photos.is_empty(), "1: no photos");
    assert!(b.files.is_empty(), "1: no files");
    assert_eq!(b.text, t1);
    del_and_wait(id1, &t1).await;
    sleep(Duration::from_secs(2)).await;

    // ---- 2. photo with caption ----
    let t2 = format!("sc2-{}", unique());
    let id2 = send_photo(&t2).await;
    let b = wait_b(&t2).await;
    assert_eq!(b.photos.len(), 1, "2: expected 1 photo");
    assert!(b.photos[0].starts_with("/photo/"));
    assert!(b.files.is_empty(), "2: no files");
    assert_eq!(b.text, t2);
    del_and_wait(id2, &t2).await;
    sleep(Duration::from_secs(2)).await;

    // ---- 3. album with 2 photos and caption ----
    let t3 = format!("sc3-{}", unique());
    let ids3 = send_media_group_photos(2, &t3).await;
    let b = wait_b(&t3).await;
    assert!(b.photos.len() >= 1, "3: expected at least 1 photo");
    assert!(b.photos.iter().all(|p| p.starts_with("/photo/")));
    del_and_wait(ids3[0], &t3).await;
    sleep(Duration::from_secs(2)).await;

    // ---- 4. document with caption (standalone) ----
    let t4 = format!("sc4-{}", unique());
    let id4 = send_doc_with_caption(&t4).await;
    let b = wait_b(&t4).await;
    assert_eq!(b.files.len(), 1, "4: expected 1 file");
    assert!(b.files[0].url.starts_with("/file/"));
    assert_eq!(b.files[0].name, "test.txt");
    assert_eq!(b.text, t4);
    del_and_wait(id4, &t4).await;
    sleep(Duration::from_secs(2)).await;

    // ---- 5. text + file reply ----
    let t5 = format!("sc5-{}", unique());
    let id5 = send_text(&t5).await;
    wait_text(&t5).await;
    send_doc_reply_to(id5).await;
    let b = wait_bf(&t5).await;
    assert_eq!(b.files.len(), 1, "5: expected 1 file by reply");
    assert_eq!(b.files[0].name, "test.txt");
    assert_eq!(b.text, t5);
    del_and_wait(id5, &t5).await;
    sleep(Duration::from_secs(2)).await;

    // ---- 6. album with 2 documents and caption ----
    let t6 = format!("sc6-{}", unique());
    let ids6 = send_media_group_docs(2, &t6).await;
    let b = wait_b(&t6).await;
    assert!(b.files.len() >= 1, "6: expected at least 1 file");
    assert_eq!(b.files[0].name, "test.txt");
    del_and_wait(ids6[0], &t6).await;
    sleep(Duration::from_secs(2)).await;

    // ---- 7. photo + caption, then file reply ----
    let t7 = format!("sc7-{}", unique());
    let id7 = send_photo(&t7).await;
    let b = wait_b(&t7).await;
    assert_eq!(b.photos.len(), 1, "7: expected 1 photo initially");
    assert!(b.files.is_empty(), "7: no files initially");

    send_doc_reply_to(id7).await;
    let b = wait_bf(&t7).await;
    assert_eq!(b.photos.len(), 1, "7: expected 1 photo after reply");
    assert_eq!(b.files.len(), 1, "7: expected 1 file by reply");
    assert_eq!(b.files[0].name, "test.txt");

    del_and_wait(id7, &t7).await;
    sleep(Duration::from_secs(2)).await;

    // ---- 8. album with 2 photos + caption, then file reply ----
    let t8 = format!("sc8-{}", unique());
    let ids8 = send_media_group_photos(2, &t8).await;
    let b = wait_b(&t8).await;
    assert!(b.photos.len() >= 1, "8: expected at least 1 photo initially");
    assert!(b.files.is_empty(), "8: no files initially");

    send_doc_reply_to(ids8[0]).await;
    let b = wait_bf(&t8).await;
    assert!(b.photos.len() >= 1, "8: expected at least 1 photo after reply");
    assert_eq!(b.files.len(), 1, "8: expected 1 file by reply");
    assert_eq!(b.files[0].name, "test.txt");

    del_and_wait(ids8[0], &t8).await;
}

// ---- entity helpers ----

fn ent_bold(offset: usize, length: usize) -> serde_json::Value {
    serde_json::json!({"type":"bold","offset":offset,"length":length})
}
fn ent_italic(offset: usize, length: usize) -> serde_json::Value {
    serde_json::json!({"type":"italic","offset":offset,"length":length})
}
fn ent_underline(offset: usize, length: usize) -> serde_json::Value {
    serde_json::json!({"type":"underline","offset":offset,"length":length})
}
fn ent_strikethrough(offset: usize, length: usize) -> serde_json::Value {
    serde_json::json!({"type":"strikethrough","offset":offset,"length":length})
}
fn ent_code(offset: usize, length: usize) -> serde_json::Value {
    serde_json::json!({"type":"code","offset":offset,"length":length})
}
fn ent_pre(offset: usize, length: usize) -> serde_json::Value {
    serde_json::json!({"type":"pre","offset":offset,"length":length})
}
fn ent_link(offset: usize, length: usize, url: &str) -> serde_json::Value {
    serde_json::json!({"type":"text_link","offset":offset,"length":length,"url":url})
}
fn ent_url(offset: usize, length: usize) -> serde_json::Value {
    serde_json::json!({"type":"url","offset":offset,"length":length})
}
fn ent_mention(offset: usize, length: usize) -> serde_json::Value {
    serde_json::json!({"type":"mention","offset":offset,"length":length})
}

async fn send_text_entities(text: &str, entities: &str) -> i64 {
    tg_post("sendMessage", &[
        ("chat_id", &channel().to_string()),
        ("text", text),
        ("entities", entities),
    ]).await["result"]["message_id"].as_i64().unwrap()
}

async fn send_photo_caption_entities(caption: &str, entities: &str) -> i64 {
    let data = tokio::fs::read("test_data/photo.jpg").await.unwrap();
    let part = reqwest::multipart::Part::bytes(data).file_name("photo.jpg");
    let form = reqwest::multipart::Form::new()
        .text("chat_id", channel().to_string())
        .text("caption", caption.to_string())
        .text("caption_entities", entities.to_string())
        .part("photo", part);
    let r = tg_multipart("sendPhoto", form).await;
    r["result"]["message_id"].as_i64().unwrap()
}

#[tokio::test]
async fn test_entities() {
    sleep(Duration::from_secs(3)).await;

    // 1. Bold
    let tok = format!("eb1-{}", unique());
    let text = format!("X {tok} X");
    let entities = serde_json::json!([ent_bold(2, tok.len())]).to_string();
    let id = send_text_entities(&text, &entities).await;
    let b = wait_text(&tok).await;
    assert_eq!(b.text, format!("X <b>{tok}</b> X"));
    del_and_wait(id, &tok).await;
    sleep(Duration::from_secs(2)).await;

    // 2. Italic
    let tok = format!("eb2-{}", unique());
    let text = format!("X {tok} X");
    let entities = serde_json::json!([ent_italic(2, tok.len())]).to_string();
    let id = send_text_entities(&text, &entities).await;
    let b = wait_text(&tok).await;
    assert_eq!(b.text, format!("X <i>{tok}</i> X"));
    del_and_wait(id, &tok).await;
    sleep(Duration::from_secs(2)).await;

    // 3. Underline
    let tok = format!("eb3-{}", unique());
    let text = format!("X {tok} X");
    let entities = serde_json::json!([ent_underline(2, tok.len())]).to_string();
    let id = send_text_entities(&text, &entities).await;
    let b = wait_text(&tok).await;
    assert_eq!(b.text, format!("X <u>{tok}</u> X"));
    del_and_wait(id, &tok).await;
    sleep(Duration::from_secs(2)).await;

    // 4. Strikethrough
    let tok = format!("eb4-{}", unique());
    let text = format!("X {tok} X");
    let entities = serde_json::json!([ent_strikethrough(2, tok.len())]).to_string();
    let id = send_text_entities(&text, &entities).await;
    let b = wait_text(&tok).await;
    assert_eq!(b.text, format!("X <s>{tok}</s> X"));
    del_and_wait(id, &tok).await;
    sleep(Duration::from_secs(2)).await;

    // 5. Code
    let tok = format!("eb5-{}", unique());
    let text = format!("X {tok} X");
    let entities = serde_json::json!([ent_code(2, tok.len())]).to_string();
    let id = send_text_entities(&text, &entities).await;
    let b = wait_text(&tok).await;
    assert_eq!(b.text, format!("X <code>{tok}</code> X"));
    del_and_wait(id, &tok).await;
    sleep(Duration::from_secs(2)).await;

    // 6. Pre
    let tok = format!("eb6-{}", unique());
    let text = format!("X {tok} X");
    let entities = serde_json::json!([ent_pre(2, tok.len())]).to_string();
    let id = send_text_entities(&text, &entities).await;
    let b = wait_text(&tok).await;
    assert_eq!(b.text, format!("X <pre>{tok}</pre> X"));
    del_and_wait(id, &tok).await;
    sleep(Duration::from_secs(2)).await;

    // 7. TextLink
    let tok = format!("eb7-{}", unique());
    let text = format!("X {tok} X");
    let entities = serde_json::json!([ent_link(2, tok.len(), &format!("https://example.com/{tok}"))]).to_string();
    let id = send_text_entities(&text, &entities).await;
    let b = wait_text(&tok).await;
    let expected = format!("X <a href=\"https://example.com/{tok}\" target=\"_blank\" rel=\"nofollow\">{tok}</a> X");
    assert_eq!(b.text, expected);
    del_and_wait(id, &tok).await;
    sleep(Duration::from_secs(2)).await;

    // 8. Url (bare URL)
    let tok = format!("eb8-{}", unique());
    let url = format!("https://{tok}.com/x");
    let text = format!("X {url} X");
    let entities = serde_json::json!([ent_url(2, url.len())]).to_string();
    let id = send_text_entities(&text, &entities).await;
    let b = wait_text(&tok).await;
    let expected = format!("<a href=\"{url}\" target=\"_blank\" rel=\"nofollow\">{url}</a>");
    assert!(b.text.contains(&expected), "expected '{expected}' in '{}'", b.text);
    del_and_wait(id, &tok).await;
    sleep(Duration::from_secs(2)).await;

    // 9. Mention
    let tok = format!("eb9-{}", unique());
    let mention = "@TestUsername123";
    let text = format!("{tok} {mention}");
    let entities = serde_json::json!([ent_mention(tok.len() + 1, mention.len())]).to_string();
    let id = send_text_entities(&text, &entities).await;
    let b = wait_text(&tok).await;
    let expected = format!("<a href=\"https://t.me/TestUsername123\" target=\"_blank\" rel=\"nofollow\">{mention}</a>");
    assert!(b.text.contains(&expected), "expected '{expected}' in '{}'", b.text);
    del_and_wait(id, &tok).await;
    sleep(Duration::from_secs(2)).await;

    // 10. Nested bold + italic
    let text = "bold Itali bold";
    let entities = serde_json::json!([ent_bold(0, 15), ent_italic(5, 5)]).to_string();
    let id = send_text_entities(text, &entities).await;
    let b = wait_text("Itali").await;
    assert_eq!(b.text, "<b>bold <i>Itali</i> bold</b>");
    del_and_wait(id, "Itali").await;
    sleep(Duration::from_secs(2)).await;

    // 11. Multiple entities
    let tok = format!("eb11-{}", unique());
    let text = format!("{tok} A B C");
    let entities = serde_json::json!([
        ent_bold(tok.len() + 1, 1),
        ent_italic(tok.len() + 3, 1),
        ent_underline(tok.len() + 5, 1),
    ]).to_string();
    let id = send_text_entities(&text, &entities).await;
    let b = wait_text(&tok).await;
    let expected = format!("{tok} <b>A</b> <i>B</i> <u>C</u>");
    assert_eq!(b.text, expected);
    del_and_wait(id, &tok).await;
    sleep(Duration::from_secs(2)).await;

    // 12. HTML escape (no entities)
    let tok = format!("eb12-{}", unique());
    let text = format!("{tok} & <x> \"quoted\"");
    let id = send_text(&text).await;
    let b = wait_text(&tok).await;
    let expected = format!("{tok} &amp; &lt;x&gt; &quot;quoted&quot;");
    assert_eq!(b.text, expected);
    del_and_wait(id, &tok).await;
    sleep(Duration::from_secs(2)).await;

    // 13. Bold in photo caption
    let tok = format!("eb13-{}", unique());
    let caption = format!("{tok} B");
    let entities = serde_json::json!([ent_bold(tok.len() + 1, 1)]).to_string();
    let id = send_photo_caption_entities(&caption, &entities).await;
    let b = wait_b(&tok).await;
    assert_eq!(b.text, format!("{tok} <b>B</b>"));
    assert!(b.photos[0].starts_with("/photo/"), "expected photo");
    del_and_wait(id, &tok).await;
}
