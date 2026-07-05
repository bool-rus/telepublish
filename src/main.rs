use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::path::Path as FilePath;
use std::process::exit;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use futures_util::StreamExt;
use clap::Parser;
use reqwest::Url;
use s3::{AddressingStyle, Auth, Client, Credentials};
use serde::{Deserialize, Serialize};
use teloxide::dispatching::{Dispatcher, UpdateFilterExt};
use teloxide::payloads::{AnswerCallbackQuerySetters, SendMessageSetters};
use teloxide::requests::Requester;
use teloxide::types::{CallbackQuery, ChatId, InlineKeyboardButton, InlineKeyboardMarkup, MessageEntityKind, Update};
use teloxide::{dptree, Bot};
use tokio::sync::Mutex;

mod storage;
mod sqlite_storage;
use storage::Storage;

fn init_logger() {
    use simplelog::*;
    let mut builder = ConfigBuilder::new();
    builder.set_time_level(LevelFilter::Off);
    TermLogger::init(LevelFilter::Info, builder.build(), TerminalMode::Mixed, ColorChoice::Auto).unwrap();
}
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logger();
    let conf = Config::parse();
    
    let storage: Arc<dyn Storage> = if conf.db_url.starts_with("sqlite:") {
        Arc::new(sqlite_storage::SqliteStorage::new(&conf.db_url).await?)
    } else {
        Arc::new(storage::YdbStorage::new(&conf.db_url).await?)
    };
    storage.migrate().await?;
    
    let post_url = Url::from_str(&conf.button_post_url)?;
    let services = Arc::new(Services::new(Duration::from_secs(conf.barrier_rate_limit), post_url));

    let s3_client = Arc::new(Client::builder(&conf.s3_endpoint)?
        .region(&conf.s3_region)
        .auth(Auth::Static(Credentials::new(&conf.s3_access_key, &conf.s3_secret_key)?))
        .addressing_style(AddressingStyle::Path)
        .build()?);

    let conf = Arc::new(conf);
    let app_state = AppState { storage: storage.clone(), s3_client: s3_client.clone(), conf: conf.clone() };
    create_server(app_state).await?;
    let bot = Bot::new(conf.token.clone());
    if let Err(e) = bot.send_message(ChatId(conf.channel), "Управление").reply_markup(InlineKeyboardMarkup::new(vec![vec![
        InlineKeyboardButton::callback("Открыть шлагбаум", "OPENSHLAG"),
    ]])).await {
        log::warn!("Failed to send startup message: {}", e);
    }

    let handler = dptree::entry()
        .branch(Update::filter_callback_query().endpoint(process_callback))
        .endpoint(process_update);

    Dispatcher::builder(bot.clone(), handler)
        .dependencies(dptree::deps![conf, storage, services, s3_client])
        .enable_ctrlc_handler()
        .build()
        .dispatch().await;
    Ok(())
}

async fn process_callback(
    bot: Bot, 
    query: CallbackQuery,
    conf: Arc<Config>,
    services: Arc<Services>,
) -> anyhow::Result<()> {
    let user = query.from;
    let member = bot.get_chat_member(ChatId(conf.channel), user.id).await?;
    if member.is_present() {
        match query.data.as_ref().map(|o|o.as_str()) {
            Some("OPENSHLAG") => {
                let answer = if services.openshlag().await? { "Открыто"} else {"Уже открыто"};
                bot.answer_callback_query(query.id).text(answer).await?;
            }
            _=> {}
        }
    } else {
        bot.answer_callback_query(query.id).text("Неа").await?;
    }
    Ok(())
}

fn group_id(s: &str) -> i32 {
    let mut h = DefaultHasher::default();
    s.hash(&mut h);
    -(h.finish() as i32).abs()
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
     .replace('<', "&lt;")
     .replace('>', "&gt;")
     .replace('"', "&quot;")
}

fn write_tag_open(result: &mut String, kind: &MessageEntityKind, entity_text: &str) {
    match kind {
        MessageEntityKind::Bold => result.push_str("<b>"),
        MessageEntityKind::Italic => result.push_str("<i>"),
        MessageEntityKind::Underline => result.push_str("<u>"),
        MessageEntityKind::Strikethrough => result.push_str("<s>"),
        MessageEntityKind::Spoiler => result.push_str("<span class=\"spoiler\">"),
        MessageEntityKind::Code => result.push_str("<code>"),
        MessageEntityKind::Pre { .. } => result.push_str("<pre>"),
        MessageEntityKind::TextLink { url } => {
            result.push_str("<a href=\"");
            result.push_str(&html_escape(url.as_str()));
            result.push_str("\" target=\"_blank\" rel=\"nofollow\">");
        }
        MessageEntityKind::Url => {
            result.push_str("<a href=\"");
            result.push_str(&html_escape(entity_text));
            result.push_str("\" target=\"_blank\" rel=\"nofollow\">");
        }
        MessageEntityKind::Email => {
            result.push_str("<a href=\"mailto:");
            result.push_str(&html_escape(entity_text));
            result.push_str("\" target=\"_blank\" rel=\"nofollow\">");
        }
        MessageEntityKind::PhoneNumber => {
            result.push_str("<a href=\"tel:");
            result.push_str(&html_escape(entity_text));
            result.push_str("\" target=\"_blank\" rel=\"nofollow\">");
        }
        MessageEntityKind::Mention => {
            result.push_str("<a href=\"https://t.me/");
            result.push_str(&html_escape(&entity_text[1..]));
            result.push_str("\" target=\"_blank\" rel=\"nofollow\">");
        }
        MessageEntityKind::Hashtag => {
            result.push_str("<a href=\"https://t.me/");
            result.push_str(&html_escape(&entity_text[1..]));
            result.push_str("\" target=\"_blank\" rel=\"nofollow\">");
        }
        MessageEntityKind::TextMention { user } => {
            let url = user.preferably_tme_url();
            result.push_str("<a href=\"");
            result.push_str(&html_escape(url.as_str()));
            result.push_str("\" target=\"_blank\" rel=\"nofollow\">");
        }
        MessageEntityKind::BotCommand | MessageEntityKind::Cashtag | MessageEntityKind::CustomEmoji { .. } => {}
    }
}

fn write_tag_close(result: &mut String, kind: &MessageEntityKind) {
    match kind {
        MessageEntityKind::Bold => result.push_str("</b>"),
        MessageEntityKind::Italic => result.push_str("</i>"),
        MessageEntityKind::Underline => result.push_str("</u>"),
        MessageEntityKind::Strikethrough => result.push_str("</s>"),
        MessageEntityKind::Spoiler => result.push_str("</span>"),
        MessageEntityKind::Code => result.push_str("</code>"),
        MessageEntityKind::Pre { .. } => result.push_str("</pre>"),
        MessageEntityKind::TextLink { .. }
        | MessageEntityKind::Url
        | MessageEntityKind::Email
        | MessageEntityKind::PhoneNumber
        | MessageEntityKind::Mention
        | MessageEntityKind::Hashtag
        | MessageEntityKind::TextMention { .. } => result.push_str("</a>"),
        MessageEntityKind::BotCommand | MessageEntityKind::Cashtag | MessageEntityKind::CustomEmoji { .. } => {}
    }
}

fn entities_to_html(text: &str, entities: &[teloxide::types::MessageEntityRef]) -> String {
    if entities.is_empty() {
        return html_escape(text);
    }

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum EventKind { Open, Close }

    struct Event<'a> {
        pos: usize,
        kind: EventKind,
        kind_ref: &'a MessageEntityKind,
        entity_start: usize,
        entity_end: usize,
    }

    let mut events: Vec<Event> = Vec::with_capacity(entities.len() * 2);
    for e in entities {
        events.push(Event {
            pos: e.start(), kind: EventKind::Open, kind_ref: e.kind(),
            entity_start: e.start(), entity_end: e.end(),
        });
        events.push(Event {
            pos: e.end(), kind: EventKind::Close, kind_ref: e.kind(),
            entity_start: e.start(), entity_end: e.end(),
        });
    }

    events.sort_by(|a, b| a.pos.cmp(&b.pos).then_with(|| {
        match (a.kind, b.kind) {
            (EventKind::Close, EventKind::Open) => std::cmp::Ordering::Less,
            (EventKind::Open, EventKind::Close) => std::cmp::Ordering::Greater,
            _ => std::cmp::Ordering::Equal,
        }
    }));

    let mut html = String::new();
    let mut text_pos = 0;
    let mut event_idx = 0;
    let mut open_tags: Vec<&MessageEntityKind> = Vec::new();

    while event_idx < events.len() {
        let ev_pos = events[event_idx].pos;

        if text_pos < ev_pos {
            html.push_str(&html_escape(&text[text_pos..ev_pos]));
            text_pos = ev_pos;
        }

        while event_idx < events.len() && events[event_idx].pos == ev_pos {
            let ev = &events[event_idx];
            match ev.kind {
                EventKind::Close => {
                    if let Some(idx) = open_tags.iter().rposition(|t| std::ptr::eq(*t, ev.kind_ref)) {
                        for tag in open_tags.drain(idx..).rev() {
                            write_tag_close(&mut html, tag);
                        }
                    }
                }
                EventKind::Open => {
                    write_tag_open(&mut html, ev.kind_ref, &text[ev.entity_start..ev.entity_end]);
                    open_tags.push(ev.kind_ref);
                }
            }
            event_idx += 1;
        }
    }

    if text_pos < text.len() {
        html.push_str(&html_escape(&text[text_pos..]));
    }

    for tag in open_tags.drain(..).rev() {
        write_tag_close(&mut html, tag);
    }

    html
}

async fn process_update(
    bot: Bot,
    upd: Update,
    conf: Arc<Config>,
    storage: Arc<dyn Storage>,
    s3_client: Arc<Client>,
) -> anyhow::Result<()> {
    use teloxide::types::UpdateKind::*;
    let msg = match upd.kind {
        ChannelPost(msg) | EditedChannelPost(msg) => msg,
        _ => return Ok(())
    };
    let chat_id = msg.chat.id.0;
    if conf.channel != chat_id {
        return Ok(())
    }

    let text = msg.text().or_else(|| msg.caption());
    let is_del = text.map(|t| t.eq_ignore_ascii_case("del")).unwrap_or(false);
    let reply_target = msg.reply_to_message().map(|r| r.id.0);
    let target_id = if let Some(mg) = msg.media_group_id() {
        group_id(&mg)
    } else {
        msg.id.0
    };

    if is_del {
        let del_id = match msg.reply_to_message() {
            Some(reply) => {
                if let Some(mg) = reply.media_group_id() {
                    group_id(mg)
                } else {
                    reply.id.0
                }
            }
            None => target_id,
        };
        let keys = storage.get_attachment_keys(del_id).await?;

        for (bid, msg_id, file_name) in &keys {
            let ext = file_name.as_deref()
                .and_then(|n| FilePath::new(n).extension())
                .and_then(|e| e.to_str())
                .unwrap_or("jpg");
            let s3_key = format!("bulletins/{}/{}_{}.{}", conf.channel, bid, msg_id, ext);
            if let Err(e) = s3_client.objects().delete(&conf.s3_bucket, &s3_key).send().await {
                log::warn!("failed to delete s3 object {}: {:?}", s3_key, e);
            }
            bot.delete_message(ChatId(chat_id), teloxide::types::MessageId(*msg_id)).await.ok();
        }

        if keys.is_empty() {
            bot.delete_message(ChatId(chat_id), teloxide::types::MessageId(del_id)).await.ok();
        }

        storage.delete_attachments_for_bulletin(del_id).await?;
        storage.delete_bulletin(del_id).await?;
        bot.delete_message(ChatId(chat_id), msg.id).await?;
        return Ok(());
    }

    let entities = msg.parse_entities()
        .or_else(|| msg.parse_caption_entities())
        .unwrap_or_default();
    let content = entities_to_html(text.unwrap_or(""), &entities);

    if let Some(photos) = msg.photo() {
        if !content.is_empty() {
            storage.upsert_bulletin(target_id, msg.date.timestamp() as u32, &content).await?;
        }

        let msg_id = msg.id.0;

        let largest = photos.last().unwrap();
        let file = bot.get_file(&largest.file.id).await?;
        let download_url = format!("https://api.telegram.org/file/bot{}/{}", conf.token, file.path);
        let bytes = reqwest::get(&download_url).await?.bytes().await?;

        let s3_key = format!("bulletins/{}/{}_{}.jpg", chat_id, target_id, msg_id);
        s3_client.objects().put(&conf.s3_bucket, &s3_key).body_bytes(bytes.to_vec()).content_type("image/jpeg").send().await?;

        let path = format!("/photo/{}/{}", target_id, msg_id);
        storage.insert_photo(target_id, &path, msg_id).await?;
    } else if let Some(doc) = msg.document() {
        let file_target = match msg.reply_to_message() {
            Some(reply) => {
                if let Some(mg) = reply.media_group_id() {
                    group_id(mg)
                } else {
                    reply.id.0
                }
            }
            None => target_id,
        };

        if reply_target.is_none() && !content.is_empty() {
            storage.upsert_bulletin(file_target, msg.date.timestamp() as u32, &content).await?;
        }

        let msg_id = msg.id.0;

        let file = bot.get_file(&doc.file.id).await?;
        let download_url = format!("https://api.telegram.org/file/bot{}/{}", conf.token, file.path);
        let bytes = reqwest::get(&download_url).await?.bytes().await?;

        let file_name = doc.file_name.as_deref().unwrap_or("file");
        let mime = doc.mime_type.as_ref().map(|m| m.to_string()).unwrap_or_else(|| "application/octet-stream".to_string());
        let ext = FilePath::new(file_name).extension().and_then(|e| e.to_str()).unwrap_or("bin");

        let s3_key = format!("bulletins/{}/{}_{}.{}", chat_id, file_target, msg_id, ext);
        s3_client.objects().put(&conf.s3_bucket, &s3_key).body_bytes(bytes.to_vec()).content_type(&mime).send().await?;

        let path = format!("/file/{}/{}.{}", file_target, msg_id, ext);
        storage.insert_file(file_target, &path, msg_id, file_name, &mime).await?;
    } else if !content.is_empty() {
        storage.upsert_bulletin(target_id, msg.date.timestamp() as u32, &content).await?;
    }

    Ok(())
}

#[derive(Serialize, Deserialize)]
struct Bulletin {
    ts: u32,
    text: String,
    photos: Vec<String>,
    files: Vec<FileAttachment>,
}

#[derive(Serialize, Deserialize)]
struct FileAttachment {
    url: String,
    name: String,
    mime: String,
}

#[derive(Serialize, Deserialize)]
struct Data {
    bulletins: Vec<Bulletin>
}

#[derive(Clone)]
struct AppState {
    storage: Arc<dyn Storage>,
    s3_client: Arc<Client>,
    conf: Arc<Config>,
}

#[derive(Parser, Debug)]
struct Config {
    ///token of telegram bot
    #[arg(long, short, env="TELEGRAM_BOT_TOKEN", hide_env_values=true)]
    token: String,
    ///comma separated ids of users who can post bulletings
    #[arg(long, short, env="CHANNEL")]
    channel: i64,
    ///database address
    #[arg(long, env="DB_URL")]
    db_url: String,
    #[arg(long, short, default_value="127.0.0.1:3000")]
    listen: String,
    ///rate limit for barrier
    #[arg(long="brl", default_value="25")]
    barrier_rate_limit: u64,
    #[arg(long, env="BUTTON_POST_URL")]
    button_post_url: String,
    #[arg(long, env="S3_ENDPOINT")]
    s3_endpoint: String,
    #[arg(long, env="S3_BUCKET")]
    s3_bucket: String,
    #[arg(long, env="S3_ACCESS_KEY")]
    s3_access_key: String,
    #[arg(long, env="S3_SECRET_KEY")]
    s3_secret_key: String,
    #[arg(long, env="S3_REGION", default_value="us-east-1")]
    s3_region: String,
}

async fn create_server(state: AppState) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(&state.conf.listen).await?;
    let app = Router::new()
        .route("/bulletins", axum::routing::get(get_bulletins))
        .route("/photo/:bulletin_id/:msg_id", axum::routing::get(get_photo))
        .route("/file/:bulletin_id/:key", axum::routing::get(get_file))
        .with_state(state);
    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            log::error!("Err on axum-serve: {:?}", e);
            exit(-1);
        };
    });
    Ok(())
}

async fn get_bulletins(State(app): State<AppState>, axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>) -> Result<Json<Data>, AppError> {
    let offset = params.get("offset").and_then(|v| v.parse().ok()).unwrap_or(0u32);
    let rows = app.storage.get_bulletins(offset).await.map_err(ae)?;

    let bulletins: Vec<Bulletin> = rows.into_iter().map(|row| {
        let files = row.files.into_iter().map(|f| FileAttachment { url: f.url, name: f.file_name, mime: f.mime_type }).collect();
        Bulletin { ts: row.ts, text: row.content, photos: row.photos, files }
    }).collect();

    Ok(Json(Data { bulletins }))
}

async fn get_photo(
    State(app): State<AppState>,
    Path((id, msg_id)): Path<(i32, i32)>,
) -> Result<Response, AppError> {
    let key = format!("bulletins/{}/{}_{}.jpg", app.conf.channel, id, msg_id);
    let obj = app.s3_client.objects().get(&app.conf.s3_bucket, &key).send().await?;
    let mut buf = Vec::new();
    let mut stream = obj.body;
    while let Some(chunk) = stream.next().await {
        buf.extend_from_slice(&chunk?);
    }
    let response = Response::builder()
        .header("Cache-Control", "public, max-age=86400")
        .header("Content-Type", "image/jpeg")
        .body(axum::body::Body::from(buf))
        .unwrap();
    Ok(response)
}

async fn get_file(
    State(app): State<AppState>,
    Path((id, key)): Path<(i32, String)>,
) -> Result<Response, AppError> {
    let (msg_id_str, ext) = key.split_once('.').unwrap_or((&key, "bin"));
    let msg_id: i32 = msg_id_str.parse()?;
    let s3_key = format!("bulletins/{}/{}_{}.{}", app.conf.channel, id, msg_id, ext);

    let obj = app.s3_client.objects().get(&app.conf.s3_bucket, &s3_key).send().await?;
    let mut buf = Vec::new();
    let mut stream = obj.body;
    while let Some(chunk) = stream.next().await {
        buf.extend_from_slice(&chunk?);
    }

    let response = Response::builder()
        .header("Cache-Control", "public, max-age=86400")
        .header("Content-Type", "application/octet-stream")
        .header("Content-Disposition", format!("inline; filename=\"{}.{}\"", msg_id, ext))
        .body(axum::body::Body::from(buf))
        .unwrap();
    Ok(response)
}

struct AppError(Box<dyn std::error::Error + Send + Sync>);
impl<E> From<E> for AppError where E: std::error::Error + Send + Sync + 'static {
    fn from(value: E) -> Self {
        Self(Box::new(value))
    }
}

fn ae(e: anyhow::Error) -> AppError {
    AppError(e.into())
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        let message = format!("{}", self.0);
        (StatusCode::INTERNAL_SERVER_ERROR, message).into_response()
    }
}


struct Services {
    tres_period: Duration,
    post_url: Url,
    shlag: Mutex<Instant>,
}



impl Services {
    fn new(tres_period: Duration, post_url: Url) -> Self{
        Self { shlag: Mutex::new(Instant::now()), tres_period , post_url}
    }
    async fn openshlag(&self) -> anyhow::Result<bool> {
        let mut opened = self.shlag.lock().await;
        if opened.elapsed() < self.tres_period {
            return Ok(false);
        }
        let client = reqwest::Client::new();
        client.post(self.post_url.clone()).send().await?;
        *opened = Instant::now();
        Ok(true)
    }
}
