use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
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
use teloxide::types::{CallbackQuery, ChatId, InlineKeyboardButton, InlineKeyboardMarkup, Update};
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
    let is_del = text == Some("del");
    let target_id = if let Some(mg) = msg.media_group_id() {
        group_id(&mg)
    } else {
        msg.id.0
    };

    if is_del {
        let photos = storage.get_bulletin_photo_keys(target_id).await?;

        for (bid, sort) in &photos {
            let s3_key = format!("bulletins/{}/{}_{}.jpg", conf.channel, bid, sort);
            if let Err(e) = s3_client.objects().delete(&conf.s3_bucket, &s3_key).send().await {
                log::warn!("failed to delete s3 object {}: {:?}", s3_key, e);
            }
        }

        storage.delete_photos_for_bulletin(target_id).await?;
        storage.delete_bulletin(target_id).await?;
        bot.delete_message(ChatId(chat_id), msg.id).await?;
        return Ok(());
    }

    let content = text.unwrap_or("");

    if let Some(photos) = msg.photo() {
        if !content.is_empty() {
            storage.upsert_bulletin(target_id, msg.date.timestamp() as u32, content).await?;
        }

        let sort = storage.get_photo_paths(target_id).await?.len() as i32;

        let largest = photos.last().unwrap();
        let file = bot.get_file(&largest.file.id).await?;
        let download_url = format!("https://api.telegram.org/file/bot{}/{}", conf.token, file.path);
        let bytes = reqwest::get(&download_url).await?.bytes().await?;

        let s3_key = format!("bulletins/{}/{}_{}.jpg", chat_id, target_id, sort);
        s3_client.objects().put(&conf.s3_bucket, &s3_key).body_bytes(bytes.to_vec()).content_type("image/jpeg").send().await?;

        let path = format!("/photo/{}/{}", target_id, sort);
        storage.insert_photo(target_id, &path, sort).await?;
    } else if !content.is_empty() {
        storage.upsert_bulletin(target_id, msg.date.timestamp() as u32, content).await?;
    }

    Ok(())
}

#[derive(Serialize, Deserialize)]
struct Bulletin {
    ts: u32,
    text: String,
    photos: Vec<String>,
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
        .route("/photo/:bulletin_id/:sort", axum::routing::get(get_photo))
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

    let mut bulletins: Vec<Bulletin> = Vec::new();
    for row in rows {
        let photos = app.storage.get_photo_paths(row.id).await.map_err(ae)?;
        bulletins.push(Bulletin { ts: row.ts, text: row.content, photos });
    }

    Ok(Json(Data { bulletins }))
}

async fn get_photo(
    State(app): State<AppState>,
    Path((id, sort)): Path<(i32, i32)>,
) -> Result<Response, AppError> {
    let key = format!("bulletins/{}/{}_{}.jpg", app.conf.channel, id, sort);
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
