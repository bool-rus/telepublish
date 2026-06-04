use std::collections::HashMap;
use std::process::exit;
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
use ydb_unofficial::sqlx::prelude::*;

async fn migrate(conn: &mut YdbConnection) -> anyhow::Result<()> {
    let migrator = sqlx_macros::migrate!("./migrations");
    migrator.run_direct(conn).await?;
    Ok(())
}

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
    
    let mut conn = YdbConnectOptions::from_str(&conf.db_url)?.connect().await?;
    migrate(&mut conn).await?;
    let conn = Arc::new(Mutex::new(conn));
    
    let post_url = Url::from_str(&conf.button_post_url)?;
    let services = Arc::new(Services::new(Duration::from_secs(conf.barrier_rate_limit), post_url));

    let s3_client = Arc::new(Client::builder(&conf.s3_endpoint)?
        .region(&conf.s3_region)
        .auth(Auth::Static(Credentials::new(&conf.s3_access_key, &conf.s3_secret_key)?))
        .addressing_style(AddressingStyle::Path)
        .build()?);

    let conf = Arc::new(conf);
    let app_state = AppState { conn: conn.clone(), s3_client: s3_client.clone(), conf: conf.clone() };
    create_server(app_state).await?;
    let bot = Bot::new(conf.token.clone());
    bot.send_message(ChatId(conf.channel), "Управление").reply_markup(InlineKeyboardMarkup::new(vec![vec![
        InlineKeyboardButton::callback("Открыть шлагбаум", "OPENSHLAG"),
    ]])).await?;

    let handler = dptree::entry()
        .branch(Update::filter_callback_query().endpoint(process_callback))
        .endpoint(process_update);

    Dispatcher::builder(bot.clone(), handler)
        .dependencies(dptree::deps![conf, conn, services, s3_client])
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

async fn process_update(
    bot: Bot,
    upd: Update,
    conf: Arc<Config>,
    conn: Arc<Mutex<YdbConnection>>,
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

    let mut conn = conn.lock().await;

    if is_del {
        let photos: Vec<(i32, i32)> = {
            let e = match conn.executor() {
                Ok(e) => e,
                Err(YdbError::NoSession) => {
                    log::warn!("received no session error, reconnecting...");
                    conn.reconnect().await?;
                    conn.executor()?
                },
                Err(e) => Err(e)?
            }.retry();
            query_as::<_, (i32, i32)>(
                "declare $id as Int32; select bulletin_id, sort_order from bulletin_photos where bulletin_id = $id;"
            ).bind(("$id", msg.id.0))
                .fetch_all(e).await?
        };

        for (bid, sort) in &photos {
            let s3_key = format!("bulletins/{}/{}_{}.jpg", conf.channel, bid, sort);
            if let Err(e) = s3_client.objects().delete(&conf.s3_bucket, &s3_key).send().await {
                log::warn!("failed to delete s3 object {}: {:?}", s3_key, e);
            }
        }

        {
            let e = match conn.executor() {
                Ok(e) => e,
                Err(YdbError::NoSession) => {
                    log::warn!("received no session error, reconnecting...");
                    conn.reconnect().await?;
                    conn.executor()?
                },
                Err(e) => Err(e)?
            }.retry();
            e.execute(
                query("declare $id as Int32; delete from bulletin_photos where bulletin_id = $id;")
                    .bind(("$id", msg.id.0))
            ).await?;
        }

        {
            let e = match conn.executor() {
                Ok(e) => e,
                Err(YdbError::NoSession) => {
                    log::warn!("received no session error, reconnecting...");
                    conn.reconnect().await?;
                    conn.executor()?
                },
                Err(e) => Err(e)?
            }.retry();
            e.execute(
                query("declare $id as Int32; delete from bulletins where id = $id;")
                    .bind(("$id", msg.id.0))
            ).await?;
        }

        bot.delete_message(ChatId(chat_id), msg.id).await?;
        return Ok(());
    }

    let content = text.unwrap_or("");

    let photo_urls: Vec<String> = if let Some(photos) = msg.photo() {
        let largest = photos.last().unwrap();
        let file = bot.get_file(&largest.file.id).await?;
        let download_url = format!("https://api.telegram.org/file/bot{}/{}", conf.token, file.path);
        let bytes = reqwest::get(&download_url).await?.bytes().await?;

        let s3_key = format!("bulletins/{}/{}_{}.jpg", chat_id, msg.id.0, 0);
        s3_client.objects().put(&conf.s3_bucket, &s3_key).body_bytes(bytes.to_vec()).content_type("image/jpeg").send().await?;

        let path = format!("/photo/{}/0", msg.id.0);

        {
            let e = match conn.executor() {
                Ok(e) => e,
                Err(YdbError::NoSession) => {
                    log::warn!("received no session error, reconnecting...");
                    conn.reconnect().await?;
                    conn.executor()?
                },
                Err(e) => Err(e)?
            }.retry();
            e.execute(
                query("declare $bid as Int32; declare $url as Utf8; upsert into bulletin_photos (bulletin_id, url, sort_order) values ($bid, $url, 0);")
                    .bind(("$bid", msg.id.0))
                    .bind(("$url", path.clone()))
            ).await?;
        }

        vec![path]
    } else {
        vec![]
    };

    if !photo_urls.is_empty() || !content.is_empty() {
        let e = match conn.executor() {
            Ok(e) => e,
            Err(YdbError::NoSession) => {
                log::warn!("received no session error, reconnecting...");
                conn.reconnect().await?;
                conn.executor()?
            },
            Err(e) => Err(e)?
        }.retry();
        e.execute(
            query("declare $id as Int32; declare $ts as Uint32; declare $content as Utf8; upsert into bulletins (id, ts, content) values ($id, cast($ts as Datetime), $content);")
                .bind(("$id", msg.id.0))
                .bind(("$ts", msg.date.timestamp() as u32))
                .bind(("$content", content.to_owned()))
        ).await?;
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
    conn: Arc<Mutex<YdbConnection>>,
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
        .route("/photo/{bulletin_id}/{sort}", axum::routing::get(get_photo))
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
    let mut conn = app.conn.lock().await;

    let rows = query_as::<_, (i32, Datetime, String)>("
        declare $offset as Uint32;
        select id, ts, content from bulletins order by ts desc limit 10 offset $offset;
    ").bind(("$offset", offset))
        .fetch_all(conn.executor()?).await?;

    let mut bulletins: Vec<Bulletin> = Vec::new();
    for (id, ts, text) in rows {
        let photos: Vec<String> = query_as::<_, (String,)>("
            declare $bid as Int32;
            select url from bulletin_photos where bulletin_id = $bid order by sort_order;
        ").bind(("$bid", id))
            .fetch_all(conn.executor()?).await?
            .into_iter().map(|(u,)| u).collect();

        bulletins.push(Bulletin { ts: ts.into(), text, photos });
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

struct AppError(Box<dyn std::error::Error>);
impl<E> From<E> for AppError where E: std::error::Error + 'static {
    fn from(value: E) -> Self {
        Self(Box::new(value))
    }
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
