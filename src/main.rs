use std::collections::VecDeque;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::PathBuf;
use std::process::{exit, ExitCode};
use std::sync::{Arc};
use std::time::Duration;
use std::collections::HashMap;

use anyhow::bail;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::{Json, Router};
use clap::Parser;
use reqwest::Url;
use serde::{Deserialize, Serialize};
use teloxide::dispatching::{Dispatcher, UpdateFilterExt};
use teloxide::payloads::{AnswerCallbackQuerySetters, SendMessageSetters};
use teloxide::requests::Requester;
use teloxide::types::{CallbackQuery, ChatId, InlineKeyboardButton, InlineKeyboardButtonKind, InlineKeyboardMarkup, Update};
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
    
    create_server(&conf, conn.clone()).await?;
    let conf = Arc::new(conf);
    let bot = Bot::new(conf.token.clone());
    bot.send_message(ChatId(conf.channel), "Управление").reply_markup(InlineKeyboardMarkup::new(vec![vec![
        InlineKeyboardButton::callback("Открыть шлагбаум", "OPENSHLAG"),
    ]])).await?;

    let handler = dptree::entry()
        .branch(Update::filter_callback_query().endpoint(process_callback))
        .endpoint(process_update);

    Dispatcher::builder(bot.clone(), handler)
        .dependencies(dptree::deps![conf, conn])
        .enable_ctrlc_handler()
        .build()
        .dispatch().await;
    Ok(())
}

async fn process_callback(
    bot: Bot, 
    query: CallbackQuery,
    conf: Arc<Config>,
) -> anyhow::Result<()> {
    let user = query.from;
    let member = bot.get_chat_member(ChatId(conf.channel), user.id).await?;
    if member.is_present() {
        match query.data.as_ref().map(|o|o.as_str()) {
            Some("OPENSHLAG") => {
                openshlag().await?;
                bot.answer_callback_query(query.id).text("Открыто").await?;
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
        upd: teloxide::types::Update,
        conf: Arc<Config>, 
        conn: Arc<Mutex<YdbConnection>>,
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

    let query = if let Some(text) = msg.text() {
        let query = if text == "del" {
            query("
                declare $id as Int32;
                delete from bulletins where id=$id;
            ").bind(("$author", chat_id))
            .bind(("$id", msg.id.0))
        } else { 
            query("
                declare $id as Int32; 
                declare $ts as Uint32; 
                declare $content as Utf8;
                upsert into bulletins (id, ts, content) values ($id, cast($ts as Datetime), $content);
                ")
            .bind(("$id", msg.id.0))
            .bind(("$ts", msg.date.timestamp() as u32))
            .bind(("$content", text.to_owned()))
        };
        Some(query)
    } else {
        None
    };
    if let Some(query) = query {
        let mut conn = conn.lock().await;
        let executor = match conn.executor() {
            Ok(e) => e,
            Err(YdbError::NoSession) => {
                log::warn!("received no session error, reconnecting...");
                conn.reconnect().await?;
                conn.executor()?
            },
            Err(e) => Err(e)?
        }.retry();
        executor.execute(query).await?;
    }
    if msg.text() == Some("del") {
        bot.delete_message(teloxide::types::ChatId(chat_id), msg.id).await?;
    }
    Ok(())
}

#[derive(Serialize, Deserialize)]
struct Bulletin {
    ts: u32,
    text: String
}

#[derive(Serialize, Deserialize)]
struct Data {
    bulletins: Vec<Bulletin>
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
}

async fn create_server(conf: &Config, conn: Arc<Mutex<YdbConnection>>) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(&conf.listen).await?;
    let app = Router::new().route("/bulletins", axum::routing::get(get_bulletins)).with_state(conn);
    let handle = tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            log::error!("Err on axum-serve: {:?}", e);
            exit(-1);
        };
    });
    Ok(())
}

async fn get_bulletins(State(conn): State<Arc<Mutex<YdbConnection>>>, axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>) -> Result<Json<Data>, AppError> {
    let offset = params.get("offset").map(|v|v.parse().ok()).flatten().unwrap_or(0u32);
    let bulletins = query_as::<_, (Datetime, String)>("
    declare $offset as Uint32;
    select ts, content from bulletins order by ts desc limit 10 offset $offset;
    ").bind(("$offset", offset))
        .fetch_all(conn.lock().await.executor()?).await?
        .into_iter().map(|(ts, text)|Bulletin{ts: ts.into(), text})
        .collect();
    let data = Data {bulletins};
    Ok(Json(data))
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

async fn openshlag() -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    client.post(Url::from_str("http://192.168.31.7:11181/activate/pull")?).send().await?;
    Ok(())
}