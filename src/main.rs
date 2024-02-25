use std::collections::VecDeque;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::PathBuf;
use std::process::{exit, ExitCode};
use std::sync::{Arc};
use std::time::Duration;

use anyhow::bail;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::{Json, Router};
use clap::Parser;
use serde::{Deserialize, Serialize};
use teloxide::dispatching::Dispatcher;
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
    tokio::time::sleep(Duration::from_secs(3600)).await;
    let conf = Arc::new(conf);
    let bot = Bot::new(conf.token.clone());

    let handler = dptree::entry()
        .endpoint(process_update);
    Dispatcher::builder(bot.clone(), handler)
        .dependencies(dptree::deps![conf, conn])
        .enable_ctrlc_handler()
        .build()
        .dispatch().await;
    Ok(())
}


async fn process_update(
        upd: teloxide::types::Update,
        conf: Arc<Config>, 
        conn: Arc<Mutex<YdbConnection>>,
    ) -> anyhow::Result<()> {
        println!("received msg");
    use teloxide::types::UpdateKind::*;
    let (msg, edit) = match upd.kind {
        Message(msg) | ChannelPost(msg) => (msg, false),
        EditedMessage(msg) | EditedChannelPost(msg) => (msg, true),
        _ => return Ok(())
    };
    let from_id = msg.from().map(|u|u.id.0).unwrap_or_default();
    if !conf.admins.contains(&from_id) {
        return Ok(())
    }
    println!("msg from admin");

    let query = if let Some(text) = msg.text() {
        println!("msg with text: {text}");
        let query = query("
        declare $author as Uint64; 
        declare $id as Int32; 
        declare $ts as Uint32; 
        declare $content as Utf8;
        upsert into bulletins (author, id, ts, content) values ($author, $id, cast($ts as Datetime), $content);
        ")
            .bind(("$author", from_id))
            .bind(("$id", msg.id.0))
            .bind(("$ts", msg.date.timestamp() as u32))
            .bind(("$content", text.to_owned()));
        Some(query)
    } else {
        None
    };
    if let Some(query) = query {
        println!("trying to store message to database");
        let mut conn = conn.lock().await;
        println!("lock received, execute...");
        let executor = match conn.executor() {
            Ok(e) => e,
            Err(YdbError::NoSession) => {
                conn.reconnect().await?;
                conn.executor()?
            },
            Err(e) => Err(e)?
        }.retry();

        executor.execute(query).await?;
        println!("msg stored to db");
    }
    Ok(())
}

#[derive(Serialize, Deserialize)]
struct Bulletin {
    ts: i64,
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
    #[arg(long, short, env="BOT_ADMINS", num_args=1.., value_delimiter=',')]
    admins: Vec<u64>,
    ///database address
    #[arg(long, env="DB_URL")]
    db_url: String,
    #[arg(long, env="SA_KEY", default_value="none")]
    sa_key: PathBuf,
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

async fn get_bulletins(State(conn): State<Arc<Mutex<YdbConnection>>>) -> Result<Json<Data>, AppError> {
    let bulletins = query_as::<_, (Datetime, String)>("select ts, content from bulletins order by ts desc;")
        .fetch_all(conn.lock().await.executor()?).await?
        .into_iter().map(|(ts, text)|{
            let ts: u32 = ts.into();
            Bulletin{ts: ts as i64, text}
        }).collect();
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
