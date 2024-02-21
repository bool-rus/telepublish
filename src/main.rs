use std::collections::VecDeque;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::PathBuf;
use std::sync::{Arc};

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

    let conf = Arc::new(conf);
    let data = load_data()?;
    let data = Arc::new(Mutex::new(data));
    let bot = Bot::new(conf.token.clone());

    let handler = dptree::entry()
        .endpoint(process_update);
    Dispatcher::builder(bot.clone(), handler)
        .dependencies(dptree::deps![conf, data, conn])
        .enable_ctrlc_handler()
        .build()
        .dispatch().await;
    Ok(())
}

fn load_data() -> anyhow::Result<Data> {
    let file = File::open("data.json")?;
    let reader = BufReader::new(file);
    serde_json::from_reader(reader).map_err(Into::into)
}

fn store_data(data: &Data) -> anyhow::Result<()> {
    let file = File::create("data.json")?;
    let writer = BufWriter::new(file);
    serde_json::to_writer(writer, data).map_err(Into::into)
}


async fn process_update(
        upd: teloxide::types::Update,
        conf: Arc<Config>, 
        data: Arc<Mutex<Data>>, 
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
        let id = (msg.id.0 as u64) * from_id;
        let mut data = data.lock().await;
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
        if edit {
            if text == "del" {
                data.bulletins.retain(|b|b.id != id);
            } else {
                if let Some(b) = data.bulletins.iter_mut().find(|b|b.id == id) {
                    b.text = text.to_owned();
                }
            }
        } else {
            data.bulletins.push_front(Bulletin {
                id: (msg.id.0 as u64) * from_id,
                ts: msg.date.timestamp(),
                important: false, 
                text: text.to_owned(),
            });
        }
        store_data(&data)?;
        Some(query)
    } else {
        None
    };
    if let Some(query) = query {
        println!("trying to store message to database");
        let mut conn = conn.lock().await;
        println!("lock received, execute...");
        let executor = conn.executor()?;

        executor.execute(query).await?;
        println!("msg stored to db");
    }
    Ok(())
}

#[derive(Serialize, Deserialize)]
struct Bulletin {
    ts: i64,
    id: u64,
    important: bool,
    text: String
}

#[derive(Serialize, Deserialize)]
struct Data {
    bulletins: VecDeque<Bulletin>
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
}