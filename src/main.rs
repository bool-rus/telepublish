use std::collections::VecDeque;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::sync::{Arc, Mutex};

use clap::Parser;
use serde::{Deserialize, Serialize};
use teloxide::dispatching::Dispatcher;
use teloxide::{dptree, Bot};



#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let conf = Config::parse();
    let conf = Arc::new(conf);
    let data = load_data()?;
    let data = Arc::new(Mutex::new(data));
    let bot = Bot::new(conf.token.clone());

    let handler = dptree::entry()
        .endpoint(process_update);
    Dispatcher::builder(bot.clone(), handler)
        .dependencies(dptree::deps![conf, data])
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
        conf: Arc<Config>, 
        data: Arc<Mutex<Data>>, 
        upd: teloxide::types::Update
    ) -> anyhow::Result<()> {
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
    if let Some(text) = msg.text() {
        let id = (msg.id.0 as u64) * from_id;
        let mut data = data.lock().unwrap();
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
}