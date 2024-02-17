use std::collections::VecDeque;
use std::fs::File;
use std::io::{BufReader, BufWriter};

use clap::Parser;
use serde::{Deserialize, Serialize};
use teloxide::Bot;
use teloxide::types::Message;

#[ctor::ctor]
static CONF: Config = Config::parse();

#[tokio::main]
async fn main() {

    let bot = Bot::new(CONF.token.clone());


    teloxide::repl(bot, |msg: Message| async move {
        let from_id = msg.from().map(|u|u.id.0).unwrap_or_default();
        if !CONF.admins.contains(&from_id) {
            return Ok(())
        }
        if let Some(text) = msg.text() {
            let b = Bulletin {
                id: (msg.id.0 as u64) * from_id,
                ts: msg.date.timestamp(),
                important: false, 
                text: text.to_owned(),
            };
            let file = File::open("data.json")?;
            let reader = BufReader::new(file);
            let mut data: Data = serde_json::from_reader(reader).unwrap();
            data.bulletins.push_front(b);
            let file = File::create("data.json")?;
            let writer = BufWriter::new(file);
            serde_json::to_writer(writer, &data).unwrap();
        }
        Ok(())
    }).await;
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