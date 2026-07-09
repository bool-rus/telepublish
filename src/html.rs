use teloxide::types::MessageEntityKind;

pub fn entities_to_html(text: &str, entities: &[teloxide::types::MessageEntityRef]) -> String {
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

    let mut out = String::new();
    let mut text_pos = 0;
    let mut event_idx = 0;
    let mut open_tags: Vec<&MessageEntityKind> = Vec::new();

    while event_idx < events.len() {
        let ev_pos = events[event_idx].pos;

        if text_pos < ev_pos {
            out.push_str(&html_escape(&text[text_pos..ev_pos]));
            text_pos = ev_pos;
        }

        while event_idx < events.len() && events[event_idx].pos == ev_pos {
            let ev = &events[event_idx];
            match ev.kind {
                EventKind::Close => {
                    if let Some(idx) = open_tags.iter().rposition(|t| std::ptr::eq(*t, ev.kind_ref)) {
                        for tag in open_tags.drain(idx..).rev() {
                            write_tag_close(&mut out, tag);
                        }
                    }
                }
                EventKind::Open => {
                    write_tag_open(&mut out, ev.kind_ref, &text[ev.entity_start..ev.entity_end]);
                    open_tags.push(ev.kind_ref);
                }
            }
            event_idx += 1;
        }
    }

    if text_pos < text.len() {
        out.push_str(&html_escape(&text[text_pos..]));
    }

    for tag in open_tags.drain(..).rev() {
        write_tag_close(&mut out, tag);
    }

    out
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
