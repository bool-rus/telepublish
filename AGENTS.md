# Goals

- [ ] Раздел Документов на сайте — фильтр объявлений по наличию файлов
  - Бэкенд: `GET /api/bulletins?with_files=true`
  - Фронтенд: один HTML с двумя режимами (по pathname)
  - Nginx: отдача `/documents/` через тот же index.html
- [ ] Продумать архитектуру наполнения всего сайта через бота

# In Progress

- [ ] Message entities — HTML-форматирование текста в БД (bold, italic, links и т.д.)

# Blocked

- (none)

# Completed

- Создан AGENTS.md
- `master` обновлён до `origin/master` (fast-forward)
- Дизайн-эксперимент с HTML — откатили (не понравился)
- `cargo clean` — 6.3GB freed
- HTML viewer: добавлена шапка + navbar с меню как на снт-подмосковье.рф
- `/api/` prefix для nginx прокси
- JOIN-запрос для get_bulletins
- Изменения в схеме: `sort_order` → `msg_id`
- Миграции разделены на ydb/sqlite
- Поддержка альбомов, фото, файлов, удаление с Telegram
- Case-insensitive `del`
- SQLite storage через sqlx
- Интеграционные тесты (full_flow, all_scenarios)
- Демо-скрипты setup/cleanup/start/demo.py

# Key Decisions

- **Message entities в HTML** — `parse_entities()` / `parse_caption_entities()` конвертируются в HTML при записи в БД
- **Документы** = те же bulletins, отфильтрованные по `with_files=true`
- **Один HTML, два режима** — JS смотрит `location.pathname` для bulletins vs documents
- **Фото в документах не показывать** — только файлы
- **Схема БД не меняется** — новая колонка/таблица не нужна
- **Файл для планов** — `AGENTS.md`
