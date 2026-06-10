# ai_detect_tool — анализ Telegram-каналов (standalone)

Самодостаточный инструмент анализа каналов. **Ждёт интеграции в основной проект** (слой `analytics`) —
см. [#781](https://github.com/axisrow/tg_content_factory/issues/781). До интеграции работает отдельными
скриптами.

- Читает основную БД **только на чтение** (`file:data/tg_search.db?mode=ro`), её не модифицирует.
- Не зависит от `src/`. Результаты (CSV/JSON/`ai_detection.db`) пишутся рядом и **не версионируются**
  (см. `.gitignore`).

## Что умеет

| Файл | Назначение |
|---|---|
| `channel_features.py` | Фичи уровня канала: brand_gap, флаги (random_username, listy, formatted), эмодзи-метрика. Без CLI. |
| `channel_survey.py` | Разведка → CSV-рейтинг. Режимы: `--all-langs`, `--eval-filter` (аудит фильтра проекта), `--calibrate`. |
| `channel_eval.py` | Рейтинг каналов LLM-судьёй по двум осям: полезность (`useful`/`useless`) × жанр (`ad`/`infobiz`/`aggregator`/`copy`/`original`). Команды: `prepare-all`, `label`, `llm`, `export`, `report`, `remap`. |
| `post_dedup.py` | Поиск каналов-клонов по хэшам постов (спам-фермы). Автономен. |
| `ai_detect.py` | Базовый per-message детектор (HeuristicAnalyzer, LlmJudge). Корневой модуль. |
| `human_eval.py` | Слепая ручная разметка сообщений; `_is_russian` (кириллическая защита). |
| `ai_detect_eval.py`, `ai_detect_audit.py` | Оценка качества / аудит эвристики. |
| `eval/` | Ground-truth сэмплы (AI / человек) для калибровки. |

## Типичный прогон рейтинга

```bash
python ai_detect_tool/channel_eval.py prepare-all                                  # набрать каналы в сессию
# судья (LLM) — через слепых субагентов; локальная команда llm — для Ollama
python ai_detect_tool/channel_eval.py export --out ai_detect_tool/channel_rating.csv
```

Итоговый рейтинг — `channel_rating.csv` (useless сверху, кандидаты на отписку).
