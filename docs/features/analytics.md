# Аналитика

Статистика каналов, контента и трендов.

## Команды

=== "CLI"
    ```bash
    python -m src.main analytics summary            # сводка генерации контента
    python -m src.main analytics top                # топ сообщений по реакциям
    python -m src.main analytics content-types      # вовлечённость по типу контента
    python -m src.main analytics hourly             # активность по часам
    python -m src.main analytics daily              # ежедневная статистика генерации
    python -m src.main analytics pipeline-stats     # статистика по пайплайнам
    python -m src.main analytics trending-topics    # трендовые темы/ключевые слова
    python -m src.main analytics trending-channels  # топ каналов по активности
    python -m src.main analytics velocity           # сообщений в день
    python -m src.main analytics peak-hours         # пиковые часы активности
    python -m src.main analytics calendar           # расписание публикаций
    python -m src.main analytics export             # экспорт данных
    ```

=== "Web"
    `GET /analytics/` — главная страница аналитики
    `GET /analytics/content` — контент-аналитика
    `GET /analytics/trends` — тренды

## Agent Tools

| Tool | Описание |
|------|----------|
| `get_analytics_summary` | Общая сводка |
| `get_pipeline_stats` | Статистика пайплайнов |
| `get_daily_stats` | Ежедневная статистика |
| `get_trending_topics` | Трендовые темы |
| `get_trending_channels` | Топ каналов |
| `get_message_velocity` | Скорость потока сообщений |
| `get_peak_hours` | Пиковые часы |
| `get_calendar` | Календарь публикаций |
