# Аналитика

Статистика каналов, контента и трендов.

## Команды

```bash
python -m tg_agent.main analytics summary            # сводка генерации контента
python -m tg_agent.main analytics top                # топ сообщений по реакциям
python -m tg_agent.main analytics content-types      # вовлечённость по типу контента
python -m tg_agent.main analytics hourly             # активность по часам
python -m tg_agent.main analytics daily              # ежедневная статистика генерации
python -m tg_agent.main analytics pipeline-stats     # статистика по пайплайнам
python -m tg_agent.main analytics trending-topics    # трендовые темы/ключевые слова
python -m tg_agent.main analytics trending-channels  # топ каналов по активности
python -m tg_agent.main analytics velocity           # сообщений в день
python -m tg_agent.main analytics peak-hours         # пиковые часы активности
python -m tg_agent.main analytics calendar           # расписание публикаций
python -m tg_agent.main analytics export             # экспорт данных
```

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
