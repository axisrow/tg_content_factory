# Фильтры каналов

`ChannelAnalyzer` оценивает каналы по нескольким критериям и помечает нерелевантные.

## Критерии фильтрации

| Критерий | Описание |
|---------|----------|
| `low_uniqueness` | Канал публикует много неуникального контента |
| `low_subscriber_ratio` | Мало подписчиков относительно просмотров |
| `cross_channel_spam` | Кросс-постинг из других каналов |
| `non_cyrillic` | Контент не на кириллице |
| `chat_noise` | Шумный чат (низкое соотношение сигнал/шум) |

## Анализ и применение

=== "CLI"
    ```bash
    python -m src.main filter analyze       # анализ без изменений
    python -m src.main filter apply         # применить фильтры
    python -m src.main filter reset         # сбросить все фильтры
    python -m src.main filter precheck      # pre-фильтр по кол-ву подписчиков
    python -m src.main filter toggle --channel-id ID  # ручное переключение
    python -m src.main filter purge         # удалить сообщения отфильтрованных
    python -m src.main filter hard-delete   # удалить каналы из БД
    ```

=== "Web"
    `GET /channels/filter/manage` · `POST /channels/filter/analyze`
    `POST /channels/filter/apply` · `POST /channels/filter/reset`

## Поведение при сборе

Отфильтрованные каналы (`is_filtered = true`) пропускаются при `collect`, если не указан `force=True`.
