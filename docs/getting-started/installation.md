# Установка

## Требования

- Python 3.11+
- Telegram API ключи ([получить на my.telegram.org](https://my.telegram.org/apps))
- SQLite (встроен в Python)

## Установка

```bash
pip install tg-agent
```

Или из исходников:

```bash
git clone https://github.com/axisrow/tg_content_factory.git
cd tg_content_factory
pip install -e ".[dev]"
```

## Опциональные зависимости

```bash
# Семантический поиск (numpy-based KNN)
pip install numpy

# Провайдеры LLM (для контент-пайплайнов)
pip install openai cohere

# Документация
pip install -e ".[docs]"
```
