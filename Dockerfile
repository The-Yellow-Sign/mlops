FROM python:3.13-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    UV_PROJECT_ENVIRONMENT="/usr/local" \
    # Включаем компиляцию байткода при установке пакетов (ускоряет старт)
    UV_COMPILE_BYTECODE=1

# 1. Устанавливаем системные зависимости
# libpq-dev нужен для работы с PostgreSQL.
# curl нужен для скачивания uv (хотя мы его копируем, иногда пригождается для healthcheck)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# 2. Устанавливаем uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# 3. Копируем файлы зависимостей
COPY pyproject.toml uv.lock ./

# 4. Устанавливаем зависимости
RUN uv sync --frozen --no-dev

COPY . .

# 6. Безопасность: создаем пользователя и переключаемся на него
# Создаем группу и юзера 'appuser'
RUN groupadd -r appuser && useradd -r -g appuser appuser
# Меняем владельца рабочей директории (если приложению нужно писать файлы, например логи)
RUN chown -R appuser:appuser /app
# Переключаемся
USER appuser

ENV PYTHONPATH="${PYTHONPATH}:/app/src"
CMD ["python", "main.py"]
