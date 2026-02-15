FROM python:3.13-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    UV_PROJECT_ENVIRONMENT="/usr/local" \
    # Включаем компиляцию байткода при установке пакетов (ускоряет старт)
    UV_COMPILE_BYTECODE=1 \
    PYTHONPATH="/app:/app/src"

# 1. Устанавливаем системные зависимости
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
# Меняем владельца рабочей директории
RUN chown -R appuser:appuser /app
# Переключаемся
USER appuser

CMD ["uvicorn", "services.webhook_service:app", "--host", "0.0.0.0", "--port", "8000"]