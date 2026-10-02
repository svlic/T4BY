FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src

RUN pip install --no-cache-dir . \
    && useradd --create-home --uid 10001 t4by \
    && mkdir -p /app/data \
    && chown -R t4by:t4by /app/data

USER t4by

VOLUME ["/app/data"]
EXPOSE 9464

CMD ["t4by"]
