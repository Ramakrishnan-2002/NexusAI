FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY backend/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY backend /app/backend
COPY workers /app/workers

ENV PYTHONPATH=/app:/app/backend
ENV PYTHONUNBUFFERED=1

CMD ["python", "workers/embedding/main.py"]
