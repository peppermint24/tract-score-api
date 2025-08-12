FROM python:3.11-slim

# System basics
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# App code
COPY . .

# Persistent disk mount target
RUN mkdir -p /data
ENV DATA_DIR=/data
ENV SCORES_PATH=./tract_lookup.json

# Start server (Render provides $PORT)
CMD ["bash","-lc","uvicorn main:app --host 0.0.0.0 --port ${PORT:-10000} --workers 1"]
