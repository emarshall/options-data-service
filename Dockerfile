# Single image shared by the ingestion, api, and migrate services in
# docker-compose.yml — they differ only in the command they run, so one
# image keeps builds simple and guarantees all three are always in sync.

FROM python:3.12-slim

WORKDIR /app

# libpq-dev/gcc needed to build psycopg from source on some platforms;
# harmless if the binary wheel is used instead.
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Overridden per-service in docker-compose.yml; this default is just so
# `docker build` + `docker run` works standalone for debugging.
CMD ["python", "-m", "service.ingestion.main"]
