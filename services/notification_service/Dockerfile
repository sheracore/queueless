FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

RUN useradd --create-home --uid 1000 appuser
USER appuser

# No HEALTHCHECK here: this image runs as a worker (no HTTP) AND as an API.
# Each container in docker-compose.yml gets the healthcheck that fits its role.
CMD ["python", "-m", "app.worker"]
