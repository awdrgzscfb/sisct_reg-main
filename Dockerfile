FROM node:20-bookworm-slim AS frontend-builder

WORKDIR /app/frontend

COPY frontend/package*.json ./
RUN npm ci

COPY frontend/ ./
RUN npm run build


FROM python:3.11-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    HOST=0.0.0.0 \
    PORT=8100 \
    APP_RELOAD=0

WORKDIR /app

COPY requirements.txt ./
COPY backend/requirements.txt ./backend/requirements.txt

RUN pip install --upgrade pip \
    && pip install -r requirements.txt \
    && python -m playwright install --with-deps chromium

COPY backend ./backend
COPY main.py ./
COPY --from=frontend-builder /app/frontend/dist ./frontend/dist

VOLUME ["/app/backend/data"]

EXPOSE 8100

CMD ["python", "main.py"]
