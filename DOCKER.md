# Docker Deployment

Default service port is `8100`.

## Start

```bash
docker compose up -d --build
```

Open:

```text
http://127.0.0.1:8100
```

## Change Port

Edit `.env` and keep `APP_PORT` within `8100-8200`, for example:

```env
APP_PORT=8110
```

Then rebuild:

```bash
docker compose up -d --build
```

## Data Persistence

SQLite data is persisted in:

```text
./backend/data
```
