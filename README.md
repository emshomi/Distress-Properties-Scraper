# Distress Properties Scraper Service

Background data ingestion service for the Minnesota distressed-property platform.
This service runs eight scrapers on cron schedules, pulling public records data
from city, county, and federal sources into Supabase.

## What this service does

Eight scrapers feeding the platform's signal layer:

| Scraper | Source | Cadence |
|---|---|---|
| `mpls_311` | Minneapolis 311 code violations | Daily 06:00 CST |
| `hennepin_sheriff` | Hennepin County sheriff sales (PDF) | Daily 06:15 CST |
| `mpls_vbr` | Minneapolis Vacant Building Registry + PVE | Daily 07:00 CST |
| `saint_paul_vacant` | Saint Paul DSI vacant buildings | Daily 07:15 CST |
| `mcro_probate` | MN Court Records Online probate filings | Daily 08:00 CST (disabled by default) |
| `usps_vacancy` | HUD/USPS Vacancy Indicator | Weekly Sunday 02:00 CST |
| `tax_forfeit` | MN tax-forfeit county pages | Monthly 1st 03:00 CST |

All times America/Chicago. Schedule is staggered to spread peak load.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     FastAPI app (this service)              │
│                                                              │
│  ┌─────────────┐  ┌──────────────┐  ┌──────────────────┐   │
│  │   Routes    │  │  Middleware  │  │    Scheduler     │   │
│  │             │  │              │  │  (APScheduler)   │   │
│  │  /health    │  │   AdminKey   │  │                  │   │
│  │  /status    │  │   required   │  │  Cron triggers   │   │
│  │  /trigger   │  │              │  │  per scraper     │   │
│  └─────────────┘  └──────────────┘  └──────────────────┘   │
│         │                 │                 │                │
│         └─────────────────┴─────────────────┘                │
│                           │                                  │
│                  ┌────────▼────────┐                         │
│                  │   BaseScraper   │                         │
│                  │   (8 scrapers)  │                         │
│                  └────────┬────────┘                         │
│                           │                                  │
│         ┌─────────────────┼─────────────────┐                │
│         │                 │                 │                │
│  ┌──────▼──────┐  ┌──────▼──────┐  ┌──────▼──────┐         │
│  │   Parcel    │  │   Event     │  │   Audit     │         │
│  │  resolver   │  │   writer    │  │   logger    │         │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘         │
└─────────┼─────────────────┼─────────────────┼──────────────┘
          │                 │                 │
          └─────────────────▼─────────────────┘
                            │
                  ┌─────────▼─────────┐
                  │     Supabase      │
                  │   (Postgres +     │
                  │    pgvector +     │
                  │    PostGIS)       │
                  └───────────────────┘
```

## HTTP API

| Endpoint | Method | Auth | Description |
|---|---|---|---|
| `/health` | GET | None | Railway deploy probe |
| `/status` | GET | None | Operator dashboard data |
| `/trigger` | GET | X-Admin-Key | List available scrapers |
| `/trigger/{name}` | POST | X-Admin-Key | Manually run a scraper |
| `/docs` | GET | None | OpenAPI Swagger UI |
| `/redoc` | GET | None | OpenAPI ReDoc UI |

## Deployment

This service is designed for Railway with a single replica.

### Required environment variables

See `.env.example` for the full list. Critical variables:

- `SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY` — database connection
- `ADMIN_API_KEY` — protects trigger endpoints (generate with `openssl rand -hex 32`)
- `SCHEDULER_TIMEZONE` — set to `America/Chicago` for Minnesota local time
- `NOMINATIM_USER_AGENT` — required by OpenStreetMap if Mapbox not configured

### Deploy steps

1. Push this repo to GitHub
2. Connect Railway to the GitHub repo
3. Set environment variables in Railway dashboard
4. Railway auto-builds via Dockerfile
5. First deploy takes 5-7 minutes (Playwright Chromium install)
6. Verify with `curl https://your-service.railway.app/health`

## Operational notes

- **Single replica only** — APScheduler in-process state requires `numReplicas: 1`
- **Sleep disabled** — Railway must keep the container running 24/7 for cron
- **Healthcheck path is `/health`** — Railway probes this for deploy validation
- **Graceful shutdown** — SIGTERM stops the scheduler; in-flight jobs continue
- **Audit logs are in Supabase** — `audit.scraper_runs` and `audit.scraper_errors`

## License

Private. All rights reserved.
