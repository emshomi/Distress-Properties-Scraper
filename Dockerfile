# syntax=docker/dockerfile:1.6

# ============================================================
# STAGE 1 — Base Python with system dependencies for Playwright
# ============================================================
FROM python:3.11-slim-bookworm AS base

# Don't write .pyc files; flush stdout immediately for log streaming
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    TZ=America/Chicago

# Install system packages required by Playwright (Chromium) + tzdata
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        tzdata \
        # Playwright Chromium runtime deps
        libnss3 \
        libnspr4 \
        libatk1.0-0 \
        libatk-bridge2.0-0 \
        libcups2 \
        libdrm2 \
        libdbus-1-3 \
        libxkbcommon0 \
        libxcomposite1 \
        libxdamage1 \
        libxfixes3 \
        libxrandr2 \
        libgbm1 \
        libpango-1.0-0 \
        libcairo2 \
        libasound2 \
        libatspi2.0-0 \
        libwayland-client0 \
        fonts-liberation \
    && rm -rf /var/lib/apt/lists/*

# Set timezone explicitly
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

WORKDIR /app


# ============================================================
# STAGE 2 — Install Python dependencies
# ============================================================
FROM base AS deps

COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip && pip install -r /app/requirements.txt

# Install Playwright browsers (Chromium only — we don't use Firefox/WebKit)
RUN python -m playwright install chromium --with-deps


# ============================================================
# STAGE 3 — Final runtime image
# ============================================================
FROM deps AS runtime

# Create a non-root user for security
RUN useradd --create-home --shell /bin/bash scraper

# Copy application source
COPY --chown=scraper:scraper . /app

# Drop privileges
USER scraper

# Default port — Railway overrides via PORT env var
ENV PORT=8001
EXPOSE 8001

# Start the FastAPI app via uvicorn
# Railway provides $PORT; we honor it but default to 8001 locally
CMD ["sh", "-c", "uvicorn src.main:app --host 0.0.0.0 --port ${PORT:-8001}"]
