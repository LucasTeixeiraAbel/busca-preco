FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright
WORKDIR /app

COPY backend/requirements.txt ./backend/requirements.txt
RUN pip install --no-cache-dir -r backend/requirements.txt \
    && python -m playwright install --with-deps chromium

RUN useradd --create-home --uid 10001 appuser \
    && chmod -R a+rX /ms-playwright
COPY backend ./backend
USER appuser

CMD ["uvicorn", "backend.worker:app", "--host", "0.0.0.0", "--port", "8000"]
