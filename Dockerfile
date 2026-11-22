FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app/src

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY src /app/src
COPY scripts /app/scripts
COPY web /app/web

RUN chmod +x /app/scripts/entrypoint.sh

EXPOSE 8000

ENTRYPOINT ["/app/scripts/entrypoint.sh"]
CMD ["uvicorn", "pm25_planner.main:app", "--host", "0.0.0.0", "--port", "8000"]
