FROM python:3.11-slim

WORKDIR /app

# Install curl for healthcheck and sqlite3 for manual DB checks
RUN apt-get update && apt-get install -y \
    curl \
    sqlite3 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create data directory for persistence
RUN mkdir -p /app/data /app/sap-o2c-data

COPY . .

# Environment variables with defaults (LLM_API_KEY set via dashboard)
ENV LLM_PROVIDER=groq
ENV LLM_MODEL=llama-3.3-70b-versatile
ENV PORT=8000

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD curl -f http://localhost:8000/api/stats || exit 1

# Run ingestion then start server (no --reload in production)
CMD ["sh", "-c", "python ingest.py && uvicorn main:app --host 0.0.0.0 --port ${PORT}"]
