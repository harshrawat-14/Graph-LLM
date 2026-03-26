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

# Environment variables with defaults
ENV LLM_PROVIDER=groq
ENV LLM_MODEL=llama-3.3-70b-versatile
ENV PORT=8000

EXPOSE 8000

# Run ingestion (only if data is missing) then start server
# We use standard uvicorn here without --reload for production
CMD ["sh", "-c", "python ingest.py && uvicorn main:app --host 0.0.0.0 --port 8000"]
