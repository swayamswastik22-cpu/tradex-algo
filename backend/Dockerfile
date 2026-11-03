# Production Dockerfile for Indian Algo Trader Backend
# Configured for Hugging Face Spaces (Port 7860, non-root user 1000)

FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PORT=7860 \
    HOME=/home/user

# Install OS runtime utilities
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user with UID 1000 (Required for Hugging Face Spaces)
RUN useradd -m -u 1000 user

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt /app/
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy backend application source
COPY . /app/

# Set permissions for non-root user
RUN chown -R user:user /app /home/user

USER user

EXPOSE 7860
EXPOSE 10000

# Launch FastAPI app with Uvicorn (supports Render $PORT or fallback to 7860)
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT:-7860}"]
