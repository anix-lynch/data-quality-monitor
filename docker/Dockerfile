FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY ../requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY ../src/ ./src/
COPY ../data/ ./data/
COPY ../scripts/ ./scripts/

# Create reports directory
RUN mkdir -p reports

# Expose port
EXPOSE 8010

# Run the FastAPI application
CMD ["python", "-m", "uvicorn", "src.dashboard_api:app", "--host", "0.0.0.0", "--port", "8010", "--reload"]
