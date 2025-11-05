# Use an official Python image
FROM python:3.11-slim

# Set work directory
WORKDIR /app

# Install system dependencies (for psycopg2 or others)
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
 && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .
COPY wait-for-it.sh /wait-for-it.sh
RUN chmod +x /wait-for-it.sh


# Expose FastAPI port
EXPOSE 8000

# Start the FastAPI server (dev mode)
CMD ["/wait-for-it.sh", "db:5432", "--", "uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
