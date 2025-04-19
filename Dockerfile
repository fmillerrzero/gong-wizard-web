# Use Python base image
FROM python:3.11-slim

# Install build essentials for compiling anything needed
RUN apt-get update && apt-get install -y \
    build-essential \
    python3-dev \
    libatlas-base-dev \
    zlib1g-dev \
    libjpeg-dev \
    && rm -rf /var/lib/apt/lists/*

# Pre-install numpy and pandas to avoid build delays and binary mismatches
RUN pip install --no-cache-dir numpy==1.24.3 pandas==2.0.3

# Now copy the app
WORKDIR /app
COPY . .

# Install remaining dependencies (excluding numpy + pandas, already installed)
RUN pip install --no-cache-dir -r requirements.txt

# Run with Gunicorn
CMD ["gunicorn", "--bind", "0.0.0.0:8000", "app:app"]