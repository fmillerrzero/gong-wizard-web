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

# Pre-install pandas to avoid build delays
RUN pip install --no-cache-dir pandas==2.0.3

# Now copy the app
WORKDIR /app
COPY . .

# Install remaining dependencies (excluding pandas, already installed)
RUN pip install --no-cache-dir -r requirements.txt

# Run the app (change if not using Streamlit)
CMD ["streamlit", "run", "app.py", "--server.port=8000", "--server.address=0.0.0.0"]