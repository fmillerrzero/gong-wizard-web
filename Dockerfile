# Use a slim Python image for smaller size
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip cache purge && pip install --no-cache-dir --no-deps -r requirements.txt

# Copy application files
COPY . .

# Expose port (Render maps internally to external port)
EXPOSE 10000

# Run with gunicorn, binding to port 10000
CMD ["gunicorn", "-w", "4", "-b", "0.0.0.0:10000", "app:app"]