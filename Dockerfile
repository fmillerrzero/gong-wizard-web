# Use official Python image
FROM python:3.11-slim

# Prevent Python from writing .pyc files and buffering stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt /app/
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy the rest of the app
COPY . /app/

# Expose the port Render uses (it sets $PORT at runtime)
EXPOSE 10000

# Use gunicorn to serve the app, binding to $PORT (Render requirement)
CMD ["gunicorn", "--bind", "0.0.0.0:$PORT", "app:app"]