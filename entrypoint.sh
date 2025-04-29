#!/bin/sh

# Use the PORT environment variable if set, otherwise default to 10000
PORT=${PORT:-10000}

# Start Gunicorn with the resolved port and increased timeout
exec gunicorn --bind 0.0.0.0:$PORT --timeout 120 app:app