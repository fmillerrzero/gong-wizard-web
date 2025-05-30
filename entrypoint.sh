#!/bin/sh
PORT=${PORT:-10000}
exec gunicorn --bind 0.0.0.0:$PORT --timeout 120 app:app
