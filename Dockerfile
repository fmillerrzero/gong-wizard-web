FROM python:3.9-slim

WORKDIR /app

COPY packages.txt .

RUN if [ -s packages.txt ]; then apt-get update && xargs -a packages.txt apt-get install -y; fi

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 10000

CMD ["sh", "-c", "exec gunicorn --bind 0.0.0.0:${PORT:-10000} app:app --workers 2 --timeout 120"]