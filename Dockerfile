FROM python:3.9-slim

WORKDIR /app

COPY packages.txt .

RUN if [ -s packages.txt ]; then apt-get update && xargs -a packages.txt apt-get install -y; fi

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 10000

CMD ["gunicorn", "--bind", "0.0.0.0:$PORT", "app:app"]