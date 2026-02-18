FROM python:3.11-slim

WORKDIR /app

# Abhängigkeiten installieren
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN apt-get update && apt-get install -y --no-install-recommends cron tzdata \
 && rm -rf /var/lib/apt/lists/*
ENV TZ=Europe/Berlin

COPY . .

# Automatisierung mit Cronjob (jeden Tag um 20 Uhr)
RUN echo "00 20 * * * root . /etc/environment; /usr/local/bin/python /app/main.py >> /var/log/cron.log 2>&1" > /etc/cron.d/etl-cron \
 && chmod 0644 /etc/cron.d/etl-cron \
 && touch /var/log/cron.log

CMD ["sh", "-c", "printenv | grep -v 'no_proxy' >> /etc/environment && cron && tail -f /var/log/cron.log"]