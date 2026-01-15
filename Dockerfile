FROM python:3.11-slim

# Arbeitsverzeichnis im Container setzen
WORKDIR /app

# Abhängigkeiten installieren
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["bash", "-c", "airflow db init && airflow scheduler & airflow webserver"] 
# CMD ["python", "main.py"]