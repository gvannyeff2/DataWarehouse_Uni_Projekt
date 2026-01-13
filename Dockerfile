# Wir nutzen ein schlankes Python-Image als Basis
FROM python:3.11-slim

# Arbeitsverzeichnis im Container setzen
WORKDIR /app

# Abhängigkeiten installieren
# Wir kopieren zuerst nur die requirements.txt, um Docker-Caching zu nutzen
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Den Rest des Codes kopieren
COPY . .

# Standard-Befehl: Das ETL-Skript ausführen
CMD ["python", "etl_pipeline_2.py"]