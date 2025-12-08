FROM apache/superset:latest

USER root
RUN apt-get update && apt-get install -y postgresql-client
RUN pip install --no-cache-dir psycopg2-binary
USER superset
