from sqlalchemy import create_engine, text, BigInteger
import pandas as pd
import time
from . import config

def wait_for_db():
    print(f"Versuche Verbindung zur Datenbank ({config.DB_HOST})...")
    retries = 10
    while retries > 0:
        try:
            engine = create_engine(config.DB_CONNECTION_STR)
            with engine.connect() as conn:
                print("Datenbank-Verbindung erfolgreich!")
                return engine
        except Exception as e:
            print(f"Datenbank noch nicht bereit, warte 3 Sekunden... (Fehler: {e})")
            time.sleep(3)
            retries -= 1
    raise Exception("Konnte keine Verbindung zur Datenbank herstellen.")

def load_data(data_dict):
    """
    Schreibt die DataFrames in die DB, korrigiert ID-Typen und setzt Primär- & Fremdschlüssel.
    """
    engine = wait_for_db()
    print("DB-Aktualisierung starten...")

    # Konvertiere alle ID-Spalten in int, um FK-Probleme zu vermeiden
    for col in ['zeit_id', 'geographie_id', 'bevoelkerung_id', 'indikator_id', 'id']:
        if col in data_dict['fact_table']:
            data_dict['fact_table'][col] = data_dict['fact_table'][col].astype(int)

    data_dict['dim_zeit']['zeit_id'] = data_dict['dim_zeit']['zeit_id'].astype(int)
    data_dict['dim_geo']['geographie_id'] = data_dict['dim_geo']['geographie_id'].astype(int)
    data_dict['dim_bev']['bevoelkerung_id'] = data_dict['dim_bev']['bevoelkerung_id'].astype(int)
    data_dict['dim_ind']['indikator_id'] = data_dict['dim_ind']['indikator_id'].astype(int)

    with engine.connect() as conn:
        # Alte Tabellen löschen
        conn.execute(text("DROP TABLE IF EXISTS fakt_gesundheitskennzahlen CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS dim_geographie CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS dim_bevoelkerung CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS dim_indikator CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS dim_zeit CASCADE"))
        conn.commit()

    # Tabellen schreiben mit explizitem bigint für ID-Spalten
    print("Schreibe Dimensionen...")
    data_dict['dim_zeit'].to_sql(
        'dim_zeit', engine, if_exists='replace', index=False,
        dtype={'zeit_id': BigInteger()}
    )
    data_dict['dim_geo'].to_sql(
        'dim_geographie', engine, if_exists='replace', index=False,
        dtype={'geographie_id': BigInteger()}
    )
    data_dict['dim_bev'].to_sql(
        'dim_bevoelkerung', engine, if_exists='replace', index=False,
        dtype={'bevoelkerung_id': BigInteger()}
    )
    data_dict['dim_ind'].to_sql(
        'dim_indikator', engine, if_exists='replace', index=False,
        dtype={'indikator_id': BigInteger()}
    )

    print("Schreibe Faktentabelle...")
    data_dict['fact_table'].to_sql(
        'fakt_gesundheitskennzahlen', engine, if_exists='replace', index=False,
        chunksize=1000,
        dtype={
            'id': BigInteger(),
            'zeit_id': BigInteger(),
            'geographie_id': BigInteger(),
            'bevoelkerung_id': BigInteger(),
            'indikator_id': BigInteger()
        }
    )

    # Constraints setzen (Primär- und Fremdschlüssel)
    print("Setze Primary & Foreign Keys...")
    with engine.connect() as conn:
        # Primärschlüssel
        conn.execute(text("ALTER TABLE dim_zeit ADD PRIMARY KEY (zeit_id)"))
        conn.execute(text("ALTER TABLE dim_geographie ADD PRIMARY KEY (geographie_id)"))
        conn.execute(text("ALTER TABLE dim_bevoelkerung ADD PRIMARY KEY (bevoelkerung_id)"))
        conn.execute(text("ALTER TABLE dim_indikator ADD PRIMARY KEY (indikator_id)"))
        conn.execute(text("ALTER TABLE fakt_gesundheitskennzahlen ADD PRIMARY KEY (id)"))

        # Fremdschlüssel
        conn.execute(text("""
            ALTER TABLE fakt_gesundheitskennzahlen 
            ADD CONSTRAINT fk_zeit FOREIGN KEY (zeit_id) REFERENCES dim_zeit(zeit_id)
        """))
        conn.execute(text("""
            ALTER TABLE fakt_gesundheitskennzahlen 
            ADD CONSTRAINT fk_geo FOREIGN KEY (geographie_id) REFERENCES dim_geographie(geographie_id)
        """))
        conn.execute(text("""
            ALTER TABLE fakt_gesundheitskennzahlen 
            ADD CONSTRAINT fk_bev FOREIGN KEY (bevoelkerung_id) REFERENCES dim_bevoelkerung(bevoelkerung_id)
        """))
        conn.execute(text("""
            ALTER TABLE fakt_gesundheitskennzahlen 
            ADD CONSTRAINT fk_ind FOREIGN KEY (indikator_id) REFERENCES dim_indikator(indikator_id)
        """))
        conn.commit()

    print("Datenbank erfolgreich aktualisiert")
