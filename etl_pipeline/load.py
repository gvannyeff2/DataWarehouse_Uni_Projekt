from sqlalchemy import create_engine, text, BigInteger, Integer, String, Float
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
                print("Datenbank erfolgreich verbindet!")
                return engine
        except Exception as e:
            print(f"Datenbank noch nicht bereit, warte... (Fehler: {e})")
            time.sleep(3)
            retries -= 1
    raise Exception("Verbindung zur Datenbank fehlgeschlagen.")

def validate_dimensions_links(df: pd.DataFrame, cols, table_name:str):
    """
    Prüft, ob alle Schlüsselspalten existieren.
    Konvertiert nach int64 (für Schlüssel-Constraints).
    """
    for col in cols: 
        if col not in df.columns:
            raise ValueError(f"{table_name}: Spalte '{col}' fehlt.")
        
        df[col] = pd.to_numeric(df[col], errors='coerce')
        missing = df[col].isna().sum()
        if missing:
            raise ValueError(
                f"{table_name}.{col} hat {missing} fehlende Werte. "
                "Bitte Transformationsschritt prüfen (fehlende Dimension-Matches)."
            )
        df[col] = df[col].astype('int64')

def load_data(data_dict):
    """
    Schreibt die DataFrames in die DB, korrigiert ID-Typen und setzt Primär- & Fremdschlüssel.
    """
    engine = wait_for_db()
    print("DB aktualisieren...")

    # Validierung Faktentabellen
    if 'fact_diabetes' in data_dict: 
        validate_dimensions_links(
            data_dict['fact_diabetes'],
            cols=['zeit_id', 'geographie_id', 'bevoelkerung_id', 'indikator_id', 'id'],
            table_name='fact_diabetes'
        )

    if 'fact_geda' in data_dict:
        validate_dimensions_links(
            data_dict['fact_geda'],
            cols=['zeit_id', 'geographie_id', 'bevoelkerung_id', 'indikator_id', 'id'],
            table_name='fact_geda'
        )

    # Konvertiere alle ID-Spalten in int, um FK-Probleme zu vermeiden
    # for key in ['fact_diabetes', 'fact_geda']:
    #     if key in data_dict:
    #         for col in ['zeit_id', 'geographie_id', 'bevoelkerung_id', 'indikator_id', 'id']:
    #             if col in data_dict[key].columns:
    #                 data_dict[key][col] = data_dict[key][col].astype('int64')

    dim_id_mapping = {
        'dim_zeit': 'zeit_id',
        'dim_geo': 'geographie_id',
        'dim_bev': 'bevoelkerung_id',
        'dim_ind': 'indikator_id'
    }

    # Sicherheitscheck: Existiert die Spalte?
    for dim_name, id_col in dim_id_mapping.items():
        if dim_name in data_dict:
            validate_dimensions_links(data_dict[dim_name], [id_col], dim_name)

    # Alte Tabellen löschen
    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS fakt_diabetes CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS fakt_geda CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS dim_geographie CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS dim_bevoelkerung CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS dim_indikator CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS dim_zeit CASCADE"))
        conn.commit()

    # Dimensionen
    print("Dimensionen schreiben...")
    data_dict['dim_zeit'].to_sql(
        'dim_zeit', engine, if_exists='replace', index=False,
        dtype={'zeit_id': BigInteger(), 'jahr': Integer(), 'periode': String()}
    )
    data_dict['dim_geo'].to_sql(
        'dim_geographie', engine, if_exists='replace', index=False,
        dtype={'geographie_id': BigInteger(), 'iso_code': String(), 'kategorie': String(), 'beschreibung': String(), 'name': String()}
    )
    data_dict['dim_bev'].to_sql(
        'dim_bevoelkerung', engine, if_exists='replace', index=False,
        dtype={'bevoelkerung_id': BigInteger(), 'geschlecht': String(), 'altersgruppe': String(), 'bildungsgruppe': String()}
    )
    data_dict['dim_ind'].to_sql(
        'dim_indikator', engine, if_exists='replace', index=False,
        dtype={'indikator_id': BigInteger(), 'name': String(), 'handlungsfeld': String(), 'einheit': String(), 'datenquellen': String()}
    )

    # Faktentabellen
    
    ## Diabetes
    print("Faktentabelle für Diabetes laden...")

    data_dict['fact_diabetes'].to_sql(
        'fakt_diabetes', engine, if_exists='replace', index=False,
        dtype={
            'id': BigInteger(),
            'zeit_id': BigInteger(),
            'geographie_id': BigInteger(),
            'bevoelkerung_id': BigInteger(),
            'indikator_id': BigInteger(),
            'wert': Float()
        }
    )

    ## GEDA
    print("Lade Faktentabelle für GEDA...") 
    data_dict['fact_geda'].to_sql(
        'fakt_geda', engine, if_exists='replace', index=False,
        dtype={
            'id': BigInteger(),
            'zeit_id': BigInteger(),
            'geographie_id': BigInteger(),
            'bevoelkerung_id': BigInteger(),
            'indikator_id': BigInteger(),
            'wert': Float()
        }
    )

    # Constraints setzen (Primär- und Fremdschlüssel)
    print("Primär- und Fremdschlüssel setzen...")
    with engine.connect() as conn:
        # Primärschlüssel von Dimensionen
        for t, pk in [
            ('dim_zeit', 'zeit_id'), 
            ('dim_geographie', 'geographie_id'), 
            ('dim_bevoelkerung', 'bevoelkerung_id'), 
            ('dim_indikator', 'indikator_id')
        ]:
            conn.execute(text(f"ALTER TABLE {t} ADD PRIMARY KEY ({pk})"))

        # Primär- und Fremdschlüssel von Faktentabellen
        for table in ['fakt_diabetes', 'fakt_geda']:
            conn.execute(text(f"ALTER TABLE {table} ADD PRIMARY KEY (id)"))
            conn.execute(text(f"ALTER TABLE {table} ADD CONSTRAINT fk_{table}_zeit FOREIGN KEY (zeit_id) REFERENCES dim_zeit(zeit_id)"))
            conn.execute(text(f"ALTER TABLE {table} ADD CONSTRAINT fk_{table}_geo FOREIGN KEY (geographie_id) REFERENCES dim_geographie(geographie_id)"))
            conn.execute(text(f"ALTER TABLE {table} ADD CONSTRAINT fk_{table}_bev FOREIGN KEY (bevoelkerung_id) REFERENCES dim_bevoelkerung(bevoelkerung_id)"))
            conn.execute(text(f"ALTER TABLE {table} ADD CONSTRAINT fk_{table}_ind FOREIGN KEY (indikator_id) REFERENCES dim_indikator(indikator_id)"))
       
        conn.commit()

    print("Datenbank erfolgreich aktualisiert")
