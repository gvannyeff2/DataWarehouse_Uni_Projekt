#!/usr/bin/env python3
"""
ETL pipeline to load RKI Diabetes-Surveillance and GEDA into a star schema in Postgres.

Expected data files (place them into ./datenquellen):
 - Diabetes-Surveillance_Indikatoren.tsv
 - Gesundheit_in_Deutschland_aktuell_-_2019-2020-EHIS.csv
"""

import os
import sys
import logging
from pathlib import Path
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# load environment variables if present
load_dotenv()

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# DB connection details (use environment variables set in docker-compose)
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "diabetes_dwh")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "admin")

DB_CONN = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Data file paths (mounted at /data in container)
DATA_DIR = Path("/data")
DIABETES_FILE = DATA_DIR / "Diabetes-Surveillance_Indikatoren.tsv"
GEDA_FILE = DATA_DIR / "Gesundheit_in_Deutschland_aktuell_-_2019-2020-EHIS.csv"

# Safety checks
if not DIABETES_FILE.exists():
    logging.error(f"Diabetes file not found at {DIABETES_FILE}")
if not GEDA_FILE.exists():
    logging.error(f"GEDA file not found at {GEDA_FILE}")

def safe_read_tsv(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path, sep="\t", encoding="utf-8")
    except Exception:
        return pd.read_csv(path, sep="\t", encoding="latin1")

def safe_read_csv(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path, encoding="utf-8")
    except Exception:
        return pd.read_csv(path, encoding="latin1")

def normalize_colnames(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]
    return df

def main():
    logging.info("Connect to DB")
    engine = create_engine(DB_CONN, echo=False, future=True)

    # ---------- EXTRACT ----------
    logging.info("Read source files")
    df_diab = safe_read_tsv(DIABETES_FILE)
    df_geda = safe_read_csv(GEDA_FILE)

    df_diab = normalize_colnames(df_diab)
    df_geda = normalize_colnames(df_geda)

    # ---------- TRANSFORM: Harmonize columns ----------
    logging.info("Harmonize and clean data")

    # Normalize column names / key fields we will use
    # DIABETES: expected columns include 'Jahr','Region_Name','Indikator_Name','Geschlecht_Name','Alter_Name','Wert','Unteres_Konfidenzintervall','Oberes_Konfidenzintervall'
    # GEDA: expected columns include 'Altersgruppe','Gender','Bundesland','Percent','LowerCL','UpperCL','Variable'

    # Clean Diabetes
    if 'Jahr' not in df_diab.columns:
        logging.warning("Diabetes source missing 'Jahr' column -- attempting to infer or set to NULL")
    df_diab['Jahr'] = df_diab.get('Jahr', pd.NA)
    df_diab['Region_Name'] = df_diab.get('Region_Name', df_diab.get('Region', df_diab.get('Region_Name', 'Deutschland')))
    df_diab['Indikator_Name'] = df_diab.get('Indikator_Name', df_diab.get('Indikator', pd.NA))
    df_diab['Geschlecht_Name'] = df_diab.get('Geschlecht_Name', df_diab.get('Gender', df_diab.get('Geschlecht', 'Gesamt')))
    df_diab['Alter_Name'] = df_diab.get('Alter_Name', df_diab.get('Altersgruppe', 'Gesamt'))
    df_diab['wert_raw'] = df_diab.get('Wert', df_diab.get('Percent', pd.NA)).astype('float', errors='ignore')
    df_diab['ci_min'] = df_diab.get('Unteres_Konfidenzintervall', df_diab.get('LowerCL', pd.NA)).astype('float', errors='ignore')
    df_diab['ci_max'] = df_diab.get('Oberes_Konfidenzintervall', df_diab.get('UpperCL', pd.NA)).astype('float', errors='ignore')
    df_diab['datenquelle'] = 'Surveillance'

    # Clean GEDA
    df_geda['Altersgruppe'] = df_geda.get('Altersgruppe', df_geda.get('Alter', pd.NA))
    df_geda['Gender'] = df_geda.get('Gender', df_geda.get('Geschlecht', 'Gesamt'))
    df_geda['Bundesland'] = df_geda.get('Bundesland', df_geda.get('Region', 'Deutschland'))
    df_geda['Variable'] = df_geda.get('Variable', pd.NA)
    df_geda['Percent'] = pd.to_numeric(df_geda.get('Percent', df_geda.get('Wert', pd.NA)), errors='coerce')
    df_geda['LowerCL'] = pd.to_numeric(df_geda.get('LowerCL', pd.NA), errors='coerce')
    df_geda['UpperCL'] = pd.to_numeric(df_geda.get('UpperCL', pd.NA), errors='coerce')
    # GEDA is 2019-2020 wave -> map to year 2020 (as per your design decision)
    df_geda['Jahr'] = 2020
    df_geda['wert_raw'] = df_geda['Percent']
    df_geda['ci_min'] = df_geda['LowerCL']
    df_geda['ci_max'] = df_geda['UpperCL']
    df_geda['datenquelle'] = 'GEDA'

    # Basic normalization for common values
    df_diab['Region_Name'] = df_diab['Region_Name'].replace({'Gesamt': 'Deutschland'}).astype(str)
    df_geda['Bundesland'] = df_geda['Bundesland'].replace({'Gesamt': 'Deutschland'}).astype(str)

    # Standardize genders
    gender_map = {
        'Weiblich': 'Frauen', 'Männlich': 'Männer',
        'Female': 'Frauen', 'Male': 'Männer',
        'F': 'Frauen', 'M': 'Männer'
    }
    df_diab['geschlecht_clean'] = df_diab['Geschlecht_Name'].astype(str).map(lambda x: gender_map.get(x, x))
    df_geda['geschlecht_clean'] = df_geda['Gender'].astype(str).map(lambda x: gender_map.get(x, x))

    # Standardize age groups (small normalization)
    def norm_age(x):
        if pd.isna(x):
            return 'Gesamt'
        s = str(x).strip()
        s = s.replace(' Jahre', '').replace(' ', '')
        s = s.replace('18-29', '18-29').replace('18-29Jahre', '18-29')
        return s
    df_diab['altersgruppe_clean'] = df_diab['Alter_Name'].map(norm_age)
    df_geda['altersgruppe_clean'] = df_geda['Altersgruppe'].map(norm_age)

    # ---------- Build Dimensions ----------

    logging.info("Build dimension: dim_quelle")
    dim_quelle = pd.DataFrame({
        'quelle_id': [1, 2],
        'quelle_name': ['Diabetes Surveillance (RKI)', 'GEDA 2019/2020 (RKI)']
    })

    logging.info("Build dimension: dim_zeit")
    years = sorted(set(pd.to_numeric(df_diab['Jahr'], errors='coerce').dropna().astype(int).unique()) | {2020})
    dim_zeit = pd.DataFrame({'zeit_id': range(1, len(years)+1), 'jahr': years})

    logging.info("Build dimension: dim_region")
    regions = sorted(set(df_diab['Region_Name'].dropna().unique()) | set(df_geda['Bundesland'].dropna().unique()))
    dim_region = pd.DataFrame({'region_id': range(1, len(regions)+1), 'bundesland_name': list(regions)})

    logging.info("Build dimension: dim_bevoelkerung (demography)")
    # unique combos of gender + age + optional education (GEDA may have education)
    df_geda['Bildungsgruppe'] = df_geda.get('Bildungsgruppe', pd.NA)
    demo = pd.concat([
        df_diab[['geschlecht_clean', 'altersgruppe_clean']].drop_duplicates(),
        df_geda[['geschlecht_clean', 'altersgruppe_clean']].drop_duplicates()
    ]).drop_duplicates().reset_index(drop=True)
    demo = demo.rename(columns={'geschlecht_clean': 'geschlecht', 'altersgruppe_clean': 'altersgruppe'})
    demo['demografie_id'] = demo.index + 1
    dim_bevoelkerung = demo[['demografie_id', 'geschlecht', 'altersgruppe']]

    logging.info("Build dimension: dim_indikator")
    # Map GEDA variable codes to human-readable names (expandable)
    geda_map = {
        'PAadiposB': 'Adipositas (BMI >=30)',
        'PAueberB': 'Übergewicht (BMI >=25)',
        'RCstatE_k3': 'Raucherstatus',
        'KAwalk2': 'Gehen (Min/Woche)',
        'KAspo2': 'Sport (Min/Woche)',
        'AMarztB': 'Arztbesuch (letzte 12 Mon)'
    }

    diab_ind = pd.DataFrame({'code': df_diab['Indikator_Name'].dropna().unique()})
    diab_ind['name'] = diab_ind['code']  # keep original as default
    diab_ind['quelle_typ'] = 'Surveillance'

    geda_ind = pd.DataFrame({'code': df_geda['Variable'].dropna().unique()})
    geda_ind['name'] = geda_ind['code'].map(lambda c: geda_map.get(c, c))
    geda_ind['quelle_typ'] = 'GEDA'

    dim_indikator = pd.concat([diab_ind, geda_ind], ignore_index=True).drop_duplicates(subset=['code']).reset_index(drop=True)
    dim_indikator['indikator_id'] = dim_indikator.index + 1
    # ensure exact column order
    dim_indikator = dim_indikator[['indikator_id', 'code', 'name', 'quelle_typ']]

    # ---------- Build Fact rows ----------
    logging.info("Map source rows to fact rows (fact_gesundheitskennzahlen)")

    # Helper lookups
    zeit_lookup = dict(zip(dim_zeit['jahr'], dim_zeit['zeit_id']))
    region_lookup = dict(zip(dim_region['bundesland_name'], dim_region['region_id']))
    demo_lookup = {(r['geschlecht'], r['altersgruppe']): r['demografie_id'] for _, r in dim_bevoelkerung.iterrows()}
    indik_lookup = dict(zip(dim_indikator['code'], dim_indikator['indikator_id']))

    fact_rows = []

    # Diabetes rows
    for _, r in df_diab.iterrows():
        try:
            year = int(r['Jahr']) if pd.notna(r['Jahr']) else None
            zeit_id = zeit_lookup.get(year, None)
            region_id = region_lookup.get(r['Region_Name'], None)
            demografie_id = demo_lookup.get((r['geschlecht_clean'], r['altersgruppe_clean']), None)
            indikator_id = indik_lookup.get(r['Indikator_Name'], None)

            # if indikator not found, create a new one dynamically
            if indikator_id is None and pd.notna(r['Indikator_Name']):
                new_id = dim_indikator['indikator_id'].max() + 1
                dim_indikator = pd.concat([dim_indikator, pd.DataFrame([{
                    'indikator_id': new_id, 'code': r['Indikator_Name'], 'name': r['Indikator_Name'], 'quelle_typ': 'Surveillance'
                }])], ignore_index=True)
                indik_lookup[r['Indikator_Name']] = new_id
                indikator_id = new_id

            # get numeric value
            wert = pd.to_numeric(r.get('wert_raw', pd.NA), errors='coerce')
            ci_min = pd.to_numeric(r.get('ci_min', pd.NA), errors='coerce')
            ci_max = pd.to_numeric(r.get('ci_max', pd.NA), errors='coerce')

            if pd.isna(wert):
                continue

            fact_rows.append({
                'zeit_id': zeit_id,
                'region_id': region_id,
                'demografie_id': demografie_id,
                'indikator_id': indikator_id,
                'wert': float(wert) if pd.notna(wert) else None,
                'ci_min': float(ci_min) if pd.notna(ci_min) else None,
                'ci_max': float(ci_max) if pd.notna(ci_max) else None,
                'datenquelle': 'Surveillance'
            })
        except Exception as e:
            logging.debug("Skipping diabetes row due to error: %s", e)
            continue

    # GEDA rows
    for _, r in df_geda.iterrows():
        try:
            year = int(r['Jahr']) if pd.notna(r['Jahr']) else 2020
            zeit_id = zeit_lookup.get(year, None)
            region_id = region_lookup.get(r['Bundesland'], None)
            demografie_id = demo_lookup.get((r['geschlecht_clean'], r['altersgruppe_clean']), None)
            indikator_id = indik_lookup.get(r['Variable'], None)

            # dynamic indicator creation if missing
            if indikator_id is None and pd.notna(r['Variable']):
                new_id = dim_indikator['indikator_id'].max() + 1
                dim_indikator = pd.concat([dim_indikator, pd.DataFrame([{
                    'indikator_id': new_id, 'code': r['Variable'], 'name': geda_map.get(r['Variable'], r['Variable']), 'quelle_typ': 'GEDA'
                }])], ignore_index=True)
                indik_lookup[r['Variable']] = new_id
                indikator_id = new_id

            wert = pd.to_numeric(r.get('wert_raw', pd.NA), errors='coerce')
            ci_min = pd.to_numeric(r.get('ci_min', pd.NA), errors='coerce')
            ci_max = pd.to_numeric(r.get('ci_max', pd.NA), errors='coerce')

            if pd.isna(wert):
                continue

            fact_rows.append({
                'zeit_id': zeit_id,
                'region_id': region_id,
                'demografie_id': demografie_id,
                'indikator_id': indikator_id,
                'wert': float(wert) if pd.notna(wert) else None,
                'ci_min': float(ci_min) if pd.notna(ci_min) else None,
                'ci_max': float(ci_max) if pd.notna(ci_max) else None,
                'datenquelle': 'GEDA'
            })
        except Exception as e:
            logging.debug("Skipping GEDA row due to error: %s", e)
            continue

    fact_df = pd.DataFrame(fact_rows).reset_index(drop=True)
    if fact_df.empty:
        logging.warning("No fact rows created. Check source files and mappings.")
    else:
        fact_df.insert(0, 'fact_id', range(1, len(fact_df)+1))

    # ---------- LOAD to Postgres ----------
    logging.info("Create schema and write tables to Postgres")

    ddl = """
    DROP TABLE IF EXISTS fact_gesundheitskennzahlen CASCADE;
    DROP TABLE IF EXISTS dim_indikator CASCADE;
    DROP TABLE IF EXISTS dim_bevoelkerung CASCADE;
    DROP TABLE IF EXISTS dim_region CASCADE;
    DROP TABLE IF EXISTS dim_zeit CASCADE;
    DROP TABLE IF EXISTS dim_quelle CASCADE;

    CREATE TABLE dim_quelle (
        quelle_id INT PRIMARY KEY,
        quelle_name VARCHAR(255)
    );

    CREATE TABLE dim_zeit (
        zeit_id INT PRIMARY KEY,
        jahr INT
    );

    CREATE TABLE dim_region (
        region_id INT PRIMARY KEY,
        bundesland_name VARCHAR(255)
    );

    CREATE TABLE dim_bevoelkerung (
        demografie_id INT PRIMARY KEY,
        geschlecht VARCHAR(50),
        altersgruppe VARCHAR(50)
    );

    CREATE TABLE dim_indikator (
        indikator_id INT PRIMARY KEY,
        code VARCHAR(255),
        name VARCHAR(255),
        quelle_typ VARCHAR(50)
    );

    CREATE TABLE fact_gesundheitskennzahlen (
        fact_id INT PRIMARY KEY,
        zeit_id INT REFERENCES dim_zeit(zeit_id),
        region_id INT REFERENCES dim_region(region_id),
        demografie_id INT REFERENCES dim_bevoelkerung(demografie_id),
        indikator_id INT REFERENCES dim_indikator(indikator_id),
        wert DOUBLE PRECISION,
        ci_min DOUBLE PRECISION,
        ci_max DOUBLE PRECISION,
        datenquelle VARCHAR(100)
    );
    """

    with engine.begin() as conn:
        conn.exec_driver_sql(ddl)
        logging.info("DDL executed - tables created")

    # write dimensions in correct order
    logging.info("Write dimension tables")
    dim_quelle.to_sql('dim_quelle', engine, if_exists='append', index=False)
    dim_zeit.to_sql('dim_zeit', engine, if_exists='append', index=False)
    dim_region.to_sql('dim_region', engine, if_exists='append', index=False)
    dim_bevoelkerung.to_sql('dim_bevoelkerung', engine, if_exists='append', index=False)
    dim_indikator.to_sql('dim_indikator', engine, if_exists='append', index=False)

    logging.info("Write fact table")
    # ensure column order matches DDL
    cols = ['fact_id','zeit_id','region_id','demografie_id','indikator_id','wert','ci_min','ci_max','datenquelle']
    # fill missing columns if needed
    for c in cols:
        if c not in fact_df.columns:
            fact_df[c] = pd.NA
    fact_df = fact_df[cols]
    fact_df.to_sql('fact_gesundheitskennzahlen', engine, if_exists='append', index=False)

    logging.info("ETL completed successfully.")

if __name__ == "__main__":
    main()
