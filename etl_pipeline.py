import pandas as pd
from sqlalchemy import create_engine
import os
import time

# --- 1. KONFIGURATION ---

# Flexibler Host: Wenn wir in Docker sind, nutzen wir 'db', sonst 'localhost'
db_host = os.environ.get('DB_HOST', 'localhost')
DB_CONNECTION_STR = f'postgresql+psycopg2://admin:password123@{db_host}:5432/health_dwh'

# Dateipfade (angepasst an deine Ordnerstruktur)
FILE_DIABETES = os.path.join('Datenquellen', 'Diabetes-Surveillance_Indikatoren.tsv')
FILE_GESUNDHEIT = os.path.join('Datenquellen', 'Gesundheit_in_Deutschland_aktuell_-_2019-2020-EHIS.csv')

def wait_for_db():
    """Wartet kurz, falls die DB noch hochfährt."""
    print(f"Versuche Verbindung zur Datenbank ({db_host})...")
    retries = 10
    while retries > 0:
        try:
            engine = create_engine(DB_CONNECTION_STR)
            with engine.connect() as conn:
                print("Datenbank-Verbindung erfolgreich!")
                return engine
        except Exception as e:
            print(f"Datenbank noch nicht bereit, warte 3 Sekunden... (Fehler: {e})")
            time.sleep(3)
            retries -= 1
    raise Exception("Konnte keine Verbindung zur Datenbank herstellen. Läuft der Docker Container?")

def run_etl():
    # DB Engine initialisieren
    engine = wait_for_db()

    print("--- START ETL PROZESS ---")
    
    # --- EXTRACT ---
    print(f"Lade Datei: {FILE_DIABETES}")
    df_diab = pd.read_csv(FILE_DIABETES, sep='\t')
    
    print(f"Lade Datei: {FILE_GESUNDHEIT}")
    df_ges = pd.read_csv(FILE_GESUNDHEIT, sep=',')

    # --- TRANSFORM ---
    print("Transformiere Daten...")

    # 1. Standardisierung
    gender_map = {'Männlich': 'Male', 'Weiblich': 'Female', 'Gesamt': 'Total', 'Männer': 'Male', 'Frauen': 'Female'}
    df_diab['clean_gender'] = df_diab['Geschlecht_Name'].map(gender_map).fillna('Unknown')
    df_ges['clean_gender'] = df_ges['Gender'].map(gender_map).fillna('Unknown')

    df_diab['clean_region'] = df_diab['Region_Name']
    df_ges['clean_region'] = df_ges['Bundesland']

    df_diab['clean_age'] = df_diab['Alter_Name']
    df_ges['clean_age'] = df_ges['Altersgruppe']

    # 2. Indikatoren vorbereiten
    df_diab['ind_name'] = df_diab['Indikator_Name']
    df_diab['ind_cat'] = "Diabetes Surveillance"
    df_diab['ind_unit'] = df_diab['Kennzahl_Name']
    
    df_ges['ind_name'] = df_ges['Variable']
    df_ges['ind_cat'] = "GEDA Survey"
    df_ges['ind_unit'] = "Prozent"

    # --- DIMENSIONEN ERSTELLEN ---
    
    # Dim Region
    all_regions = set(df_diab['clean_region']) | set(df_ges['clean_region'])
    dim_region = pd.DataFrame(list(all_regions), columns=['bundesland'])
    dim_region.sort_values('bundesland', inplace=True)
    dim_region['region_id'] = range(1, len(dim_region) + 1)

    # Dim Bevoelkerung
    cols_bev = ['clean_gender', 'clean_age']
    dim_bev = pd.concat([df_diab[cols_bev], df_ges[cols_bev]]).drop_duplicates().reset_index(drop=True)
    dim_bev.columns = ['geschlecht', 'altersgruppe']
    dim_bev['bevoelkerung_id'] = range(1, len(dim_bev) + 1)

    # Dim Indikator
    cols_ind = ['ind_name', 'ind_cat', 'ind_unit']
    dim_ind = pd.concat([df_diab[cols_ind], df_ges[cols_ind]]).drop_duplicates().reset_index(drop=True)
    dim_ind.columns = ['name', 'kategorie', 'einheit']
    dim_ind['beschreibung'] = "Importiert aus " + dim_ind['kategorie']
    dim_ind['indikator_id'] = range(1, len(dim_ind) + 1)

    # Dim Zeit
    years_diab = set(df_diab['Jahr'])
    years_ges = {2019}
    all_years = list(years_diab | years_ges)
    dim_zeit = pd.DataFrame(all_years, columns=['jahr'])
    dim_zeit['periode'] = dim_zeit['jahr'].astype(str)
    dim_zeit.loc[dim_zeit['jahr'] == 2019, 'periode'] = "2019-2020 (GEDA)"
    dim_zeit['zeit_id'] = range(1, len(dim_zeit) + 1)

    # --- FAKTEN ZUSAMMENBAUEN ---
    
    def get_fk(df_data, df_dim, left_cols, right_cols, id_col_name):
        merged = pd.merge(df_data, df_dim, left_on=left_cols, right_on=right_cols, how='left')
        return merged[id_col_name]

    # Diabetes Fakten
    print("Erstelle Fakten für Diabetes...")
    df_diab_fact = pd.DataFrame()
    df_diab_fact['zeit_id'] = get_fk(df_diab, dim_zeit, ['Jahr'], ['jahr'], 'zeit_id')
    df_diab_fact['region_id'] = get_fk(df_diab, dim_region, ['clean_region'], ['bundesland'], 'region_id')
    df_diab_fact['bevoelkerung_id'] = get_fk(df_diab, dim_bev, ['clean_gender', 'clean_age'], ['geschlecht', 'altersgruppe'], 'bevoelkerung_id')
    df_diab_fact['indikator_id'] = get_fk(df_diab, dim_ind, ['ind_name', 'ind_unit'], ['name', 'einheit'], 'indikator_id')
    df_diab_fact['wert'] = df_diab['Wert']
    df_diab_fact['ci_min'] = df_diab['Unteres_Konfidenzintervall']
    df_diab_fact['ci_max'] = df_diab['Oberes_Konfidenzintervall']
    df_diab_fact['datenquelle'] = 'Diabetes Surveillance'

    # GEDA Fakten
    print("Erstelle Fakten für GEDA...")
    df_ges_copy = df_ges.copy()
    df_ges_copy['Jahr_Fix'] = 2019
    
    df_ges_fact = pd.DataFrame()
    df_ges_fact['zeit_id'] = get_fk(df_ges_copy, dim_zeit, ['Jahr_Fix'], ['jahr'], 'zeit_id')
    df_ges_fact['region_id'] = get_fk(df_ges_copy, dim_region, ['clean_region'], ['bundesland'], 'region_id')
    df_ges_fact['bevoelkerung_id'] = get_fk(df_ges_copy, dim_bev, ['clean_gender', 'clean_age'], ['geschlecht', 'altersgruppe'], 'bevoelkerung_id')
    df_ges_fact['indikator_id'] = get_fk(df_ges_copy, dim_ind, ['ind_name', 'ind_unit'], ['name', 'einheit'], 'indikator_id')
    df_ges_fact['wert'] = df_ges['Percent']
    df_ges_fact['ci_min'] = df_ges['LowerCL']
    df_ges_fact['ci_max'] = df_ges['UpperCL']
    df_ges_fact['datenquelle'] = 'GEDA 2019/2020'

    # Union
    fact_table = pd.concat([df_diab_fact, df_ges_fact], ignore_index=True)

    # --- LOAD (SQLAlchemy to Postgres) ---
    print("Schreibe in PostgreSQL Datenbank...")
    
    # if_exists='replace' droppt die Tabelle und erstellt sie neu. 
    # Ideal für ETL-Tests. Später evtl. 'append'.
    dim_zeit.to_sql('dim_zeit', engine, if_exists='replace', index=False)
    dim_region.to_sql('dim_region', engine, if_exists='replace', index=False)
    dim_bev.to_sql('dim_bevoelkerung', engine, if_exists='replace', index=False)
    dim_ind.to_sql('dim_indikator', engine, if_exists='replace', index=False)
    
    # Chunksize hilft bei großen Fakten-Tabellen
    fact_table.to_sql('fakt_gesundheitskennzahlen', engine, if_exists='replace', index=False, chunksize=1000)

    print("--- ETL ERFOLGREICH ABGESCHLOSSEN ---")
    print(f"Gespeicherte Datensätze: {len(fact_table)}")

if __name__ == "__main__":
    run_etl()