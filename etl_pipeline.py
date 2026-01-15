import pandas as pd
from sqlalchemy import create_engine, text
import os
import time
import pycountry
import requests
import sys
import config

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

def get_iso_code(region_name):
    if region_name == 'Deutschland': return 'DE'
    if region_name in config.COMBI_REGIONS: return None 
    try:
        regions = pycountry.subdivisions.get(country_code='DE')
        for r in regions:
            if r.name == region_name: return r.code
        country = pycountry.countries.get(name=region_name)
        if country: return country.alpha_2
    except: pass
    return None

def determine_category(row):
    name, iso = row['name'], row['iso_code']
    if name == 'Deutschland': return 'Land'
    if name in config.COMBI_REGIONS: return 'Kombinationsregion'
    if iso and iso.startswith('DE-'): return 'Bundesland'
    if iso and len(iso) == 2: return 'Land'
    return 'Unbekannt'

def download_file_from_source(url, local_path):
    """
    Lädt Datenquellen
    """
    try:
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        print(f"Prüfe Updates für {local_path}...")
        
        try:
            head = requests.head(url, timeout=5)
            remote_size = int(head.headers.get('content-length', 0))
        except requests.exceptions.RequestException:
            print("Fehlgeschlagen. Versuche direkten Download...")
            remote_size = 0 

        local_size = 0
        if os.path.exists(local_path):
            local_size = os.path.getsize(local_path)
            
        if local_size != remote_size or local_size == 0:
            print(f"Lade herunter...")
            response = requests.get(url, stream=True, timeout=30)
            if response.status_code == 200:
                with open(local_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                print("Download erfolgreich!")
                return True
            else:
                print(f"Fehler beim Download: Status {response.status_code}")
                return False
        else:
            print("Datei ist aktuell.")
            return False
            
    except Exception as e:
        print(f"Offline oder Fehler beim Update-Check: {e}")
        return False

def run_etl():
    """
    Führt den vollständigen ETL-Prozess aus
    """
    engine = wait_for_db()
    print("START ETL PROZESS")
    
    # Live Aktualisierung
    download_file_from_source(config.URL_DIABETES, config.FILE_DIABETES)
    download_file_from_source(config.URL_GESUNDHEIT, config.FILE_GESUNDHEIT)

    # # Prüfen, ob Dateien vorhanden sind
    for datei_pfad in [config.FILE_DIABETES, config.FILE_GESUNDHEIT]:
        if not os.path.exists(datei_pfad) or os.path.getsize(datei_pfad) == 0:
            print(f"WARNUNG: Datei {datei_pfad} fehlt lokal. Breche ab.")
            return

    # Extrahieren
    try:
        df_diab = pd.read_csv(config.FILE_DIABETES, sep='\t')
        df_ges = pd.read_csv(config.FILE_GESUNDHEIT, sep=',')
    except Exception as e:
        print(f"FEHLER beim Lesen der Daten: {e}")
        return

    # Transformieren
    df_diab['clean_gender'] = df_diab['Geschlecht_Name'].map(config.GENDER_MAP).fillna('Unbekannt')
    df_ges['clean_gender'] = df_ges['Gender'].map(config.GENDER_MAP).fillna('Unbekannt')
    
    df_diab['clean_region'] = df_diab['Region_Name']
    df_ges['clean_region'] = df_ges['Bundesland']
    
    df_diab['clean_age'] = df_diab['Alter_Name']
    df_ges['clean_age'] = df_ges['Altersgruppe']
    
    df_diab['clean_edu'] = df_diab['Bildung_Casmin_Name'].fillna('Gesamt')
    df_ges['clean_edu'] = df_ges['Bildungsgruppe'].replace({'Gesamt': 'Gesamt'}).fillna('Gesamt')

    df_diab['ind_name'] = df_diab['Indikator_Name']
    df_diab['ind_cat'] = "Diabetes Surveillance"
    
    df_diab['ind_unit'] = df_diab['Kennzahl_Definition'] 
    
    df_diab['ind_desc'] = "Datenquelle: Diabetes Surveillance RKI"
    
    df_ges['ind_cat'] = "GEDA Survey"
    df_ges['ind_unit'] = "Prozent"
    df_ges['Variable'] = df_ges['Variable'].astype(str).str.strip()
    df_ges['ind_name'] = df_ges['Variable']
    df_ges['ind_desc'] = df_ges['Variable'].map(config.GEDA_MAPPING).fillna("Code: " + df_ges['Variable'])

    # Dimensionen

    ## Geographie-Dimension
    all_regions = {r for r in (set(df_diab['clean_region']) | set(df_ges['clean_region'])) if pd.notna(r)}
    dim_geo = pd.DataFrame(list(all_regions), columns=['name'])
    dim_geo['iso_code'] = dim_geo['name'].apply(get_iso_code)
    dim_geo['kategorie'] = dim_geo.apply(determine_category, axis=1)
    dim_geo = dim_geo[dim_geo['kategorie'] != 'Unbekannt']
    dim_geo.sort_values(['kategorie', 'name'], inplace=True)
    dim_geo['geographie_id'] = range(1, len(dim_geo) + 1)

    ## Bevölkerungsdimension
    cols_bev = ['clean_gender', 'clean_age', 'clean_edu']
    dim_bev = pd.concat([df_diab[cols_bev], df_ges[cols_bev]]).drop_duplicates().reset_index(drop=True)
    dim_bev.columns = ['geschlecht', 'altersgruppe', 'bildungsgruppe']
    dim_bev['bevoelkerung_id'] = range(1, len(dim_bev) + 1)

    ## Bevölkerungsdimension
    cols_ind = ['ind_name', 'ind_cat', 'ind_unit', 'ind_desc']
    dim_ind = pd.concat([df_diab[cols_ind], df_ges[cols_ind]]).drop_duplicates().reset_index(drop=True)
    dim_ind.columns = ['name', 'kategorie', 'einheit', 'beschreibung']
    dim_ind['indikator_id'] = range(1, len(dim_ind) + 1)

    ## Bevölkerungsdimension
    years_diab = set(df_diab['Jahr'])
    all_years = list(years_diab | {2019})
    dim_zeit = pd.DataFrame(all_years, columns=['jahr'])
    dim_zeit['periode'] = dim_zeit['jahr'].astype(str)
    dim_zeit.loc[dim_zeit['jahr'] == 2019, 'periode'] = "2019-2020 (GEDA)"
    dim_zeit['zeit_id'] = range(1, len(dim_zeit) + 1)

    # Faktentabelle
    def get_fk(df_data, df_dim, left_cols, right_cols, id_col_name):
        """
        Ermittelt Fremdschlüssel durch Join auf Dimensionstabellen.
        """
        return pd.merge(df_data, df_dim, left_on=left_cols, right_on=right_cols, how='left')[id_col_name]

    ## Fakten Diabetes
    df_diab_fact = pd.DataFrame()
    df_diab_fact['zeit_id'] = get_fk(df_diab, dim_zeit, ['Jahr'], ['jahr'], 'zeit_id')
    df_diab_fact['geographie_id'] = get_fk(df_diab, dim_geo, ['clean_region'], ['name'], 'geographie_id')
    df_diab_fact['bevoelkerung_id'] = get_fk(df_diab, dim_bev, ['clean_gender', 'clean_age', 'clean_edu'], ['geschlecht', 'altersgruppe', 'bildungsgruppe'], 'bevoelkerung_id')
    df_diab_fact['indikator_id'] = get_fk(df_diab, dim_ind, ['ind_name', 'ind_unit'], ['name', 'einheit'], 'indikator_id')
    df_diab_fact['wert'] = df_diab['Wert']
    df_diab_fact['datenquelle'] = 'Diabetes Surveillance'

    ## Fakten GEDA
    df_ges_copy = df_ges.copy()
    df_ges_copy['Jahr_Fix'] = 2019
    df_ges_fact = pd.DataFrame()
    df_ges_fact['zeit_id'] = get_fk(df_ges_copy, dim_zeit, ['Jahr_Fix'], ['jahr'], 'zeit_id')
    df_ges_fact['geographie_id'] = get_fk(df_ges_copy, dim_geo, ['clean_region'], ['name'], 'geographie_id')
    df_ges_fact['bevoelkerung_id'] = get_fk(df_ges_copy, dim_bev, ['clean_gender', 'clean_age', 'clean_edu'], ['geschlecht', 'altersgruppe', 'bildungsgruppe'], 'bevoelkerung_id')
    df_ges_fact['indikator_id'] = get_fk(df_ges_copy, dim_ind, ['ind_name', 'ind_unit'], ['name', 'einheit'], 'indikator_id')
    df_ges_fact['wert'] = df_ges['Percent']
    df_ges_fact['datenquelle'] = 'GEDA 2019/2020'

    ## Merge
    fact_table = pd.concat([df_diab_fact, df_ges_fact], ignore_index=True)
    fact_table = fact_table.dropna(subset=['geographie_id'])
    fact_table.reset_index(inplace=True)
    fact_table.rename(columns={'index': 'id'}, inplace=True)
    fact_table['id'] = fact_table['id'] + 1 

    # Laden
    print("Schreibe Daten in die PostgreSQL-Datenbank...")
    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS dim_region CASCADE")) 
        conn.commit()

    dim_zeit.to_sql('dim_zeit', engine, if_exists='replace', index=False)
    dim_geo.to_sql('dim_geographie', engine, if_exists='replace', index=False)
    dim_bev.to_sql('dim_bevoelkerung', engine, if_exists='replace', index=False)
    dim_ind.to_sql('dim_indikator', engine, if_exists='replace', index=False)
    fact_table.to_sql('fakt_gesundheitskennzahlen', engine, if_exists='replace', index=False, chunksize=1000)
    
    print("ETL ERFOLGREICH ABGESCHLOSSEN")

if __name__ == "__main__":
    """
    Endlosschleife zur regelmäßigen Aktualisierung der Daten (Docker-Container).
    """
    print(" ETL Service gestartet (mit Auto-Update).")
    
    while True:
        try:
            print(f"Prüfe auf Updates - {time.ctime()}...")
            run_etl()
            print("Schlafe 60 Sekunden...")
            time.sleep(60) 
        except KeyboardInterrupt:
            print("Watcher beendet.")
            break
        except Exception as e:
            print(f"Fehler im Watcher Loop: {e}")
            time.sleep(60)