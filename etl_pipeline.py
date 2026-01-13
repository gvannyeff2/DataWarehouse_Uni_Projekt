import pandas as pd
from sqlalchemy import create_engine, text
import os
import time
import pycountry

# Konfiguration

db_user = os.environ.get('POSTGRES_USER', 'admin')
db_password = os.environ.get('POSTGRES_PASSWORD', 'password123')
db_name = os.environ.get('POSTGRES_DB', 'health_dwh')
db_port = os.environ.get('POSTGRES_PORT', '5432')
db_host = os.environ.get('DB_HOST', 'localhost')

DB_CONNECTION_STR = f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'

FILE_DIABETES = os.path.join('Datenquellen', 'Diabetes-Surveillance_Indikatoren.tsv')
FILE_GESUNDHEIT = os.path.join('Datenquellen', 'Gesundheit_in_Deutschland_aktuell_-_2019-2020-EHIS.csv')

COMBI_REGIONS = [
    'Nordost', 'Nordwest', 'Mitte-Ost', 'Mitte-West', 'Süden',
    'Ost', 'West'
]

# Mapping für GEDA-Variablen
GEDA_MAPPING = {
    'AMarztB': 'Medikamenteneinnahme (ärztlich verordnet)',
    'GVzahnsa_k': 'Mundgesundheit',
    'IAarzt14B_k': 'Inanspruchnahme: Zahnmedizinische Versorgung',
    'IAarzt1B_k': 'Inanspruchnahme: Allgemeinärztliche oder hausärztliche Versorgung',
    'IAarzt8C': 'Inanspruchnahme: Psycholog:in',
    'IAfa_k': 'Inanspruchnahme: Fachärztliche Versorgung',
    'IAnotkhs': 'Inanspruchnahme: Notaufnahme im Krankenhaus',
    'IAther2B': 'Inanspruchnahme: Physiotherapie',
    'Iakhs': 'Inanspruchnahme: Stationäre Versorgung',
    
    'Akrausch': 'Alkohol: Rauschtrinken',
    'Akrisiko_k': 'Alkohol: Riskanter Konsum',
    'RCstatE_k3': 'Rauchen: Tabakprodukte',
    'RCpass4B_k2': 'Rauchen: Passivrauchbelastung',

    'ENcolaBtgl': 'Ernährung: Täglich zuckerhaltige Erfrischungsgetränke',
    'ENobgemtgl': 'Ernährung: Täglich Obst und Gemüse',
    'ENgemDtgl': 'Ernährung: Täglich Gemüse',
    'ENobstDtgl': 'Ernährung: Täglich Obst',
    'EnsaftBtgl': 'Ernährung: Täglich Obst- oder Gemüsesaft',

    'PAadiposB': 'Körpergewicht: Adipositas',
    'PAueberB': 'Körpergewicht: Übergewicht',
    'PAnormalB': 'Körpergewicht: Normalgewicht',
    'PAunterB': 'Körpergewicht: Untergewicht',
    'KAarbeit': 'Körperliche Aktivität: Arbeitsbezogene Aktivität',
    'KAcyc1': 'Körperliche Aktivität: Fahrradfahren von Ort zu Ort',
    'KAwalk2': 'Körperliche Aktivität: Zu Fuß gehen von Ort zu Ort',
    'KAspo2': 'Körperliche Aktivität: Freizeitbezogene Aktivität',
    'KAgfmk': 'Körperliche Aktivität: Muskelkräftigung',
    'KAgfa': 'Körperliche Aktivität: Ausdaueraktivität und Muskelkräftigung',
    'KAgfaB': 'Körperliche Aktivität: Ausdaueraktivität',

    'KHBBsa12': 'Schlaganfall',
    'IAhypus_k': 'Vorsorge: Blutdruckmessung',
    'IAkfutyp4B_lz_k2': 'Vorsorge: Darmspiegelung',
    'IAkfutyp2B_lz_k': 'Vorsorge: Test auf Blut im Stuhl',
    'IAcholus_k': 'Vorsorge: Blutfettwertebestimmung',
    'IAdiabus_k': 'Vorsorge: Blutzuckermessung',
    'KHab12': 'Asthma',
    'KHalgi112': 'Allergien',
    'KHcb12B': 'Chronische Bronchitis (COPD)',
    'KHdge12': 'Arthrose',
    'KHdiabB12': 'Diabetes',
    'KHmyokhk12': 'Koronare Herzerkrankung',
    'GZmehm1_k': 'Subjektive Gesundheit',
    
    'PKPHQ8_k6': 'Depressive Symptomatik (PHQ-8)',
    'GZmehm2D_k3': 'Einschränkung durch Krankheit',
    'GZmehm3C': 'Chronische Krankheit',
}

def wait_for_db():
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
    raise Exception("Konnte keine Verbindung zur Datenbank herstellen.")

def get_iso_code(region_name):
    if region_name == 'Deutschland':
        return 'DE'
    if region_name in COMBI_REGIONS:
        return None 
    try:
        regions = pycountry.subdivisions.get(country_code='DE')
        for r in regions:
            if r.name == region_name:
                return r.code
        country = pycountry.countries.get(name=region_name)
        if country:
            return country.alpha_2
    except:
        pass
    return None

def determine_category(row):
    name = row['name']
    iso = row['iso_code']
    if name == 'Deutschland':
        return 'Land'
    if name in COMBI_REGIONS:
        return 'Kombinationsregion'
    if iso and iso.startswith('DE-'):
        return 'Bundesland'
    if iso and len(iso) == 2:
        return 'Land'
    return 'Unbekannt'

def run_etl():
    engine = wait_for_db()
    print("--- ETL PROZESS STARTEN ---")
    
    # Extrahieren
    print("Lade Daten...")
    for datei_pfad in [FILE_DIABETES, FILE_GESUNDHEIT]:
        if not os.path.exists(datei_pfad):
            raise FileNotFoundError(f"FEHLER: Datei nicht gefunden: {datei_pfad}")
        if os.path.getsize(datei_pfad) == 0:
            raise ValueError(f"FEHLER: Datei ist leer: {datei_pfad}")

    try:
        df_diab = pd.read_csv(FILE_DIABETES, sep='\t')
    except pd.errors.EmptyDataError:
        raise ValueError("Fehler beim Lesen der Diabetes TSV.")

    try:
        df_ges = pd.read_csv(FILE_GESUNDHEIT, sep=',')
    except pd.errors.EmptyDataError:
        raise ValueError("Fehler beim Lesen der Gesundheit CSV.")

    # Daten transformieren
    print("Transformiere Daten...")

    # Basis Mapping
    gender_map = {'Männlich': 'Männlich', 'Weiblich': 'Weiblich', 'Gesamt': 'Gesamt', 'Männer': 'Männlich', 'Frauen': 'Weiblich'}
    df_diab['clean_gender'] = df_diab['Geschlecht_Name'].map(gender_map).fillna('Unbekannt')
    df_ges['clean_gender'] = df_ges['Gender'].map(gender_map).fillna('Unbekannt')

    df_diab['clean_region'] = df_diab['Region_Name']
    df_ges['clean_region'] = df_ges['Bundesland']

    df_diab['clean_age'] = df_diab['Alter_Name']
    df_ges['clean_age'] = df_ges['Altersgruppe']

    df_diab['clean_edu'] = df_diab['Bildung_Casmin_Name'].fillna('Gesamt')
    df_ges['clean_edu'] = df_ges['Bildungsgruppe'].replace({'Gesamt': 'Gesamt'}).fillna('Gesamt')

    # Indikatoren Logik
    
    ## Diabetes Quelle
    df_diab['ind_name'] = df_diab['Indikator_Name']
    df_diab['ind_cat'] = "Diabetes Surveillance"
    df_diab['ind_unit'] = df_diab['Kennzahl_Name']
    df_diab['ind_desc'] = "Datenquelle: Diabetes Surveillance RKI"
    
    ## GEDA Quelle
    df_ges['ind_cat'] = "GEDA Survey"
    df_ges['ind_unit'] = "Prozent"
    
    ## Mapping anwenden
    df_ges['Variable'] = df_ges['Variable'].astype(str).str.strip()
    
    ## Name = Code (z.B. AMarztB)
    df_ges['ind_name'] = df_ges['Variable'] 
    
    ## Beschreibung -> Lesbarer Name (z.B. Medikamenteneinnahme)
    ## Falls kein Mapping existiert, schreiben wir "Code: ABC"
    df_ges['ind_desc'] = df_ges['Variable'].map(GEDA_MAPPING).fillna("Code: " + df_ges['Variable'])

    # Dimensionen
    print("Erstelle Dimensionstabellen...")
    
    all_regions = set(df_diab['clean_region']) | set(df_ges['clean_region'])
    all_regions = {r for r in all_regions if pd.notna(r)}
    dim_geo = pd.DataFrame(list(all_regions), columns=['name'])
    
    dim_geo['iso_code'] = dim_geo['name'].apply(get_iso_code)
    dim_geo['kategorie'] = dim_geo.apply(determine_category, axis=1)
    
    dim_geo = dim_geo[dim_geo['kategorie'] != 'Unbekannt']
    dim_geo.sort_values(['kategorie', 'name'], inplace=True)
    dim_geo['geographie_id'] = range(1, len(dim_geo) + 1)

    cols_bev = ['clean_gender', 'clean_age', 'clean_edu']
    dim_bev = pd.concat([df_diab[cols_bev], df_ges[cols_bev]]).drop_duplicates().reset_index(drop=True)
    dim_bev.columns = ['geschlecht', 'altersgruppe', 'bildungsgruppe']
    dim_bev['bevoelkerung_id'] = range(1, len(dim_bev) + 1)

    cols_ind = ['ind_name', 'ind_cat', 'ind_unit', 'ind_desc']
    dim_ind = pd.concat([df_diab[cols_ind], df_ges[cols_ind]]).drop_duplicates().reset_index(drop=True)
    dim_ind.columns = ['name', 'kategorie', 'einheit', 'beschreibung']
    dim_ind['indikator_id'] = range(1, len(dim_ind) + 1)

    years_diab = set(df_diab['Jahr'])
    years_ges = {2019}
    all_years = list(years_diab | years_ges)
    dim_zeit = pd.DataFrame(all_years, columns=['jahr'])
    dim_zeit['periode'] = dim_zeit['jahr'].astype(str)
    dim_zeit.loc[dim_zeit['jahr'] == 2019, 'periode'] = "2019-2020 (GEDA)"
    dim_zeit['zeit_id'] = range(1, len(dim_zeit) + 1)

    # Faktentabelle erstellen
    print("Erstelle Faktentabelle...")

    def get_fk(df_data, df_dim, left_cols, right_cols, id_col_name):
        merged = pd.merge(df_data, df_dim, left_on=left_cols, right_on=right_cols, how='left')
        return merged[id_col_name]

    df_diab_fact = pd.DataFrame()
    df_diab_fact['zeit_id'] = get_fk(df_diab, dim_zeit, ['Jahr'], ['jahr'], 'zeit_id')
    df_diab_fact['geographie_id'] = get_fk(df_diab, dim_geo, ['clean_region'], ['name'], 'geographie_id')
    df_diab_fact['bevoelkerung_id'] = get_fk(df_diab, dim_bev, ['clean_gender', 'clean_age', 'clean_edu'], ['geschlecht', 'altersgruppe', 'bildungsgruppe'], 'bevoelkerung_id')
    df_diab_fact['indikator_id'] = get_fk(df_diab, dim_ind, ['ind_name', 'ind_unit'], ['name', 'einheit'], 'indikator_id')
    df_diab_fact['wert'] = df_diab['Wert']
    df_diab_fact['ci_min'] = df_diab['Unteres_Konfidenzintervall']
    df_diab_fact['ci_max'] = df_diab['Oberes_Konfidenzintervall']
    df_diab_fact['datenquelle'] = 'Diabetes Surveillance'

    df_ges_copy = df_ges.copy()
    df_ges_copy['Jahr_Fix'] = 2019
    df_ges_fact = pd.DataFrame()
    df_ges_fact['zeit_id'] = get_fk(df_ges_copy, dim_zeit, ['Jahr_Fix'], ['jahr'], 'zeit_id')
    df_ges_fact['geographie_id'] = get_fk(df_ges_copy, dim_geo, ['clean_region'], ['name'], 'geographie_id')
    df_ges_fact['bevoelkerung_id'] = get_fk(df_ges_copy, dim_bev, ['clean_gender', 'clean_age', 'clean_edu'], ['geschlecht', 'altersgruppe', 'bildungsgruppe'], 'bevoelkerung_id')
    df_ges_fact['indikator_id'] = get_fk(df_ges_copy, dim_ind, ['ind_name', 'ind_unit'], ['name', 'einheit'], 'indikator_id')
    df_ges_fact['wert'] = df_ges['Percent']
    df_ges_fact['ci_min'] = df_ges['LowerCL']
    df_ges_fact['ci_max'] = df_ges['UpperCL']
    df_ges_fact['datenquelle'] = 'GEDA 2019/2020'

    fact_table = pd.concat([df_diab_fact, df_ges_fact], ignore_index=True)
    fact_table = fact_table.dropna(subset=['geographie_id'])
    
    fact_table.reset_index(inplace=True)
    fact_table.rename(columns={'index': 'id'}, inplace=True)
    fact_table['id'] = fact_table['id'] + 1 

    # Laden
    print("Schreibe in PostgreSQL Datenbank...")
    
    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS dim_region CASCADE"))
        conn.commit()

    dim_zeit.to_sql('dim_zeit', engine, if_exists='replace', index=False)
    dim_geo.to_sql('dim_geographie', engine, if_exists='replace', index=False)
    dim_bev.to_sql('dim_bevoelkerung', engine, if_exists='replace', index=False)
    dim_ind.to_sql('dim_indikator', engine, if_exists='replace', index=False)
    fact_table.to_sql('fakt_gesundheitskennzahlen', engine, if_exists='replace', index=False, chunksize=1000)

    print("--- ETL ERFOLGREICH ---")
    print(f"Gespeicherte Datensätze: {len(fact_table)}")

if __name__ == "__main__":
    run_etl()