import os
import sys

# Datenbank Konfiguration
DB_USER = os.environ.get('POSTGRES_USER')
DB_PASSWORD = os.environ.get('POSTGRES_PASSWORD')
DB_NAME = os.environ.get('POSTGRES_DB')
DB_PORT = os.environ.get('POSTGRES_PORT', '5432')
DB_HOST = os.environ.get('DB_HOST', 'db')

if not DB_USER or not DB_PASSWORD or not DB_NAME:
    print("Fehler: Datenbankzugangsdaten (USER, PASSWORD, DB) fehlen")
    sys.exit(1)

DB_CONNECTION_STR = f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

# Dateipfad-
DATA_DIR = 'Datenquellen'
FILE_DIABETES = os.path.join(DATA_DIR, 'Diabetes-Surveillance_Indikatoren.tsv')
FILE_GESUNDHEIT = os.path.join(DATA_DIR, 'Gesundheit_in_Deutschland_aktuell_-_2019-2020-EHIS.csv')

# Externe URLs
URL_DIABETES = os.environ.get('URL_DIABETES')
URL_GESUNDHEIT = os.environ.get('URL_GESUNDHEIT')


# Mapping und Liste
COMBI_REGIONS = [
    'Nordost', 'Nordwest', 'Mitte-Ost', 'Mitte-West', 'Süden',
    'Ost', 'West'
]

GENDER_MAP = {
    'Männlich': 'Männlich', 'Weiblich': 'Weiblich', 'Gesamt': 'Gesamt', 
    'Männer': 'Männlich', 'Frauen': 'Weiblich'
}

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
    'KAgfkaB': 'Körperliche Aktivität: Ausdaueraktivität',
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