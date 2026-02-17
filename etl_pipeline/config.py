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

## Kombination von Regionen in DE
COMBI_REGIONS = [
    'Nordost', 'Nordwest', 'Mitte-Ost', 'Mitte-West', 'Süden',
    'Ost', 'West'
]

COMBI_REGION_DESCRIPTION = {
    'Nordost': 'Berlin, Brandenburg, Mecklenburg-Vorpommern',
    'Nordwest': 'Schleswig-Holstein, Hamburg, Niedersachsen, Bremen',
    'Mitte-Ost': 'Sachsen, Sachsen-Anhalt, Thüringen',
    'Mitte-West': 'Nordrhein-Westfalen, Hessen, Rheinland-Pfalz, Saarland',
    'Süden': 'Baden-Württemberg, Bayern',
    'Ost': 'Berlin, Brandenburg, Mecklenburg-Vorpommern, Sachsen, Sachsen-Anhalt, Thüringen',
    'West': 'Baden-Württemberg, Bayern, Bremen, Hamburg, Hessen, Niedersachsen, Nordrhein-Westfalen, Rheinland-Pfalz, Saarland, Schleswig-Holstein'
}

## Geschlecht
GENDER_MAP = {
    'Männlich': 'Männlich', 'Weiblich': 'Weiblich', 'Gesamt': 'Gesamt', 
    'Männer': 'Männlich', 'Frauen': 'Weiblich'
}

## Mapping für Indikatoren (basiert auf Diabetes Survelliance)
SHORT_INDIKATOR_MAPPING = {
    'Handlungsfeld 1 - Diabetesrisiko reduzieren': 'Risiko',
    'Handlungsfeld 2 - Diabetesfrüherkennung und -behandlung verbessern': 'Versorgung',
    'Handlungsfeld 3 - Diabeteskomplikationen reduzieren': 'Erkrankung',
    'Handlungsfeld 4 - Krankheitslast und Krankheitskosten senken': 'Lebensqualität',
}

## Gesundheitsindikatoren aus GEDA
GEDA_MAPPING = {
    ### Handlungsfeld 1 - Diabetesrisiko reduzieren
    'Akrausch': {
        'name': 'Alkohol: Rauschtrinken', 
        'cat': 'Risiko',
    },
    'Akrisiko_k': {
        'name': 'Alkohol: Riskanter Konsum',
        'cat': 'Risiko',
    },
    'RCstatE_k3': {
        'name': 'Rauchen: Tabakprodukte',
        'cat': 'Risiko',
    },
    'RCpass4B_k2': {
        'name': 'Rauchen: Passivrauchbelastung',
        'cat': 'Risiko',
    },
    'ENcolaBtgl': {
        'name': 'Ernährung: Täglich zuckerhaltige Erfrischungsgetränke',
        'cat': 'Risiko',
    },
    'ENobgemtgl': {
        'name': 'Ernährung: Täglich Obst und Gemüse',
        'cat': 'Risiko',
    },
    'ENgemDtgl': {
        'name': 'Ernährung: Täglich Gemüse',
        'cat': 'Risiko',
    },
    'ENobstDtgl': {
        'name': 'Ernährung: Täglich Obst',
        'cat': 'Risiko',
    },
    'EnsaftBtgl': {
        'name': 'Ernährung: Täglich Obst- oder Gemüsesaft',
        'cat': 'Risiko',
    },
    'PAadiposB': {
        'name': 'Körpergewicht: Adipositas',
        'cat': 'Risiko',
    },
    'PAueberB': {
        'name': 'Körpergewicht: Übergewicht',
        'cat': 'Risiko',
    },
    'PAnormalB': {
        'name': 'Körpergewicht: Normalgewicht',
        'cat': 'Risiko',
    },
    'PAunterB': {
        'name': 'Körpergewicht: Untergewicht',
        'cat': 'Risiko',
    },
    'KAarbeit': {
        'name': 'Körperliche Aktivität: Arbeitsbezogene Aktivität',
        'cat': 'Risiko',
    },
    'KAcyc1': {
        'name': 'Körperliche Aktivität: Fahrradfahren von Ort zu Ort',
        'cat': 'Risiko',
    },
    'KAwalk2': {
        'name': 'Körperliche Aktivität: Zu Fuß gehen von Ort zu Ort',
        'cat': 'Risiko',
    },
    'KAspo2': {
        'name': 'Körperliche Aktivität: Freizeitbezogene Aktivität',
        'cat': 'Risiko',
    },
    'KAgfmk': {
        'name': 'Körperliche Aktivität: Muskelkräftigung',
        'cat': 'Risiko',
    },
    'KAgfa': {
        'name': 'Körperliche Aktivität: Ausdaueraktivität und Muskelkräftigung',
        'cat': 'Risiko',
    },
    'KAgfkaB': {
        'name': 'Körperliche Aktivität: Ausdaueraktivität',
        'cat': 'Risiko',
    },

    # --- Handlungsfeld 2: Versorgung ---
    'AMarztB': {
        'name': 'Medikamenteneinnahme (ärztlich verordnet)',
        'cat': 'Versorgung',
    },
    'IAhypus_k': {
        'name': 'Vorsorge: Blutdruckmessung',
        'cat': 'Versorgung',
    },
    'IAkfutyp4B_lz_k2': {
        'name': 'Vorsorge: Darmspiegelung',
        'cat': 'Versorgung',
    },
    'IAkfutyp2B_lz_k': {
        'name': 'Vorsorge: Test auf Blut im Stuhl',
        'cat': 'Versorgung',
    },
    'IAcholus_k': {
        'name': 'Vorsorge: Blutfettwertebestimmung',
        'cat': 'Versorgung',
    },
    'IAdiabus_k': {
        'name': 'Vorsorge: Blutzuckermessung',
        'cat': 'Versorgung',
    },
    'IAarzt14B_k': {
        'name': 'Inanspruchnahme: Zahnmedizinische Versorgung',
        'cat': 'Versorgung',
    },
    'IAarzt1B_k': {
        'name': 'Inanspruchnahme: Allgemeinärztliche oder hausärztliche Versorgung',
        'cat': 'Versorgung',
    },
    'IAarzt8C': {
        'name': 'Inanspruchnahme: Psycholog:in',
        'cat': 'Versorgung',
    },
    'IAfa_k': {
        'name': 'Inanspruchnahme: Fachärztliche Versorgung',
        'cat': 'Versorgung',
    },
    'IAnotkhs': {
        'name': 'Inanspruchnahme: Notaufnahme im Krankenhaus',
        'cat': 'Versorgung',
    },
    'IAther2B': {
        'name': 'Inanspruchnahme: Physiotherapie',
        'cat': 'Versorgung',
    },
    'Iakhs': {
        'name': 'Inanspruchnahme: Stationäre Versorgung',
        'cat': 'Versorgung',
    },

    # --- Handlungsfeld 3: Erkrankung ---
    'KHdiabB12': {
        'name': 'Diagnose: Diabetes (12-Monats-Prävalenz)',
        'cat': 'Erkrankung',
    },
    'KHab12': {
        'name': 'Diagnose: Asthma',
        'cat': 'Erkrankung',
    },
    'KHalgi112': {
        'name': 'Diagnose: Allergien',
        'cat': 'Erkrankung',
    },
    'KHBBsa12': {
        'name': 'Diagnose: Schlaganfall',
        'cat': 'Erkrankung',
    },
    'KHcb12B': {
        'name': 'Diagnose: Chronische Bronchitis (COPD)',
        'cat': 'Erkrankung',
    },
    'KHdge12': {
        'name': 'Diagnose: Arthrose',
        'cat': 'Erkrankung',
    },
    'KHmyokhk12': {
        'name': 'Diagnose: Koronare Herzerkrankung',
        'cat': 'Erkrankung',
    },
    'PKPHQ8_k6': {
        'name': 'Depressive Symptomatik (PHQ-8)',
        'cat': 'Erkrankung',
    },

    # --- Handlungsfeld 4: Lebensqualität ---
    'GZmehm1_k': {
        'name': 'Status: Subjektive Gesundheit',
        'cat': 'Lebensqualität',
    },
    'GZmehm2D_k3': {
        'name': 'Status: Einschränkung durch Krankheit',
        'cat': 'Lebensqualität',
    },
    'GZmehm3C': {
        'name': 'Status: Vorliegen einer chronischen Krankheit',
        'cat': 'Lebensqualität',
    },
    'GVzahnsa_k': {
        'name': 'Status: Mundgesundheit',
        'cat': 'Lebensqualität',
    },
}
