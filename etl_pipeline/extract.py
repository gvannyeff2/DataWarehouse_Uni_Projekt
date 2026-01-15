import pandas as pd
import requests
import os
from . import config

def download_rawfile(url, local_path):
    """
    Lädt eine Datei von einer URL herunter, wenn sie neuer ist oder lokal fehlt.
    """
    try:
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        print(f"Prüfe Updates für {local_path}...")
        
        try:
            head = requests.head(url, timeout=5)
            remote_size = int(head.headers.get('content-length', 0))
        except requests.exceptions.RequestException:
            print("Request fehlgeschlagen. Versuche, die Datei direkt herunterzuladen...")
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
                print("Herunterladen erfolgreich!")
                return True
            else:
                print(f"Fehler beim Herunterladen: Status {response.status_code}")
                return False
        else:
            print("Datei ist aktuell.")
            return False
            
    except Exception as e:
        print(f"Offline oder Fehler beim Update-Check: {e}")
        return False

def extract_data():
    """
    Lädt Updates herunter und liest die CSV/TSV Dateien ein.
    Gibt zwei DataFrames zurück: df_diab, df_ges
    """
    # Live Update versuchen
    download_rawfile(config.URL_DIABETES, config.FILE_DIABETES)
    download_rawfile(config.URL_GESUNDHEIT, config.FILE_GESUNDHEIT)

    # Prüfen ob Dateien existieren
    for datei_pfad in [config.FILE_DIABETES, config.FILE_GESUNDHEIT]:
        if not os.path.exists(datei_pfad) or os.path.getsize(datei_pfad) == 0:
            raise FileNotFoundError(f"FEHLER: Datei {datei_pfad} fehlt oder ist leer.")

    # Einlesen
    try:
        print("Lade Diabetes-Daten (TSV)...")
        df_diab = pd.read_csv(config.FILE_DIABETES, sep='\t')
        print("Lade Gesundheits-Daten (CSV)...")
        df_ges = pd.read_csv(config.FILE_GESUNDHEIT, sep=',')
        return df_diab, df_ges
    except Exception as e:
        print(f"FEHLER beim Einlesen der Daten: {e}")
        raise