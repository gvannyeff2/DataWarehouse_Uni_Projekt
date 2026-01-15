import pandas as pd
import requests
import os
from . import config # Relative import

def download_rawfile(url, local_path):
    """
    Lädt eine Datei herunter mit Fortschrittsanzeige.
    """
    try:
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        print(f"Prüfe Updates für {local_path}...")
        
        try:
            head = requests.head(url, timeout=5)
            remote_size = int(head.headers.get('content-length', 0))
        except:
            remote_size = 0

        local_size = 0
        if os.path.exists(local_path):
            local_size = os.path.getsize(local_path)
            
        if local_size != remote_size or local_size == 0:
            print(f"-> Start Download ({remote_size / 1024 / 1024:.2f} MB)...")
            response = requests.get(url, stream=True, timeout=60)
            if response.status_code == 200:
                with open(local_path, 'wb') as f:
                    downloaded = 0
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                        downloaded += len(chunk)
                        # Einfacher Log alle 5 MB damit man sieht dass es lebt
                        if downloaded % (5 * 1024 * 1024) < 8192: 
                            print(f"   ... {downloaded / 1024 / 1024:.2f} MB geladen")
                print("-> Download vollständig!")
                return True
            else:
                print(f"-> Fehler: Status {response.status_code}")
                return False
        else:
            print("-> Datei ist aktuell.")
            return False
            
    except Exception as e:
        print(f"-> Fehler beim Download: {e}")
        return False

def extract_data():
    """
    Herunterladen und Einlesen von CSV/TSV Dateien
    """
    # Rohdaten herunterladen 
    download_rawfile(config.URL_DIABETES, config.FILE_DIABETES)
    download_rawfile(config.URL_GESUNDHEIT, config.FILE_GESUNDHEIT)

    # Prüfen
    for datei_pfad in [config.FILE_DIABETES, config.FILE_GESUNDHEIT]:
        if not os.path.exists(datei_pfad) or os.path.getsize(datei_pfad) == 0:
            raise FileNotFoundError(f"FEHLER: Datei {datei_pfad} fehlt.")

    # 3. Einlesen
    try:
        print("Lade Diabetes-Daten (TSV)...")
        df_diab = pd.read_csv(config.FILE_DIABETES, sep='\t')
        print("Lade Gesundheits-Daten (CSV)...")
        df_ges = pd.read_csv(config.FILE_GESUNDHEIT, sep=',')
        return df_diab, df_ges
    except Exception as e:
        print(f"FEHLER beim Einlesen: {e}")
        raise