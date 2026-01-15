import time
from etl_pipeline import extract, transform, load

def run_pipeline():
    """
    ETL-Strecke definiert
    Extrahieren -> Transformieren (Reinigung, usw.) -> Laden
    """
    try:
        # Extrahieren
        df_diab, df_ges = extract.extract_data()
        
        # Transformieren
        data_dict = transform.transform_data(df_diab, df_ges)
        
        # Laden
        load.load_data(data_dict)
        
    except Exception as e:
        print(f"Fehler in Pipeline: {e}")

if __name__ == "__main__":
    print("ETL Service gestartet (Auto-Aktualisierung)...")
    
    # Initialer Lauf
    run_pipeline()
    
    # Watcher Loop
    while True:
        try:
            print(f">>> Watcher aktiv - {time.ctime()}... (Prüfung alle 60s)")
            
            run_pipeline()
            
            time.sleep(60) 
        except KeyboardInterrupt:
            print("Watcher beendet.")
            break
        except Exception as e:
            print(f"Fehler im Watcher Loop: {e}")
            time.sleep(60)