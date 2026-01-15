import time, sys
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
    try:
        run_pipeline()
        sys.exit(0)   
    except Exception as e:
        print(f"ETL Fehler: {e}")
        sys.exit(1) 
