import pandas as pd
import pycountry
import re
from . import config

def get_iso_code(region_name):
    """
    Ermittelt ISO-Code, wenn ein Name für Regionen oder Deutschland vorhanden ist
    """
    if pd.isna(region_name):
        return None

    region_name = str(region_name).strip()

    if region_name == 'Deutschland':
        return 'DE'

    if region_name in config.COMBI_REGIONS:
        return None

    try:
        regions = pycountry.subdivisions.get(country_code='DE')
        for r in regions:
            if r.name == region_name:
                return r.code
    except Exception:
        pass
    return None


def determine_geographie(row):
    """
    Geographie klassifizieren, also DE, Kombi-Regionen, DE Region
    """
    name, iso = row['name'], row['iso_code']
    if pd.isna(name):
        return 'Unbekannt'

    name = str(name).strip()
    if name == 'Deutschland':
        return 'Land'
    if name in config.COMBI_REGIONS:
        return 'Kombinationsregion'
    if iso and isinstance(iso, str) and iso.startswith('DE-'):
        return 'Bundesland'
    if iso and isinstance(iso, str) and len(iso) == 2:
        return 'Land'
    return 'Unbekannt'


def get_geo_description(row):
    """
    Fügt für Kombinationsregionen die Liste der Bundesländer hinzu.
    """
    if row.get('kategorie') == 'Kombinationsregion':
        return config.COMBI_REGION_DESCRIPTION.get(row.get('name'), "")
    return ""

_geda_age_re = re.compile(r'^\s*(\d+)\s*-\s*(\d+)\s*Jahre\s*$', flags=re.IGNORECASE)
_geda_age_plus_re = re.compile(r'^\s*(\d+)\s*\+?\s*(?:Jahre)?\s*$', flags=re.IGNORECASE)

def normalize_geda_age(age_str):
    """
    Mappt GEDA-Altersgruppen, z.B. '18 - 29 Jahre' -> '18-29'
    """
    if pd.isna(age_str):
        return None
    s = str(age_str).strip()
    if s.lower() == 'gesamt':
        return 'Gesamt'

    m = _geda_age_re.match(s)
    if m:
        return f"{int(m.group(1))}-{int(m.group(2))}"  # z.B. 18-29
    # z.B. "65+" or "65 + Jahre"
    if '+' in s:
        digits = re.findall(r'\d+', s)
        if digits:
            return f"{int(digits[0])}+"
    return s


def normalize_edu(value):
    """
    Bildung standardisieren
    """
    if pd.isna(value):
        return None
    s = str(value).strip()
    if s.lower() == 'gesamt':
        return 'Gesamt'
    # normalize case
    mapping = {
        'Untere': 'untere',
        'Mittlere': 'mittlere',
        'Obere': 'obere',
        'untere': 'untere',
        'mittlere': 'mittlere',
        'obere': 'obere',
        'mittlere/obere': 'mittlere/obere',
        'Mittlere/Obere': 'mittlere/obere',
    }
    return mapping.get(s, s)


def diabetes_keep_row(row):
    """
    Diabetes: Zeilenfilter für 18+ Vergleichbarkeit mit GEDA.
    Lebensphase_ID = 0 behalten (also Erwachsene). Der Rest wurde weggeworfen
    """
    lp = row.get('Lebensphase_ID')
    age = row.get('Alter_ID')

    if pd.isna(lp):
        return False

    try:
        lp = int(lp)
    except Exception:
        return False

    # Kinder/Jugendliche
    if lp == 1:
        return False

    # Erwachsene: Alter kann '00+' sein (= Gesamt Erwachsene)
    if lp == 0:
        if pd.isna(age):
            return True
        age = str(age).strip()
        if age == "00+":
            return True
        return is_adult_age(age)

    # Alle Altersgruppen: nur 18+ Altersgruppen behalten
    if lp == 2:
        if pd.isna(age):
            return False
        age = str(age).strip()
        if age == "00+":
            return False
        return is_adult_age(age)

    return False


def is_adult_age(age_id):
    """
    True, wenn die Altersgruppe ab 18 beginnt (z.B. 18-29, 65+).
    """
    if pd.isna(age_id):
        return False

    age_id = str(age_id).strip()

    if age_id == "00+":
        return False

    # z.B. "75+"
    if age_id.endswith("+"):
        try:
            lower = int(age_id[:-1])
            return lower >= 18
        except Exception:
            return False

    # z.B. "18-29"
    if "-" in age_id:
        try:
            lower = int(age_id.split("-")[0])
            return lower >= 18
        except Exception:
            return False

    return False


def diabetes_clean_age(row):
    """
    Harmonisiert Diabetes-Alter
    """
    age = row.get('Alter_ID')
    lp = row.get('Lebensphase_ID')

    if pd.isna(age):
        return None

    age = str(age).strip()

    try:
        lp = int(lp)
    except Exception:
        lp = None

    if age == "00+" and lp == 0:
        return "Gesamt"
    return age

# Daten transformieren
def transform_data(df_diab, df_ges):
    """
    Führt alle Transformationen durch und erstellt die Dimensionen und Fakten.
    """
    print("Daten transformieren...")

    # Diabetes Daten reinigen und filtern
    # Lebensphase_ID numerisch
    if 'Lebensphase_ID' in df_diab.columns:
        df_diab = df_diab.copy()
        df_diab['Lebensphase_ID'] = pd.to_numeric(df_diab['Lebensphase_ID'], errors='coerce')
        print(f"Diabetes Zeilen (raw): {len(df_diab)}")
        df_diab = df_diab[df_diab.apply(diabetes_keep_row, axis=1)].copy()
        print(f"Diabetes Zeilen (nach 18+ / Adults-Logik): {len(df_diab)}")
    else:
        print("WARNUNG: Spalte 'Lebensphase_ID' nicht gefunden! Diabetes wird nicht altersgefiltert.")
        df_diab = df_diab.copy()

    df_diab['clean_gender'] = df_diab['Geschlecht_Name'].map(config.GENDER_MAP).fillna('Unbekannt')
    df_diab['clean_region'] = df_diab['Region_Name'].astype(str).str.strip()
    df_diab['clean_age'] = df_diab.apply(diabetes_clean_age, axis=1)
    df_diab['clean_edu'] = df_diab['Bildung_Casmin_Name'].apply(normalize_edu)

    # Nur Deutschland / Bundesländer / Kombinationsregionen
    print(f"Diabetes Zeilen (vor Geo-Filter): {len(df_diab)}")
    mask_german = df_diab['clean_region'].apply(
        lambda x: (get_iso_code(x) is not None) or (x in config.COMBI_REGIONS)
    )
    df_diab = df_diab[mask_german].copy()
    print(f"Diabetes Zeilen (nach Geo-Filter nur DE inkl. Kombi): {len(df_diab)}")

    if 'Wert' in df_diab.columns:
        df_diab['Wert'] = pd.to_numeric(df_diab['Wert'], errors='coerce')

    # GEDA Daten filtern und reinigen
    df_ges = df_ges.copy()
    df_ges['clean_gender'] = df_ges['Gender'].map(config.GENDER_MAP).fillna('Unbekannt')
    df_ges['clean_region'] = df_ges['Bundesland'].astype(str).str.strip()
    df_ges['clean_age'] = df_ges['Altersgruppe'].apply(normalize_geda_age)
    df_ges['clean_edu'] = df_ges['Bildungsgruppe'].apply(normalize_edu)

    id_cols = ['Variable', 'clean_gender', 'clean_region', 'clean_age', 'clean_edu']
    df_ges_melted = df_ges.melt(
        id_vars=id_cols,
        value_vars=['Percent', 'Frequency'],
        var_name='einheit',
        value_name='wert'
    )

    df_ges_melted['wert'] = pd.to_numeric(df_ges_melted['wert'], errors='coerce')

    # Zeit als Periode 2019-2020 anstatt von Jahr
    df_ges_melted['periode'] = '2019-2020'
    df_ges_melted['jahr'] = pd.NA 

    # Dimensionen

    # Dim Indikator
    ## Diabetes
    df_diab_ind = df_diab[['Indikator_Name', 'Kennzahl_Definition', 'Handlungsfeld_Name']].drop_duplicates().copy()
    df_diab_ind.columns = ['name', 'einheit', 'hf_raw']
    df_diab_ind['handlungsfeld'] = df_diab_ind['hf_raw'].map(config.SHORT_INDIKATOR_MAPPING).fillna(df_diab_ind['hf_raw'])
    df_diab_ind['datenquellen'] = 'Diabetes Surveillance'

    ## GEDA
    df_ges_ind = df_ges_melted[['Variable', 'einheit']].drop_duplicates().copy()
    df_ges_ind['name'] = df_ges_ind['Variable'].apply(lambda x: config.GEDA_MAPPING.get(x, {}).get('name', x))
    df_ges_ind['handlungsfeld'] = df_ges_ind['Variable'].apply(lambda x: config.GEDA_MAPPING.get(x, {}).get('cat'))
    df_ges_ind['datenquellen'] = 'GEDA Survey'

    dim_ind = pd.concat([
        df_diab_ind[['name', 'handlungsfeld', 'einheit', 'datenquellen']],
        df_ges_ind[['name', 'handlungsfeld', 'einheit', 'datenquellen']]
    ], ignore_index=True).drop_duplicates().reset_index(drop=True)
    dim_ind['indikator_id'] = range(1, len(dim_ind) + 1)

    # Dim Geographie 
    all_regions = set(df_diab['clean_region'].dropna()) | set(df_ges['clean_region'].dropna())
    dim_geo = pd.DataFrame({"name": sorted(list(all_regions))})
    dim_geo['iso_code'] = dim_geo['name'].apply(get_iso_code)
    dim_geo['kategorie'] = dim_geo.apply(determine_geographie, axis=1)
    dim_geo['beschreibung'] = dim_geo.apply(get_geo_description, axis=1)
    dim_geo['geographie_id'] = range(1, len(dim_geo) + 1)

    # Dim Bevölkerung
    cols_bev = ['clean_gender', 'clean_age', 'clean_edu']
    dim_bev = pd.concat([df_diab[cols_bev], df_ges[cols_bev]], ignore_index=True).drop_duplicates().reset_index(drop=True)
    dim_bev.columns = ['geschlecht', 'altersgruppe', 'bildungsgruppe']
    dim_bev['bevoelkerung_id'] = range(1, len(dim_bev) + 1)

    # Dim Zeit
    years_diab = sorted(set(pd.to_numeric(df_diab['Jahr'], errors='coerce').dropna().astype(int)))
    periods = ['2019-2020']  # aktuell nur GEDA
    dim_zeit = pd.DataFrame({
        "jahr": years_diab + [pd.NA] * len(periods),
        "periode": [pd.NA] * len(years_diab) + periods
    })
    dim_zeit['zeit_id'] = range(1, len(dim_zeit) + 1)

    # Faktentabelle
    def get_fk(df_data, df_dim, left_cols, right_cols, id_col_name):
        merged = df_data.merge(df_dim, left_on=left_cols, right_on=right_cols, how='left')
        return merged[id_col_name]

    ## Faktentabelle Diabetes
    fact_diabetes = pd.DataFrame()
    fact_diabetes['zeit_id'] = get_fk(df_diab, dim_zeit, ['Jahr'], ['jahr'], 'zeit_id')
    fact_diabetes['geographie_id'] = get_fk(df_diab, dim_geo, ['clean_region'], ['name'], 'geographie_id')
    fact_diabetes['bevoelkerung_id'] = get_fk(
        df_diab, dim_bev,
        ['clean_gender', 'clean_age', 'clean_edu'],
        ['geschlecht', 'altersgruppe', 'bildungsgruppe'],
        'bevoelkerung_id'
    )
    fact_diabetes['indikator_id'] = get_fk(df_diab, dim_ind, ['Indikator_Name', 'Kennzahl_Definition'], ['name', 'einheit'], 'indikator_id')
    fact_diabetes['wert'] = df_diab['Wert']
    fact_diabetes['id'] = range(1, len(fact_diabetes) + 1)

    ## Faktentabelle GEDA
    df_ges_melted['mapped_name'] = df_ges_melted['Variable'].apply(lambda x: config.GEDA_MAPPING.get(x, {}).get('name', x))

    fact_geda = pd.DataFrame()
    fact_geda['zeit_id'] = get_fk(df_ges_melted, dim_zeit, ['jahr', 'periode'], ['jahr', 'periode'], 'zeit_id')
    fact_geda['geographie_id'] = get_fk(df_ges_melted, dim_geo, ['clean_region'], ['name'], 'geographie_id')
    fact_geda['bevoelkerung_id'] = get_fk(
        df_ges_melted, dim_bev,
        ['clean_gender', 'clean_age', 'clean_edu'],
        ['geschlecht', 'altersgruppe', 'bildungsgruppe'],
        'bevoelkerung_id'
    )
    fact_geda['indikator_id'] = get_fk(df_ges_melted, dim_ind, ['mapped_name', 'einheit'], ['name', 'einheit'], 'indikator_id')
    fact_geda['wert'] = df_ges_melted['wert']
    fact_geda['id'] = range(1, len(fact_geda) + 1)

    for fact_name, fact_df in [('fact_diabetes', fact_diabetes), ('fact_geda', fact_geda)]:
        for col in ['zeit_id', 'geographie_id', 'bevoelkerung_id', 'indikator_id']:
            missing = fact_df[col].isna().sum()
            if missing:
                print(f"WARNUNG: {fact_name} hat {missing} fehlende FK(s) in {col}.")

    return {
        'dim_zeit': dim_zeit,
        'dim_geo': dim_geo,
        'dim_bev': dim_bev,
        'dim_ind': dim_ind,
        'fact_diabetes': fact_diabetes,
        'fact_geda': fact_geda,
    }
