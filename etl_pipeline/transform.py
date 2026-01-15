import pandas as pd
import pycountry
from . import config

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

def transform_data(df_diab, df_ges):
    """
    Führt alle Transformationen durch und erstellt die Dimensionen und Fakten.
    """
    print("Transformiere Daten...")

    # REINIGUNG UND MAPPING
    ## Diabetes Daten
    df_diab['clean_gender'] = df_diab['Geschlecht_Name'].map(config.GENDER_MAP).fillna('Unbekannt')
    df_diab['clean_region'] = df_diab['Region_Name']
    df_diab['clean_age'] = df_diab['Alter_Name']
    df_diab['clean_edu'] = df_diab['Bildung_Casmin_Name'].fillna('Gesamt')
    
    df_diab['ind_name'] = df_diab['Indikator_Name']
    df_diab['ind_cat'] = "Diabetes Surveillance"
    df_diab['ind_unit'] = df_diab['Kennzahl_Definition'] # Fix: Einheit aus Definition
    df_diab['ind_desc'] = "Datenquelle: Diabetes Surveillance RKI"

    ## GEDA Daten
    df_ges['clean_gender'] = df_ges['Gender'].map(config.GENDER_MAP).fillna('Unbekannt')
    df_ges['clean_region'] = df_ges['Bundesland']
    df_ges['clean_age'] = df_ges['Altersgruppe']
    df_ges['clean_edu'] = df_ges['Bildungsgruppe'].replace({'Gesamt': 'Gesamt'}).fillna('Gesamt')

    df_ges['ind_cat'] = "GEDA Survey"
    df_ges['ind_unit'] = "Prozent"
    df_ges['Variable'] = df_ges['Variable'].astype(str).str.strip()
    df_ges['ind_name'] = df_ges['Variable']
    df_ges['ind_desc'] = df_ges['Variable'].map(config.GEDA_MAPPING).fillna("Code: " + df_ges['Variable'])

    # DIMENSIONEN ERSTELLEN
    
    ## Dim Geographie
    all_regions = {r for r in (set(df_diab['clean_region']) | set(df_ges['clean_region'])) if pd.notna(r)}
    dim_geo = pd.DataFrame(list(all_regions), columns=['name'])
    dim_geo['iso_code'] = dim_geo['name'].apply(get_iso_code)
    dim_geo['kategorie'] = dim_geo.apply(determine_category, axis=1)
    dim_geo = dim_geo[dim_geo['kategorie'] != 'Unbekannt'].copy()
    dim_geo.sort_values(['kategorie', 'name'], inplace=True)
    dim_geo['geographie_id'] = range(1, len(dim_geo) + 1)

    ## Dim Bevölkerung
    cols_bev = ['clean_gender', 'clean_age', 'clean_edu']
    dim_bev = pd.concat([df_diab[cols_bev], df_ges[cols_bev]]).drop_duplicates().reset_index(drop=True)
    dim_bev.columns = ['geschlecht', 'altersgruppe', 'bildungsgruppe']
    dim_bev['bevoelkerung_id'] = range(1, len(dim_bev) + 1)

    ## Dim Indikator
    cols_ind = ['ind_name', 'ind_cat', 'ind_unit', 'ind_desc']
    dim_ind = pd.concat([df_diab[cols_ind], df_ges[cols_ind]]).drop_duplicates().reset_index(drop=True)
    dim_ind.columns = ['name', 'kategorie', 'einheit', 'beschreibung']
    dim_ind['indikator_id'] = range(1, len(dim_ind) + 1)

    ## Dim Zeit
    years_diab = set(df_diab['Jahr'])
    all_years = list(years_diab | {2019})
    dim_zeit = pd.DataFrame(all_years, columns=['jahr'])
    dim_zeit['periode'] = dim_zeit['jahr'].astype(str)
    dim_zeit.loc[dim_zeit['jahr'] == 2019, 'periode'] = "2019-2020 (GEDA)"
    dim_zeit['zeit_id'] = range(1, len(dim_zeit) + 1)

    # FAKTEN ZUSAMMENBAUEN
    def get_fk(df_data, df_dim, left_cols, right_cols, id_col_name):
        return pd.merge(df_data, df_dim, left_on=left_cols, right_on=right_cols, how='left')[id_col_name]

    ## Fakt Diabetes
    df_diab_fact = pd.DataFrame()
    df_diab_fact['zeit_id'] = get_fk(df_diab, dim_zeit, ['Jahr'], ['jahr'], 'zeit_id')
    df_diab_fact['geographie_id'] = get_fk(df_diab, dim_geo, ['clean_region'], ['name'], 'geographie_id')
    df_diab_fact['bevoelkerung_id'] = get_fk(df_diab, dim_bev, ['clean_gender', 'clean_age', 'clean_edu'], ['geschlecht', 'altersgruppe', 'bildungsgruppe'], 'bevoelkerung_id')
    df_diab_fact['indikator_id'] = get_fk(df_diab, dim_ind, ['ind_name', 'ind_unit'], ['name', 'einheit'], 'indikator_id')
    df_diab_fact['wert'] = df_diab['Wert']
    df_diab_fact['datenquelle'] = 'Diabetes Surveillance'

    ## Fakt GEDA
    df_ges_copy = df_ges.copy()
    df_ges_copy['Jahr_Fix'] = 2019
    df_ges_fact = pd.DataFrame()
    df_ges_fact['zeit_id'] = get_fk(df_ges_copy, dim_zeit, ['Jahr_Fix'], ['jahr'], 'zeit_id')
    df_ges_fact['geographie_id'] = get_fk(df_ges_copy, dim_geo, ['clean_region'], ['name'], 'geographie_id')
    df_ges_fact['bevoelkerung_id'] = get_fk(df_ges_copy, dim_bev, ['clean_gender', 'clean_age', 'clean_edu'], ['geschlecht', 'altersgruppe', 'bildungsgruppe'], 'bevoelkerung_id')
    df_ges_fact['indikator_id'] = get_fk(df_ges_copy, dim_ind, ['ind_name', 'ind_unit'], ['name', 'einheit'], 'indikator_id')
    df_ges_fact['wert'] = df_ges['Percent']
    df_ges_fact['datenquelle'] = 'GEDA 2019/2020'

    ## Zusammenbauen und bereinigen
    fact_table = pd.concat([df_diab_fact, df_ges_fact], ignore_index=True)
    fact_table = fact_table.dropna(subset=['geographie_id'])
    fact_table.reset_index(inplace=True)
    fact_table.rename(columns={'index': 'id'}, inplace=True)
    fact_table['id'] = fact_table['id'] + 1 

    return {
        'dim_zeit': dim_zeit,
        'dim_geo': dim_geo,
        'dim_bev': dim_bev,
        'dim_ind': dim_ind,
        'fact_table': fact_table
    }