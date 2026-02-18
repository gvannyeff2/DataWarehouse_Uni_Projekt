# Data-Warehouse-Projekt (Universitätsprojekt)

Dieses Repository enthält ein Data-Warehouse-Projekt, das im Rahmen eines Universitätskurses im Masterstudium **Medical Data Science** entwickelt wurde.

> Hinweis: Das Projekt dient **ausschließlich akademischen Zwecken** (Lern-/Demoprojekt).

---
## Projektziel

Ziel des Projekts ist die Entwicklung eines Data Warehouses zur Analyse diabetesbezogener Gesundheitsindikatoren in Deutschland.

Konkret wurden:
- Zwei medizinische Datenquellen integriert
- Konzeption eines Data-Warehouse-Schemas (Star Schema)  
- Implementierung einer automatisierten ETL-Pipeline  
- Speicherung der Daten in einer PostgreSQL-Datenbank  
- Explorative Analysen und Visualisierungen mit Power BI erstellt

## Fachlicher Analysefokus

Im Mittelpunkt des Projekts steht die explorative Analyse möglicher Zusammenhänge zwischen:

- Diabetesprävalenz
- Lebensstilfaktoren (z.B. körperliche Inaktivität, Adipositas)
- Screening-Verhalten
- diabetesbezogenen Komplikationen (z.B. Amputationsraten)
- soziodemografischen Merkmalen (Geschlecht, Altersgruppe, Bildung)
- regionalen Unterschieden (Bundesländer)

Die Daten wurden harmonisiert, aggregiert und so strukturiert, dass analytische Fragestellungen über mehrere Dimensionen hinweg möglich sind.

Beispielhafte Analysefragen:

- Gibt es geschlechtsspezifische Unterschiede in der Diabetesprävalenz?
- Besteht ein Zusammenhang zwischen Lebensstilindikatoren und Komplikationsraten?
- Unterscheiden sich Screening-Raten regional?
- Lassen sich Korrelationen zwischen Risikofaktoren und Versorgungsindikatoren erkennen?

## Datenquellen
Die verwendeten Rohdaten stammen aus öffentlich zugänglichen Repositorien des **Robert Koch-Institut (RKI)**:

1. Gesundheit in Deutschland Aktuell (GEDA)

    Quelle: [https://github.com/robert-koch-institut/Gesundheit_in_Deutschland_Aktuell](https://github.com/robert-koch-institut/Gesundheit_in_Deutschland_Aktuell)

    Lizenz: Creative Commons Attribution 4.0 International (CC BY 4.0) [https://creativecommons.org/licenses/by/4.0/](https://creativecommons.org/licenses/by/4.0/)

2. Diabetes-Surveillance

    Quelle: [https://github.com/robert-koch-institut/Diabetes-Surveillance](https://github.com/robert-koch-institut/Diabetes-Surveillance)
    
    Lizenz: Creative Commons Attribution 4.0 International (CC BY 4.0) [https://creativecommons.org/licenses/by/4.0/](https://creativecommons.org/licenses/by/4.0/)

*Die inhaltliche Verantwortung für die Originaldaten liegt beim Robert Koch-Institut (RKI).*

Im Rahmen dieses Projekts wurden die Daten extrahiert, transformiert und in ein eigenes Data-Warehouse-Schema überführt (ETL-Prozess). Dabei erfolgten:

- Bereinigung inkonsistenter Werte
- Harmonisierung von Altersdefinitionen
- Vereinheitlichung von Geschlechtsangaben
- Aggregationen auf Bundeslandebene
- Berechnung zusätzlicher Analysekennzahlen (z.B. kombinierte Risikoindikatoren, Korrelationen)

## Projektüberblick

**Technischer Stack**
- Python (ETL)
- PostgreSQL (Data Warehouse)
- Docker & Docker Compose (Containerisierung/Orchestrierung)
- pandas
- SQLAlchemy/psycopg2
- Power BI (Visualisierung und Analyse)

**ETL-Architektur**

Die ETL-Pipeline ist modular aufgebaut:

1. **Extract**  
   - Download der Rohdaten  
   - Einlesen in Pandas DataFrames  

2. **Transform**  
   - Datenbereinigung  
   - Harmonisierung von Altersgruppen und Geschlecht  
   - Erstellung von Dimensionstabellen  
   - Aggregationen für Analysezwecke  

3. **Load**  
   - Schreiben in PostgreSQL  
   - Setzen von Primär- und Fremdschlüsseln  
   - Sicherstellung referenzieller Integrität  

Einstiegspunkt der Anwendung ist ``main.py``

## Umfang
- Analyse von Rohdaten zur Identifikation relevanter Felder und Strukturen  
- Konzeption eines Data-Warehouse-Schemas inkl. ER-Diagramm  
- Implementierung automatisierter ETL-Pipelines mit Python  
- Speicherung der bereinigten Daten in einer PostgreSQL-Datenbank  
- Geplante Integration von Power BI zur Datenanalyse und -visualisierung

---
## Datenbankschema

Das Data Warehouse folgt einem Star-Schema mit:
- Faktentabellen (z.B. Diabetes, GEDA)
- Dimensionstabellen (Zeit, Geographie, Bevölkerung, Indikatoren)

**ER-Diagramm**

![ERD](Documentations/erd.png)

---
## Visualisierung (Power BI – Beispielseiten)

**Auswertung 1: Geschlechtsspezifische Analyse**

![Seite1](Visualisierung_powerbi/images/geschlecht_page.png)

- Vergleich der Diabetesprävalenz nach Geschlecht
- Darstellung geschlechtsspezifischer Unterschiede in Screening-Raten
- Analyse möglicher Korrelationen zwischen Risikofaktoren und Prävalenz

**Auswertung 2: Lebensstilbezogene Analyse**

- Zusammenhang zwischen körperlicher Inaktivität und Diabetesindikatoren
- Vergleich von Adipositasraten und Komplikationsindikatoren
- Regionale Unterschiede im Risikoprofil

![Seite2](Visualisierung_powerbi/images/lebensstill_page.png)

**Auswertung 3: Screening-Analyse**

- Screening-Raten nach Bundesland
- Zusammenhang zwischen Screening und Amputationsraten
- Explorative Korrelationsanalyse zwischen Versorgungs- und Outcome-Indikatoren

![Seite3](Visualisierung_powerbi/images/screening_page.png)

## Voraussetzungen

Zur lokalen Ausführung werden benötigt: 
- Docker und Docker Compose 
- Internetzugang (für den Download der Rohdaten aus den RKI-Repos)

## Konfiguration

Vor dem Start muss eine ``.env``-Datei im Projektverzeichnis erstellt werden. Siehe Beispiel: [.env.example](.env.example)

Ohne diese Umgebungsvariablen kann die Anwendung nicht gestartet werden.

## Ausführung mit Docker

```cmd
docker compose up --build
docker compose up --build service_name
docker compose down
````

## Automatisierung mit Cron

Die ETL-Strecke wird innerhalb des Docker-Containers automatisch per Cronjob ausgeführt. 

**Standardkonfiguration**
- Ausführung: täglich um 20 Uhr
- Zeitzone: Europe/Berlin

Die Zeitzone wird im Dockerfile gesetzt:

```Dockerfile
ENV TZ=Europe/Berlin
```
Dadurch entspricht die Ausführungszeit der deutschen Zeit ink. Sommer- und Winterzeit.

## Anpassung der Cron-Zeit

Im Dockerfile befindet sich folgende Zeile: 

```Dockerfile
RUN echo "00 20 * * * root . /etc/environment; /usr/local/bin/python /app/main.py >> /var/log/cron.log 2>&1" > /etc/cron.d/etl-cron
```
Format:

```
MINUTE STUNDE * * *
```
Beispiele:

- 07:30 Uhr → 30 07 * * *
- 01:00 Uhr → 00 01 * * *
- 23:15 Uhr → 15 23 * * *

Nach Änderung des Dockerfiles muss der Container neue gebaut werden: 
```cmd
docker compose up --build
```
---
## Logs

Die Cron-Ausgaben werden in folgende Datei geschrieben:

```
/var/log/cron.log
```
Die Logs werden im Container kontinuierlich ausgegeben.

## Projektstruktur

```
.
├── etl_pipeline/
│   └── extract.py
│   └── transform.py
│   └── load.py
│   └── config.py
├── Documentations/
│   └── erd.png
├── Visualisierung_powerbi/
│   └── images/
│   │   └── geschlecht_page.png
│   │   └── lebensstill_page.png
│   │   └── screening_page.png
│   └── DataWarehouse.pbix
├── Dockerfile
├── docker-compose.yml
├── main.py
├── requirements.txt
```

## Lizenz
Dieses Projekt steht unter der [MIT-Lizenz](https://opensource.org/license/MIT).

Die verwendeten Rohdaten stammen vom Robert Koch-Institut (RKI) und unterliegen der Lizenz Creative Commons Attribution 4.0 International (CC BY 4.0). Die entsprechenden Quellen- und Lizenzangaben sind im Abschnitt „Datenquellen“ dokumentiert.