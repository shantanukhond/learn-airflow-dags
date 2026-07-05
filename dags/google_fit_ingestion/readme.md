# Google Fit Ingestion Project

Medallion pipeline as classic **ETL** in one DAG: extract → transform → load.

## Architecture

```
Google Fit API
      ↓
google_fit_sync
  extract → transform → load
      ↓         ↓         ↓
   bronze    silver     gold
```

## DAG: `google_fit_sync`

| Task | What it does | DB layer |
|------|--------------|----------|
| `extract` | Fetch raw JSON from Google Fit API | `bronze.fitness_raw` |
| `transform` | Parse JSON into typed records | (in memory / XCom) |
| `load` | Save silver tables + gold summary | `silver.*`, `gold.daily_summary` |

Task order:

```python
extracted >> transformed >> loaded
```

Data still flows via TaskFlow args: `transform(extracted)`, `load(transformed)`.

## Database (fitness DB — localhost:5434)

```sql
SELECT * FROM bronze.fitness_raw;
SELECT * FROM silver.daily_steps;
SELECT * FROM gold.daily_summary;
```

User: `fitness` / Password: `fitness` / Database: `fitness`

## Connections

| Connection ID | Purpose |
|---------------|---------|
| `google_fit_api` | Google OAuth (Generic) |
| `google_fit_postgres` | Fitness database (auto-set in docker-compose) |

## Demo

1. Trigger **`google_fit_sync`** in the Airflow UI
2. Watch **extract → transform → load** run in order
3. Query Postgres on port **5434**

## Setup scripts

```bash
python scripts/get_token.py
python scripts/print_raw_json.py
pytest tests/ -v
```
