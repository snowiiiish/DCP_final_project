# DCP_final_project
Data Collection &amp; Preparation. Final Project

# Carbon Intensity Data Pipeline Project

**Team:** DCP Team  

## 1. Project Goal
This project demonstrates a complete end-to-end data pipeline that collects, cleans, stores, and analyzes real-time Carbon Intensity data from the UK National Grid.

## 2. API Justification
**Selected API:** [Carbon Intensity UK](https://carbon-intensity.github.io/api-definitions/)
*   **Category:** Weather / Environment (Allowed).
*   **Justification:** This API provides high-quality, frequently updated data regarding the carbon impact of electricity generation. It returns clean JSON structure, supports historical/forecast data, and is highly stable without requiring complex authentication keys, making it ideal for a reproducible academic project.

## 3. Architecture Overview

### 3.1 Components
*   **Orchestrator:** Apache Airflow (Dockerized, LocalExecutor with Postgres).
*   **Message Broker:** Apache Kafka (Handles streaming buffer).
*   **Storage:** SQLite (Lightweight relational database).
*   **Analysis:** Pandas & SQL.

### 3.2 Pipeline Flow
1.  **Job 1 (Ingestion):** A pseudo-streaming producer runs every 15 minutes (looping internally for 14.5 mins). It fetches data every **30 seconds** from the API and pushes raw JSON to the Kafka topic `raw_events`.
2.  **Job 2 (Cleaning):** Runs **Hourly**. It consumes messages from Kafka, flattens the JSON structure, extracts `forecast` vs `actual` values, and inserts them into the SQLite `events` table.
3.  **Job 3 (Analytics):** Runs **Daily**. It reads the cleaned data, categorizes rows by "Part of Day" (Morning, Afternoon, Evening, Night), calculates aggregates (Mean, Min, Max), and saves results to the `daily_summary` table.

## 4. Database Schema

### Table: `events` (Cleaned Data)
| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER (PK) | Auto-incrementing ID |
| `ingestion_timestamp` | TEXT | Time when data was fetched |
| `forecast_intensity` | INTEGER | Predicted Carbon Intensity (gCO2/kWh) |
| `actual_intensity` | INTEGER | Actual Carbon Intensity (gCO2/kWh) |
| `index_intensity` | TEXT | Category (e.g., 'moderate', 'low') |
| `source` | TEXT | Data source URL |
| `created_at` | TIMESTAMP | Record insertion time |

### Table: `daily_summary` (Analytics)
| Column | Type | Description |
|--------|------|-------------|
| `summary_date` | DATE | Date of the analysis |
| `part_of_day` | TEXT | Morning, Afternoon, Evening, Night |
| `avg_forecast` | REAL | Average predicted intensity |
| `avg_actual` | REAL | Average actual intensity |
| `max_intensity` | INTEGER | Peak intensity for that period |
| `min_intensity` | INTEGER | Lowest intensity for that period |

## 5. Repository Structure
```text
project/
│   README.md
│   requirements.txt
│   docker-compose.yaml
│   Dockerfile
│   analysis.ipynb         
├── src/                   
│   ├── job1_producer.py
│   ├── job2_cleaner.py
│   ├── job3_analytics.py
│   └── db_utils.py
├── airflow/
│   └── dags/           
│       ├── job1_ingestion_dag.py
│       ├── job2_clean_store_dag.py
│       └── job3_daily_summary_dag.py
└── data/
    └── app.db
```

## 6. How to Run

1.  **Prerequisites:** Docker and Docker Compose installed.
2.  **Start Services:**
    ```bash
    docker-compose up -d --build
    ```
3.  **Initialize Airflow (First time only):**
    ```bash
    docker-compose run airflow-webserver airflow db init
    docker-compose run airflow-webserver airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com
    docker-compose up -d
    ```
4.  **Access UI:** Open `http://localhost:8080` (Username: admin ; Password: admin).
5.  **Trigger DAGs:** Enable `job1_ingestion_dag` to start data collection.

## 7. Kafka topic
```bash
    docker-compose exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic raw_events --from-beginning
```
