import pandas as pd
import sqlite3
import logging
from db_utils import get_db_connection

logger = logging.getLogger("airflow.task")

def get_part_of_day(hour):
    if 5 <= hour < 12:
        return 'Morning'
    elif 12 <= hour < 17:
        return 'Afternoon'
    elif 17 <= hour < 21:
        return 'Evening'
    else:
        return 'Night'

def run_analytics_job():
    logger.info("--- Starting Analytics Job ---")
    
    conn = get_db_connection()
    
    try:
        df = pd.read_sql_query("SELECT * FROM events", conn)
        
        if df.empty:
            logger.warning("No data found.")
            return

        df['dt'] = pd.to_datetime(df['ingestion_timestamp'])
        df['hour'] = df['dt'].dt.hour
        df['part_of_day'] = df['hour'].apply(get_part_of_day)
        df['date_col'] = df['dt'].dt.date

        summary = df.groupby(['date_col', 'part_of_day']).agg(
            avg_forecast=('forecast_intensity', 'mean'),
            avg_actual=('actual_intensity', 'mean'),
            max_intensity=('actual_intensity', 'max'),
            min_intensity=('actual_intensity', 'min')
        ).reset_index()

        summary['avg_actual'] = summary['avg_actual'].fillna(summary['avg_forecast'])
        summary['max_intensity'] = summary['max_intensity'].fillna(summary['avg_forecast'])
        summary['avg_forecast'] = summary['avg_forecast'].round(1)
        summary['avg_actual'] = summary['avg_actual'].round(1)

        summary = summary.rename(columns={'date_col': 'summary_date'})

        summary.to_sql('daily_summary', conn, if_exists='append', index=False)
        
        logger.info(f"SUCCESS: Written {len(summary)} rows to 'daily_summary'.")
        logger.info(f"\n{summary}")
        
    except Exception as e:
        logger.error(f"Analytics failed: {e}")
    finally:
        conn.close()

