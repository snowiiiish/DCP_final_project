import sqlite3
import os
import logging

logger = logging.getLogger("airflow.task")

db_path = "/opt/airflow/data/app.db"

def get_db_connection():
    """
    Creates a connection to the SQLite database.
    """
    conn = sqlite3.connect(db_path)
    return conn

def init_db():
    """
    Creates the 'events' table if it doesn't exist.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ingestion_timestamp TEXT,
            forecast_intensity INTEGER,
            actual_intensity INTEGER,
            index_intensity TEXT,
            source TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS daily_summary (
            summary_date DATE,
            part_of_day TEXT,
            avg_forecast REAL,
            avg_actual REAL,
            max_intensity INTEGER,
            min_intensity INTEGER
        )
    ''')
    
    conn.commit()
    conn.close()
    logger.info("Database initialized and tables verified.")