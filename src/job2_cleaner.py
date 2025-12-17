import json
import time
import logging
import pandas as pd
from kafka import KafkaConsumer
from db_utils import get_db_connection, init_db

logger = logging.getLogger("airflow.task")

kafka_topic = 'raw_events'
kafka_group= 'cleaner_group'

def get_consumer(servers):
    return KafkaConsumer(
        kafka_topic,
        bootstrap_servers=servers,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=kafka_group,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=5000
    )

def run_cleaning_job(kafka_servers):
    logger.info("--- Starting Hourly Cleaning Job (Pandas Edition) ---")

    init_db()
    
    consumer = get_consumer(kafka_servers)
    
    raw_data_list = []
    
    try:
        for message in consumer:
            raw_val = message.value
            try:
                payload = raw_val.get('payload', {}).get('intensity', {})
                
                row = {
                    'ingestion_timestamp': raw_val.get('timestamp'),
                    'forecast_intensity': payload.get('forecast'),
                    'actual_intensity': payload.get('actual'),
                    'index_intensity': payload.get('index'),
                    'source': raw_val.get('source', 'unknown')
                }
                raw_data_list.append(row)
            except Exception as e:
                logger.warning(f"Skipping malformed message: {e}")
                
    except Exception as e:
        logger.error(f"Error consuming Kafka: {e}")
    finally:
        consumer.close()

    if raw_data_list:
        df = pd.DataFrame(raw_data_list)
        initial_count = len(df)
        
        df = df.dropna(subset=['forecast_intensity'])
        
        df = df.drop_duplicates()
        
        if 'index_intensity' in df.columns:
            df['index_intensity'] = df['index_intensity'].astype(str).str.lower().str.strip()
        
        if 'source' in df.columns:
            df['source'] = df['source'].astype(str).str.lower().str.strip()

        df['forecast_intensity'] = pd.to_numeric(df['forecast_intensity'], errors='coerce')
        df['actual_intensity'] = pd.to_numeric(df['actual_intensity'], errors='coerce')
        
        df['ingestion_timestamp'] = pd.to_datetime(df['ingestion_timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        logger.info(f"Cleaning Stats: {initial_count} raw -> {len(df)} cleaned.")

        conn = get_db_connection()
        try:
            df.to_sql('events', conn, if_exists='append', index=False)
            logger.info(f"SUCCESS: Stored {len(df)} records into SQLite.")
        except Exception as e:
            logger.error(f"Database insertion failed: {e}")
        finally:
            conn.close()
    else:
        logger.info("No new messages to process.")

    logger.info("Cleaning Job Finished")
