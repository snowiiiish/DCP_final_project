import json
import time
import logging
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
    logger.info("--- Starting Hourly Cleaning Job ---")

    init_db()
    
    consumer = get_consumer(kafka_servers)
    
    messages_processed = 0
    data_to_insert = []
    
    try:
        for message in consumer:
            raw_val = message.value
            
            try:
                payload = raw_val['payload']['intensity']

                clean_record = (
                    raw_val.get('timestamp'),       
                    payload.get('forecast'),
                    payload.get('actual'),
                    payload.get('index'),
                    raw_val.get('source', 'unknown')
                )
                data_to_insert.append(clean_record)
                messages_processed += 1
            except KeyError as e:
                logger.warning(f"Skipping malformed message: {e}")
                
    except Exception as e:
        logger.error(f"Error consuming Kafka: {e}")
    finally:
        consumer.close()

    if data_to_insert:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.executemany('''
            INSERT INTO events (ingestion_timestamp, forecast_intensity, actual_intensity, index_intensity, source)
            VALUES (?, ?, ?, ?, ?)
        ''', data_to_insert)
        
        conn.commit()
        conn.close()
        logger.info(f"SUCCESS: Cleaned and stored {len(data_to_insert)} records into SQLite.")
    else:
        logger.info("No new messages to process.")

    logger.info("Cleaning Job Finished")
