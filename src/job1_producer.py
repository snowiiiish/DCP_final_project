import time
import json
import requests
import logging
from datetime import datetime
from kafka import KafkaProducer

logger = logging.getLogger("airflow.task")

api_url = "https://api.carbonintensity.org.uk/intensity"

def get_kafka_producer(servers):
    """
    Connect to Kafka with retries.
    """
    producer = None
    for i in range(3):
        try:
            producer = KafkaProducer(
                bootstrap_servers=servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                request_timeout_ms=5000
            )
            logger.info(f"Connected to Kafka at {servers}")
            return producer
        except Exception as e:
            logger.warning(f"Connection attempt {i+1} failed: {e}")
            time.sleep(2)
    
    raise Exception("Could not connect to Kafka after 3 attempts")

def run_ingestion_loop(kafka_topic, kafka_servers, run_duration_minutes=14.5):
    """
    Fetches data every 30 seconds for a set duration.
    """
    logger.info("--- Starting Ingestion Job (Src Module) ---")
    
    producer = get_kafka_producer(kafka_servers)
    
    end_time = time.time() + (run_duration_minutes * 60)
    iteration = 0

    while time.time() < end_time:
        iteration += 1
        try:
            response = requests.get(api_url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()

                message = {
                    "ingestion_id": iteration,
                    "timestamp": datetime.now().isoformat(),
                    "source": "carbonintensity.org.uk",
                    "payload": data.get('data', [])[0]
                }
                
                producer.send(kafka_topic, value=message)
                producer.flush()
                
                intensity = message['payload']['intensity']['forecast']
                logger.info(f"[{iteration}] SENT to '{kafka_topic}': Forecast {intensity}")
            else:
                logger.error(f"API Error: {response.status_code}")

        except Exception as e:
            logger.error(f"Loop Error: {e}")

        time.sleep(30)

    logger.info("Ingestion Job Finished")