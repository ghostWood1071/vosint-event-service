import sys

from models.kafka_consumer_event import KafkaConsumer_event_class
from dotenv import load_dotenv

if __name__ == "__main__":
    load_dotenv("./.env")
    kafka_consumer = KafkaConsumer_event_class()

    while True:
        kafka_consumer.run(topic="events", group_ids="events")
