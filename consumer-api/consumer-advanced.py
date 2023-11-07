import json
import logging
import os

from dotenv import load_dotenv
from kafka import TopicPartition, OffsetAndMetadata
from kafka.consumer import KafkaConsumer


load_dotenv(dotenv_path="..\\.env", verbose=True)
BOOTSTRAP_SERVERS = os.environ.get('BOOTSTRAP_SERVERS')
TOPIC_NAME = os.environ.get('TOPICS_PEOPLE_ADV_NAME')
CONSUMER_GROUP = os.environ.get('CONSUMER_GROUP_ADV')


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__file__)


def people_key_deserializer(key):
    return key.decode('utf-8')

def people_value_deserializer(value):
    return json.loads(value.decode('utf-8'))

def main():
    logger.info(
        f"""Started Python Consumer
        for topic {TOPIC_NAME}
        """
    )

    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP,
        key_deserializer=people_key_deserializer,
        value_deserializer=people_value_deserializer,
        enable_auto_commit=False
    )
    consumer.subscribe([TOPIC_NAME])
    for record in consumer:
        logger.info(
            f"""Consumed person {record.value}
            with key '{record.key}'
            from partition {record.partition}
            at offset {record.offset}
            """
        )

        topic_partition = TopicPartition(
            record.topic,
            record.partition
        )
        offset = OffsetAndMetadata(
            record.offset + 1,
            record.timestamp
        )
        consumer.commit({
            topic_partition: offset
        })


if __name__ ==  '__main__':
    main()
