import logging
import os

from dotenv import load_dotenv

from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

import schemas
from models import Person


load_dotenv(dotenv_path="..\\.env", verbose=True)
BOOTSTRAP_SERVERS = os.environ.get('BOOTSTRAP_SERVERS')
SCHEMA_URL = os.environ.get('SCHEMA_REGISTRY_URL')
TOPIC_NAME = os.environ.get('TOPICS_PEOPLE_AVRO_NAME')
TOPIC_PARTITIONS: int = int(os.environ.get('TOPICS_PEOPLE_AVRO_PARTITIONS'))
TOPIC_REPLICAS: int = int(os.environ.get('TOPICS_PEOPLE_AVRO_REPLICAS'))
CONSUMER_GROUP = os.environ.get('CONSUMER_GROUP_AVRO')


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


def make_schema_registry() -> SchemaRegistryClient:
    return SchemaRegistryClient({
        'url': SCHEMA_URL
    })

def make_avro_deserializer() -> AvroDeserializer:
    return AvroDeserializer(
        make_schema_registry(),
        schemas.person_value_v1,
        lambda data, ctx: Person(**data)
    )

def make_producer() -> DeserializingConsumer:
    return DeserializingConsumer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'key.deserializer': StringDeserializer('utf_8'),
        'value.deserializer': make_avro_deserializer(),
        'group.id': CONSUMER_GROUP
        'linger.ms': 300,
        'enable.idempotence': 'true',
        'max.in.flight.requests.per.connection': 1,
        'acks': 'all',
        'partitioner': 'murmur2_random'
    })

def make_consumer() -> DeserializingConsumer:
    pass

def main():
    logger.info(f"Started Python Avro consumer for topic {TOPIC_NAME}")


if __name__ == '__main__':
    main()
