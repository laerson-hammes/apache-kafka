import logging
import os
from typing import List

from dotenv import load_dotenv
from faker import Faker
from flask import Flask
from flask_pydantic import validate

from confluent_kafka import SerializingProducer
from confluent_kafka.admin import (
    AdminClient,
    NewTopic
)

from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.cimpl import Message

import schemas
from commands import CreatePeopleCommand
from models import Person


load_dotenv(dotenv_path="..\\.env", verbose=True)
BOOTSTRAP_SERVERS = os.environ.get('BOOTSTRAP_SERVERS')
SCHEMA_URL = os.environ.get('SCHEMA_REGISTRY_URL')
TOPIC_NAME = os.environ.get('TOPICS_PEOPLE_AVRO_NAME')
TOPIC_PARTITIONS: int = int(os.environ.get('TOPICS_PEOPLE_AVRO_PARTITIONS'))
TOPIC_REPLICAS: int = int(os.environ.get('TOPICS_PEOPLE_AVRO_REPLICAS'))


app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


class ProducerCallback:

    def __init__(self, person: Person) -> None:
        self.person = person

    def __call__(self, err, msg: Message):
        if err:
            logger.error(f"Failed to produce {self.person}", exc_info=err)
        else:
            logger.info(
                f"""Successfully produced {self.person}
                to partition {msg.partition()}
                at offset {msg.offset()}"""
            )


@app.before_request
async def start():
    client = AdminClient({
        'bootstrap.servers': BOOTSTRAP_SERVERS
    })
    try:
        futures = client.create_topics([
            NewTopic(
                TOPIC_NAME,
                num_partitions=TOPIC_PARTITIONS,
                replication_factor=TOPIC_REPLICAS
            )
        ])
        for topic_name, future in futures.items():
            future.result()
            logger.info(f"Create topic {topic_name}")
    except Exception as e:
        logger.warning(e)

def make_schema_registry() -> SchemaRegistryClient:
    return SchemaRegistryClient({
        'url': SCHEMA_URL
    })

def make_avro_serializer() -> AvroSerializer:
    return AvroSerializer(
        make_schema_registry(),
        schemas.person_value_v1,
        lambda person, ctx: person.model_dump()
    )

def make_producer() -> SerializingProducer:
    return SerializingProducer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'linger.ms': 300,
        'enable.idempotence': 'true',
        'max.in.flight.requests.per.connection': 1,
        'acks': 'all',
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': make_avro_serializer(),
        'partitioner': 'murmur2_random'
    })

@app.route('/api/people', methods=['POST'])
@validate(body=CreatePeopleCommand, on_success_status=201)
def create_people(body: CreatePeopleCommand):
    people: List[Person] = []

    faker: Faker = Faker()
    producer = make_producer()

    for _ in range(body.count):
        person = Person(
            name=faker.name(),
            title=faker.job().title()
        )
        people.append(person.model_dump(mode="json"))
        producer.produce(
            topic=TOPIC_NAME,
            key=person.title.lower().replace(r"s+", "-"),
            value=person,
            on_delivery=ProducerCallback(person)
        )

    producer.flush()
    return people

@app.route("/", methods=["GET"])
def index():
    return "Hello World..."


if __name__ == '__main__':
    app.run(debug=True)
