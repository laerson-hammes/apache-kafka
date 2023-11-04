import os
import uuid
from typing import List
import logging

from dotenv import load_dotenv
from flask import Flask
from flask_pydantic import validate
from faker import Faker
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable
from kafka.producer import KafkaProducer
from commands import CreatePeopleCommand
from entities import Person


load_dotenv(dotenv_path="..\\.env", verbose=True)
BOOTSTRAP_SERVERS = os.environ.get('BOOTSTRAP_SERVERS')
TOPIC_NAME = os.environ.get('TOPICS_PEOPLE_ADV_NAME')
TOPIC_RETRIES = int(os.environ.get('TOPICS_PEOPLE_ADV_RETRIES'))
TOPIC_INFLIGH_REQS = int(os.environ.get('TOPICS_PEOPLE_ADV_INFLIGH_REQS'))
TOPIC_LINGER_MS = int(os.environ.get('TOPICS_PEOPLE_ADV_LINGER_MS'))
TOPIC_ACKS = os.environ.get('TOPICS_PEOPLE_ADV_ACKS')
TOPIC_PARTITIONS = int(os.environ.get('TOPICS_PEOPLE_ADV_PARTITIONS'))
TOPIC_REPLICAS = int(os.environ.get('TOPICS_PEOPLE_ADV_REPLICAS'))


app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


class SuccessHandler:

    def __init__(self, person) -> None:
        self.person = person

    def __call__(self, rec_metadata):
        logger.info(
            f"""Successfully produced person {self.person}
            to topic {rec_metadata.topic}
            and partition {rec_metadata.partition}
            at offset {rec_metadata.offset}"""
        )


class ErrorHandler:

    def __init__(self, person) -> None:
        self.person = person

    def __call__(self, exception):
        logger.error(f"Failed producing person {self.person}", exc_info=exception)


@app.before_request
async def start():
    client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    try:
        client.create_topics([
            NewTopic(
                name=TOPIC_NAME,
                num_partitions=TOPIC_PARTITIONS,
                replication_factor=TOPIC_REPLICAS
            )
        ])
    except TopicAlreadyExistsError as e:
        print(e)
    finally:
        client.close()

def make_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        linger_ms=TOPIC_LINGER_MS,
        retries=TOPIC_RETRIES,
        max_in_flight_requests_per_connection=TOPIC_INFLIGH_REQS,
        acks=TOPIC_ACKS
    )

@app.route('/api/people', methods=['POST'])
@validate(body=CreatePeopleCommand, on_success_status=201)
def create_people(body: CreatePeopleCommand):
    people: List[Person] = []

    faker: Faker = Faker()
    producer = make_producer()

    for _ in range(body.count):
        person = Person(
            id=str(uuid.uuid4()),
            name=faker.name(),
            title=faker.job().title()
        )
        people.append(person.model_dump(mode="json"))
        producer.send(
            topic=TOPIC_NAME,
            key=person.title.lower().replace(r"s+", "-").encode("utf-8"),
            value=person.model_dump_json().encode('utf-8'),
        ).add_callback(
            SuccessHandler(person)
        ).add_errback(
            ErrorHandler(person)
        )

    producer.flush()
    return people

@app.route("/", methods=["GET"])
def index():
    return "Hello World..."


if __name__ == "__main__":
    app.run(debug=True)
