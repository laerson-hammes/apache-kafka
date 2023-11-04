import os
import uuid
from typing import List

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
TOPIC_NAME = os.environ.get('TOPICS_PEOPLE_BASIC_NAME')
TOPIC_PARTITIONS = int(os.environ.get('TOPICS_PEOPLE_BASIC_PARTITIONS'))
TOPIC_REPLICAS = int(os.environ.get('TOPICS_PEOPLE_BASIC_REPLICAS'))


app = Flask(__name__)


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
        bootstrap_servers=BOOTSTRAP_SERVERS
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
        )

    producer.flush()
    return people

@app.route("/", methods=["GET"])
def index():
    return "Hello World..."


if __name__ == "__main__":
    app.run(debug=True)
