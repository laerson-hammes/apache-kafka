import logging
import os

from dotenv import load_dotenv
from flask import Flask

from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError


load_dotenv(dotenv_path="..\\.env", verbose=True)
BOOTSTRAP_SERVERS = os.environ.get('BOOTSTRAP_SERVERS')
TOPIC_NAME = os.environ.get('TOPICS_PEOPLE_BASIC_NAME')
TOPIC_PARTITIONS = int(os.environ.get('TOPICS_PEOPLE_BASIC_PARTITIONS'))
TOPIC_REPLICAS = int(os.environ.get('TOPICS_PEOPLE_BASIC_REPLICAS'))


app = Flask(__name__)
logger = logging.getLogger()


@app.before_request
async def start():
    client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    topic = NewTopic(
        name=TOPIC_NAME,
        num_partitions=TOPIC_PARTITIONS,
        replication_factor=TOPIC_REPLICAS,
    )
    try:
        client.create_topics([topic])
    except TopicAlreadyExistsError as _:
        logger.warning("Topic already exists")
    finally:
        client.close()

@app.get('/hello-world')
async def index():
    return {
        "message": "Hello"
    }


if __name__ == "__main__":
    app.run(debug=True)
