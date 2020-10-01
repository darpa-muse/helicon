import json
import os
import redis
import logging

from kafka import KafkaConsumer
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Make this an ENV variable
# output_location = "/mnt/ssd/graphql/"

logging.basicConfig(
    format="%(asctime)s : %(levelname)s : %(message)s", level=logging.INFO
)


redis_host = os.environ["REDIS_HOST"]
redis_port = int(os.environ["REDIS_PORT"])
kafka_host = os.environ["KAFKA_HOST"]
kafka_port = int(os.environ["KAFKA_PORT"])
kafka_string = kafka_host + ":" + str(kafka_port)
output_location = os.environ["OUTPUT_LOCATION"]

r = redis.Redis(host=redis_host, port=redis_port, db=0)


def extract(id, project):
    logging.info("Starting extractor on project " + id)

    project_location = output_location + id
    os.system("mkdir -p " + project_location)
    # curl project

    tar_file = project["node"]["defaultBranchRef"]["target"]["zipballUrl"]

    command = "cd " + project_location + " && curl -O " + tar_file
    os.system(command)

    # unzip project
    file_name = tar_file.split("/")[-1]
    os.system(
        "cd " + project_location + " && unzip -o " + file_name + " && rm " + file_name
    )

    # run push notification to topics
    code_location = project_location
    send_to_kafka(id, code_location)


def send_to_kafka(id, code_location):
    producer = KafkaProducer(
        bootstrap_servers=[kafka_string],
        value_serializer=lambda m: json.dumps(m).encode("ascii"),
    )

    logging.info("Sending to kafka")
    logging.info(project)
    producer.send("jjhenkel", {id: code_location})
    producer.send("file_predictor", {id: code_location})
    producer.send("doxygen", {id: code_location})
    producer.flush()


def update_redis(id, project):
    project_json = json.dumps(project)
    r.set(id, project_json)
    get_redis(id)


def get_redis(id):
    try:
        project = json.loads(r.get(id).decode("utf-8"))
        project_build = r.get(id + "-build").decode("utf-8")
    except:
        project = "None"
        project_build = "No data"
    logging.info("Project: " + id)
    logging.info(project)
    logging.info("Build status: " + project_build)


def main():

    # To consume latest messages and auto-commit offsets
    consumer = KafkaConsumer(
        "graphql_projects",
        group_id="graphql-1",
        enable_auto_commit=True,
        auto_offset_reset="earliest",
        auto_commit_interval_ms=1000,
        bootstrap_servers=[kafka_string],
    )
    for message in consumer:
        # message value and key are raw bytes -- decode if necessary!
        # e.g., for unicode: `message.value.decode('utf-8')`
        logging.info(message.topic)
        # logging.info(message.key)
        # logging.info(message.value)

        project = message.value.decode("utf-8")
        project = json.loads(project)
        logging.info(project)
        # logging.info(project.keys())
        key = list(project.keys())[0]
        project = json.loads(project[key])

        extract(key, project)
        # start_jjhenkel(key,project)
        # start_doxygen(key,project)
        # file_predictor(key,project)

    # consume earliest available messages, don't commit offsets
    KafkaConsumer(auto_offset_reset="earliest", enable_auto_commit=False)

    # consume json messages
    KafkaConsumer(value_deserializer=lambda m: json.loads(m.decode("ascii")))

    # consume msgpack
    KafkaConsumer(value_deserializer=msgpack.unpackb)

    # StopIteration if no message after 1sec
    KafkaConsumer(consumer_timeout_ms=1000)


if __name__ == "__main__":
    main()
