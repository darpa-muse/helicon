import json
import logging
import os


from kafka import KafkaConsumer
import redis

# Make this an ENV variable
# output_location = "/mnt/ssd/graphql/"

redis_host = os.environ["REDIS_HOST"]
redis_port = int(os.environ["REDIS_PORT"])
kafka_host = os.environ["KAFKA_HOST"]
kafka_port = int(os.environ["KAFKA_PORT"])
kafka_string = kafka_host + ":" + str(kafka_port)
output_location = os.environ["OUTPUT_LOCATION"]

r = redis.Redis(host=redis_host, port=redis_port, db=0)

logging.basicConfig(
    format="%(asctime)s : %(levelname)s : %(message)s", level=logging.INFO
)


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

    # run jjhenkel here
    logging.info("Starting jjhenkel build process on project " + id)
    code_location = project_location
    start_jjhenkel(id, code_location)
    logging.info("Starting file_predictor process on project " + id)
    file_predictor(id, code_location)
    # start_doxygen(id,code_location)
    update_redis(id, project)


def start_jjhenkel(id, code_location):
    os.system("bash jjhenkel.sh " + id + " " + code_location)
    logging.info("Built project " + id)
    return


def start_doxygen(id, code_location):
    os.system("bash doxy.sh " + id + " " + code_location)
    logging.info("Built project " + id)
    return


def file_predictor(id, code_location):
    os.system("bash md5hash.sh " + id + " " + code_location)
    logging.info("Predicted on " + id)
    return
    # $project/code_hashdeep.txt $uid /home/twosix/file_maker/output.csv


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
        "graphql_projects", group_id="graphql-1", bootstrap_servers=[kafka_string]
    )
    for message in consumer:
        # message value and key are raw bytes -- decode if necessary!
        # e.g., for unicode: `message.value.decode('utf-8')`
        logging.info(message.topic)

        project = message.value.decode("utf-8")
        project = json.loads(project)
        logging.info(project)
        key = list(project.keys())[0]
        project = json.loads(project[key])

        extract(key, project)

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
