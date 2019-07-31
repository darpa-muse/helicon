from kafka import KafkaConsumer
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import os
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


def id_to_hex(id):
    return id.encode("hex")


def in_redis(id):
    print("Checking if id " + id + " is in redis")
    in_redis = True
    try:
        status = r.get(id).decode("utf-8")
    except:
        status = "None"
        in_redis = False
    print("Redis status is " + status)
    return in_redis



def clone(id,project):
    print("Starting clone on project " + id)

    # hex_id = id_to_hex(id)
    # first_four = hex_id[0:4]
    if in_redis(id):
        project_location = output_location + id
        os.system("mkdir -p " + project_location)
        # curl project

        gitUrl = project["node"]["url"]

        command = "cd " + project_location + " && git clone " + gitUrl
        print(command)
        os.system(command)

        # run push notification to topics
        code_location = project_location
        update_redis(id, project)
        send_to_kafka(id, code_location)

def extract(id, project):
    print("Starting extractor on project " + id)

    # hex_id = id_to_hex(id)
    # first_four = hex_id[0:4]
    if not in_redis(id+"-curled"):
        project_location = output_location + id
        os.system("mkdir -p " + project_location)
        # curl project

        tar_file = project["node"]["defaultBranchRef"]["target"]["zipballUrl"]

        command = "cd " + project_location + " && curl -O " + tar_file
        print(command)
        os.system(command)

        # unzip project
        print("Tar file: "+tar_file)
        file_name = tar_file.split("/")[-1]
        print("File name: "+file_name)
        os.system(
            "cd "
            + project_location
            + " && unzip -o "
            + file_name
        )

        # run push notification to topics
        code_location = project_location
        update_redis(id, project)
        send_to_kafka(id, code_location)


def send_to_kafka(id, code_location):
    producer = KafkaProducer(
        bootstrap_servers=[kafka_string],
        value_serializer=lambda m: json.dumps(m).encode("ascii"),
    )

    print("Sending to kafka")
    print(project)
    producer.send("jjhenkel", {id: code_location})
    producer.send("file_predictor", {id: code_location})
    producer.send("doxygen", {id: code_location})
    producer.flush()


def update_redis(id, project):
    #project_json = json.dumps(project)
    r.set(id+"-curled", "yes")
    #get_redis(id)


def get_redis(id):
    try:
        project = json.loads(r.get(id).decode("utf-8"))
        project_build = r.get(id + "-build").decode("utf-8")
    except:
        project = "None"
        project_build = "No data"
    print("Project: " + id)
    print(project)
    print("Build status: " + project_build)


# To consume latest messages and auto-commit offsets
consumer = KafkaConsumer(
    "projects",
    group_id="projects-1",
    enable_auto_commit=True,
    auto_offset_reset="latest",
    auto_commit_interval_ms=3000,
    bootstrap_servers=[kafka_string],
)
for message in consumer:
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    print(message.topic)
    # print(message.key)
    # print(message.value)

    project = message.value.decode("utf-8")
    project = json.loads(project)
    print("Kafka consumed:")
    print(project)
    # print(project.keys())
    key = list(project.keys())[0]
    print("Kafka key: "+key)
    project = json.loads(project[key])

    
    extract(key, project)
    # start_jjhenkel(key,project)
    # start_doxygen(key,project)
    # file_predictor(key,project)

