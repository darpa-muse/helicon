from kafka import KafkaConsumer
import json
import os
import redis

#Make this an ENV variable
#output_location = "/mnt/ssd/graphql/"

redis_host = os.environ["REDIS_HOST"]
redis_port = int(os.environ["REDIS_PORT"])
kafka_host = os.environ["KAFKA_HOST"]
kafka_port = int(os.environ["KAFKA_PORT"])
kafka_string = kafka_host+":"+str(kafka_port)
output_location = os.environ["OUTPUT_LOCATION"]

r = redis.Redis(host=redis_host, port=redis_port, db=0)


def file_predictor(id,code_location):
    os.system("bash md5hash.sh "+id+" "+code_location)
    print("Predicted on "+id)
    return
    #$project/code_hashdeep.txt $uid /home/twosix/file_maker/output.csv


def update_redis(id,project):
    project_json = json.dumps(project)
    r.set(id, project_json)
    get_redis(id)


def get_redis(id):
    try:
      project = json.loads(r.get(id).decode('utf-8'))
      project_build = r.get(id+"-build").decode('utf-8')
    except:
      project = "None"
      project_build = "No data"
    print ("Project: "+id)  
    print(project)
    print("Build status: "+project_build)



# To consume latest messages and auto-commit offsets
consumer = KafkaConsumer('file_predictor',
                         group_id='file_predictor-1',
                         bootstrap_servers=[kafka_string]
                         )
for message in consumer:
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    print(message.topic)
    #print(message.key)
    #print(message.value)

    project = message.value.decode("utf-8")
    project = json.loads(project)
    print(project)
    #print(project.keys())
    proj_id = list(project.keys())[0]
    code_location = project[proj_id]

    print(proj_id)
    print(code_location)

    file_predictor(proj_id,code_location)
    
    # start_doxygen(key,project)
    # file_predictor(key,project)

# consume earliest available messages, don't commit offsets
KafkaConsumer(auto_offset_reset='earliest', enable_auto_commit=False)

# consume json messages
KafkaConsumer(value_deserializer=lambda m: json.loads(m.decode('ascii')))

# consume msgpack
KafkaConsumer(value_deserializer=msgpack.unpackb)

# StopIteration if no message after 1sec
KafkaConsumer(consumer_timeout_ms=1000)


# print("does it get here?")

# Subscribe to a regex topic pattern
# consumer = KafkaConsumer()
# consumer.subscribe(pattern='^graphql*')

# # Use multiple consumers in parallel w/ 0.9 kafka brokers
# # typically you would run each on a different server / process / CPU
# consumer1 = KafkaConsumer('graphql_projects',
#                           group_id='graphql-1',
#                           bootstrap_servers='100.120.0.7:1025')
# consumer2 = KafkaConsumer('graphql_projects',
#                           group_id='graphql-1',
#                           bootstrap_servers='100.120.0.6:1026')