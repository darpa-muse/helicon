import json
import os
import redis

#Make this an ENV variable
#output_location = "/mnt/ssd/graphql/"

redis_host = os.environ["REDIS_HOST"]
redis_port = int(os.environ["REDIS_PORT"])
output_location = os.environ["OUTPUT_LOCATION"]

r = redis.Redis(host=redis_host, port=redis_port, db=0)

def in_redis(id):
    print("Checking if id "+id+" is in redis")
    in_redis= True
    try:
      status = r.get(id+"-build").decode("utf-8") 
    except:
      status = "None"
      in_redis = False
    print("Redis status is "+status)
    return in_redis


for key in r.scan_iter("*"):
  #print(key)
  value = r.get(key)

  try:
      project = json.loads(value.decode('utf-8'))
      #print(project)
      #start_jjhenkel(key)
      if project["node"]["primaryLanguage"]:
        lang = project["node"]["primaryLanguage"]["name"]
        if lang == "Go":
          os.system("bash jjhenkel_go.sh "+key)
          print("Built project "+key)

  except:
      #print("not a project")
      continue
    

