import json
import os
import redis
from os import path

#Make this an ENV variable
#output_location = "/mnt/ssd/graphql/"

redis_host = os.environ["REDIS_HOST"]
redis_port = int(os.environ["REDIS_PORT"])
output_location = os.environ["OUTPUT_LOCATION"]

r = redis.Redis(host=redis_host, port=redis_port, db=0)

def start_doxygen(id,code_location):
    os.system("bash doxy.sh "+id+" "+code_location)
    print("Started doxygen on project "+id)
    return


def in_redis(id):
    print("Checking if id "+id+" is in redis")
    in_redis= True
    try:
      status = r.get(id+"-doxygen").decode("utf-8") 
    except:
      status = "None"
      in_redis = False
    print("Redis status is "+status)
    return in_redis


for key in r.scan_iter("*"):
  #print(key)
  #value = r.get(key)
  if path.exists(output_location+key+"/doxygen/doxygen.json"):
    proj_key = key +"-doxygen"
    r.set(proj_key, "done")
    continue
  



  try:
      #project = json.loads(value.decode('utf-8'))
      #print(project)
      #start_jjhenkel(key)
      print("Starting doxygen process on project "+key)
      if not in_redis(key):
        os.system("bash doxy.sh "+key+" "+output_location+key)
      print("Doxygen started on project "+key)

  except:
      #print("not a project")
      continue
    
    

