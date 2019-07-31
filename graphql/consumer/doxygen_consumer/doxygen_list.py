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


fo = open("projects.txt", 'r')
projects = fo.readlines()

for project in projects:
  proj = project.strip()
  print(proj)
  #value = r.get(key)
  if path.exists(output_location+proj+"/doxygen/doxygen.json"):
    proj_key = proj +"-doxygen"
    r.set(proj_key, "done")
    print("Project: "+proj+" is already done")
    continue
  
  try:
      #project = json.loads(value.decode('utf-8'))
      #print(project)
      #start_jjhenkel(key)
      print("Starting doxygen process on project "+proj)
      os.system("bash doxy.sh "+proj+" "+output_location+proj)

  except:
      #print("not a project")
      continue
    

fo.close()