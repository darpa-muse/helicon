import json
import os
import redis

#Make this an ENV variable
#output_location = "/mnt/ssd/graphql/"

redis_host = "10.12.1.6"
redis_port = 16379



r = redis.Redis(host=redis_host, port=redis_port, db=0)

def start_jjhenkel(id,code_location):
    print("Starting jjhenkel build process on project "+id)
    if not in_redis(id):
      os.system("bash jjhenkel.sh "+id+" "+code_location)
      print("Built project "+id)
    return

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
      print("Starting jjhenkel build process on project "+key)
      if not in_redis(key):
        os.system("bash jjhenkel-helper.sh "+key)
      print("Built project "+key)

  except:
      #print("not a project")
      continue

  
  #code_location = project[proj_id]
  #start_jjhenkel(proj_id,code_location)




    
    
