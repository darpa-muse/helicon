import redis
import sys
import os

proj_id = sys.argv[1] 

redis_host = os.environ["REDIS_HOST"]
redis_port = int(os.environ["REDIS_PORT"])


proj_key = proj_id +"-doxygen"

r = redis.Redis(host=redis_host, port=redis_port, db=0)

r.set(proj_key, "done")

# try:
#   foo = r.get(proj_key).decode("utf-8")
# except:
#   foo = ""  
# print(foo)
