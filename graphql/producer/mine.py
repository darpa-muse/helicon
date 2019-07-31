# An example to get the remaining rate limit using the Github GraphQL API.

import requests
import os
import time
import redis
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json


api_key = os.environ["GITHUB_TOKEN"]
redis_host = os.environ["REDIS_HOST"]
redis_port = int(os.environ["REDIS_PORT"])
kafka_host = os.environ["KAFKA_HOST"]
kafka_port = int(os.environ["KAFKA_PORT"])
kafka_string = kafka_host+":"+str(kafka_port)


# add go
# add variable
# 

language_array = ["Go", "PHP", "HTML", "CoffeeScript", "Perl", "Objective-C", "Scala", "Haskell", "TeX", "Lua", "Java" , "JavaScript" , "C", "C++", "Python", "Ruby", "Swift", "C#", "Shell"]
license_array = ["mit", "apache-2.0", "gpl-2.0" , "gpl-3.0" , "bsd-3-clause" , "agpl-3.0", "lgpl-3.0","bsd-2-clause","unlicense","isc","mpl-2.0", "lgpl-2.1","cc0-1.0","ep1-1.0","wtfpl"]


stargazers_array = [10, 100, 1000, 5000, 10000, 100000]

lang = ""
lice = ""

# The GraphQL query (with a few aditional bits included) itself defined as a multi-line string.
query = """
{
  search(query: "mirror:false language:LANG_REPLACE license:LICE_REPLACE stars:<STAR_REPLACE", type: REPOSITORY, first: 100) {
    repositoryCount
    edges {
      node {
        ... on Repository {
          name
          description
          languages(first: 10) {
            edges {
              node {
                name
              }
            }
          }
          labels(first: 10) {
            edges {
              node {
                name
              }
            }
          }
          stargazers {
            totalCount
          }
          forks {
            totalCount
          }
          defaultBranchRef {
            target {
              ... on Commit {
                zipballUrl
              }
            }
          }
          updatedAt
          createdAt
          diskUsage
          primaryLanguage {
            name
          }
          id
          databaseId
          licenseInfo {
            name
          }
          url
          sshUrl
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
  rateLimit {
    limit
    cost
    remaining
    resetAt
  }
}
"""


query2 = (
    """
{
  search(query: "mirror:false language:LANG_REPLACE license:LICE_REPLACE stars:<STAR_REPLACE", type: REPOSITORY, first: 100, after: \"END_REPLACE\") {
    repositoryCount
    edges {
      node {
        ... on Repository {
          name
          description
          languages(first: 10) {
            edges {
              node {
                name
              }
            }
          }
          labels(first: 10) {
            edges {
              node {
                name
              }
            }
          }
          stargazers {
            totalCount
          }
          forks {
            totalCount
          }
          defaultBranchRef {
            target {
              ... on Commit {
                zipballUrl
              }
            }
          }
          updatedAt
          createdAt
          primaryLanguage {
            name
          }
          diskUsage
          id
          databaseId
          licenseInfo {
            name
          }
          url
          sshUrl
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
  rateLimit {
    limit
    cost
    remaining
    resetAt
  }
}
"""
)

headers = {"Authorization": "Bearer " + api_key}
r = redis.Redis(host=redis_host, port=redis_port, db=0)


lang = language_array[0]
lice = license_array[0]
stars = str(stargazers_array[0])
endCursor = ""
this_query = query.replace("LANG_REPLACE",lang).replace("LICE_REPLACE",lice).replace("STAR_REPLACE",stars)
print("Starting fresh")



def run_query(
    query
):  # A simple function to use requests.post to make the API call. Note the json= section.
    request = requests.post(
        "https://api.github.com/graphql", json={"query": query}, headers=headers
    )
    if request.status_code == 200:
        return request.json()
    else:
        print("Failed request: "+ str(request))
        print(query)
        time.sleep(30)
        print("Retrying...")
        run_query(query)



def print_data(result):
    projects = result["data"]["search"]["edges"]
    counter = 1

    # Get all metadata here
    for project in projects:
        #print("Project "+str(counter)+" in batch")
        counter = counter + 1
        #print(project["node"]["name"])
        #if project["node"]["licenseInfo"]:
            #print(project["node"]["licenseInfo"]["name"])
        #else:
            #print(project["node"]["licenseInfo"])

        diskUsage = int(project["node"]["diskUsage"] / 1000)

        if diskUsage == 0:
            diskUsageString = str(project["node"]["diskUsage"]) + " KB"
        elif diskUsage < 1000:
            diskUsageString = str(diskUsage) + " MB"
        else:
            diskUsage = diskUsage / 1000
            diskUsageString = str(diskUsage) + " GB"

        #print(diskUsageString)
        #print (project["node"]["diskUsage"])

        #print(project["node"]["url"])
        #print(project["node"]["stargazers"]["totalCount"])

        # if project["node"]["primaryLanguage"]:
        #   print(project["node"]["primaryLanguage"]["name"])
        # else:
        #   print("No primaryLanguage")  

        # print(project["node"]["id"])
        # print(project["node"]["databaseId"])
        # print(project["node"]["defaultBranchRef"]["target"]["zipballUrl"])

        langstring=""
        langcount=0
        for lang in project["node"]["languages"]["edges"]:
          langcount = langcount + 1

          if langcount == len(project["node"]["languages"]["edges"]):
            langstring = langstring+lang["node"]["name"]
          else:
            langstring = langstring+lang["node"]["name"]+"|"

        #print (langstring)  



        #print("\n")


        #try redis stuff here
        send_to_kafka(project)
    #print("done")



def send_to_kafka(project):
    producer = KafkaProducer(bootstrap_servers=[kafka_string], 
      value_serializer=lambda m: json.dumps(m).encode('ascii')
      )
    
    proj_id = project["node"]["id"]
    if not in_redis(proj_id):
      #print("Sending to kafka")
      #print(project)
      update_redis(proj_id,project)
      producer.send('projects', {proj_id: json.dumps(project)})
      producer.flush()



def in_redis(id):
    print("Checking if id "+id+" is in redis")
    in_redis= True
    try:
      status = r.get(id).decode("utf-8") 
    except:
      status = "None"
      in_redis = False
    print(status)
    return in_redis


def update_redis(id,project):
    project_json = json.dumps(project)
    r.set(id, project_json)


def write_to_csv(result):
    projects = result["data"]["search"]["edges"]

    # Get all metadata here
    for project in projects:
        print(project["node"]["name"])
        if project["node"]["licenseInfo"]:
            print(project["node"]["licenseInfo"]["name"])
        else:
            print(project["node"]["licenseInfo"])

        diskUsage = int(project["node"]["diskUsage"] / 1000)

        if diskUsage == 0:
            diskUsageString = str(project["node"]["diskUsage"]) + " KB"
        elif diskUsage < 1000:
            diskUsageString = str(diskUsage) + " MB"
        else:
            diskUsage = diskUsage / 1000
            diskUsageString = str(diskUsage) + " GB"

        print(diskUsageString)

        print(project["node"]["url"])
        print(project["node"]["id"])
        print(project["node"]["defaultBranchRef"]["target"]["zipballUrl"])
        print("\n")






result = run_query(this_query)  # Execute the query
remaining_rate_limit = result["data"]["rateLimit"]["remaining"]
print("Remaining rate limit - {}".format(remaining_rate_limit))


print_data(result)

print("finished batch, on to next one")
print(result["data"]["search"]["pageInfo"]["hasNextPage"])
hasNextPage = result["data"]["search"]["pageInfo"]["hasNextPage"]
endCursor = result["data"]["search"]["pageInfo"]["endCursor"]

print("hasNextPage:" +str(hasNextPage))
print("endCursor: " + str(endCursor))




#comment out for now...
lang_counter = 0
lice_counter = 0
star_counter = 0

while True:
    
    print("endCursor: "+endCursor)
    r.set("endCursor", endCursor)

    if hasNextPage == False:
      this_query = query
    else:
      this_query = query2  

    try:
      result = run_query(this_query.replace("END_REPLACE",endCursor).replace("LANG_REPLACE",lang).replace("LICE_REPLACE",lice).replace("STAR_REPLACE",stars))  # Execute the query
      #ßprint(result)
      remaining_rate_limit = result["data"]["rateLimit"]["remaining"]
      print("Remaining rate limit - {}".format(remaining_rate_limit))

      # Gather
      print_data(result)
      hasNextPage = result["data"]["search"]["pageInfo"]["hasNextPage"]
      endCursor = result["data"]["search"]["pageInfo"]["endCursor"]

    except:
      continue


    # check rate limit
    if int(remaining_rate_limit) == 0:
        # add some logic to check rate limit
        time.sleep(3600)



    if hasNextPage == False:
      print("time to start new query")
      endCursor = ""
      r.delete("endCursor")

      #increment counters
      lang_counter = lang_counter +1

      if lang_counter > len(language_array)-1:
        lang_counter = 0
        lice_counter = lice_counter +1

      if lice_counter > len(license_array)-1:
        star_counter = star_counter + 1
        lice_counter = 0

      if star_counter > len(stargazers_array)-1:
        star_counter = 0        

      lang = language_array[lang_counter]
      lice = license_array[lice_counter]
      stars = str(stargazers_array[star_counter])









