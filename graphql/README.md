# GraphQL Miner

This project will mine github projects for metadata and code. it will also attempt to build projects, and extract doxygen comments.


## Producer

This has the mining script to gather metadata for each project, and store output in redis database. Kafka is used to send updates for downstream tasks to begin.

### Environment variables needed
api_key = os.environ["GITHUB_TOKEN"]  
redis_host = os.environ["REDIS_HOST"]   
redis_port = int(os.environ["REDIS_PORT"])   
kafka_host = os.environ["KAFKA_HOST"]   
kafka_port = int(os.environ["KAFKA_PORT"])  

### Run Miner

To build Docker image   
./build.sh 

To run locally   
python mine.py


## Consumer

These are downstream tasks that require data to be in the redis database

### Curler

This has the script to download projects


### Environment variables needed
api_key = os.environ["GITHUB_TOKEN"]  
redis_host = os.environ["REDIS_HOST"]   
redis_port = int(os.environ["REDIS_PORT"])   
kafka_host = os.environ["KAFKA_HOST"]   
kafka_port = int(os.environ["KAFKA_PORT"])  

### Run Curler

To build Docker image   
./build.sh 

To run locally   
python curler_kafka.py

### Doxygen

This has the script to run doxygen on projects


### Environment variables needed
api_key = os.environ["GITHUB_TOKEN"]  
redis_host = os.environ["REDIS_HOST"]   
redis_port = int(os.environ["REDIS_PORT"])   
kafka_host = os.environ["KAFKA_HOST"]   
kafka_port = int(os.environ["KAFKA_PORT"])  

### Run Doxygen

To build Docker image   
./build.sh 

To run locally   
python doxygen_list.py

Note: projects.txt should be filled with github projects you want to run doxygen on.

### Builder (jjhenkel)

This has the script to run doxygen on projects


### Environment variables needed
api_key = os.environ["GITHUB_TOKEN"]  
redis_host = os.environ["REDIS_HOST"]   
redis_port = int(os.environ["REDIS_PORT"])   

### Run Builder

To build Docker image   
./build.sh 

To run locally   
python jjhenkel_consume.py










