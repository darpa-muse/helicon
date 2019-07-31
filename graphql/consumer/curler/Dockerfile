FROM python:3.6.8
RUN apt-get update
RUN apt-get install -y vim curl unzip
RUN mkdir -p /home/muse
WORKDIR /home/muse
COPY curler_kafka.py /home/muse

#pip installs
RUN pip install redis
RUN pip install kafka
RUN pip install requests

# Start command
CMD python curler_kafka.py
