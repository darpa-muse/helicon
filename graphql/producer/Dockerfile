FROM python:3.6.8
RUN apt-get update
RUN apt-get install -y vim curl
RUN mkdir -p /home/muse
WORKDIR /home/muse
COPY mine.py /home/muse

#pip installs
RUN pip install redis
RUN pip install kafka
RUN pip install requests

# Start command
CMD python mine.py
