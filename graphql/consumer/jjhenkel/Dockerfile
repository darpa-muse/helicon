FROM python:2.7.15
RUN apt-get update
RUN apt-get install -y vim curl unzip make cmake
RUN mkdir -p /home/muse
WORKDIR /home/muse
COPY jjhenkel_consume.py /home/muse
COPY update_build.py /home/muse
COPY jjhenkel.sh /home/muse


#pip installs
RUN pip install redis
RUN pip install kafka
RUN pip install requests

# Start command
CMD python jjhenkel_consume.py
