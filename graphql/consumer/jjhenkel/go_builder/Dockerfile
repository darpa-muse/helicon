FROM python:2.7.15
RUN apt-get update
RUN apt-get install -y vim curl unzip make cmake tar
RUN mkdir -p /home/muse
WORKDIR /home/muse
COPY jjhenkel_consume.py /home/muse
COPY update_build.py /home/muse
COPY jjhenkel_go.sh /home/muse

#install go
COPY go1.12.4.linux-amd64.tar.gz /home/muse
RUN tar -C /usr/local/ -xzf go1.12.4.linux-amd64.tar.gz
ENV PATH="/usr/local/go/bin:${PATH}"


#pip installs
RUN pip install redis
RUN pip install kafka
RUN pip install requests

# Start command
CMD python jjhenkel_consume.py
