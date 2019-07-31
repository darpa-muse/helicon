FROM python:3.6.8
RUN apt-get update
RUN apt-get install -y vim curl unzip hashdeep
RUN mkdir -p /home/muse
WORKDIR /home/muse
COPY corpus_files.py /home/muse
COPY file_predictor_consume.py /home/muse
COPY md5hash.sh /home/muse


#pip installs
RUN pip install redis
RUN pip install kafka
RUN pip install requests

# Start command
CMD python jjhenkel_consume.py
