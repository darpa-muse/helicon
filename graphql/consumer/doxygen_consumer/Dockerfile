FROM 100.120.0.2:5000/extractor:doxygen


RUN apt-get update
RUN apt-get install -y python-pip
RUN mkdir /doxygen
WORKDIR /home/muse
COPY doxygen /doxygen
COPY update_doxygen.py /home/muse
COPY doxy.sh /home/muse
COPY doxygen_redis.py /home/muse
COPY start_doxygen.py /home/muse
COPY doxygen_list.py /home/muse
COPY projects.txt /home/muse

#pip installs
RUN pip install redis
RUN pip install requests

# Start command
CMD python doxygen_list.py
