#FROM python:3.6-alpine3.10
#FROM python:3.9.5-alpine3.13
#RUN apk --update add --no-cache g++
#RUN pip install pandas

FROM nickgryg/alpine-pandas:latest

ARG SPARK_VERSION=3.0.2
ARG HADOOP_VERSION_SHORT=3.2
ARG HADOOP_VERSION=3.2.0

RUN apk add --no-cache bash openjdk8-jre && \
  apk add --no-cache libc6-compat && \
  ln -s /lib/libc.musl-x86_64.so.1 /lib/ld-linux-x86-64.so.2  && \
   pip install pyspark pytest

#RUN pip install pytest

# Download and extract Spark
RUN wget -qO- https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION_SHORT}.tgz | tar zx -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION_SHORT} /opt/spark

ENV PATH="/opt/spark/bin:${PATH}"
ENV PYSPARK_PYTHON=python3
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9-src.zip:${PYTHONPATH}"

#Copy python script for batch
ADD analyzer /app/analyzer
WORKDIR /app/

# Define default command
#CMD ["/bin/bash"]
ENTRYPOINT ["python", "analyzer/processor.py"]
