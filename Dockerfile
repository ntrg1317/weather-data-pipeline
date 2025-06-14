FROM apache/airflow:2.10.2-python3.12

ENV AIRFLOW_HOME=/opt/airflow
WORKDIR $AIRFLOW_HOME

# Copy and install Python dependencies
USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade -r requirements.txt
RUN pip install cqlsh

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps \
        curl \
        bzip2 \
        parallel && \
    apt-get autoremove -yqq --purge && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* \

USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
RUN export JAVA_HOME