FROM apache/airflow:3.3.0

USER root

RUN apt-get update && apt-get install -y --no-install-recommends default-jre-headless procps &&  apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"
USER airflow

RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark apache-airflow-providers-postgres pyspark==3.5.3 pandas numpy
