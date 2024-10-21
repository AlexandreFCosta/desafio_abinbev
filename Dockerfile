FROM apache/airflow:2.7.1-python3.11

USER root

# Criação de diretórios e ajuste de permissões
RUN mkdir -p /opt/airflow/silver_layer && \
    chown -R root:root /opt/airflow && \
    chmod -R 777 /opt/airflow

RUN mkdir -p /opt/airflow/gold_layer && \
    chown -R root:root /opt/airflow && \
    chmod -R 777 /opt/airflow

RUN mkdir -p /opt/airflow/bronze_layer && \
    chown -R root:root /opt/airflow && \
    chmod -R 777 /opt/airflow

RUN mkdir -p /opt/airflow/data_quality && \
    chown -R root:root /opt/airflow && \
    chmod -R 777 /opt/airflow

# Install necessary packages, including curl
RUN apt-get update && \
    apt-get install -y jq gcc python3-dev openjdk-11-jdk curl && \
    apt-get clean

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64

# Instalar Spark
RUN curl -L https://archive.apache.org/dist/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz -o spark.tgz \
    && tar -xzvf spark.tgz -C /usr/local/ \
    && mv /usr/local/spark-3.5.3-bin-hadoop3 /usr/local/spark \
    && rm spark.tgz

ENV SPARK_HOME=/usr/local/spark

ENV HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop
ENV YARN_CONF_DIR=/usr/local/hadoop/etc/hadoop

RUN groupadd -r airflow && useradd -r -g airflow myuser

USER airflow

# Install Python packages
RUN pip install apache-airflow[apache.spark]==2.7.1 apache-airflow-providers-openlineage>=1.8.0 apache-airflow-providers-apache-spark>=4.0.0 apache-airflow-providers-papermill papermill pyspark pytest requests
