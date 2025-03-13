FROM bitnami/spark

USER root

RUN apt-get -y update && apt-get -y install wget

RUN wget https://jdbc.postgresql.org/download/postgresql-42.2.5.jar

RUN mv postgresql-42.2.5.jar /opt/bitnami/spark/jars

RUN mkdir -p /opt/bitnami/app/scripts_spark

RUN mkdir -p /opt/bitnami/app/data

RUN chmod -R 777 /opt/bitnami/app

COPY requirements.txt /opt/bitnami/app/scripts_spark

RUN pip3 install -r /opt/bitnami/app/scripts_spark/requirements.txt