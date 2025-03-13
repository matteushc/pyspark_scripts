.EXPORT_ALL_VARIABLES:

BD_SERVER_URL = jdbc:postgresql://code-challenge_db_1:5432/northwind
BD_USER = northwind_user
BD_PASSWORD = thewindisblowing

build:
	docker build -t bitnami/pyspark ./spark_docker/

start_environment:
	mkdir data/tables
	docker-compose up -d

run_pipeline:
	docker exec \
		-e BD_SERVER_URL=$$BD_SERVER_URL -e BD_USER=$$BD_USER -e BD_PASSWORD=$$BD_PASSWORD \
    	code-challenge_spark_1 spark-submit \
    	--jars /opt/bitnami/spark/jars/postgresql-42.2.5.jar /opt/bitnami/app/scripts_spark/pipeline.py

run_pipeline_date:
	docker exec \
		-e BD_SERVER_URL=$$BD_SERVER_URL -e BD_USER=$$BD_USER -e BD_PASSWORD=$$BD_PASSWORD \
    	code-challenge_spark_1 spark-submit \
    	--jars /opt/bitnami/spark/jars/postgresql-42.2.5.jar /opt/bitnami/app/scripts_spark/pipeline.py -ed $$DATA

stop_environment:
	docker-compose down
