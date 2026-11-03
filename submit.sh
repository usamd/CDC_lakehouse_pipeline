@echo off
docker compose exec spark /opt/spark/bin/spark-submit --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 --conf spark.jars.ivy=/tmp/.ivy /opt/app/src/%1 %2


