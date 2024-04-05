#!/bin/bash

# Submit preprocess_job.py to Spark cluster
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 named-entity.py kafka:9092 topic1 topic2
