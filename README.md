# StreamingRedditData

Install required packages:
    `pip install praw kafka-python python-dotenv pyspark spacy`

Starting Zookeeper and Kafka (from Kafka dir):
1) Remove old logs that cause issues for some reason
    `rm -rf /tmp/kafka-logs /tmp/zookeeper`
2) Start Zookeeper
    `bin/zookeeper-server-start.sh config/zookeeper.properties`
3) Start Kafa
    `bin/kafka-server-start.sh config/server.properties`
4) Check if topic1 and topic2 are created
    `bin/kafka-topics.sh --bootstrap-server localhost:9092 --list`
    - If topics not created
    topic1 - `bin/kafka-topics.sh --create --topic topic1 --bootstrap-server localhost:9092`
    topic2 - `bin/kafka-topics.sh --create --topic topic2 --bootstrap-server localhost:9092`


Create 1 MS Terminals tab with 4 views:
    In StreamingRedditData dir:
        1) Gets and outputs streaming data to topic1
            `$SPARK_HOME/bin/spark-submit reddit_stream.py localhost:9092 subscribe topic1`
        2) Reads from topic1, does NER, writes table to topic2
            `$SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 ner_counter.py localhost:9092 subscribe topic1 topic2`
    In Kafka dir:
        3) Output messages in topic1
            `bin/kafka-console-consumer.sh --topic topic1  --bootstrap-server localhost:9092`
        4) Output messages in topic2
            `bin/kafka-console-consumer.sh --topic topic2  --bootstrap-server localhost:9092`

Note:
- If you want to write to console and output logs to a file.
    `$SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 ner_counter.py localhost:9092 subscribe topic1 topic2 2>&1 | tee logs.txt`

TODO:
- Writing to Kafka requires a "value" column of type String. Might need to format the table as a string and then send it to topic2?