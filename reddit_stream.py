#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
 Consumes messages from one or more topics in Kafka and does wordcount.
 Usage: structured_kafka_wordcount.py <bootstrap-servers> <subscribe-type> <topics>
   <bootstrap-servers> The Kafka "bootstrap.servers" configuration. A
   comma-separated list of host:port.
   <subscribe-type> There are three kinds of type, i.e. 'assign', 'subscribe',
   'subscribePattern'.
   |- <assign> Specific TopicPartitions to consume. Json string
   |  {"topicA":[0,1],"topicB":[2,4]}.
   |- <subscribe> The topic list to subscribe. A comma-separated list of
   |  topics.
   |- <subscribePattern> The pattern used to subscribe to topic(s).
   |  Java regex string.
   |- Only one of "assign, "subscribe" or "subscribePattern" options can be
   |  specified for Kafka source.
   <topics> Different value format depends on the value of 'subscribe-type'.

 Run the example
    `$ bin/spark-submit examples/src/main/python/sql/streaming/structured_kafka_wordcount.py \
    host1:port1,host2:port2 subscribe topic1,topic2`
"""
from __future__ import print_function

import sys
import os
import time
import datetime

import praw
from kafka import KafkaProducer
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("""
        Usage: reddit_stream.py <bootstrap-servers> <subscribe-type> <dst_topic>
        """, file=sys.stderr)
        sys.exit(-1)

    bootstrapServers = sys.argv[1]
    subscribeType = sys.argv[2]
    dst_topic = sys.argv[3]

    config = load_dotenv()

    kafka_producer = KafkaProducer(bootstrap_servers=bootstrapServers)
    
    spark = SparkSession\
        .builder\
        .appName("RedditStream")\
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # reddit = praw.Reddit(
    #     client_id=os.environ["client_id"],
    #     client_secret=os.environ["client_secret"],
    #     user_agent=os.environ["user_agent"],
    # )


    # Get new batch of data every x seconds
    sec_wait = 5

    try:
        while True:
            # Get the kafka producer 
            producer = KafkaProducer(bootstrap_servers=bootstrapServers)

            # Load and send the data to kafka topic. Loads 100 comments at a time.
            # for index, comment in enumerate(reddit.subreddit("all").comments()):
            for index, comment in enumerate(["Obama said this from the White House.", "Obama did NOT say this from the White House."]):
                # Send the comment text to Kafka topic
                # producer.send(dst_topic, comment.body.encode("utf-8"))
                producer.send(dst_topic, f"{datetime.datetime.now().strftime('%x_%X')}__{comment}".encode("utf-8"))

            # Flush all the messages to the Kafka topic
            kafka_producer.flush()
    
            # Sleep for a bit to wait for new batch of data
            time.sleep(sec_wait)
    except Exception:
        producer.close()
        exit(0)