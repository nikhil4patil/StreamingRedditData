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

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import explode, split, count

import praw
from kafka import KafkaProducer, KafkaConsumer
from dotenv import load_dotenv

import string
import spacy
import spacy.cli
# Load the English language model. small (sm), medium (md) and big (bg)
spacy.cli.download("en_core_web_md")
nlp = spacy.load("en_core_web_md")

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("""
        Usage: structured_kafka_wordcount.py <bootstrap-servers> <subscribe-type> <topics>
        """, file=sys.stderr)
        sys.exit(-1)

    bootstrapServers = sys.argv[1]
    subscribeType = sys.argv[2]
    src_topic = sys.argv[3]
    dst_topic = sys.argv[4]

    spark = SparkSession\
        .builder\
        .appName("NERCounter")\
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Create DataSet representing the stream of input lines from kafka
    lines = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", bootstrapServers)\
        .option(subscribeType, src_topic)\
        .load()\
        .selectExpr("CAST(value AS STRING)")
    
    # Can we convert lines to rdd, do hwk1 process, return back to DF?
    # We are getting sentences now. How do we do NER on it?
    # Output to Kafka must have value column, of type string. What else can we do?
    
    # Filter words that are named entities
    lines.withColumn("NEs", nlp(lines.value))

    # Generate running word count
    comment_counts = lines.filter("value LIKE '% NOT %'")
    
    # Start running the query that prints the running counts to the console
    query = comment_counts\
        .writeStream\
        .option("checkpointLocation", "/tmp/")\
        .outputMode('update')\
        .format('kafka')\
        .option("kafka.bootstrap.servers", bootstrapServers)\
        .option("topic", dst_topic)\
        .start()

    query.awaitTermination()

















































# #
# # Licensed to the Apache Software Foundation (ASF) under one or more
# # contributor license agreements.  See the NOTICE file distributed with
# # this work for additional information regarding copyright ownership.
# # The ASF licenses this file to You under the Apache License, Version 2.0
# # (the "License"); you may not use this file except in compliance with
# # the License.  You may obtain a copy of the License at
# #
# #    http://www.apache.org/licenses/LICENSE-2.0
# #
# # Unless required by applicable law or agreed to in writing, software
# # distributed under the License is distributed on an "AS IS" BASIS,
# # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# # See the License for the specific language governing permissions and
# # limitations under the License.
# #

# """
#  Consumes messages from one or more topics in Kafka and does wordcount.
#  Usage: structured_kafka_wordcount.py <bootstrap-servers> <subscribe-type> <topics>
#    <bootstrap-servers> The Kafka "bootstrap.servers" configuration. A
#    comma-separated list of host:port.
#    <subscribe-type> There are three kinds of type, i.e. 'assign', 'subscribe',
#    'subscribePattern'.
#    |- <assign> Specific TopicPartitions to consume. Json string
#    |  {"topicA":[0,1],"topicB":[2,4]}.
#    |- <subscribe> The topic list to subscribe. A comma-separated list of
#    |  topics.
#    |- <subscribePattern> The pattern used to subscribe to topic(s).
#    |  Java regex string.
#    |- Only one of "assign, "subscribe" or "subscribePattern" options can be
#    |  specified for Kafka source.
#    <topics> Different value format depends on the value of 'subscribe-type'.

#  Run the example
#     `$ bin/spark-submit examples/src/main/python/sql/streaming/structured_kafka_wordcount.py \
#     host1:port1,host2:port2 subscribe topic1,topic2`
# """
# from __future__ import print_function

# import sys

# from pyspark.sql import SparkSession
# from pyspark.sql.types import StringType
# from pyspark.sql.functions import explode, split, count

# if __name__ == "__main__":
#     if len(sys.argv) != 5:
#         print("""
#         Usage: structured_kafka_wordcount.py <bootstrap-servers> <subscribe-type> <topics>
#         """, file=sys.stderr)
#         sys.exit(-1)

#     bootstrapServers = sys.argv[1]
#     subscribeType = sys.argv[2]
#     src_topic = sys.argv[3]
#     dst_topic = sys.argv[4]

#     spark = SparkSession\
#         .builder\
#         .appName("NERCounter")\
#         .getOrCreate()

#     spark.sparkContext.setLogLevel("ERROR")

#     # Create DataSet representing the stream of input lines from kafka
#     lines = spark\
#         .readStream\
#         .format("kafka")\
#         .option("kafka.bootstrap.servers", bootstrapServers)\
#         .option(subscribeType, src_topic)\
#         .load()\
#         .selectExpr("CAST(value AS STRING)")

#     # Split the lines into words
#     words = lines.select(
#         # explode turns each item in an array into a separate row
#         explode(
#             split(lines.value, ' ')
#         ).alias('word')
#     )

#     # Generate running word count
#     wordCounts = words\
#         .groupBy('word')\
#         .count()\
#         .withColumnRenamed("count","value")
    
#     wordCounts = wordCounts.withColumn('value', wordCounts['value'].cast(StringType()))
#     # wordCounts = words.groupBy('word').agg(count("word").alias("value"))

#     # Start running the query that prints the running counts to the console
#     query = wordCounts\
#         .writeStream\
#         .option("checkpointLocation", "/tmp/")\
#         .outputMode('update')\
#         .format('kafka')\
#         .option("kafka.bootstrap.servers", bootstrapServers)\
#         .option("topic", dst_topic)\
#         .start()

#     query.awaitTermination()




















































# from __future__ import print_function

# import sys
# import os
# import time

# if __name__ == "__main__":
#     if len(sys.argv) != 5:
#         print("""
#         Usage: ner_counter.py <bootstrap-servers> <subscribe-type> <src_topic> <dst_topic>
#         """, file=sys.stderr)
#         sys.exit(-1)
    
#     import praw
#     from kafka import KafkaProducer, KafkaConsumer
#     from dotenv import load_dotenv

#     from pyspark.sql import SparkSession
#     from pyspark.sql.functions import explode
#     from pyspark.sql.functions import split

#     import string
#     import spacy
#     import spacy.cli
#     # Load the English language model. small (sm), medium (md) and big (bg)
#     spacy.cli.download("en_core_web_md")
#     nlp = spacy.load("en_core_web_md")

#     bootstrapServers = sys.argv[1]
#     subscribeType = sys.argv[2]
#     src_topic = sys.argv[3]
#     dst_topic = sys.argv[4]
    
#     config = load_dotenv()
    
#     src_consumer = KafkaConsumer(src_topic)
#     producer = KafkaProducer(bootstrap_servers=bootstrapServers)
    
#     spark = SparkSession\
#         .builder\
#         .appName("NERCounter")\
#         .getOrCreate()
        
#     spark.sparkContext.setLogLevel("ERROR")

#     print("======================") 
#     print("======================")
#     print("Starting computation")
#     print("======================")
#     print("======================")
    
#     # Create DataSet representing the stream of input comments from kafka
#     comments = spark\
#         .readStream\
#         .format("kafka")\
#         .option("kafka.bootstrap.servers", bootstrapServers)\
#         .option(subscribeType, src_topic)\
#         .load()\
#         .selectExpr("CAST(value AS STRING)")
    
#     # # Split the comments into words
#     # words = comments.select(
#     #     # explode turns each item in an array into a separate row
#     #     explode(
#     #         split(comments.text, ' ')
#     #     ).alias('word')
#     # )
    
#     # Generate running word count
#     comment_counts = comments.filter("value LIKE '% NOT %'")
    
#     # Start running the query that prints the running counts to the console
#     query = comment_counts\
#         .writeStream\
#         .option("checkpointLocation", "/tmp/")\
#         .outputMode('update')\
#         .format('kafka')\
#         .option("kafka.bootstrap.servers", bootstrapServers)\
#         .option("topic", dst_topic)\
#         .start()

#     query.awaitTermination()
    
#     # # Filter out words that are named entities
#     # # Process the text with spaCy nlp
#     # doc = nlp(text)

#     # # Found the named entities labels from this notebook, there could be more
#     # # https://www.kaggle.com/code/curiousprogrammer/entity-extraction-and-classification-using-spacy?scriptVersionId=11364473&cellId=9
#     # named_entities = ["PERSON", "ORG", "GPE", "LOC", "EVENT", "WORK_OF_ART"]

#     # # Iterate over the labeled entities
#     # for entity in doc.ents:
#     #     # Check if the label is a named entity
#     #     if entity.label_.upper() in named_entities:
#     #         # print(entity.text, entity.label_)
#     #         ret.append(entity.text)

#     # return ret
    
#     # Generate running word count
#     # wordCounts = words.groupBy('word').count()
