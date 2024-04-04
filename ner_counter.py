from __future__ import print_function

import sys
import os
import time

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("""
        Usage: ner_counter.py <bootstrap-servers> <subscribe-type> <src_topic> <dst_topic>
        """, file=sys.stderr)
        sys.exit(-1)
    
    import praw
    from kafka import KafkaProducer, KafkaConsumer
    from dotenv import load_dotenv

    from pyspark.sql import SparkSession
    from pyspark.sql.functions import explode
    from pyspark.sql.functions import split

    import string
    import spacy
    import spacy.cli
    # Load the English language model. small (sm), medium (md) and big (bg)
    spacy.cli.download("en_core_web_md")
    nlp = spacy.load("en_core_web_md")

    bootstrapServers = sys.argv[1]
    subscribeType = sys.argv[2]
    src_topic = sys.argv[3]
    dst_topic = sys.argv[4]
    
    config = load_dotenv()
    
    src_consumer = KafkaConsumer(src_topic)
    producer = KafkaProducer(bootstrap_servers=bootstrapServers)
    
    spark = SparkSession\
        .builder\
        .appName("NERCounter")\
        .getOrCreate()

    print("Starting computation")

    # Create DataSet representing the stream of input comments from kafka
    comments = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", bootstrapServers)\
        .option(subscribeType, src_topic)\
        .load()\
        .selectExpr("CAST(value AS STRING)")
    
    print("======================")
    print("======================")
    print(type(comments))
    comments.show()
    print("======================")
    print("======================")
        
    # # Split the comments into words
    # words = comments.select(
    #     # explode turns each item in an array into a separate row
    #     explode(
    #         split(comments.value, ' ')
    #     ).alias('word')
    # )
    
    # # Filter out words that are named entities
    # # Process the text with spaCy nlp
    # doc = nlp(text)

    # # Found the named entities labels from this notebook, there could be more
    # # https://www.kaggle.com/code/curiousprogrammer/entity-extraction-and-classification-using-spacy?scriptVersionId=11364473&cellId=9
    # named_entities = ["PERSON", "ORG", "GPE", "LOC", "EVENT", "WORK_OF_ART"]

    # # Iterate over the labeled entities
    # for entity in doc.ents:
    #     # Check if the label is a named entity
    #     if entity.label_.upper() in named_entities:
    #         # print(entity.text, entity.label_)
    #         ret.append(entity.text)

    # return ret
    
    # Generate running word count
    wordCounts = words.groupBy('word').count()

    spark.sparkContext.setLogLevel("ERROR")