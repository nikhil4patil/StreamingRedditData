import json
import string
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import explode, split, udf, current_timestamp, window
from kafka import KafkaProducer

print("Loading spacy")
import spacy
import spacy.cli

# Load the English language model. small (sm), medium (md) and big (bg)
spacy.cli.download("en_core_web_md")
nlp = spacy.load("en_core_web_md")

def preprocess_text(text):
	# Remove punctuations
	return "".join([ch.lower() for ch in text if ch == " " or (ch.isalpha() and ch not in string.punctuation)])

# Function to extract named entities from text
def extract_entities(text):
	doc = nlp(preprocess_text(text))
	return [ent.text for ent in doc.ents]

def send_to_kafka(out_df, id):
	print("Inside send_to_kafka")
	producer = KafkaProducer(bootstrap_servers='kafka:9092')
	data = out_df.rdd.collect()
	print("data", data)
	for row in data:
		print(row)
		message = {"entity": row.entity, "count": row["count"]}
		producer.send("topic2", json.dumps(message).encode('utf-8'))
	producer.flush()
	producer.close()

if __name__ == "__main__":
	if len(sys.argv) != 4:
		print("""
		Usage: structured_kafka_wordcount.py <bootstrap-servers> <subscribe-type> <topics>
		""", file=sys.stderr)
		sys.exit(-1)

	bootstrapServers = sys.argv[1]
	src_topic = sys.argv[2]
	dst_topic = sys.argv[3]

	spark = SparkSession.builder \
					.appName("NamedEntityCounter") \
					.getOrCreate()

	spark.sparkContext.setLogLevel("ERROR")

	print("Starting computation")

	# Define the schema for the named entity count
	schema = StructType([
		StructField("entity", StringType(), True),
		StructField("count", IntegerType(), True),
		StructField("curr_timestamp", TimestampType(), True),
	])

	print("Extracting entities")
			# Define a user-defined function (UDF) to extract named entities
	extract_entities_udf = udf(extract_entities, ArrayType(StringType()))

	df = spark.readStream \
		.format("kafka") \
		.option("kafka.bootstrap.servers", bootstrapServers) \
		.option("subscribe", src_topic) \
		.load()

	print("Tokenizing text")
	# Tokenize the message text
	# Split the lines into words
	df = df.select(
		# explode turns each item in an array into a separate row
		explode(
				split(df.value, ' ')
		).alias('word')
	)

	# Extract named entities from the text
	df = df.withColumn("curr_timestamp", current_timestamp())
	df = df.withColumn("entities", extract_entities_udf("word"))

	# Explode the array of named entities to individual rows
	df = df.select(explode("entities").alias("entity"), "curr_timestamp")

	# Group by entity and count occurrences
	agg_df = df \
		.withWatermark("curr_timestamp", "31 seconds") \
		.groupBy(
			window("curr_timestamp", "31 seconds", "31 seconds"),
			"entity"
		).count()

	print(f"About to send data to kafka {dst_topic}")

	query = agg_df.writeStream \
			.outputMode("append") \
			.foreachBatch(send_to_kafka) \
			.start()

	query.awaitTermination()