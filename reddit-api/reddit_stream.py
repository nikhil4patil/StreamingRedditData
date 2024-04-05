import sys
import os
import time
import praw
from kafka import KafkaProducer
from dotenv import load_dotenv

if __name__ == "__main__":
	if len(sys.argv) != 2:
		print("""
		Usage: reddit_stream.py <bootstrap-servers> <topics>
		""", file=sys.stderr)
		sys.exit(-1)

	print("Sys args: ", sys.argv)
	# bootstrapServers = sys.argv[1]
	# subscribeType = sys.argv[2]
	topics = sys.argv[1]

	config = load_dotenv()

	kafka_producer = KafkaProducer(bootstrap_servers="kafka:9092")
	print("Kafka producer created")

	reddit = praw.Reddit(
		client_id=os.environ["client_id"],
		client_secret=os.environ["client_secret"],
		user_agent=os.environ["user_agent"],
	)
	print("Reddit API connected")

	# Get new batch of data every x seconds
	sec_wait = 30

	while True:
		# Get the kafka producer 
		producer = KafkaProducer(bootstrap_servers="kafka:9092")

		# Load and send the data to kafka topic. Loads 100 comments at a time.
		for index, comment in enumerate(reddit.subreddit("all").comments()):
			# Send the comment text to Kafka topic
			producer.send(topics, comment.body.encode("utf-8"))

		# Flush all the messages to the Kafka topic
		kafka_producer.flush()
 
		# Sleep for a bit to wait for new batch of data
		time.sleep(sec_wait)
