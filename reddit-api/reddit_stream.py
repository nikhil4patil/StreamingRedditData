import sys
import os
import time
import praw
from kafka import KafkaProducer
from dotenv import load_dotenv

if __name__ == "__main__":
	if len(sys.argv) != 2:
		print("""
		Usage: reddit_stream.py <topics>
		""", file=sys.stderr)
		sys.exit(-1)

	topics = sys.argv[1]

	config = load_dotenv()

	print("Kafka producer created")

	reddit = praw.Reddit(
		client_id=os.environ["client_id"],
		client_secret=os.environ["client_secret"],
		user_agent=os.environ["user_agent"],
	)
	print("Reddit API connected")

	# Get new batch of data every x seconds
	sec_wait = os.environ["polling_interval_seconds"]

	# List of subreddits to stream data from
	subreddits = os.environ["subreddits"]

	# Get the kafka producer 
	producer = KafkaProducer(bootstrap_servers="kafka:9092")
	while True:

		# Load and send the data to kafka topic. Loads new posts in r/all subbreddit.
		for index, submission in enumerate(reddit.subreddit(subreddits).new()):
			# Send the post title text to Kafka topic
			producer.send(topics, submission.title.encode('utf-8'))

		# Flush all the messages to the Kafka topic
		producer.flush()
 
		# Sleep for a bit to wait for new batch of data
		time.sleep(int(sec_wait))
