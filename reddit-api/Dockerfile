FROM python:3.11.9-alpine3.19
USER root
COPY requirements.txt /app/requirements.txt
WORKDIR /app
RUN pip install -r requirements.txt

COPY . /app/

# Run the script with spark-submit
CMD ["python", "reddit_stream.py", "topic1"]