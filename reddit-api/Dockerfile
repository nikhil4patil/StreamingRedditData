FROM python:3.11.9-alpine3.19
USER root
COPY requirements.txt /app/requirements.txt
WORKDIR /app

# Install dependencies from requirements.txt
RUN pip install -r requirements.txt

# Copy the rest of the files to the working directory
COPY . /app/

# Run the reddit_stream.py script with the topic1 argument
CMD ["python", "reddit_stream.py", "topic1"]