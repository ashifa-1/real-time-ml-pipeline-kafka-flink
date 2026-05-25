FROM python:3.10
WORKDIR /app
COPY . .
RUN pip install kafka-python python-dotenv
CMD ["python", "producer/producer.py"]