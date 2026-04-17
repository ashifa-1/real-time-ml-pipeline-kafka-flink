FROM python:3.10

WORKDIR /app

COPY . .

RUN pip install kafka-python streamlit

CMD ["python", "producer/producer.py"]