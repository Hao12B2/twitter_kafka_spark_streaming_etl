from time import sleep
import csv
import json
from kafka import KafkaProducer


producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'], 
    value_serializer = lambda x: json.dumps(x).encode('utf-8')
)


with open('..\\model_sparkml\\twitter_validation.csv', encoding='utf-8') as file_object:
    reader = csv.reader(file_object)
    for data in reader:
        producer.send('tweets', value=data)
        sleep(3)