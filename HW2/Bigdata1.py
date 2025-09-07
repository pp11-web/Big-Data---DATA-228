import csv
from confluent_kafka import Producer


def produce_to_kafka(csv_file, bootstrap_servers, topic):
    producer = Producer({'bootstrap.servers': bootstrap_servers})

    with open(csv_file, 'r', encoding='utf-8') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            print(row)
            key = None  # Assuming no key
            value = ','.join(row)  # Assuming each row is a CSV record
            producer.produce(topic, key=key, value=value)
            producer.flush()

# Example usage:
csv_file = 'c:\\SINDHUNAGESHA\\redditsindhu1.csv'
bootstrap_servers = 'localhost:9092'
topic = 'redditsindhu'
produce_to_kafka(csv_file, bootstrap_servers, topic)


