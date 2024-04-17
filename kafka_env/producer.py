import json
from kafka import KafkaProducer
from time import sleep

# function to load the preprocessed data
def load_dataset(file_path):
    with open(file_path, 'r', encoding= 'utf-8') as file:
        return json.load(file)

# create an instance of producer
producer= KafkaProducer(bootstrap_servers= ['localhost:9092'], value_serializer= lambda key: json.dumps(key).encode('utf-8'))

# function to send data to a kafka topic
def transmit_data(producer, topic, data, interval=2):
    for record in data:
        producer.send(topic, value=record)
        producer.flush()
        print(f"Record sent to Kafka topic {topic}: {record}")
        sleep(interval) 

if __name__ == "__main__":
    file_path= 'preprocessed_dataset.json'
    dataset= load_dataset(file_path)
    topic_name= 'amazon_metadata_stream'
    transmit_data(producer, topic_name, dataset, interval= 2) # interval passed here is 2 meaning 2 second interval between each record being streamed
