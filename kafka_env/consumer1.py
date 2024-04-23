from confluent_kafka import Consumer, KafkaError
from collections import defaultdict
from itertools import combinations
from pymongo import MongoClient
import json


# MongoDB connection setup
client = MongoClient('mongodb://localhost:27017')  
db = client['AprioriDB']
apriori_collection= db['apriori_scores']


class Apriori:
    def __init__(self, min_support, min_confidence):
        self.min_support = min_support
        self.min_confidence = min_confidence
        self.itemsets = defaultdict(int)
        self.pair_freq = defaultdict(int)

    def process_message(self, message):
        try:
            data = json.loads(message.value())
            if 'title' in data:
                title = data['title']
                self.itemsets[title] += 1
        except Exception as e:
            print(f"Error processing message: {e}")

    def generate_associations(self):
        frequent_itemsets = {itemset: support for itemset, support in self.itemsets.items() if support >= self.min_support}
        pairs = list(combinations(frequent_itemsets.keys(), 2))
        
        for pair in pairs:
            pair_support = min(self.itemsets[pair[0]], self.itemsets[pair[1]])
            self.pair_freq[pair] = pair_support

        print("\nFrequent Pairs and Their Counts:")
        for pair, support in self.pair_freq.items():
            print(f"{pair}: {support}")

        print("\nAssociation Rules and Their Confidence Scores Saved In Database:")
        for pair, support in self.pair_freq.items():
            rule_confidence = support / self.itemsets[pair[0]]
            if rule_confidence >= self.min_confidence:
                # print(f"{pair[0]} -> {pair[1]}: {rule_confidence:.2f}")
                # Insert rule confidence into MongoDB
                try:
                    apriori_collection.insert_one({
                        "confidence": rule_confidence
                    })
                except Exception as e:
                    print(f"An error occurred while inserting data: {e}")

# Kafka consumer configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'apriori-group',
    'auto.offset.reset': 'earliest'
}

# Kafka topic to consume from
topic = 'amazon_metadata_stream'

if __name__ == "__main__":
    apriori = Apriori(min_support=4, min_confidence=0.5)  # Set your minimum support and confidence threshold here
    consumer = Consumer(conf)
    consumer.subscribe([topic])

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition
                    continue
                else:
                    print(msg.error())
                    break
            else:
                apriori.process_message(msg)
                apriori.generate_associations()
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
