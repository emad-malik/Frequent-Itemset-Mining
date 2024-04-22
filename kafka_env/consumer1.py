from confluent_kafka import Consumer, KafkaError
from collections import defaultdict
from itertools import combinations
import json

class Apriori:
    def _init_(self, min_support):
        self.min_support = min_support
        self.itemsets = defaultdict(int)

    def process_message(self, message):
        try:
            data = json.loads(message.value())
            if 'title' in data:
                self.itemsets[data['title']] += 1
        except Exception as e:
            print(f"Error processing message: {e}")

    def generate_associations(self):
        frequent_itemsets = {itemset: support for itemset, support in self.itemsets.items() if support >= self.min_support}
        print("Frequent Itemsets:")
        pairs = list(combinations(frequent_itemsets.keys(), 2))
        for pair in pairs:
            pair_frequency = min(frequent_itemsets[pair[0]], frequent_itemsets[pair[1]])
            print(f"{pair}: {pair_frequency}")
        
# Kafka consumer configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'apriori-group',
    'auto.offset.reset': 'earliest'
}

# Kafka topic to consume from
topic = 'amazon_metadata_stream'

if _name_ == "_main_":
    apriori = Apriori(min_support=2)  # Set your minimum support threshold here
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
