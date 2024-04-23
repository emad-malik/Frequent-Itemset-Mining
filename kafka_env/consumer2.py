from kafka import KafkaConsumer
import json
from collections import defaultdict
from itertools import combinations
import sys

# Function to initialize the PCY algorithm
def initialize_pcy(bucket_size):
    return [0] * bucket_size, [set() for _ in range(bucket_size)], defaultdict(int)



# Function to update the PCY bitmap
def update_bitmap(bitmap, hash_value):
    bitmap[hash_value] += 1

# Function to update the frequent itemsets
def update_frequent_itemsets(bitmap, item_counts, frequent_itemsets, support_threshold):
    for i, count in enumerate(bitmap):
        if count >= support_threshold:
            for itemset in combinations(item_counts[i], 2):
                frequent_itemsets[itemset] += 1


# Function to process Kafka messages and apply PCY algorithm
def process_messages(consumer, bucket_size, support_threshold):
    bitmap, item_counts, frequent_itemsets = initialize_pcy(bucket_size)
    for message in consumer:
        data = json.loads(message.value)
        
        # Process data and update PCY algorithm
        for item in data:
            hash_value = hash(item) % bucket_size
            update_bitmap(bitmap, hash_value)
            item_counts[hash_value].add(item)
        
        # Update frequent itemsets if necessary
        if sys.getsizeof(frequent_itemsets) > 100: # Adjust based on memory availability
            update_frequent_itemsets(bitmap, item_counts, frequent_itemsets, support_threshold)
            # Convert defaultdict to regular dictionary for printing
            frequent_itemsets_dict = dict(frequent_itemsets)
            # Print the frequent itemsets
            print("Frequent Itemsets:")
            for itemset, count in frequent_itemsets_dict.items():
                # Filter out invalid column names from the itemset
                valid_itemset = [col for col in itemset if col in data]
                if valid_itemset:
                    itemset_values = [data[col] for col in valid_itemset]
                    print("Itemset:", valid_itemset, "Values:", itemset_values, "Count:", count)


if __name__ == "__main__":
    bootstrap_servers = ['localhost:9092']
    topic_name = 'amazon_metadata_stream'
    bucket_size = 10 # Choose an appropriate bucket size
    support_threshold = 2 # Choose an appropriate support threshold

    consumer = KafkaConsumer(topic_name, bootstrap_servers=bootstrap_servers)
    process_messages(consumer, bucket_size, support_threshold)




