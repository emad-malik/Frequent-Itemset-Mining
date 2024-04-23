import json
import time
from collections import deque, defaultdict, Counter
from kafka import KafkaConsumer

# Initialize the global dictionary for product co-purchase counts
product_copurchase = defaultdict(Counter)

# Function to update co-purchase information
def update_copurchase(asin, also_buy):
    if not also_buy:
        return
    for associated_asin in also_buy:
        product_copurchase[asin][associated_asin] += 1
        product_copurchase[associated_asin][asin] += 1  # Co-purchase is bidirectional

# Function to process each window
def transform_data(window):
    itemsets = {}
    for transaction_id, (transaction, _) in enumerate(window):
        asin = transaction['asin']
        title = transaction['title']
        also_buy = transaction.get('also_buy', [])
        update_copurchase(asin, also_buy)  # Update co-purchase data based on 'also_buy' info
        itemsets.setdefault(asin, set()).add(transaction_id)
        itemsets.setdefault(title, set()).add(transaction_id)
    return itemsets

# Function to analyze data in windows
def analyze_data(window):
    itemsets = transform_data(window)
    min_support = len(window) * 0.1
    initial_items = [(item, transactions) for item, transactions in itemsets.items() if len(transactions) >= min_support - 1]
    initial_items.sort(key=lambda x: -len(x[1]))  # Sort by frequency (descending)
    frequent_itemsets = {}
    eclat([], initial_items, min_support, frequent_itemsets)

# Function to find frequent itemsets using ECLAT
def eclat(prefix, items, min_support, frequent_itemsets):
    while items:
        item, transactions = items.pop()
        if len(transactions) >= min_support:
            new_prefix = prefix + [item]
            frequent_itemsets[frozenset(new_prefix)] = len(transactions)
            suffix_items = [(other_item, transactions.intersection(other_transactions)) for other_item, other_transactions in items if len(transactions.intersection(other_transactions)) >= min_support]
            suffix_items.sort(key=lambda x: len(x[1]), reverse=True)
            eclat(new_prefix, suffix_items, min_support, frequent_itemsets)

# Kafka consumer setup
consumer3 = KafkaConsumer('amazon_metadata_stream', bootstrap_servers=['localhost:9092'], auto_offset_reset='earliest', value_deserializer=lambda x: json.loads(x.decode('utf-8')))
window_size = 20
window = deque(maxlen=window_size)
message_count = 0  # Initialize a message counter

for message in consumer3:
    data = message.value
    asin = data['asin']
    also_buy = data.get('also_buy', [])
    window.append(({'asin': asin, 'title': data.get('title', ''), 'also_buy': also_buy}, time.time()))

    if len(window) == window_size:
        analyze_data(window)
        window.clear()

    # Increment the message counter
    message_count += 1

    # Print top recommendations periodically (e.g., every 100 messages)
    if message_count % 100 == 0:
        print("Product Recommendations Based on Co-Purchases:")
        for asin, counts in product_copurchase.items():
            print(f"For ASIN {asin}: Top Recommendations: {counts.most_common(3)}")

# Close the consumer when done
consumer3.close()



