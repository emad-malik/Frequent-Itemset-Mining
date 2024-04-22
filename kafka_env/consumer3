import json
import time
from collections import deque
from kafka import KafkaConsumer

# function to process each window
def transform_data(window):
    itemsets= {}
    for transaction_id, (transaction, _) in enumerate(window):
        asin= transaction['asin']
        title= transaction['title']
        itemsets.setdefault(asin, set()).add(transaction_id)
        itemsets.setdefault(title, set()).add(transaction_id)
    return itemsets
# analyze the data in windows
def analyze_data(window):
    itemsets= transform_data(window)
    min_support= len(window) * 0.2  # 20% of the transactions in the window
    initial_items= [(item, transactions) for item, transactions in itemsets.items() if len(transactions) >= min_support - 1]
    initial_items.sort(key= lambda x: -len(x[1]))  # Sort by frequency (descending) using lambda function
    frequent_itemsets= {} # frequent itemsets get appeneded with support
    eclat([], initial_items, min_support, frequent_itemsets) # call eclat function
    for itemset, support in frequent_itemsets.items():
        print(f"Frequent itemset: {set(itemset)}, Support: {support}")
# this function is used to find frequent itemsets using Equivalence Class Clustering and Bottom-up Lattice Traversal algo
def eclat(prefix, items, min_support, frequent_itemsets):
    while items:
        item, transactions= items.pop()
        if len(transactions) >= min_support:
            new_prefix= prefix + [item]
            frequent_itemsets[frozenset(new_prefix)]= len(transactions)
            suffix_items= [(other_item, transactions.intersection(other_transactions)) for other_item, other_transactions in items if len(transactions.intersection(other_transactions)) >= min_support]
            suffix_items.sort(key=lambda x: len(x[1]), reverse=True)
            eclat(new_prefix, suffix_items, min_support, frequent_itemsets) # recursive call

# creating a consumer instance
consumer3= KafkaConsumer(
    'amazon_metadata_stream',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    value_deserializer= lambda x: json.loads(x.decode('utf-8'))
)

window_size= 20
window= deque(maxlen=window_size)
# Kafka consumer loop
for message in consumer3:
    window.append(({
        'asin': message.value['asin'],
        'title': message.value['title']
    }, time.time()))

    if len(window) == window.maxlen:
        analyze_data(window)
        window.clear()
