# **Streaming Data Insights** #
I would like to thank my group members for their help.
<br>**Muhammad Subhan**</br>
**Hamza Asad**
### **Introduction** ###
This report outlines the design, implementation, and evaluation of a streaming data analysis system developed for analyzing Amazon Metadata. The system consists of a producer application, three consumer applications, and a Kafka topic to facilitate real-time data streaming and processing. Each consumer application is responsible for a specific task, including Apriori algorithm, PCY algorithm, and innovative analysis for recommendation generation based on co-purchase patterns.
### Producer Application ###
The producer application streams preprocessed data from the Amazon dataset to a Kafka topic named amazon_metadata_stream. The data is preprocessed to ensure cleanliness and suitability for subsequent analysis tasks.
### Consumer Applications ###
    1. Consumer 1 - Apriori Algorithm
The first consumer application implements the Apriori algorithm to identify frequent item sets and association rules from the streamed data. The application processes messages from the Kafka topic, updates itemset frequencies, generates associations, and stores association rules with confidence scores in a MongoDB database named AprioriDB.

    2. Consumer 2 - PCY Algorithm
The second consumer application implements the PCY (Park Chen Yu) algorithm to efficiently find frequent itemsets from the streamed data. The application utilizes a bitmap-based approach to count item frequencies and updates frequent itemsets based on a specified support threshold. Frequent itemset counts are stored in a MongoDB database named PCY.

    3. Consumer 3 - Innovative Analysis for Recommendations and ECLAT
The third consumer application conducts innovative analysis to generate product recommendations based on co-purchase patterns. It utilizes a sliding window approach to process data, updates co-purchase information, identifies frequent itemsets using the Equivalence Class Clustering and Bottom-Up Lattice Traversal(ECLAT) algorithm, and generates association rules with minimum confidence thresholds. Top recommendations for each product are stored in a MongoDB database named RecommendationDB, as the result of our recommendation engine.
### Kafka Topic ###
The Kafka topic amazon_metadata_stream serves as the communication channel between the producer and consumer applications. It facilitates the real-time streaming of preprocessed data from the Amazon dataset, enabling concurrent processing by multiple consumer applications.
### Conclusion ###
The developed streaming data analysis system demonstrates effective utilization of Kafka for real-time data streaming and processing. By implementing Apriori, PCY, ECLAT, and a product recommendation mechanism, the system enables insights extraction, association discovery, and personalized recommendation generation from the streaming dataset. The integration with MongoDB ensures efficient storage and retrieval of processed results for further analysis and decision-making.

#### **NOTE** ####
To run this project, just run the bash script given in this repo. It will connect to all Kafka components, MongoDB, producer, and consumer applications.
