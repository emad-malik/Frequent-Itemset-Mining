#!/bin/bash

echo "Starting MongoDB..."
mongod --dbpath /path/to/your/mongodb/data/folder &
# sleep 5

# Start Zookeeper
echo "Starting Zookeeper..."
cd kafka
bin/zookeeper-server-start.sh config/zookeeper.properties &
sleep 5

# Start Kafka Server
echo "Starting Kafka server..."
bin/kafka-server-start.sh config/server.properties &
sleep 5  

# directory containing the producer and consumer scripts
cd ../Documents/BD_A3

# Start the Producer
echo "Starting Producer..."
python3 producer.py &

# Start the Consumers
echo "Starting Consumers in separate terminals..."
gnome-terminal -- python3 consumer1.py &
gnome-terminal -- python3 consumer2.py &
gnome-terminal -- python3 consumer3.py &
wait 
echo "All processes have finished."
