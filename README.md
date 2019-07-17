# Kafka for beginners
This is a project using Apache Kafka with Java 8 on Windows.

Kafka Commands:

In summary, for Windows
Download and Setup Java 8 JDK

Download the Kafka binaries from https://kafka.apache.org/downloads

Extract Kafka at the root of C:\

Setup Kafka bins in the Environment variables section by editing Path

Try Kafka commands using kafka-topics.bat (for example)

Edit Zookeeper & Kafka configs using NotePad++ https://notepad-plus-plus.org/download/

zookeeper.properties: dataDir=C:/kafka_2.12-2.0.0/data/zookeeper (yes the slashes are inverted)

server.properties: log.dirs=C:/kafka_2.12-2.0.0/data/kafka (yes the slashes are inverted)

Start Zookeeper in one command line: zookeeper-server-start.bat config\zookeeper.properties

Start Kafka in another command line: kafka-server-start.bat config\server.properties