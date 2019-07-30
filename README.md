# Kafka in Practice
This is a project using Apache Kafka with Java 8 on Windows.

## Get started guide (Windows)

#### Download and Setup Java 8 JDK

#### Download the Kafka binaries from https://kafka.apache.org/downloads

#### Extract Kafka at the root of ```C:\```

#### Setup Kafka bins in the Environment variables section by editing Path

#### Try Kafka commands using ```kafka-topics.bat``` (for example)

#### Edit Zookeeper & Kafka configs

- zookeeper.properties: ```dataDir=C:/kafka_2.12-2.0.0/data/zookeeper``` (yes the slashes are inverted)

- server.properties: ```log.dirs=C:/kafka_2.12-2.0.0/data/kafka``` (yes the slashes are inverted)

#### Start Zookeeper in one command line: ```zookeeper-server-start.bat config\zookeeper.properties```

#### Start Kafka in another command line: ```kafka-server-start.bat config\server.properties```

## Kafka Commands

#### Create new topic

```kafka-topics --create --zookeeper 127.0.0.1:2181 --replication-factor 1 --partitions 2 --topic <topic_name>```

#### Produce on topic

```kafka-console-producer.bat --broker-list 127.0.0.1:9092 --topic <topic_name>```

#### Consume on topic

```kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic <topic_name> --from-beginning```

#### Consume on group

```kafka-consumer-groups --bootstrap-server 127.0.0.1:9092 --group my-first-application --topic twitter_tweets```
- Option ```--execute``` to start consuming on topic ```twitter_tweets``` with group ```my-first-application```
- Option ```--reset-offsets --shift-by -2``` to reset offsets in partition by 2
- Option ```--describe``` to see topic ```twitter_tweets``` summary
