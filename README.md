# kafka-exercise

* JAVA kafka producer reading from twitter by any given interesting tweets topic and publishing to kafka cluster.

* Using kafka stream to filter out tweets from kafka cluster by any given criteria and forward the filtered tweets to the next kafka topic.

* A JAVA kafka consumer eventually consumes the streams and insert them into elasticsearch on bonsai. 

#### Environment

- Ubuntu 18.04.1 x86_64
- Java 8
- Kafka 2.3.1
- kafka streams 2.4.0
- twitter streaming API  - Hosebird client 2.2.0
- Bonsai Elasticsearch
-- Elasticsearch-rest-high-level-client 7.5
- IntelliJ IDEA 2019.2.3 (Ultimate Edition)

#### Quick Start
- Make sure zookeeper and kafka server is running beforehand
```
 zookeeper-server-start.sh ~/kafka_2.12-2.3.1/config/zookeeper.properties
```
```
kafka-server-start.sh ~/kafka_2.12-2.3.1/config/server.properties
```
- Create a topic called twitter_tweets. Producer will stream to this topic
```
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1  --partitions 4 --topic twitter_tweets
```

- Create a topic called popular_tweets. Kafka stream will redirect filtered streams to this topic
```
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1  --partitions 4 --topic popular_tweets
```

- Finally, run the following components respectively 
    1. kafka-producer-twitter
    2. kafka-streams
    3. kafka-consumer-elasticsearch 