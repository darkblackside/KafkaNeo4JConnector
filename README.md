# KafkaNeo4JConnector
Connector to execute Neo4J-Commands written in Cypher or JCypher via a Kafka-Topic

## Differences to other Neo4J Connectors
This connector support JCypher and uses the Apache-Kafka-Streams model. Other Connectors only support simple Cypher-Commands and use an architecture that uses a Apache Kafka producer and a consumer to listen and print to topics.

## Manual
### Start
```sh
Java –jar KafkaNeo4JConnector.jar [Settingsfile.config | Neo4JServerUrl Neo4JUsername Neo4JPassword kafkaServers kafkaInputTopic kafkaOutputTopic kafkaGroupid]
```

You can give in options via a Settingsfile.config with the format:

```config
Kafka.BootstrapServers = localhost:9092
Kafka.ZookeperConnect = localhost:2181
Kafka.GroupId = myconsumer
Kafka.TwitterImprovedTopic = topic2
Kafka.Neo4JConfirmationTopic = topic3
Neo4J.Server = bolt://localhost:7687
Neo4J.Username = neo4j
Neo4J.Password = neo4j
```

Or give every value in the command:

-	Neo4JServerUrl
-	Neo4JUsername
-	Neo4JPassword
-	kafkaServers
-	kafkaInputTopic
-	kafkaOutputTopic
-	kafkaGroupid

### Usage
Transform a Neo4JCommand-object in a string. This can be done with the help of the Neo4JCommandSchema. Then send this command via a Kafka Topic to this program. Then get the result on the output-topic and deserialize the Neo4JCommand-object back into java. You can now access the errors and the results via methods getResults() and getError(). The results in getResults() are a little bit complicated, to use it please check the json-format and check all types before casting the objects.
