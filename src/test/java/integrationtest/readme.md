#Integrationstests

Erzeugen der Topics (falls sie noch nicht da sin)

./scripts/createTopic.sh test-topic-in

./scripts/createTopic.sh test-topic-out


Starten des KafkaNeo4JConnectors mit der test.conf im Ordner /scripts/

Also: java -jar KafkaNeo4JConnector-0.1.0-Snapshot.jar (oder wie die generierte Datei heißt) path/to/test.conf


Dann starten des lookups:

./scripts/lookAtTopic.sh test-topic-out

Hier können die Ergebnisse dann angeschaut werden, hier muss überprüft werden, ob alles Ordnungsgemäß funktioniert hat.


Falls gewünscht auch noch:

./scripts/lookAtTopic.sh test-topic-in


Danach starten des Neo4JCommandproducers:

java -cp KafkaNeo4JConnector-0.1.0-Snapshot.jar integrationstest.Neo4JProducer path/to/test.conf