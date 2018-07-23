package de.dortmund.skbmtp.KafkaNeo4JConnector;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Queue;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import de.dortmund.skbmtp.KafkaNeo4JConnector.logic.Neo4JCommandSchema;
import de.dortmund.skbmtp.KafkaNeo4JConnector.logic.RunCommandMapper;
import de.dortmund.skbmtp.KafkaNeo4JConnector.logic.Settings;
import de.dortmund.skbmtp.KafkaNeo4JConnector.model.Neo4JCommand;

public class Neo4JConnectorSource implements Runnable
{
	private Consumer<String, Neo4JCommand> consumer;
	private Producer<String, Neo4JCommand> producer;
	private Settings settings;
	public boolean end = false;
	Queue<Neo4JCommand> resultQueue;
	
	public Neo4JConnectorSource()
	{
		settings = Settings.getInstance();
		consumer = createConsumer();
		producer = createProducer();
		resultQueue = new LinkedList<Neo4JCommand>();
	}
	

	public static void main(String[] args) throws IOException
	{
		Neo4JConnectorSource connector = new Neo4JConnectorSource();
		Thread t = new Thread(connector);
		t.run();
		System.in.read();
		connector.end = true;
	}
	
	public void run()
	{
		while (!end) {
			while(!resultQueue.isEmpty())
			{
				ProducerRecord<String, Neo4JCommand> record;
				record = new ProducerRecord<String, Neo4JCommand>(settings.getKafkaTopicNeo4JFeedback(), resultQueue.poll());
				producer.send(record);
			}
			
			ConsumerRecords<String, Neo4JCommand> results = consumer.poll(1000);
			
			for(ConsumerRecord<String, Neo4JCommand> result : results)
			{
				RunCommandMapper mapper = new RunCommandMapper(settings.getNeo4JServer(), settings.getNeo4JUser(), settings.getNeo4JPassword(), resultQueue, result.value());
				Thread t = new Thread(mapper);
				t.start();
			}
		}
	}


	private Consumer<String, Neo4JCommand> createConsumer() {
		Properties propsTwitter = new Properties();
		propsTwitter.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, settings.getBootstrapServers());
		propsTwitter.put(ConsumerConfig.CLIENT_ID_CONFIG, "Neo4JConsumer");
		propsTwitter.put("group.id", settings.getGroupId());
		propsTwitter.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		propsTwitter.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Neo4JCommandSchema.class.getName());

		return new KafkaConsumer<>(propsTwitter);
	}

	private Producer<String, Neo4JCommand> createProducer() {
		Properties propsTwitter = new Properties();
		propsTwitter.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, settings.getBootstrapServers());
		propsTwitter.put(ProducerConfig.CLIENT_ID_CONFIG, "Neo4JProducer");
		propsTwitter.put("group.id", settings.getGroupId());
		propsTwitter.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		propsTwitter.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Neo4JCommandSchema.class.getName());

		return new KafkaProducer<String, Neo4JCommand>(propsTwitter);
}
}
