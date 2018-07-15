package de.dortmund.skbmtp.KafkaNeo4JConnector;

import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.dortmund.skbmtp.KafkaNeo4JConnector.logic.Neo4JCommandSchema;
import de.dortmund.skbmtp.KafkaNeo4JConnector.logic.Neo4JWriter;
import de.dortmund.skbmtp.KafkaNeo4JConnector.logic.Settings;
import de.dortmund.skbmtp.KafkaNeo4JConnector.logic.Util;
import de.dortmund.skbmtp.KafkaNeo4JConnector.model.Neo4JCommand;

/**
 * Hello world!
 *
 */
public class KafkaNeo4JConnector implements Runnable
{
	private static final Logger LOGGER = LogManager.getLogger(KafkaNeo4JConnector.class);

	public static final String NEO4J_URL = "bolt://localhost:7687";
	public static final String NEO4J_USERNAME = "neo4j";
	public static final String NEO4J_PASSWORD = "neo4jadmin";
	public static final String KAFKA_SERVERS = "localhost:9092";
	public static final String KAFKA_INPUTTOPIC = "topic2";
	public static final String KAFKA_OUTPUTTOPIC = "topic3";
	public static final String KAFKA_GROUPID = "myconsumer";
	
	String neo4jUrl = NEO4J_URL;
	String neo4jUsername = NEO4J_USERNAME;
	String neo4jPassword = NEO4J_PASSWORD;
	String kafkaServers = KAFKA_SERVERS;
	String kafkaInput = KAFKA_INPUTTOPIC;
	String kafkaOutput = KAFKA_OUTPUTTOPIC;
	String kafkaGroupId = KAFKA_GROUPID;
	Settings settings = null;

	public KafkaNeo4JConnector(String[] args)
	{

		if(args.length == 0)
		{
			settings = Settings.getInstance();
			LOGGER.info("Load default settings");
		}
		if(args.length == 1)
		{
			settings = Settings.getInstance(args[0]);
			LOGGER.info("Load settings from file: " + args[0]);
		}

		if(args.length >= 2)
		{
			neo4jUrl = args[0];
			neo4jUsername = args[1];
		}
		if(args.length >= 3)
		{
			neo4jPassword = args[2];
		}
		if(args.length >= 4)
		{
			kafkaServers = args[3];
		}
		if(args.length >= 5)
		{
			kafkaInput = args[4];
		}
		if(args.length >= 6)
		{
			kafkaOutput = args[5];
		}
		if(args.length >= 7)
		{
			kafkaGroupId = args[6];
		}
		
		if(settings != null)
		{
			neo4jUrl = settings.getNeo4JServer();
			neo4jPassword = settings.getNeo4JPassword();
			neo4jUsername = settings.getNeo4JUser();
			kafkaServers = settings.getBootstrapServers();
			kafkaInput = settings.getKafkaTopicImprovedTwitter();
			kafkaOutput = settings.getKafkaTopicNeo4JFeedback();
			kafkaGroupId = settings.getGroupId();
		}
	}

	public void run()
	{
		final String finNeo4JUrl = neo4jUrl;
		final String finNeo4JUsername = neo4jUsername;
		final String finNeo4JPassword = neo4jPassword;

		final StreamsBuilder builder = new StreamsBuilder();
		KStream<String, Neo4JCommand> source = builder.stream(kafkaInput, Consumed.with(Serdes.String(), new Neo4JCommandSchema()));
		LOGGER.info("created streams build");
		KStream<String, Neo4JCommand> mappedValues = source.<Neo4JCommand>mapValues(value -> (Neo4JWriter.write(value, finNeo4JUrl, finNeo4JUsername, finNeo4JPassword)));
		mappedValues.to(kafkaOutput, Produced.with(Serdes.String(), new Neo4JCommandSchema()));

		final Topology topology = builder.build();
		final KafkaStreams streams = new KafkaStreams(topology, Util.createStreamsProperties(kafkaServers, kafkaGroupId));
		final CountDownLatch latch = new CountDownLatch(1);

		Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
			@Override
			public void run() {
				streams.close();
				latch.countDown();
			}
		});

		try {
			streams.start();
			latch.await();
		} catch (Throwable e) {
			System.exit(1);
		}
		System.exit(0);
	}
	
	public static void main(String[] args) throws Exception
	{
		KafkaNeo4JConnector conn = new KafkaNeo4JConnector(args);
		Thread t = new Thread(conn);
		t.start();
	}
}
