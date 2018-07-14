package integrationtest;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;

import de.dortmund.skbmtp.KafkaNeo4JConnector.logic.Neo4JCommandSchema;
import de.dortmund.skbmtp.KafkaNeo4JConnector.logic.Settings;
import de.dortmund.skbmtp.KafkaNeo4JConnector.model.Neo4JCommand;
import scala.Console;

public class Neo4JProducer implements Runnable {
	private static boolean end = false;
	private Settings settings;

	public Neo4JProducer(String filename)
	{
		settings = Settings.getInstance(Neo4JProducer.class, filename);
	}
	public Neo4JProducer()
	{
		settings = Settings.getInstance(Neo4JProducer.class);
	}

	@Override
	public void run() {
		final Producer<Long, Neo4JCommand> producer = createProducer();

		try {
			ProducerRecord<Long, Neo4JCommand> record;
			for (long msgRead = 0; !end; msgRead++)
			{
				long currentDatetime = System.currentTimeMillis();
				Neo4JCommand command = new Neo4JCommand("CREATE (n:Test) {datetime: '"+currentDatetime+"'}", "Create-Command", "Sends the create-command");
				record = new ProducerRecord<>(settings.getKafkaTopicRawTwitter(), msgRead, command);
				producer.send(record);
				command = new Neo4JCommand("MATCH (n:Test) RETURN (n)", "Read-Command", "Sends the read-command");
				record = new ProducerRecord<>(settings.getKafkaTopicRawTwitter(), msgRead, command);
				producer.send(record);
				
				Thread.sleep(30000);
			}
			Neo4JCommand command = new Neo4JCommand("MATCH (n:Test) DELETE (n)", "Delete-Command", "Sends the delete-command and removes all Test-Nodes");
			record = new ProducerRecord<>(settings.getKafkaTopicRawTwitter(), 1000000l, command);
			producer.send(record);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		producer.close();
	}

	public static void main(String[] args)
	{
		Thread t = null;
		
		if(args.length == 0)
		{
			t = new Thread(new Neo4JProducer());
		}
		else if(args.length == 1)
		{
			t = new Thread(new Neo4JProducer(args[0]));
		}
		
		if(t != null)
		{
			t.start();
		}

		Console.readLine();
		end = true;
	}

	private Producer<Long, Neo4JCommand> createProducer() {
		Properties propsTwitter = new Properties();
		propsTwitter.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, settings.getBootstrapServers());
		propsTwitter.put(ProducerConfig.CLIENT_ID_CONFIG, "CommandProducerTest");
		propsTwitter.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
		propsTwitter.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Neo4JCommandSchema.class.getName());

		return new KafkaProducer<>(propsTwitter);
	}
}