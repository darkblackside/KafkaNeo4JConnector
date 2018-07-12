package de.dortmund.skbmtp.KafkaNeo4JConnector.logic;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

public class Util
{
	public static Properties createStreamsProperties(String bootstrapServers, String groupId)
	{
		Properties properties = new Properties();
		properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "Neo4JStreamingConnector");
		properties.put("group.id", groupId);
		properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Neo4JCommandSchema.class.getName());

		return properties;
	}

	public static List<String> splitStringBySeparator(String csv, String delimiter) {
		List<String> splitted = new ArrayList<String>();

		if(csv != null && delimiter != null)
		{
			String[] trackLanguagesArray = csv.split(delimiter);
			for (String trackLanguage : trackLanguagesArray) {
				splitted.add(trackLanguage);
			}
		}

		return splitted;
	}

}
