package de.dortmund.skbmtp.KafkaNeo4JConnector.logic;

import java.io.IOException;
import java.util.Map;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import de.dortmund.skbmtp.KafkaNeo4JConnector.model.Neo4JCommand;

public class Neo4JCommandSchema implements Serializer<Neo4JCommand>, Deserializer<Neo4JCommand>, Serde<Neo4JCommand>,
SerializationSchema<Neo4JCommand>, DeserializationSchema<Neo4JCommand> {
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LogManager.getLogger(Neo4JCommandSchema.class);

	@Override
	public Neo4JCommand deserialize(String id, byte[] data) {
		LOGGER.info("deserialize command: " + id);
		System.out.println("deserialize command: " + id);
		try {
			ObjectMapper mapper = new ObjectMapper();
			Neo4JCommand command = mapper.readValue(data, Neo4JCommand.class);
			LOGGER.info("Deserialize neo4jcommandIdentifier: " + command.getCommandIdentifier());
			System.out.println("Deserialize neo4jcommandIdentifier: " + command.getCommandIdentifier());
			return command;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public byte[] serialize(String id, Neo4JCommand data) {
		LOGGER.info("Serialize id: " + id);
		System.out.println("Serialize id: " + id);
		LOGGER.info("Serialize neo4jcommandIdentifier: " + data.getCommandIdentifier());
		System.out.println("Serialize neo4jcommandIdentifier: " + data.getCommandIdentifier());
		ObjectMapper mapper = new ObjectMapper();
		mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

		data.toCypherNativeQuery();
		try {
			return mapper.writeValueAsBytes(data);
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		// Do nothing, because it is not needed
	}

	@Override
	public void close() {
		// Do nothing, because it is not needed
	}

	@Override
	public Serializer<Neo4JCommand> serializer() {
		return this;
	}

	@Override
	public Deserializer<Neo4JCommand> deserializer() {
		return this;
	}

	@Override
	public TypeInformation<Neo4JCommand> getProducedType() {
		return TypeExtractor.getForClass(Neo4JCommand.class);
	}

	@Override
	public Neo4JCommand deserialize(byte[] message) throws IOException {
		return this.deserialize("inputTopic", message);
	}

	@Override
	public boolean isEndOfStream(Neo4JCommand nextElement) {
		return false;
	}

	@Override
	public byte[] serialize(Neo4JCommand element) {
		return this.serialize("outputTopic", element);
	}
}
