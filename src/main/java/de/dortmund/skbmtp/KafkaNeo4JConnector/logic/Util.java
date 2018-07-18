package de.dortmund.skbmtp.KafkaNeo4JConnector.logic;

import java.io.NotSerializableException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.value.LossyCoercion;
import org.neo4j.driver.v1.exceptions.value.Uncoercible;
import org.neo4j.driver.v1.types.Entity;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Path;
import org.neo4j.driver.v1.types.Relationship;

import de.dortmund.skbmtp.KafkaNeo4JConnector.model.result.Neo4JEntity;
import de.dortmund.skbmtp.KafkaNeo4JConnector.model.result.Neo4JNode;
import de.dortmund.skbmtp.KafkaNeo4JConnector.model.result.Neo4JPath;
import de.dortmund.skbmtp.KafkaNeo4JConnector.model.result.Neo4JRelationship;
import de.dortmund.skbmtp.KafkaNeo4JConnector.model.result.Neo4JValue;

public class Util
{
	private static final Logger LOGGER = LogManager.getLogger(Util.class);

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

	public static Neo4JValue<?> getNeo4JResult(Value value) throws NotSerializableException
	{
		try
		{
			return trySimpleDatatypes(value);
		}
		catch(Uncoercible e)
		{
			LOGGER.debug("is not a simple datatype");
		}

		try
		{
			return tryListAndMap(value);
		}
		catch(Uncoercible e)
		{
			LOGGER.debug("is not a list or a map");
		}

		try	{
			Path actualValue = value.asPath();
			return pathToString(actualValue);
		} catch(Uncoercible e) {
			LOGGER.debug("Object is not a path");
		}

		try	{
			Node actualValue = value.asNode();
			return nodeToString(actualValue);
		} catch(Uncoercible e) {
			LOGGER.debug("Object is not a node");
		}

		try {
			Entity actualValue = value.asEntity();
			return fromEntity(actualValue);
		} catch(Uncoercible e) {
			LOGGER.debug("Object is not a entity");
		}

		try	{
			Relationship actualValue = value.asRelationship();
			return fromRelationship(actualValue);
		} catch(Uncoercible e) {
			LOGGER.debug("Object is not a relationship");
		}

		throw new NotSerializableException("Missing implementation for object-type");
	}

	private static Neo4JValue<Neo4JEntity> fromEntity(Entity value) throws NotSerializableException	{
		Neo4JEntity result = Neo4JEntity.fromEntity(value);
		return new Neo4JValue<Neo4JEntity>(result);
	}

	private static Neo4JValue<Neo4JPath> pathToString(Path value) throws NotSerializableException {
		Neo4JPath path = Neo4JPath.fromPath(value);
		return new Neo4JValue<Neo4JPath>(path);
	}

	private static Neo4JValue<Neo4JRelationship> fromRelationship(Relationship relationship) throws NotSerializableException
	{
		Neo4JRelationship result = Neo4JRelationship.fromRelationship(relationship);

		return new Neo4JValue<Neo4JRelationship>(result);
	}

	private static Neo4JValue<Neo4JNode> nodeToString(Node node) throws NotSerializableException
	{
		Neo4JNode result = Neo4JNode.fromNode(node);

		return new Neo4JValue<Neo4JNode>(result);
	}

	private static Neo4JValue<?> tryListAndMap(Value value) throws Uncoercible, NotSerializableException
	{
		try	{
			List<Neo4JValue<?>> resultList = new ArrayList<Neo4JValue<?>>();

			List<Object> actualValue = value.asList();

			for(Object o : actualValue)
			{
				if(o instanceof Value)
				{
					resultList.add(getNeo4JResult((Value)o));
				}
				else
				{
					resultList.add(new Neo4JValue<String>(o.toString()));
					//TODO - Björn Merschmeier - 16.07.2018 - do something with an object
				}
			}

			return new Neo4JValue<List<Neo4JValue<?>>>(resultList);
		} catch(Uncoercible e) {
			LOGGER.debug("Object is not a list");
		}

		try	{
			Map<String, Object> actualValue = value.asMap();
			Map<String, Neo4JValue<?>> resultMap = new HashMap<String, Neo4JValue<?>>();

			for(String key : actualValue.keySet())
			{
				Object keyValue = value.get(key);

				if(keyValue instanceof Value)
				{
					resultMap.put(key, getNeo4JResult((Value)keyValue));
				}
				else
				{
					resultMap.put(key, new Neo4JValue<String>(keyValue.toString()));
					//TODO - Björn Merschmeier - 16.07.2018 - do something with an object
				}
			}

			return new Neo4JValue<Map<String, Neo4JValue<?>>>(resultMap);
		} catch(Uncoercible e) {
			LOGGER.debug("Object is not a list");
		}

		throw new Uncoercible("Unknown", "List, Map");
	}

	private static Neo4JValue<?> trySimpleDatatypes(Value value) throws Uncoercible
	{
		try	{
			Boolean actualValue = new Boolean(value.asBoolean());
			return new Neo4JValue<Boolean>(actualValue);
		} catch(Uncoercible e) {
			LOGGER.debug("Object is not a boolean");
		}

		try	{
			String actualValue = new String(value.asByteArray());
			return new Neo4JValue<String>(actualValue);
		} catch(Uncoercible e) {
			LOGGER.debug("Object is not a byte-array");
		}

		try	{
			Integer actualValue = new Integer(value.asInt());
			return new Neo4JValue<Integer>(actualValue);
		} catch(Uncoercible | LossyCoercion e) {
			LOGGER.debug("Object is not a int");
		}

		try {
			Double actualValue = new Double(value.asDouble());
			return new Neo4JValue<Double>(actualValue);
		} catch(Uncoercible | LossyCoercion e) {
			LOGGER.debug("Object is not a double");
		}

		try	{
			Float actualValue = new Float(value.asFloat());
			return new Neo4JValue<Float>(actualValue);
		} catch(Uncoercible | LossyCoercion e) {
			LOGGER.debug("Object is not a float");
		}

		try	{
			Long actualValue = new Long(value.asLong());
			return new Neo4JValue<Long>(actualValue);
		} catch(Uncoercible | LossyCoercion e) {
			LOGGER.debug("Object is not a long");
		}

		try {
			Number actualValue = value.asNumber();
			return new Neo4JValue<Number>(actualValue);
		} catch(Uncoercible e) {
			LOGGER.debug("Object is not a Number");
		}

		try	{
			String actualValue = value.asString();
			return new Neo4JValue<String>(actualValue);
		} catch(Uncoercible e) {
			LOGGER.debug("Object is not a string");
		}

		throw new Uncoercible("Unknown", "Double, Float, Int, Long, Number, String, Boolean, Byte-Array");
	}
}
