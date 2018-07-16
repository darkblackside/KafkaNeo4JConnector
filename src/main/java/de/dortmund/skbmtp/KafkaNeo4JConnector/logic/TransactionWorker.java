package de.dortmund.skbmtp.KafkaNeo4JConnector.logic;

import java.io.NotSerializableException;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.TransactionWork;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.value.Uncoercible;
import org.neo4j.driver.v1.types.Entity;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Path;
import org.neo4j.driver.v1.types.Relationship;

import com.fasterxml.jackson.databind.ObjectMapper;

import de.dortmund.skbmtp.KafkaNeo4JConnector.model.Neo4JCommand;

public class TransactionWorker implements TransactionWork<String>
{
	private static final Logger LOGGER = LogManager.getLogger(TransactionWorker.class);

	private Neo4JCommand command;

	public TransactionWorker(Neo4JCommand command)
	{
		this.command = command;
	}

	@Override
	public String execute(Transaction tx)
	{
		LOGGER.debug("run command");
		StatementResult result = tx.run(command.getStringCommand());

		return writeToResultObject(command, result);
	}

	private String writeToResultObject(Neo4JCommand command, StatementResult result)
	{
		LOGGER.debug("Write results to result object");
		String results = "[";
		boolean first = true;

		ObjectMapper mapper = new ObjectMapper();

		while(result.hasNext())
		{
			Record record = result.next();

			if(first)
			{
				first = false;
			}
			else
			{
				results = results + ",";
			}

			List<String> keys = record.keys();

			boolean firstInner = true;

			for(String key : keys)
			{
				if(firstInner)
				{
					firstInner = false;
				}
				else
				{
					results += ",";
				}

				Value actualValue = record.get(key);

				try {
					results += "{\"" + key + "\":" + getJsonFromValues(actualValue) + "}";
				}
				catch (NotSerializableException e)
				{
					LOGGER.error("Couldn't get json from values", e.getMessage());
				}
			}
		}

		results = results + "]";
		return results;
	}

	private String getJsonFromValues(Value actualKey) throws NotSerializableException
	{
		try	{
			boolean value = actualKey.asBoolean();
			return value + "";
		} catch(Uncoercible e) {
			LOGGER.debug("Object is not a boolean");
		}

		try	{
			byte[] value = actualKey.asByteArray();
			return new String(value);
		} catch(Uncoercible e) {
			LOGGER.debug("Object is not a byte-array");
		}

		try {
			double value = actualKey.asDouble();
			return value + "";
		} catch(Uncoercible e) {
			LOGGER.debug("Object is not a double");
		}

		try	{
			float value = actualKey.asFloat();
			return value + "";
		} catch(Uncoercible e) {
			LOGGER.debug("Object is not a float");
		}

		try	{
			int value = actualKey.asInt();
			return value + "";
		} catch(Uncoercible e) {
			LOGGER.debug("Object is not a int");
		}

		try	{
			long value = actualKey.asLong();
			return value + "";
		} catch(Uncoercible e) {
			LOGGER.debug("Object is not a long");
		}

		try {
			Number value = actualKey.asNumber();
			return value + "";
		} catch(Uncoercible e) {
			LOGGER.debug("Object is not a Number");
		}

		try	{
			String value = actualKey.asString();
			return value + "";
		} catch(Uncoercible e) {
			LOGGER.debug("Object is not a string");
		}

		try	{
			List<Object> value = actualKey.asList();

			String result = "[";
			boolean firstInner = true;
			for(Object o : value)
			{
				if(firstInner)
				{
					firstInner = false;
				}
				else
				{
					result += ",";
				}

				if(o instanceof Value)
				{
					result += getJsonFromValues((Value)o);
				}
				else
				{
					//TODO - Björn Merschmeier - 16.07.2018 - do something with an object
					result += o.toString();
				}
			}
			result +="]";
			return result;
		} catch(Uncoercible e) {
			LOGGER.debug("Object is not a list");
		}

		try	{
			Map<String, Object> value = actualKey.asMap();
			String result ="{";
			boolean firstInner = true;
			for(String key : value.keySet())
			{
				if(firstInner)
				{
					firstInner = false;
				}
				else
				{
					result += ",";
				}
				Object keyValue = value.get(key);

				if(keyValue instanceof Value)
				{
					result += "\"" + key + "\":" + getJsonFromValues((Value) keyValue);
				}
				else
				{
					//TODO - Björn Merschmeier - 16.07.2018 - do something with an object
					if(keyValue instanceof String || keyValue instanceof Date)
					{
						result += key + ":'" + keyValue.toString() + "'";
					}
					else
					{
						result += key + ":" + keyValue.toString();
					}
				}
			}

			result += "}";
			return result;
		} catch(Uncoercible e) {
			LOGGER.debug("Object is not a list");
		}

		try	{
			Path value = actualKey.asPath();
			return pathToString(value);
		} catch(Uncoercible e) {
			LOGGER.debug("Object is not a path");
		}

		try	{
			Node value = actualKey.asNode();
			return nodeToString(value);
		} catch(Uncoercible e) {
			LOGGER.debug("Object is not a node");
		}

		try {
			Entity value = actualKey.asEntity();
			return entityToString(value);
		} catch(Uncoercible e) {
			LOGGER.debug("Object is not a entity");
		}

		try	{
			Relationship value = actualKey.asRelationship();
			return relationshipToString(value);
		} catch(Uncoercible e) {
			LOGGER.debug("Object is not a relationship");
		}

		throw new NotSerializableException("Missing implementation for object-type");
	}

	private String entityToString(Entity value) throws NotSerializableException	{
		Iterable<String> keys = value.keys();

		String result = "[";

		boolean firstInner = true;
		for(String key : keys)
		{
			if(firstInner)
			{
				firstInner = false;
			}
			else
			{
				result += ",";
			}

			Value actualValue = value.get(key);
			result = "{\"" + key + "\":" + getJsonFromValues(actualValue) + "}";
		}

		result += "]";
		return result;
	}

	private String pathToString(Path value) throws NotSerializableException {
		String result = "{";
		result += nodesToString(value);
		result += ",";
		result += relationshipsToString(value);
		result += "}";
		return result;
	}

	private String nodesToString(Path value) throws NotSerializableException {
		String result = "nodes:[";
		boolean firstInner = true;
		for(Node node : value.nodes())
		{
			if(firstInner)
			{
				firstInner = false;
			}
			else
			{
				result += ",";
			}

			result += nodeToString(node);
		}
		result += "]";
		return result;
	}

	private String relationshipsToString(Path value) throws NotSerializableException {
		String result = "\"relationships\":[";
		boolean firstInner;
		firstInner = true;
		for(Relationship relationship : value.relationships())
		{
			if(firstInner)
			{
				firstInner = false;
			}
			else
			{
				result += ",";
			}

			result += relationshipToString(relationship);
		}
		result += "]";
		return result;
	}

	private String relationshipToString(Relationship relationship) throws NotSerializableException
	{
		Iterable<String> keys = relationship.keys();

		String result = "{keys:[";

		boolean firstInner = true;
		for(String key : keys)
		{
			if(firstInner)
			{
				firstInner = false;
			}
			else
			{
				result += ",";
			}

			Value actualValue = relationship.get(key);
			result = "{\"" + key + "\":" + getJsonFromValues(actualValue) + "}";
		}

		result += "], \"startNodeId\": " + relationship.startNodeId() + ", \"endNodeId\": " + relationship.endNodeId() + "}";
		return result;
	}

	private String nodeToString(Node node) throws NotSerializableException
	{
		Iterable<String> keys = node.keys();

		String result = "[";

		boolean firstInner = true;
		for(String key : keys)
		{
			if(firstInner)
			{
				firstInner = false;
			}
			else
			{
				result += ",";
			}

			Value actualValue = node.get(key);
			result = "{\"" + key + "\":" + getJsonFromValues(actualValue) + "}";
		}

		result += "]";
		return result;
	}
}
