package de.dortmund.skbmtp.KafkaNeo4JConnector.model.result;

import java.io.NotSerializableException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;

import org.neo4j.driver.v1.Record;

import de.dortmund.skbmtp.KafkaNeo4JConnector.logic.Util;

public class Neo4JResults extends HashMap<String, Neo4JValue<?>> implements Serializable
{
	private static final long serialVersionUID = 1L;

	public Neo4JResults()
	{
		super();
	}

	public static Neo4JResults fromRecord(Record record) throws NotSerializableException
	{
		Neo4JResults thisResults = new Neo4JResults();

		List<String> keys = record.keys();
		for(String key : keys)
		{
			Neo4JValue<?> currentValue = Util.getNeo4JResult(record.get(key));

			thisResults.put(key, currentValue);
		}

		return thisResults;
	}
}