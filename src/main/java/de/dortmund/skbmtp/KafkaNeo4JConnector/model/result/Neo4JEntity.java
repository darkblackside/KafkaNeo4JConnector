package de.dortmund.skbmtp.KafkaNeo4JConnector.model.result;

import java.io.NotSerializableException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.v1.types.Entity;

import de.dortmund.skbmtp.KafkaNeo4JConnector.logic.Util;

public class Neo4JEntity implements Serializable
{
	private static final long serialVersionUID = 1L;
	private Map<String, Neo4JValue<?>> properties;

	/**
	 * Do not use, default constructor only for serialization/deserialization
	 */
	public Neo4JEntity()
	{
		
	}
	
	public Neo4JEntity(Map<String, Neo4JValue<?>> properties)
	{
		this.setProperties(properties);
	}

	public static Neo4JEntity fromEntity(Entity value) throws NotSerializableException
	{
		Map<String, Neo4JValue<?>> properties = new HashMap<String, Neo4JValue<?>>();

		for(String key : value.keys())
		{
			properties.put(key, Util.getNeo4JResult(value.get(key)));
		}

		return new Neo4JEntity(properties);
	}

	public Map<String, Neo4JValue<?>> getProperties() {
		return properties;
	}

	private void setProperties(Map<String, Neo4JValue<?>> properties) {
		this.properties = properties;
	}
}
