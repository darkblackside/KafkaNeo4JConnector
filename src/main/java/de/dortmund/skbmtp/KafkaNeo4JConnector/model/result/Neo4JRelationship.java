package de.dortmund.skbmtp.KafkaNeo4JConnector.model.result;

import java.io.NotSerializableException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.types.Relationship;

import de.dortmund.skbmtp.KafkaNeo4JConnector.logic.Util;

public class Neo4JRelationship
{

	private Map<String, Neo4JValue<?>> properties;
	private long id;
	private long startNodeId;
	private long endNodeId;

	public Neo4JRelationship(long id, long startNodeId, long endNodeId, Map<String, Neo4JValue<?>> properties)
	{
		this.setProperties(properties);
		this.setId(id);
		this.setStartNodeId(startNodeId);
		this.setEndNodeId(endNodeId);
	}

	public static List<Neo4JRelationship> fromRelationships(Iterable<Relationship> relationships) throws NotSerializableException {
		List<Neo4JRelationship> resultList = new ArrayList<Neo4JRelationship>();

		for(Relationship r : relationships)
		{
			resultList.add(fromRelationship(r));
		}

		return resultList;
	}

	public static Neo4JRelationship fromRelationship(Relationship r) throws NotSerializableException
	{
		Map<String, Neo4JValue<?>> properties = new HashMap<String, Neo4JValue<?>>();

		for(String key : r.keys())
		{
			properties.put(key, Util.getNeo4JResult(r.get(key)));
		}

		return new Neo4JRelationship(r.id(), r.startNodeId(), r.endNodeId(), properties);
	}

	public Map<String, Neo4JValue<?>> getProperties() {
		return properties;
	}

	public long getId() {
		return id;
	}

	public long getStartNodeId() {
		return startNodeId;
	}

	public long getEndNodeId() {
		return endNodeId;
	}

	private void setProperties(Map<String, Neo4JValue<?>> properties) {
		this.properties = properties;
	}

	private void setId(long id) {
		this.id = id;
	}

	private void setStartNodeId(long startNodeId) {
		this.startNodeId = startNodeId;
	}

	private void setEndNodeId(long endNodeId) {
		this.endNodeId = endNodeId;
	}
}
