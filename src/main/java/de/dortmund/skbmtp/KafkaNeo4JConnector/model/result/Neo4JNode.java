package de.dortmund.skbmtp.KafkaNeo4JConnector.model.result;

import java.io.NotSerializableException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.types.Node;

import de.dortmund.skbmtp.KafkaNeo4JConnector.logic.Util;

public class Neo4JNode implements Serializable
{
	private static final long serialVersionUID = 1L;
	private Map<String, Neo4JValue<?>> properties;
	private long id;

	/**
	 * Do not use, default constructor only for serialization/deserialization
	 */
	public Neo4JNode()
	{
		
	}
	
	public Neo4JNode(long id, Map<String, Neo4JValue<?>> properties)
	{
		this.setProperties(properties);
		this.setId(id);
	}

	public static List<Neo4JNode> fromNodes(Iterable<Node> nodes) throws NotSerializableException
	{
		List<Neo4JNode> resultlist = new ArrayList<Neo4JNode>();

		for(Node n : nodes)
		{
			resultlist.add(fromNode(n));
		}

		return resultlist;
	}

	public static Neo4JNode fromNode(Node n) throws NotSerializableException
	{
		Map<String, Neo4JValue<?>> properties = new HashMap<String, Neo4JValue<?>>();

		for(String key : n.keys())
		{
			properties.put(key, Util.getNeo4JResult(n.get(key)));
		}

		return new Neo4JNode(n.id(), properties);
	}

	public Map<String, Neo4JValue<?>> getProperties() {
		return properties;
	}

	public long getId() {
		return id;
	}

	private void setProperties(Map<String, Neo4JValue<?>> properties) {
		this.properties = properties;
	}

	private void setId(long id) {
		this.id = id;
	}
}
