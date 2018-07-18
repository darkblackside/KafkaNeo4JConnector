package de.dortmund.skbmtp.KafkaNeo4JConnector.model.result;

import java.io.NotSerializableException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.driver.v1.types.Path;

public class Neo4JPath implements Serializable
{
	private static final long serialVersionUID = 1L;

	public List<Neo4JNode> nodes;
	public List<Neo4JRelationship> relationships;

	public Neo4JPath()
	{
		nodes = new ArrayList<Neo4JNode>();
		relationships = new ArrayList<Neo4JRelationship>();
	}

	public Neo4JPath(List<Neo4JNode> nodes, List<Neo4JRelationship> relationships)
	{
		this.nodes = nodes;
		this.relationships = relationships;
	}

	public List<Neo4JNode> getNodes()
	{
		return nodes;
	}

	public List<Neo4JRelationship> getRelationships()
	{
		return relationships;
	}

	public static Neo4JPath fromPath(Path neo4jPath) throws NotSerializableException
	{
		List<Neo4JNode> nodes = Neo4JNode.fromNodes(neo4jPath.nodes());
		List<Neo4JRelationship> relationships = Neo4JRelationship.fromRelationships(neo4jPath.relationships());

		return new Neo4JPath(nodes, relationships);
	}
}
