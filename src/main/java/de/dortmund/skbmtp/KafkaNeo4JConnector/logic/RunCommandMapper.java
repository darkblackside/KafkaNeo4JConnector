package de.dortmund.skbmtp.KafkaNeo4JConnector.logic;

import org.apache.kafka.streams.kstream.ValueMapper;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;

import de.dortmund.skbmtp.KafkaNeo4JConnector.model.Neo4JCommand;

public class RunCommandMapper implements ValueMapper<Neo4JCommand, Neo4JCommand>
{
	private Session session;
	private String neo4jUrl;
	private String neo4jUsername;
	private String neo4jPassword;

	public RunCommandMapper(Session s)
	{
		this.session = s;
	}
	
	public RunCommandMapper(String neo4jUrl, String neo4jUsername, String neo4jPassword) {
		this.neo4jUrl = neo4jUrl;
		this.neo4jUsername = neo4jUsername;
		this.neo4jPassword = neo4jPassword;
	}

	@Override
	public Neo4JCommand apply(Neo4JCommand value)
	{
		final Driver driver = GraphDatabase.driver(neo4jUrl, AuthTokens.basic(neo4jUsername, neo4jPassword));
		
		Neo4JCommand result = Neo4JWriter.write(session, value);
		
		Session s = driver.session();
		s.close();
		driver.close();
		
		return result;
	}
}
