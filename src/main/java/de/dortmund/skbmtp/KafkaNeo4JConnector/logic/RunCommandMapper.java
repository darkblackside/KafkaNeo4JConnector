package de.dortmund.skbmtp.KafkaNeo4JConnector.logic;

import java.util.Queue;

import org.apache.kafka.streams.kstream.ValueMapper;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;

import de.dortmund.skbmtp.KafkaNeo4JConnector.model.Neo4JCommand;

public class RunCommandMapper implements ValueMapper<Neo4JCommand, Neo4JCommand>, Runnable
{
	private String neo4jUrl;
	private String neo4jUsername;
	private String neo4jPassword;
	private Queue<Neo4JCommand> resultQueue;
	private Neo4JCommand value;
	
	public RunCommandMapper(String neo4jUrl, String neo4jUsername, String neo4jPassword) {
		this.neo4jUrl = neo4jUrl;
		this.neo4jUsername = neo4jUsername;
		this.neo4jPassword = neo4jPassword;
	}
	
	public RunCommandMapper(String neo4jUrl, String neo4jUsername, String neo4jPassword, Queue<Neo4JCommand> resultQueue, Neo4JCommand value) {
		this.neo4jUrl = neo4jUrl;
		this.neo4jUsername = neo4jUsername;
		this.neo4jPassword = neo4jPassword;
		this.resultQueue = resultQueue;
		this.value = value;
	}

	@Override
	public Neo4JCommand apply(Neo4JCommand value)
	{
		final Driver driver = GraphDatabase.driver(neo4jUrl, AuthTokens.basic(neo4jUsername, neo4jPassword));
		
		Session s = driver.session();
		
		Neo4JCommand result = Neo4JWriter.write(s, value);
		s.close();
		driver.close();
		
		return result;
	}

	@Override
	public void run()
	{
		final Driver driver = GraphDatabase.driver(neo4jUrl, AuthTokens.basic(neo4jUsername, neo4jPassword));
		
		Session s = driver.session();
		
		Neo4JCommand result = Neo4JWriter.write(s, value);
		
		s.close();
		driver.close();
		
		resultQueue.add(result);
	}
}
