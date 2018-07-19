package de.dortmund.skbmtp.KafkaNeo4JConnector.logic;

import org.apache.kafka.streams.kstream.ValueMapper;
import org.neo4j.driver.v1.Session;

import de.dortmund.skbmtp.KafkaNeo4JConnector.model.Neo4JCommand;

public class RunCommandMapper implements ValueMapper<Neo4JCommand, Neo4JCommand>
{
	private Session session;

	public RunCommandMapper(Session s)
	{
		this.session = s;
	}
	
	@Override
	public Neo4JCommand apply(Neo4JCommand value)
	{
		return Neo4JWriter.write(session, value);
	}
}
