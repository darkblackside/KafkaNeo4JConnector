package de.dortmund.skbmtp.KafkaNeo4JConnector.logic;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;

import de.dortmund.skbmtp.KafkaNeo4JConnector.model.Neo4JCommand;

public class Neo4JWriter
{
	private static final Logger LOGGER = LogManager.getLogger(Neo4JWriter.class);

	public Neo4JWriter()
	{
	}

	public static Neo4JCommand write(Neo4JCommand command, String serverUri, String serverUsername, String serverPassword)
	{
		LOGGER.info("Write neo4j command!");

		if(command.getStringCommand() != null)
		{
			Driver driver = GraphDatabase.driver(serverUri, AuthTokens.basic(serverUsername, serverPassword));

			try (Session session = driver.session()) {
				String result = session.writeTransaction(new TransactionWorker(command));
				command.setResult(result);
			}
			catch(Exception e)
			{
				command.setError(e.toString());
			}
		}

		return command;
	}
}
