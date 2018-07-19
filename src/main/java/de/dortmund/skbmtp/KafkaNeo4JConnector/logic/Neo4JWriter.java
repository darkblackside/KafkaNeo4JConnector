package de.dortmund.skbmtp.KafkaNeo4JConnector.logic;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;

import de.dortmund.skbmtp.KafkaNeo4JConnector.model.Neo4JCommand;
import de.dortmund.skbmtp.KafkaNeo4JConnector.model.result.Neo4JResults;

public class Neo4JWriter
{
	private static final Logger LOGGER = LogManager.getLogger(Neo4JWriter.class);

	public Neo4JWriter()
	{
	}

	public static Neo4JCommand write(Session session, Neo4JCommand command)
	{
		LOGGER.info("Write neo4j command!");

		if(command.getStringCommand() != null)
		{

			try {
				List<Neo4JResults> result = session.writeTransaction(new TransactionWorker(command));
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
