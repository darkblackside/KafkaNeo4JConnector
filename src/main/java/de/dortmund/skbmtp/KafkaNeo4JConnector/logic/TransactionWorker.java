package de.dortmund.skbmtp.KafkaNeo4JConnector.logic;

import java.io.NotSerializableException;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.TransactionWork;

import de.dortmund.skbmtp.KafkaNeo4JConnector.model.Neo4JCommand;
import de.dortmund.skbmtp.KafkaNeo4JConnector.model.result.Neo4JResults;

public class TransactionWorker implements TransactionWork<List<Neo4JResults>>
{
	private static final Logger LOGGER = LogManager.getLogger(TransactionWorker.class);

	private Neo4JCommand command;

	public TransactionWorker(Neo4JCommand command)
	{
		this.command = command;
	}

	@Override
	public List<Neo4JResults> execute(Transaction tx)
	{
		LOGGER.debug("run command");
		StatementResult result = tx.run(command.getStringCommand());

		try
		{
			return writeToResultObject(result);
		}
		catch (NotSerializableException e)
		{
			LOGGER.error("Can't deserialize command results", e.getMessage());
			throw new RuntimeException(e);
		}
	}

	private List<Neo4JResults> writeToResultObject(StatementResult result) throws NotSerializableException
	{
		LOGGER.debug("Write results to result object");

		List<Neo4JResults> resultObject = new ArrayList<Neo4JResults>();

		LOGGER.debug("Result has next:" + result.hasNext());
		while(result.hasNext())
		{
			LOGGER.debug("Results to object now");
			Record record = result.next();

			Neo4JResults currentResults = Neo4JResults.fromRecord(record);

			resultObject.add(currentResults);
		}

		return resultObject;
	}
}
