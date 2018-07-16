package de.dortmund.skbmtp.KafkaNeo4JConnector.logic;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.TransactionWork;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.dortmund.skbmtp.KafkaNeo4JConnector.model.Neo4JCommand;

public class TransactionWorker implements TransactionWork<String>
{
	private static final Logger LOGGER = LogManager.getLogger(TransactionWorker.class);

	private Neo4JCommand command;

	public TransactionWorker(Neo4JCommand command)
	{
		this.command = command;
	}

	@Override
	public String execute(Transaction tx) {
		LOGGER.debug("run command");
		StatementResult result = tx.run(command.getStringCommand());

		return writeToResultObject(command, result);
	}

	private String writeToResultObject(final Neo4JCommand command, StatementResult result)
	{
		LOGGER.info("Write results to result object");
		String results = "[";
		boolean first = true;

		List<Record> recordList = result.list();

		ObjectMapper mapper = new ObjectMapper();

		/*while ( result.hasNext())
		{
			Record record = result.next();



			try
			{
				LOGGER.info("Keys" + mapper.writeValueAsString(record.keys()));
			}
			catch (JsonProcessingException e1)
			{
				e1.printStackTrace();
			}

			if(first)
			{
				first = false;
			}
			else
			{
				results = results + ",";
			}

			try
			{
				results = results + mapper.writeValueAsString(record.asMap()) + "";
			}
			catch (JsonProcessingException e)
			{
				LOGGER.info("Error while mapping record to json");
				e.printStackTrace();
			}
		}*/

		results = results + "]";

		LOGGER.debug("try serialize all records once");

		try
		{
			results = mapper.writeValueAsString(recordList);
			LOGGER.debug(results);
		}
		catch (JsonProcessingException e)
		{
			LOGGER.error("Could not serialize whole list");
			e.printStackTrace();
		}
		return results;
	}
}
