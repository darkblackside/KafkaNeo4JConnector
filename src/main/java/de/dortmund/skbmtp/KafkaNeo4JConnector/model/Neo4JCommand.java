package de.dortmund.skbmtp.KafkaNeo4JConnector.model;

import java.io.Serializable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import iot.jcypher.query.JcQuery;
import iot.jcypher.query.writer.Format;
import iot.jcypher.util.Util;

public class Neo4JCommand implements Serializable
{
	private static final long serialVersionUID = -99940557970072382L;

	@JsonIgnore
	private static final Logger LOGGER = LogManager.getLogger(Neo4JCommand.class);

	@JsonInclude
	private String stringCommand;

	/**
	 * JcQuery-Command https://github.com/Wolfgang-Schuetzelhofer/jcypher/wiki
	 */
	@JsonIgnore
	private JcQuery jcQueryCommand;

	@JsonInclude
	private String commandIdentifier;
	@JsonInclude
	private String commandMessage;

	@JsonInclude
	private String stringErrors;

	@JsonInclude
	private String result;

	/**
	 * Do not use this Constructor! It is only needed for serialization
	 */
	public Neo4JCommand()
	{

	}

	public Neo4JCommand(String stringCommand)
	{
		LOGGER.info("Generated string command");
		this.stringCommand = stringCommand;
	}

	/**
	 *
	 * @param stringCommand
	 * @param commandIdentifier for your purposes, given in the response
	 */
	public Neo4JCommand(String stringCommand, String commandIdentifier)
	{
		this(stringCommand);
		this.commandIdentifier = commandIdentifier;
	}

	/**
	 *
	 * @param stringCommand
	 * @param commandIdentifier for your purposes, given in the response
	 * @param commandMessage for your purposes, given in the response
	 */
	public Neo4JCommand(String stringCommand, String commandIdentifier, String commandMessage)
	{
		this(stringCommand, commandIdentifier);
		this.commandMessage = commandMessage;
	}


	public Neo4JCommand(JcQuery jcQueryCommand)
	{
		LOGGER.info("Generated jcQuery command");
		this.jcQueryCommand = jcQueryCommand;
	}

	/**
	 *
	 * @param jcQueryCommand
	 * @param commandIdentifier for your purposes, given in the response
	 */
	public Neo4JCommand(JcQuery jcQueryCommand, String commandIdentifier)
	{
		this(jcQueryCommand);
		this.commandIdentifier = commandIdentifier;
	}

	/**
	 *
	 * @param jcQueryCommand
	 * @param commandIdentifier for your purposes, given in the response
	 * @param commandMessage for your purposes, given in the response
	 */
	public Neo4JCommand(JcQuery jcQueryCommand, String commandIdentifier, String commandMessage)
	{
		this(jcQueryCommand, commandIdentifier);
		this.commandMessage = commandMessage;
	}


	public String getStringCommand()
	{
		return stringCommand;
	}

	public String getCommandIdentifier()
	{
		return commandIdentifier;
	}
	public void setCommandIdentifier(String commandIdentifier)
	{
		this.commandIdentifier = commandIdentifier;
	}

	public String getCommandMessage()
	{
		return commandMessage;
	}
	public void setCommandMessage(String commandMessage)
	{
		this.commandMessage = commandMessage;
	}

	/**
	 * The result of the command in Neo4J
	 * @return
	 */
	public String getResult()
	{
		return result;
	}
	/**
	 * Only for internal use to set result when command is excecuted
	 * @return
	 */
	public void setResult(String result)
	{
		this.result = result;
	}

	public void setError(String string)
	{
		this.stringErrors = string;
	}

	public String getError()
	{
		return stringErrors;
	}

	public void toCypherNativeQuery()
	{
		if(stringCommand == null && jcQueryCommand != null)
		{
			stringCommand = Util.toCypher(jcQueryCommand, Format.PRETTY_3);
		}
	}
}
