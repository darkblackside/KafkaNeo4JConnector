package de.dortmund.skbmtp.KafkaNeo4JConnector.model.result;

import java.io.Serializable;

public class Neo4JValue<T> implements Serializable
{
	private static final long serialVersionUID = 1L;
	private T value;

	public Neo4JValue(T value)
	{
		this.value = value;
	}

	public T getValue() {
		return value;
	}

	public Class<?> getType()
	{
		return value.getClass();
	}
}
