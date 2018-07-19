package de.dortmund.skbmtp.KafkaNeo4JConnector.model;

public class SourceConstant {
	public static final String TWITTER_CONSUMER_KEY_KEY = "Twitter.ConsumerKeyKey";
	public static final String TWITTER_CONSUMER_SECRET_KEY = "Twitter.ConsumerSecretKey";
	public static final String TWITTER_ACCESS_TOKEN_KEY = "Twitter.AccessTokenKey";
	public static final String TWITTER_ACCESS_TOKEN_SECRET_KEY = "Twitter.AccessTokenSecretKey";
	public static final String KAFKA_BOOTSTRAP_SERVERS = "Kafka.BootstrapServers";
	public static final String KAFKA_ZOOKEEPER_CONNECT = "Kafka.ZookeperConnect";
	public static final String KAFKA_GROUP_ID = "Kafka.GroupId";
	public static final String KAFKA_TWITTER_TOPIC = "Kafka.TwitterRawTopic";
	public static final String KAFKA_TWITTER_IMPROVED_TOPIC = "Kafka.TwitterImprovedTopic";
	public static final String KAFKA_NEO4J_CONFIRMATION_TOPIC = "Kafka.Neo4JConfirmationTopic";
	public static final String KAFKA_RESULT_TOPIC = "Kafka.ResultTopic";
	public static final String TWITTER_FILTER_MESSAGES_PER_SECOND = "Twitter.FilterMessagesPerMinute";
	public static final String TWITTER_TRACK_TERMS = "Twitter.TrackTerms";
	public static final String TWITTER_TRACK_TERMS_SEPARATOR = "Twitter.TrackTerms.Separator";
	public static final String JSON_DATE_FORMAT = "Json.DateFormat";
	public static final String TWITTER_TRACK_LANGUAGES = "Twitter.TrackLanguages";
	public static final String TWITTER_TRACK_LANGUAGES_SEPARATOR = "Twitter.TrackLanguages.Separator";
	public static final String NEO4J_SERVER = "Neo4J.Server";
	public static final String NEO4J_USER = "Neo4J.Username";
	public static final String NEO4J_PASSWORD = "Neo4J.Password";
	public static final String BOTTOKEN = "Telegram.Bottoken";
}