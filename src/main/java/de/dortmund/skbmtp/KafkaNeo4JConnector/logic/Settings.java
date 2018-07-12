package de.dortmund.skbmtp.KafkaNeo4JConnector.logic;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.dortmund.skbmtp.KafkaNeo4JConnector.model.SourceConstant;

public class Settings {
	private static final Logger LOGGER = LogManager.getLogger(Settings.class);
	private static Settings instance;

	private Properties props;

	private String bootstrapServers;
	private String twitterConsumerKey;
	private String twitterConsumerSecret;
	private String twitterAccessToken;
	private String twitterAccessTokenSecret;
	private String zookeeperUrl;
	private String groupId;
	private int filterTwittermessagesPerSecond;
	private List<String> trackTerms;
	private List<String> trackLanguages;

	private String rawTwitterTopic;
	private String improvedTwitterTopic;
	private String neo4jFeedbackTopic;
	private String resultTopic;

	private String dateFormat;

	private String neo4jServer;
	private String neo4jUser;
	private String neo4jPassword;

	private Settings() {
	}

	public static Settings getInstance(Class<?> startingClass, String filename) {
		if (instance == null) {
			instance = new Settings();
			instance.loadSettings(startingClass, filename);
		}

		return instance;
	}

	public static Settings getInstance(Class<?> startingClass) {
		return getInstance(startingClass, null);
	}

	public String getBootstrapServers() {
		return bootstrapServers;
	}

	public String getTwitterConsumerKey() {
		return twitterConsumerKey;
	}

	public String getTwitterConsumerSecret() {
		return twitterConsumerSecret;
	}

	public String getTwitterAccessToken() {
		return twitterAccessToken;
	}

	public String getTwitterAccessTokenSecret() {
		return twitterAccessTokenSecret;
	}

	public String getZookeeperUrl() {
		return zookeeperUrl;
	}

	public String getGroupId() {
		return groupId;
	}

	public int getTwitterMessagesPerSecond() {
		return filterTwittermessagesPerSecond;
	}

	public List<String> getTrackTerms() {
		return trackTerms;
	}

	public List<String> getLanguages() {
		return trackLanguages;
	}

	public String getKafkaTopicRawTwitter() {
		return rawTwitterTopic;
	}

	public String getKafkaTopicImprovedTwitter() {
		return improvedTwitterTopic;
	}

	public String getKafkaTopicNeo4JFeedback() {
		return neo4jFeedbackTopic;
	}

	public String getKafkaTopicResult() {
		return resultTopic;
	}

	public String getJsonDateFormat() {
		return dateFormat;
	}

	public String getNeo4JServer() {
		return neo4jServer;
	}

	public String getNeo4JUser() {
		return neo4jUser;
	}

	public String getNeo4JPassword() {
		return neo4jPassword;
	}

	private void loadSettings(Class<?> startingClass, String filename) {
		props = new Properties();

		try {
			if (filename != null) {
				LOGGER.info("Load settings from file " + filename);
				props.load(new InputStreamReader(startingClass.getResourceAsStream(filename)));
			} else {
				LOGGER.info("Load settings from file /conf.properties");
				props.load(new InputStreamReader(startingClass.getResourceAsStream("/conf.properties")));
			}
		} catch (IOException e1) {
			LOGGER.info("Error loading properties");
			throw new RuntimeException("Properties konnten nicht geladen werden", e1);
		}
		bootstrapServers = props.getProperty(SourceConstant.KAFKA_BOOTSTRAP_SERVERS, "localhost:9092");
		twitterConsumerKey = props.getProperty(SourceConstant.TWITTER_CONSUMER_KEY_KEY);
		twitterConsumerSecret = props.getProperty(SourceConstant.TWITTER_CONSUMER_SECRET_KEY);
		twitterAccessToken = props.getProperty(SourceConstant.TWITTER_ACCESS_TOKEN_KEY);
		twitterAccessTokenSecret = props.getProperty(SourceConstant.TWITTER_ACCESS_TOKEN_SECRET_KEY);
		zookeeperUrl = props.getProperty(SourceConstant.KAFKA_ZOOKEEPER_CONNECT, "localhost:2181");
		groupId = props.getProperty(SourceConstant.KAFKA_GROUP_ID, "myconsumer");
		filterTwittermessagesPerSecond = Integer
				.parseInt(props.getProperty(SourceConstant.TWITTER_FILTER_MESSAGES_PER_SECOND, "10"));
		dateFormat = props.getProperty(SourceConstant.JSON_DATE_FORMAT);

		rawTwitterTopic = props.getProperty(SourceConstant.KAFKA_TWITTER_TOPIC, "topic1");
		improvedTwitterTopic = props.getProperty(SourceConstant.KAFKA_TWITTER_IMPROVED_TOPIC, "topic2");
		neo4jFeedbackTopic = props.getProperty(SourceConstant.KAFKA_NEO4J_CONFIRMATION_TOPIC, "topic3");
		resultTopic = props.getProperty(SourceConstant.KAFKA_RESULT_TOPIC, "topic4");

		neo4jServer = props.getProperty(SourceConstant.NEO4J_SERVER, "bolt://localhost:7687");
		neo4jUser = props.getProperty(SourceConstant.NEO4J_USER, "neo4j");
		neo4jPassword = props.getProperty(SourceConstant.NEO4J_PASSWORD, "neo4jadmin");

		trackTerms = Util.splitStringBySeparator(props.getProperty(SourceConstant.TWITTER_TRACK_TERMS),
				props.getProperty(SourceConstant.TWITTER_TRACK_TERMS_SEPARATOR));
		trackLanguages = Util.splitStringBySeparator(props.getProperty(SourceConstant.TWITTER_TRACK_LANGUAGES),
				props.getProperty(SourceConstant.TWITTER_TRACK_LANGUAGES_SEPARATOR));
	}
}
