package com.redis.kafka.connect.common;

import io.lettuce.core.RedisURI;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;

/** Defines the common Redis connection configuration options. */
public abstract class RedisConfigDef extends ConfigDef {

	/** Configuration key for {@code redis.cluster}. */
	public static final String CLUSTER_CONFIG = "redis.cluster";

	private static final boolean CLUSTER_DEFAULT = false;

	private static final String CLUSTER_DOC = "Connect to a Redis Cluster database";

	/** Configuration key for {@code redis.cluster.address.remap.enabled}. */
	public static final String CLUSTER_ADDRESS_REMAP_CONFIG = "redis.cluster.address.remap.enabled";

	private static final boolean CLUSTER_ADDRESS_REMAP_DEFAULT = false;

	private static final String CLUSTER_ADDRESS_REMAP_DOC = "Rewrite the port of every cluster address so all nodes"
			+ " can be reached through one load balancer (e.g. AWS PrivateLink), each on its own port. Requires"
			+ " redis.cluster=true and a redis.port that is a multiple of 1000 between 1000 and 64000 (the base port)."
			+ " redis.host stays the cluster configuration endpoint and is reached on the base port, whatever its name."
			+ " Node hostnames end in -SSSS-NNN, where SSSS is the shard number (0001...) and NNN the node number"
			+ " within the shard (001...); port = base + SSSS * 100 + NNN. Example with redis.port=7000:"
			+ " mycluster-0001-002.<domain> -> 7102, mycluster-0002-001.<domain> -> 7201. At most 9 shards."
			+ " Hostnames that don't fit the rule fail to connect.";

	/** Configuration key for {@code redis.host}. */
	public static final String HOST_CONFIG = "redis.host";

	private static final String HOST_DEFAULT = "localhost";

	private static final String HOST_DOC = "The Redis host to connect to";

	/** Configuration key for {@code redis.port}. */
	public static final String PORT_CONFIG = "redis.port";

	private static final int PORT_DEFAULT = RedisURI.DEFAULT_REDIS_PORT;

	private static final String PORT_DOC = "The Redis port to connect to";

	/** Configuration key for {@code redis.uri}. */
	public static final String URI_CONFIG = "redis.uri";

	private static final String URI_DEFAULT = "";

	private static final String URI_DOC = "URI of the Redis database to connect to, e.g. redis://redis-12000.redis.com:12000. For secure connections use rediss URI scheme, e.g. rediss://...";

	/** Configuration key for {@code redis.username}. */
	public static final String USERNAME_CONFIG = "redis.username";

	private static final String USERNAME_DEFAULT = "";

	private static final String USERNAME_DOC = "Username to use to connect to Redis";

	/** Configuration key for {@code redis.password}. */
	public static final String PASSWORD_CONFIG = "redis.password";

	private static final String PASSWORD_DEFAULT = "";

	private static final String PASSWORD_DOC = "Password to use to connect to Redis";

	/** Configuration key for {@code redis.timeout}. */
	public static final String TIMEOUT_CONFIG = "redis.timeout";

	private static final long TIMEOUT_DEFAULT = RedisURI.DEFAULT_TIMEOUT;

	private static final String TIMEOUT_DOC = "Redis command timeout in seconds";

	/** Configuration key for {@code redis.pool}. */
	public static final String POOL_MAX_CONFIG = "redis.pool";

	private static final int POOL_MAX_DEFAULT = 8;

	private static final String POOL_MAX_DOC = "Max pool connections";

	/** Configuration key for {@code redis.tls}. */
	public static final String TLS_CONFIG = "redis.tls";

	private static final boolean TLS_DEFAULT = false;

	private static final String TLS_DOC = "Establish a secure TLS connection";

	/** Configuration key for {@code redis.insecure}. */
	public static final String INSECURE_CONFIG = "redis.insecure";

	private static final boolean INSECURE_DEFAULT = false;

	private static final String INSECURE_DOC = "Allow insecure connections (e.g. invalid certificates) to Redis when using SSL";

	/** Configuration key for {@code redis.key.file}. */
	public static final String KEY_CONFIG = "redis.key.file";

	/** Default value for {@link #KEY_CONFIG}. */
	public static final String KEY_DEFAULT = "";

	private static final String KEY_DOC = "PKCS#8 private key file to authenticate with (PEM format)";

	/** Configuration key for {@code redis.key.cert}. */
	public static final String KEY_CERT_CONFIG = "redis.key.cert";

	/** Default value for {@link #KEY_CERT_CONFIG}. */
	public static final String KEY_CERT_DEFAULT = "";

	private static final String KEY_CERT_DOC = "X.509 certificate chain file to authenticate with (PEM format)";

	/** Configuration key for {@code redis.key.password}. */
	public static final String KEY_PASSWORD_CONFIG = "redis.key.password";

	private static final String KEY_PASSWORD_DEFAULT = "";

	private static final String KEY_PASSWORD_DOC = "Password of the private key file. Leave empty if key file is not password-protected";

	/** Configuration key for {@code redis.cacert}. */
	public static final String CACERT_CONFIG = "redis.cacert";

	/** Default value for {@link #CACERT_CONFIG}. */
	public static final String CACERT_DEFAULT = "";

	private static final String CACERT_DOC = "X.509 CA certificate file to verify with";

	/** Creates the configuration definition. */
	protected RedisConfigDef() {
		defineConfigs();
	}

	/**
	 * Creates the configuration definition.
	 *
	 * @param base base configuration to copy
	 */
	protected RedisConfigDef(ConfigDef base) {
		super(base);
		defineConfigs();
	}

	private void defineConfigs() {
		define(CLUSTER_CONFIG, Type.BOOLEAN, CLUSTER_DEFAULT, Importance.MEDIUM, CLUSTER_DOC);
		define(CLUSTER_ADDRESS_REMAP_CONFIG, Type.BOOLEAN, CLUSTER_ADDRESS_REMAP_DEFAULT, Importance.LOW,
				CLUSTER_ADDRESS_REMAP_DOC);
		define(HOST_CONFIG, Type.STRING, HOST_DEFAULT, Importance.HIGH, HOST_DOC);
		define(PORT_CONFIG, Type.INT, PORT_DEFAULT, Importance.HIGH, PORT_DOC);
		define(URI_CONFIG, Type.STRING, URI_DEFAULT, Importance.MEDIUM, URI_DOC);
		define(TLS_CONFIG, Type.BOOLEAN, TLS_DEFAULT, Importance.MEDIUM, TLS_DOC);
		define(INSECURE_CONFIG, Type.BOOLEAN, INSECURE_DEFAULT, Importance.MEDIUM, INSECURE_DOC);
		define(PASSWORD_CONFIG, Type.PASSWORD, PASSWORD_DEFAULT, Importance.MEDIUM, PASSWORD_DOC);
		define(USERNAME_CONFIG, Type.STRING, USERNAME_DEFAULT, Importance.MEDIUM, USERNAME_DOC);
		define(TIMEOUT_CONFIG, Type.LONG, TIMEOUT_DEFAULT, Range.atLeast(1), Importance.MEDIUM, TIMEOUT_DOC);
		define(POOL_MAX_CONFIG, Type.INT, POOL_MAX_DEFAULT, Range.atLeast(1), Importance.MEDIUM, POOL_MAX_DOC);
		define(KEY_CONFIG, Type.STRING, KEY_DEFAULT, Importance.MEDIUM, KEY_DOC);
		define(KEY_CERT_CONFIG, Type.STRING, KEY_CERT_DEFAULT, Importance.MEDIUM, KEY_CERT_DOC);
		define(KEY_PASSWORD_CONFIG, Type.PASSWORD, KEY_PASSWORD_DEFAULT, Importance.MEDIUM, KEY_PASSWORD_DOC);
		define(CACERT_CONFIG, Type.STRING, CACERT_DEFAULT, Importance.MEDIUM, CACERT_DOC);
	}

	@Override
	public Map<String, ConfigValue> validateAll(Map<String, String> props) {
		Map<String, ConfigValue> results = super.validateAll(props);
		ConfigValue remap = results.get(CLUSTER_ADDRESS_REMAP_CONFIG);
		if (remap == null || !Boolean.TRUE.equals(remap.value()) || !remap.errorMessages().isEmpty()) {
			return results;
		}
		ConfigValue cluster = results.get(CLUSTER_CONFIG);
		ConfigValue port = results.get(PORT_CONFIG);
		if (!cluster.errorMessages().isEmpty() || !port.errorMessages().isEmpty()) {
			return results;
		}
		Integer basePort = basePort(props.get(URI_CONFIG), (Integer) port.value());
		String error = basePort == null ? null : addressRemapError(Boolean.TRUE.equals(cluster.value()), basePort);
		if (error != null) {
			remap.addErrorMessage(error);
		}
		return results;
	}

	/**
	 * Checks the settings address remapping depends on.
	 *
	 * @param cluster value of {@code redis.cluster}
	 * @param basePort port of the configured endpoint ({@code redis.port}, or the port in {@code redis.uri})
	 * @return an error message, or {@code null} when remapping can be used
	 */
	public static String addressRemapError(boolean cluster, int basePort) {
		if (!cluster) {
			return CLUSTER_ADDRESS_REMAP_CONFIG + "=true requires " + CLUSTER_CONFIG + "=true";
		}
		return ClusterAddressRemap.basePortError(basePort);
	}

	private static Integer basePort(String uri, Integer port) {
		if (uri == null || uri.trim().isEmpty()) {
			return port;
		}
		try {
			return RedisURI.create(uri.trim()).getPort();
		} catch (RuntimeException e) {
			return null; // an invalid URI is reported when the connector parses it
		}
	}

}
