package com.redis.kafka.connect.common;

import com.redis.kafka.connect.sink.RedisSinkConfig;
import com.redis.kafka.connect.sink.RedisSinkConfigDef;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.internal.HostAndPort;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class ClusterAddressRemapTest {

    private static final String DOMAIN = "cascade-acceptance-004-memorydb-v2.0azdkj.memorydb.us-east-2.amazonaws.com";

    private static final String SEED = "clustercfg." + DOMAIN;

    private final ClusterAddressRemap remap = new ClusterAddressRemap(SEED, 7000);

    @Test
    void seedHostUsesBasePort() {
        Assertions.assertEquals(7000, remap.port(SEED));
        Assertions.assertEquals(7000, remap.port(SEED.toUpperCase()));
    }

    @Test
    void seedHostNeedNotStartWithClustercfg() {
        // e.g. ElastiCache without TLS puts "clustercfg" mid-name; any configured seed host works.
        String seed = "mycluster.abc123.clustercfg.use2.cache.amazonaws.com";
        Assertions.assertEquals(7000, new ClusterAddressRemap(seed, 7000).port(seed));
    }

    @Test
    void anotherClustercfgHostIsNotTheSeed() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> remap.port("clustercfg.other.example.com"));
    }

    @Test
    void seedHostIsRequired() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> new ClusterAddressRemap(" ", 7000));
        Assertions.assertThrows(IllegalArgumentException.class, () -> new ClusterAddressRemap(null, 7000));
    }

    @ParameterizedTest
    @CsvSource({ "0001-001, 7101", "0001-002, 7102", "0001-006, 7106", "0002-001, 7201", "0002-002, 7202",
            "0009-006, 7906" })
    void nodePortIsBasePlusShardTimes100PlusNode(String suffix, int expected) {
        Assertions.assertEquals(expected, remap.port("cascade-acceptance-004-memorydb-v2-" + suffix + "." + DOMAIN));
    }

    @Test
    void keepsHostnameAndReplacesPort() {
        String host = "cascade-acceptance-004-memorydb-v2-0001-002." + DOMAIN;
        HostAndPort mapped = remap.apply(HostAndPort.of(host, 6379));
        Assertions.assertEquals(host, mapped.getHostText());
        Assertions.assertEquals(7102, mapped.getPort());
    }

    @Test
    void otherBasePorts() {
        Assertions.assertEquals(2101, new ClusterAddressRemap(SEED, 2000).port("c-0001-001." + DOMAIN));
        Assertions.assertEquals(64905, new ClusterAddressRemap(SEED, 64000).port("c-0009-005." + DOMAIN));
    }

    @ParameterizedTest
    @ValueSource(strings = { "10.240.98.224", "other.example.com", "cluster-0001.example.com",
            "cluster-001-001.example.com", "cluster-0001-001" })
    void hostnamesOutsideTheRuleFailLoudly(String host) {
        IllegalArgumentException error = Assertions.assertThrows(IllegalArgumentException.class, () -> remap.port(host));
        Assertions.assertTrue(error.getMessage().contains(host), error.getMessage());
    }

    @Test
    void shardAboveNineFails() {
        IllegalArgumentException error = Assertions.assertThrows(IllegalArgumentException.class,
                () -> remap.port("c-0010-001." + DOMAIN));
        Assertions.assertTrue(error.getMessage().contains("shard 10"), error.getMessage());
    }

    @Test
    void shardOrNodeZeroFails() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> remap.port("c-0000-001." + DOMAIN));
        Assertions.assertThrows(IllegalArgumentException.class, () -> remap.port("c-0001-000." + DOMAIN));
    }

    @ParameterizedTest
    @ValueSource(ints = { 6379, 7500, 999, 0, 65000, -1000 })
    void basePortMustBeAMultipleOf1000InRange(int port) {
        Assertions.assertNotNull(ClusterAddressRemap.basePortError(port));
        Assertions.assertThrows(IllegalArgumentException.class, () -> new ClusterAddressRemap(SEED, port));
    }

    @ParameterizedTest
    @ValueSource(ints = { 1000, 6000, 7000, 64000 })
    void allowedBasePorts(int port) {
        Assertions.assertNull(ClusterAddressRemap.basePortError(port));
    }

    @Test
    void configRequiresClusterMode() {
        Map<String, String> props = props();
        props.put(RedisConfigDef.CLUSTER_CONFIG, "false");
        ConfigException error = Assertions.assertThrows(ConfigException.class, () -> new RedisSinkConfig(props));
        Assertions.assertTrue(error.getMessage().contains("requires redis.cluster=true"), error.getMessage());
        assertValidationError(props, "requires redis.cluster=true");
    }

    @Test
    void configRequiresAlignedBasePort() {
        Map<String, String> props = props();
        props.put(RedisConfigDef.PORT_CONFIG, "6379");
        ConfigException error = Assertions.assertThrows(ConfigException.class, () -> new RedisSinkConfig(props));
        Assertions.assertTrue(error.getMessage().contains("multiple of 1000"), error.getMessage());
        assertValidationError(props, "multiple of 1000");
    }

    @Test
    void configUsesTheUriPortAsBase() {
        Map<String, String> props = props();
        props.remove(RedisConfigDef.HOST_CONFIG);
        props.remove(RedisConfigDef.PORT_CONFIG);
        props.put(RedisConfigDef.URI_CONFIG, "rediss://clustercfg." + DOMAIN + ":6379");
        Assertions.assertThrows(ConfigException.class, () -> new RedisSinkConfig(props));
        props.put(RedisConfigDef.URI_CONFIG, "rediss://clustercfg." + DOMAIN + ":7000");
        Assertions.assertTrue(new RedisSinkConfig(props).isAddressRemapEnabled());
    }

    @Test
    void validConfigCreatesAClientThatShutsDown() {
        Map<String, String> props = props();
        RedisSinkConfig config = new RedisSinkConfig(props);
        Assertions.assertTrue(config.isAddressRemapEnabled());
        Map<String, ConfigValue> results = new RedisSinkConfigDef().validateAll(props);
        Assertions.assertTrue(results.get(RedisConfigDef.CLUSTER_ADDRESS_REMAP_CONFIG).errorMessages().isEmpty());
        AbstractRedisClient client = config.client();
        Assertions.assertInstanceOf(RemappedClusterClient.class, client);
        client.shutdown();
        client.shutdown(); // a second shutdown is a no-op
    }

    @Test
    void disabledByDefault() {
        Map<String, String> props = props();
        props.remove(RedisConfigDef.CLUSTER_ADDRESS_REMAP_CONFIG);
        props.put(RedisConfigDef.PORT_CONFIG, "6379");
        RedisSinkConfig config = new RedisSinkConfig(props);
        Assertions.assertFalse(config.isAddressRemapEnabled());
        AbstractRedisClient client = config.client();
        Assertions.assertFalse(client instanceof RemappedClusterClient);
        client.shutdown();
    }

    private static Map<String, String> props() {
        Map<String, String> props = new HashMap<>();
        props.put(RedisConfigDef.CLUSTER_CONFIG, "true");
        props.put(RedisConfigDef.CLUSTER_ADDRESS_REMAP_CONFIG, "true");
        props.put(RedisConfigDef.HOST_CONFIG, "clustercfg." + DOMAIN);
        props.put(RedisConfigDef.PORT_CONFIG, "7000");
        props.put(RedisConfigDef.TLS_CONFIG, "true");
        return props;
    }

    private static void assertValidationError(Map<String, String> props, String expected) {
        Map<String, ConfigValue> results = new RedisSinkConfigDef().validateAll(props);
        String messages = String.join("; ", results.get(RedisConfigDef.CLUSTER_ADDRESS_REMAP_CONFIG).errorMessages());
        Assertions.assertTrue(messages.contains(expected), messages);
    }

}
