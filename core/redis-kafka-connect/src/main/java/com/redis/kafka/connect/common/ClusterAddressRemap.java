package com.redis.kafka.connect.common;

import io.lettuce.core.internal.HostAndPort;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Rewrites the port of each Redis Cluster address so one load balancer can expose every node through a single
 * PrivateLink endpoint, with a different port per node.
 *
 * <p>
 * Node hostnames end in {@code -SSSS-NNN} (MemoryDB and node-based ElastiCache), for example
 * {@code mycluster-0001-002.mycluster.abc123.memorydb.us-east-2.amazonaws.com}:
 * <ul>
 * <li>{@code SSSS} is the shard number: four digits, zero-padded, starting at {@code 0001}.</li>
 * <li>{@code NNN} is the node number within the shard: three digits, zero-padded, starting at {@code 001}. It does
 * not say which node is the primary.</li>
 * </ul>
 * The rule is {@code port = basePort + SSSS * 100 + NNN}. The configured seed host ({@code redis.host}, normally the
 * cluster configuration endpoint) uses {@code basePort} itself, whatever its name. With base port 7000,
 * {@code -0001-002} maps to 7102 and {@code -0002-001} to 7201.
 *
 * <p>
 * Only the port changes. The hostname is kept, so TLS still verifies the real hostname. A hostname that doesn't fit
 * the rule fails loudly instead of connecting to the wrong node.
 */
public final class ClusterAddressRemap {

    /** Highest shard number the rule supports (the shard number occupies the hundreds digit). */
    public static final int MAX_SHARD = 9;

    /** Highest node number the rule supports (the node number occupies the last two digits). */
    public static final int MAX_NODE = 99;

    /** Smallest allowed base port. */
    public static final int MIN_BASE_PORT = 1000;

    /** Largest allowed base port, so the 1000-port block stays within 65535. */
    public static final int MAX_BASE_PORT = 64000;

    private static final Pattern NODE_HOSTNAME = Pattern.compile("^[^.]+-(\\d{4})-(\\d{3})\\..+$");

    private final String seedHost;

    private final int basePort;

    /**
     * Creates the rule for a seed host and base port.
     *
     * @param seedHost the configured host ({@code redis.host}), reached on the base port
     * @param basePort the seed host's port, a multiple of 1000 between 1000 and 64000
     * @throws IllegalArgumentException if the seed host is empty or the base port is not allowed
     */
    public ClusterAddressRemap(String seedHost, int basePort) {
        if (seedHost == null || seedHost.trim().isEmpty()) {
            throw new IllegalArgumentException("Address remapping requires a seed host (redis.host)");
        }
        String error = basePortError(basePort);
        if (error != null) {
            throw new IllegalArgumentException(error);
        }
        this.seedHost = seedHost.trim();
        this.basePort = basePort;
    }

    /**
     * Checks a base port.
     *
     * @param basePort candidate base port
     * @return an error message, or {@code null} when the port is allowed
     */
    public static String basePortError(int basePort) {
        if (basePort % 1000 != 0 || basePort < MIN_BASE_PORT || basePort > MAX_BASE_PORT) {
            return "Address remapping requires redis.port to be a multiple of 1000 between " + MIN_BASE_PORT + " and "
                    + MAX_BASE_PORT + " (the base port, e.g. 7000), but it is " + basePort;
        }
        return null;
    }

    /** {@return the base port} */
    public int basePort() {
        return basePort;
    }

    /**
     * Returns the port to connect to for a hostname.
     *
     * @param host the seed host or a node hostname
     * @return the mapped port
     * @throws IllegalArgumentException if the hostname doesn't fit the rule
     */
    public int port(String host) {
        if (host != null && host.equalsIgnoreCase(seedHost)) {
            return basePort;
        }
        Matcher matcher = host == null ? null : NODE_HOSTNAME.matcher(host);
        if (matcher == null || !matcher.matches()) {
            throw new IllegalArgumentException("Address remapping can't map host '" + host
                    + "': expected the configured redis.host (" + seedHost
                    + ") or a node hostname ending in -SSSS-NNN, e.g. mycluster-0001-002.<domain>");
        }
        int shard = Integer.parseInt(matcher.group(1));
        int node = Integer.parseInt(matcher.group(2));
        if (shard < 1 || shard > MAX_SHARD) {
            throw new IllegalArgumentException("Address remapping can't map host '" + host + "': shard " + shard
                    + " is outside 1-" + MAX_SHARD + " (port = base + SSSS * 100 + NNN)");
        }
        if (node < 1 || node > MAX_NODE) {
            throw new IllegalArgumentException("Address remapping can't map host '" + host + "': node " + node
                    + " is outside 1-" + MAX_NODE + " (port = base + SSSS * 100 + NNN)");
        }
        return basePort + shard * 100 + node;
    }

    /**
     * Maps an address, keeping the hostname and replacing the port.
     *
     * @param address address announced by the cluster or configured as the seed
     * @return the address to connect to
     */
    public HostAndPort apply(HostAndPort address) {
        return HostAndPort.of(address.getHostText(), port(address.getHostText()));
    }

}
