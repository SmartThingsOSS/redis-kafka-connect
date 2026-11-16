package com.redis.kafka.connect.common;

import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.MappingSocketAddressResolver;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A cluster client whose connections go through {@link ClusterAddressRemap}.
 *
 * <p>
 * The remapping lives in the client's own {@link ClientResources}. Lettuce does not shut down resources passed in by
 * the caller, so this client shuts them down itself; callers keep calling {@code shutdown()} as for any other client.
 */
final class RemappedClusterClient extends RedisClusterClient {

    private final ClientResources ownedResources;

    private final AtomicBoolean resourcesShutdown = new AtomicBoolean();

    private RemappedClusterClient(ClientResources resources, RedisURI uri) {
        super(resources, Collections.singleton(uri));
        this.ownedResources = resources;
    }

    /**
     * Creates a cluster client that connects through the remapping rule.
     *
     * @param uri seed URI (the cluster configuration endpoint)
     * @param remap the port rule
     * @return the client; the caller must shut it down
     */
    static RemappedClusterClient create(RedisURI uri, ClusterAddressRemap remap) {
        ClientResources resources = ClientResources.builder()
                .socketAddressResolver(MappingSocketAddressResolver.create(remap::apply))
                .build();
        return new RemappedClusterClient(resources, uri);
    }

    @Override
    public CompletableFuture<Void> shutdownAsync(long quietPeriod, long timeout, TimeUnit timeUnit) {
        return super.shutdownAsync(quietPeriod, timeout, timeUnit).thenCompose(ignored -> {
            if (!resourcesShutdown.compareAndSet(false, true)) {
                return CompletableFuture.completedFuture(null);
            }
            CompletableFuture<Void> done = new CompletableFuture<>();
            ownedResources.shutdown(quietPeriod, timeout, timeUnit).addListener(future -> {
                if (future.isSuccess()) {
                    done.complete(null);
                } else {
                    done.completeExceptionally(future.cause());
                }
            });
            return done;
        });
    }

}
