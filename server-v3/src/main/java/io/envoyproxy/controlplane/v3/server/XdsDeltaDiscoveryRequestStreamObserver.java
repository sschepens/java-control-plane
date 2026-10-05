package io.envoyproxy.controlplane.v3.server;

import io.envoyproxy.controlplane.v3.cache.DeltaWatch;
import io.envoyproxy.controlplane.v3.cache.Resources;
import io.envoyproxy.controlplane.v3.cache.TrackedResources;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryResponse;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;

/**
 * {@code XdsDiscoveryRequestStreamObserver} is a lightweight implementation of {@link DiscoveryRequestStreamObserver}
 * tailored for non-ADS streams which handle a single watch.
 */
public class XdsDeltaDiscoveryRequestStreamObserver extends DeltaDiscoveryRequestStreamObserver {
  // mutated by this stream and read by the cache's snapshot thread, both under its monitor
  private final TrackedResources trackedResources;
  private final boolean isWildcard;
  private volatile DeltaWatch watch;
  private volatile String latestVersion;
  private final ConcurrentMap<String, LatestDeltaDiscoveryResponse> responses;

  XdsDeltaDiscoveryRequestStreamObserver(String defaultTypeUrl,
                                         StreamObserver<DeltaDiscoveryResponse> responseObserver,
                                         long streamId,
                                         ScheduledExecutorService executor,
                                         DiscoveryServer discoveryServer) {
    super(defaultTypeUrl, responseObserver, streamId, executor, discoveryServer);
    this.trackedResources = new TrackedResources();
    this.isWildcard = defaultTypeUrl.equals(Resources.CLUSTER_TYPE_URL)
        || defaultTypeUrl.equals(Resources.LISTENER_TYPE_URL)
        || defaultTypeUrl.equals(Resources.SCOPED_ROUTE_TYPE_URL);
    responses = new ConcurrentHashMap<>();
  }

  @Override
  public void onNext(DeltaDiscoveryRequest request) {
    super.onNext(request);
  }

  @Override
  void cancel() {
    if (watch != null) {
      watch.cancel();
    }
  }

  @Override
  boolean ads() {
    return false;
  }

  @Override
  void setLatestVersion(String typeUrl, String version) {
    latestVersion = version;
  }

  @Override
  String latestVersion(String typeUrl) {
    return latestVersion;
  }

  @Override
  void setResponse(String typeUrl, String nonce, LatestDeltaDiscoveryResponse response) {
    responses.put(nonce, response);
  }

  @Override
  LatestDeltaDiscoveryResponse clearResponse(String typeUrl, String nonce) {
    return responses.remove(nonce);
  }

  @Override
  int responseCount(String typeUrl) {
    return responses.size();
  }

  @Override
  boolean isWildcard(String typeUrl) {
    return isWildcard;
  }

  @Override
  TrackedResources trackedResources(String typeUrl) {
    return trackedResources;
  }

  @Override
  void cancelWatch(String typeUrl) {
    cancel();
  }

  @Override
  void setWatch(String typeUrl, DeltaWatch watch) {
    this.watch = watch;
  }
}
