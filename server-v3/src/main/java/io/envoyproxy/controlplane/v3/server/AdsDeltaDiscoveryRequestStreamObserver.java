package io.envoyproxy.controlplane.v3.server;

import static io.envoyproxy.controlplane.v3.server.DiscoveryServer.ANY_TYPE_URL;

import io.envoyproxy.controlplane.v3.cache.DeltaWatch;
import io.envoyproxy.controlplane.v3.cache.Resources;
import io.envoyproxy.controlplane.v3.cache.TrackedResources;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;

/**
 * {@code AdsDiscoveryRequestStreamObserver} is an implementation of {@link DiscoveryRequestStreamObserver} tailored for
 * ADS streams, which handle multiple watches for all TYPE_URLS.
 */
public class AdsDeltaDiscoveryRequestStreamObserver extends DeltaDiscoveryRequestStreamObserver {
  private final ConcurrentMap<String, DeltaWatch> watches;
  private final ConcurrentMap<String, String> latestVersion;
  private final ConcurrentMap<String, ConcurrentHashMap<String, LatestDeltaDiscoveryResponse>> responses;
  // mutated by this stream and read by the cache's snapshot thread, both under each entry's monitor
  private final ConcurrentMap<String, TrackedResources> trackedResources;

  AdsDeltaDiscoveryRequestStreamObserver(StreamObserver<DeltaDiscoveryResponse> responseObserver,
                                         long streamId,
                                         ScheduledExecutorService executor,
                                         DiscoveryServer discoveryServer) {
    super(ANY_TYPE_URL, responseObserver, streamId, executor, discoveryServer);
    this.watches = new ConcurrentHashMap<>(Resources.TYPE_URLS.size());
    this.latestVersion = new ConcurrentHashMap<>(Resources.TYPE_URLS.size());
    this.responses = new ConcurrentHashMap<>(Resources.TYPE_URLS.size());
    this.trackedResources = new ConcurrentHashMap<>(Resources.TYPE_URLS.size());
  }

  @Override
  public void onNext(DeltaDiscoveryRequest request) {
    if (request.getTypeUrl().isEmpty()) {
      rejectStream(
          Status.UNKNOWN
              .withDescription(String.format("[%d] type URL is required for ADS", streamId))
              .asRuntimeException());

      return;
    }

    super.onNext(request);
  }

  @Override
  void cancel() {
    watches.values().forEach(DeltaWatch::cancel);
  }

  @Override
  boolean ads() {
    return true;
  }

  @Override
  void setLatestVersion(String typeUrl, String version) {
    latestVersion.put(typeUrl, version);
    if (typeUrl.equals(Resources.CLUSTER_TYPE_URL)) {
      hasClusterChanged = true;
    } else if (typeUrl.equals(Resources.ENDPOINT_TYPE_URL)) {
      hasClusterChanged = false;
    }
  }

  @Override
  String latestVersion(String typeUrl) {
    return latestVersion.get(typeUrl);
  }

  @Override
  void setResponse(String typeUrl, String nonce, LatestDeltaDiscoveryResponse response) {
    responses.computeIfAbsent(typeUrl, s -> new ConcurrentHashMap<>())
        .put(nonce, response);
  }

  @Override
  LatestDeltaDiscoveryResponse clearResponse(String typeUrl, String nonce) {
    return responses.computeIfAbsent(typeUrl, s -> new ConcurrentHashMap<>())
        .remove(nonce);
  }

  @Override
  int responseCount(String typeUrl) {
    return responses.computeIfAbsent(typeUrl, s -> new ConcurrentHashMap<>())
        .size();
  }

  @Override
  boolean isWildcard(String typeUrl) {
    return typeUrl.equals(Resources.CLUSTER_TYPE_URL)
        || typeUrl.equals(Resources.LISTENER_TYPE_URL)
        || typeUrl.equals(Resources.SCOPED_ROUTE_TYPE_URL);
  }

  @Override
  TrackedResources trackedResources(String typeUrl) {
    return trackedResources.computeIfAbsent(typeUrl, s -> new TrackedResources());
  }

  @Override
  void cancelWatch(String typeUrl) {
    DeltaWatch watch = watches.get(typeUrl);
    if (watch != null) {
      watch.cancel();
    }
  }

  @Override
  void setWatch(String typeUrl, DeltaWatch watch) {
    watches.put(typeUrl, watch);
  }
}
