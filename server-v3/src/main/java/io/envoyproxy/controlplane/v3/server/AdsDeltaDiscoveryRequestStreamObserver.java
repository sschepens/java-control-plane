package io.envoyproxy.controlplane.v3.server;

import static io.envoyproxy.controlplane.v3.server.DiscoveryServer.ANY_TYPE_URL;

import io.envoyproxy.controlplane.v3.cache.DeltaWatch;
import io.envoyproxy.controlplane.v3.cache.Resources;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

/**
 * {@code AdsDiscoveryRequestStreamObserver} is an implementation of {@link DiscoveryRequestStreamObserver} tailored for
 * ADS streams, which handle multiple watches for all TYPE_URLS.
 */
public class AdsDeltaDiscoveryRequestStreamObserver extends DeltaDiscoveryRequestStreamObserver {
  private final ConcurrentMap<String, DeltaWatch> watches;
  private final ConcurrentMap<String, String> latestVersion;
  private final ConcurrentMap<String, ConcurrentHashMap<String, LatestDeltaDiscoveryResponse>> responses;
  // tracked and pending resources are always accessed in the observer
  // so they are safe from race, no need for concurrent map
  private final Map<String, Map<String, String>> trackedResourceMap;
  private final Map<String, Set<String>> pendingResourceMap;

  AdsDeltaDiscoveryRequestStreamObserver(StreamObserver<DeltaDiscoveryResponse> responseObserver,
                                         long streamId,
                                         ScheduledExecutorService executor,
                                         DiscoveryServer discoveryServer) {
    super(ANY_TYPE_URL, responseObserver, streamId, executor, discoveryServer);
    this.watches = new ConcurrentHashMap<>(Resources.TYPE_URLS.size());
    this.latestVersion = new ConcurrentHashMap<>(Resources.TYPE_URLS.size());
    this.responses = new ConcurrentHashMap<>(Resources.TYPE_URLS.size());
    this.trackedResourceMap = new HashMap<>(Resources.TYPE_URLS.size());
    this.pendingResourceMap = new HashMap<>(Resources.TYPE_URLS.size());
  }

  @Override
  public void onNext(DeltaDiscoveryRequest request) {
    if (request.getTypeUrl().isEmpty()) {
      closeWithError(
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
  Map<String, String> resourceVersions(String typeUrl) {
    return trackedResourceMap.getOrDefault(typeUrl, Collections.emptyMap());
  }

  @Override
  Set<String> pendingResources(String typeUrl) {
    return pendingResourceMap.getOrDefault(typeUrl, Collections.emptySet());
  }

  @Override
  boolean isWildcard(String typeUrl) {
    return typeUrl.equals(Resources.CLUSTER_TYPE_URL)
        || typeUrl.equals(Resources.LISTENER_TYPE_URL)
        || typeUrl.equals(Resources.SCOPED_ROUTE_TYPE_URL);
  }

  @Override
  void updateTrackedResources(String typeUrl,
                              Map<String, String> resourcesVersions,
                              List<String> removedResources) {

    Map<String, String> trackedResources = trackedResourceMap.computeIfAbsent(typeUrl, s -> new HashMap<>());
    Set<String> pendingResources = pendingResourceMap.computeIfAbsent(typeUrl, s -> new HashSet<>());
    resourcesVersions.forEach((k, v) -> {
      trackedResources.put(k, v);
      pendingResources.remove(k);
    });
    removedResources.forEach(trackedResources::remove);
    pendingResources.removeAll(removedResources);
  }

  @Override
  void updateSubscriptions(String typeUrl, List<String> resourceNamesSubscribe, List<String> resourceNamesUnsubscribe) {
    Map<String, String> trackedResources = trackedResourceMap.computeIfAbsent(typeUrl, s -> new HashMap<>());
    Set<String> pendingResources = pendingResourceMap.computeIfAbsent(typeUrl, s -> new HashSet<>());
    // unsubscribe first
    resourceNamesUnsubscribe.forEach(s -> {
      trackedResources.remove(s);
      pendingResources.remove(s);
    });
    pendingResources.addAll(resourceNamesSubscribe);
  }

  @Override
  void computeWatch(String typeUrl, Supplier<DeltaWatch> watchCreator) {
    watches.compute(typeUrl, (s, watch) -> {
      if (watch != null) {
        watch.cancel();
      }

      return watchCreator.get();
    });
  }
}
