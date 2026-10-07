package io.envoyproxy.controlplane.v3.cache;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.google.protobuf.Message;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DiscoveryRequest;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.concurrent.GuardedBy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code SimpleCache} provides a default implementation of {@link SnapshotCache}. It maintains a single versioned
 * {@link Snapshot} per node group. For the protocol to work correctly in ADS mode, EDS/RDS requests are responded to
 * only when all resources in the snapshot xDS response are named as part of the request. It is expected that the CDS
 * response names all EDS clusters, and the LDS response names all RDS routes in a snapshot, to ensure that Envoy makes
 * the request for all EDS clusters or RDS routes eventually.
 *
 * <p>The snapshot can be partial, e.g. only include RDS or EDS resources.
 */
public class SimpleCache<T> implements SnapshotCache<T> {

  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleCache.class);

  private final NodeGroup<T> groups;

  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  private final Lock readLock = lock.readLock();
  private final Lock writeLock = lock.writeLock();

  @GuardedBy("lock")
  private final Map<T, Snapshot> snapshots = new HashMap<>();

  // Keep a stable lock for each group even after clearSnapshot: replacing a lock while a push is in flight
  // would allow two pushes for the same group to run concurrently.
  private final ConcurrentMap<T, Lock> pushLocks = new ConcurrentHashMap<>();
  private final ConcurrentMap<T, ConcurrentMap<String, CacheStatusInfo<T>>> statuses = new ConcurrentHashMap<>();

  private AtomicLong watchCount = new AtomicLong();

  /**
   * Constructs a simple cache.
   *
   * @param groups maps an envoy host to a node group
   */
  public SimpleCache(NodeGroup<T> groups) {
    this.groups = groups;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean clearSnapshot(T group) {
    // we take a writeLock to prevent watches from being created
    writeLock.lock();
    try {
      Map<String, CacheStatusInfo<T>> status = statuses.get(group);

      // If we don't know about this group, do nothing.
      if (status != null && status.values().stream().mapToLong(CacheStatusInfo::numWatches).sum() > 0) {
        LOGGER.warn("tried to clear snapshot for group with existing watches, group={}", group);

        return false;
      }

      statuses.remove(group);
      snapshots.remove(group);

      return true;
    } finally {
      writeLock.unlock();
    }
  }

  public Watch createWatch(
      boolean ads,
      DiscoveryRequest request,
      Set<String> knownResourceNames,
      Consumer<Response> responseConsumer) {
    return createWatch(ads, request, knownResourceNames, responseConsumer, false);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Watch createWatch(
      boolean ads,
      DiscoveryRequest request,
      Set<String> knownResourceNames,
      Consumer<Response> responseConsumer,
      boolean hasClusterChanged) {

    checkArgument(request.getTypeUrl().isEmpty() || Resources.TYPE_URLS.contains(request.getTypeUrl()),
        "unsupported type URL %s", request.getTypeUrl());
    T group = groups.hash(request.getNode());
    // even though we're modifying, we take a readLock to allow multiple watches to be created in parallel since it
    // doesn't conflict
    readLock.lock();
    try {
      CacheStatusInfo<T> status = statuses.computeIfAbsent(group, g -> new ConcurrentHashMap<>())
          .computeIfAbsent(request.getTypeUrl(), s -> new CacheStatusInfo<>(group));
      status.setLastWatchRequestTime(System.currentTimeMillis());

      Snapshot snapshot = snapshots.get(group);
      String version = snapshot == null ? "" : snapshot.version(request.getTypeUrl(), request.getResourceNamesList());

      Watch watch = new Watch(ads, request, responseConsumer);

      if (snapshot != null) {
        Set<String> requestedResources = new HashSet<>(request.getResourceNamesList());

        // If the request is asking for resources we haven't sent to the proxy yet, see if we have additional resources.
        if (!knownResourceNames.equals(requestedResources)) {
          Sets.SetView<String> newResourceHints = Sets.difference(requestedResources, knownResourceNames);

          // If any of the newly requested resources are in the snapshot respond immediately. If not we'll fall back to
          // version comparisons.
          if (snapshot.resources(request.getTypeUrl())
              .keySet()
              .stream()
              .anyMatch(newResourceHints::contains)) {
            respond(watch, snapshot, group);

            return watch;
          }
        } else if (hasClusterChanged && request.getTypeUrl().equals(Resources.ENDPOINT_TYPE_URL)) {
          respond(watch, snapshot, group);

          return watch;
        }
      }

      // If the requested version is up-to-date or missing a response, leave an open watch.
      if (snapshot == null || request.getVersionInfo().equals(version)) {
        long watchId = watchCount.incrementAndGet();

        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("open watch {} for {}[{}] from node {} for version {}",
              watchId,
              request.getTypeUrl(),
              String.join(", ", request.getResourceNamesList()),
              group,
              request.getVersionInfo());
        }

        status.setWatch(watchId, watch);

        watch.setStop(() -> status.removeWatch(watchId));

        return watch;
      }

      // Otherwise, the watch may be responded immediately
      boolean responded = respond(watch, snapshot, group);

      if (!responded) {
        long watchId = watchCount.incrementAndGet();

        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("did not respond immediately, leaving open watch {} for {}[{}] from node {} for version {}",
              watchId,
              request.getTypeUrl(),
              String.join(", ", request.getResourceNamesList()),
              group,
              request.getVersionInfo());
        }

        status.setWatch(watchId, watch);

        watch.setStop(() -> status.removeWatch(watchId));
      }

      return watch;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public DeltaWatch createDeltaWatch(DeltaDiscoveryRequest request,
                                     String requesterVersion,
                                     TrackedResources trackedResources,
                                     boolean isWildcard,
                                     Consumer<DeltaResponse> responseConsumer,
                                     boolean hasClusterChanged) {
    // The tracked resources are also read by setSnapshot and mutated by the owning stream, always under their
    // lock. Take it before the cache lock: setSnapshot never holds the write lock while taking it.
    return trackedResources.locked(() -> createDeltaWatchLocked(request, requesterVersion, trackedResources,
        isWildcard, responseConsumer, hasClusterChanged));
  }

  private DeltaWatch createDeltaWatchLocked(DeltaDiscoveryRequest request,
                                            String requesterVersion,
                                            TrackedResources trackedResources,
                                            boolean isWildcard,
                                            Consumer<DeltaResponse> responseConsumer,
                                            boolean hasClusterChanged) {
    checkArgument(request.getTypeUrl().isEmpty() || Resources.TYPE_URLS.contains(request.getTypeUrl()),
        "unsupported type URL %s", request.getTypeUrl());
    T group = groups.hash(request.getNode());
    // even though we're modifying, we take a readLock to allow multiple watches to be created in parallel since it
    // doesn't conflict
    readLock.lock();
    try {
      CacheStatusInfo<T> status = statuses.computeIfAbsent(group, g -> new ConcurrentHashMap<>())
          .computeIfAbsent(request.getTypeUrl(), s -> new CacheStatusInfo<>(group));
      status.setLastWatchRequestTime(System.currentTimeMillis());

      Snapshot snapshot = snapshots.get(group);
      String version = snapshot == null ? "" : snapshot.version(request.getTypeUrl());
      DeltaWatch watch = new DeltaWatch(request, trackedResources, requesterVersion, isWildcard, responseConsumer);

      // If no snapshot, leave an open watch.
      if (snapshot == null) {
        long watchId = setDeltaWatch(status, watch);
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("open watch {} for {} ({} tracked) from node {} for version {}",
              watchId,
              request.getTypeUrl(),
              watch.trackedResources().size(),
              group,
              requesterVersion);
        }

        return watch;
      }

      // If the requested version is up-to-date or missing a response, leave an open watch.
      if (version.equals(requesterVersion)) {
        if (hasClusterChanged && request.getTypeUrl().equals(Resources.ENDPOINT_TYPE_URL)) {
          ResponseState responseState = respondDeltaTracked(
              watch,
              snapshot.resources(request.getTypeUrl()),
              version,
              group);
          if (responseState.equals(ResponseState.RESPONDED) || responseState.equals(ResponseState.CANCELLED)) {
            return watch;
          }
        } else if (!isWildcard && watch.pendingResources().size() != 0) {
          // If the request is not wildcard, we have pending resources and we have them, we should respond immediately.
          // If any of the pending resources are in the snapshot respond immediately. If not we'll fall back to
          // version comparisons.
          Map<String, SnapshotResource<?>> resources = snapshot.resources(request.getTypeUrl());
          Map<String, SnapshotResource<?>> requestedResources = watch.pendingResources()
              .stream()
              .filter(resources::containsKey)
              .collect(Collectors.toUnmodifiableMap(Function.identity(), resources::get));
          ResponseState responseState = respondDelta(watch,
              requestedResources,
              findRemovedResources(watch, resources),
              version,
              group);
          if (responseState.equals(ResponseState.RESPONDED) || responseState.equals(ResponseState.CANCELLED)) {
            return watch;
          }
        }

        long watchId = setDeltaWatch(status, watch);
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("open watch {} for {} ({} tracked) from node {} for version {}",
              watchId,
              request.getTypeUrl(),
              watch.trackedResources().size(),
              group,
              requesterVersion);
        }

        return watch;
      }

      // Otherwise, version is different, the watch may be responded immediately
      ResponseState responseState = respondDeltaTracked(watch,
          snapshot.resources(request.getTypeUrl()),
          version,
          group);
      if (responseState.equals(ResponseState.RESPONDED) || responseState.equals(ResponseState.CANCELLED)) {
        return watch;
      }

      long watchId = setDeltaWatch(status, watch);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("did not respond immediately, leaving open watch {} for {} ({} tracked) from node {} for version {}",
            watchId,
            request.getTypeUrl(),
            watch.trackedResources().size(),
            group,
            requesterVersion);
      }

      return watch;
    } finally {
      readLock.unlock();
    }
  }

  private long setDeltaWatch(CacheStatusInfo<T> status, DeltaWatch watch) {
    long watchId = watchCount.incrementAndGet();
    status.setDeltaWatch(watchId, watch);
    watch.setStop(() -> status.removeDeltaWatch(watchId));
    return watchId;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Snapshot getSnapshot(T group) {
    readLock.lock();

    try {
      return snapshots.get(group);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Collection<T> groups() {
    return Collections.unmodifiableSet(statuses.keySet());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setSnapshot(T group, Snapshot snapshot) {
    // Serialize the entire replace-and-notify operation for this group while allowing unrelated groups to push.
    Lock pushLock = pushLocks.computeIfAbsent(group, g -> new ReentrantLock());
    pushLock.lock();
    try {
      // Prevent watches from being created while we update the snapshot.
      ConcurrentMap<String, CacheStatusInfo<T>> status;
      Snapshot previousSnapshot;
      writeLock.lock();
      try {
        previousSnapshot = snapshots.put(group, snapshot);
        status = statuses.get(group);
      } finally {
        writeLock.unlock();
      }

      if (status == null) {
        return;
      }

      // Preserve the resource type ordering within each group.
      respondWithSpecificOrder(group, previousSnapshot, snapshot, status);
    } finally {
      pushLock.unlock();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public StatusInfo statusInfo(T group) {
    readLock.lock();

    try {
      ConcurrentMap<String, CacheStatusInfo<T>> statusMap = statuses.get(group);
      if (statusMap == null || statusMap.isEmpty()) {
        return null;
      }

      return new GroupCacheStatusInfo<>(statusMap.values());
    } finally {
      readLock.unlock();
    }
  }

  @VisibleForTesting
  protected void respondWithSpecificOrder(T group,
                                          Snapshot previousSnapshot,
                                          Snapshot snapshot,
                                          ConcurrentMap<String, CacheStatusInfo<T>> statusMap) {
    for (String typeUrl : Resources.TYPE_URLS) {
      CacheStatusInfo<T> status = statusMap.get(typeUrl);
      if (status == null) {
        continue;
      }

      status.watchesRemoveIf((id, watch) -> {
        String version = snapshot.version(watch.request().getTypeUrl(), watch.request().getResourceNamesList());

        if (!watch.request().getVersionInfo().equals(version)) {
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("responding to open watch {}[{}] with new version {}",
                id,
                String.join(", ", watch.request().getResourceNamesList()),
                version);
          }

          respond(watch, snapshot, group);

          // Discard the watch. A new watch will be created for future snapshots once envoy ACKs the response.
          return true;
        }

        // Do not discard the watch. The request version is the same as the snapshot version, so we wait to respond.
        return false;
      });


      Map<String, SnapshotResource<?>> previousResources = previousSnapshot == null
          ? Collections.emptyMap()
          : previousSnapshot.resources(typeUrl);
      Map<String, SnapshotResource<?>> snapshotResources = snapshot.resources(typeUrl);

      Map<String, SnapshotResource<?>> snapshotChangedResources = snapshotResources.entrySet()
          .stream()
          .filter(entry -> {
            SnapshotResource<?> snapshotResource = previousResources.get(entry.getKey());
            return snapshotResource == null || !snapshotResource.version().equals(entry.getValue().version());
          })
          .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

      Set<String> snapshotRemovedResources = previousResources.keySet()
          .stream()
          .filter(s -> !snapshotResources.containsKey(s))
          .collect(Collectors.toSet());

      // The stream mutates the tracked resources under the same lock, and cancels the watch before doing so.
      status.deltaWatchesRemoveIf((id, watch) -> watch.locked(
          () -> respondDeltaWatch(id, watch, snapshot, snapshotChangedResources, snapshotRemovedResources, group)));
    }
  }

  /**
   * Evaluates an open delta watch against a new snapshot. Must run inside {@link DeltaWatch#locked}.
   *
   * @return whether the watch should be discarded
   */
  private boolean respondDeltaWatch(long id,
                                    DeltaWatch watch,
                                    Snapshot snapshot,
                                    Map<String, SnapshotResource<?>> snapshotChangedResources,
                                    Set<String> snapshotRemovedResources,
                                    T group) {
    String version = snapshot.version(watch.request().getTypeUrl());

    if (!watch.version().equals(version)) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("responding to open watch {} ({} tracked) with new version {}",
            id,
            watch.trackedResources().size(),
            version);
      }

      List<String> removedResources = snapshotRemovedResources.stream()
          .filter(s -> watch.trackedResources().containsKey(s))
          .toList();

      ResponseState responseState = respondDeltaTracked(watch,
          snapshotChangedResources,
          removedResources,
          version,
          group);
      // Discard the watch if it was responded or cancelled.
      // A new watch will be created for future snapshots once envoy ACKs the response.
      return ResponseState.RESPONDED.equals(responseState) || ResponseState.CANCELLED.equals(responseState);
    }

    // Do not discard the watch. The request version is the same as the snapshot version, so we wait to respond.
    return false;
  }

  private List<String> findRemovedResources(DeltaWatch watch, Map<String, SnapshotResource<?>> snapshotResources) {
    // remove resources for which client has a tracked version or is waiting a response
    return Stream.concat(watch.trackedResources().keySet().stream(), watch.pendingResources().stream())
        .filter(s -> !snapshotResources.containsKey(s))
        .distinct()
        .toList();
  }

  private Response createResponse(DiscoveryRequest request,
                                  Map<String, SnapshotResource<?>> resources,
                                  String version) {
    Collection<? extends Message> filtered = request.getResourceNamesList().isEmpty()
        ? resources.values()
        .stream()
        .map(SnapshotResource::resource)
        .toList()
        : request.getResourceNamesList().stream()
        .map(resources::get)
        .filter(Objects::nonNull)
        .map(SnapshotResource::resource)
        .toList();

    return Response.create(request, filtered, version);
  }

  private boolean respond(Watch watch, Snapshot snapshot, T group) {
    Map<String, SnapshotResource<?>> snapshotResources = snapshot.resources(watch.request().getTypeUrl());

    if (!watch.request().getResourceNamesList().isEmpty() && watch.ads()) {
      Collection<String> missingNames = watch.request().getResourceNamesList().stream()
          .filter(name -> !snapshotResources.containsKey(name))
          .toList();

      if (!missingNames.isEmpty()) {
        LOGGER.info(
            "not responding in ADS mode for {} from node {} at version {} for request [{}] since [{}] not in snapshot",
            watch.request().getTypeUrl(),
            group,
            snapshot.version(watch.request().getTypeUrl(), watch.request().getResourceNamesList()),
            String.join(", ", watch.request().getResourceNamesList()),
            String.join(", ", missingNames));

        return false;
      }
    }

    String version = snapshot.version(watch.request().getTypeUrl(), watch.request().getResourceNamesList());

    LOGGER.debug("responding for {} from node {} at version {} with version {}",
        watch.request().getTypeUrl(),
        group,
        watch.request().getVersionInfo(),
        version);

    Response response = createResponse(
        watch.request(),
        snapshotResources,
        version);

    try {
      watch.respond(response);
      return true;
    } catch (WatchCancelledException e) {
      LOGGER.error(
          "failed to respond for {} from node {} at version {} with version {} because watch was already cancelled",
          watch.request().getTypeUrl(),
          group,
          watch.request().getVersionInfo(),
          version);
    }

    return false;
  }

  /**
   * Responds a delta watch using resource version comparison.
   *
   * @return if the watch has been responded.
   */
  private ResponseState respondDeltaTracked(DeltaWatch watch,
                                            Map<String, SnapshotResource<?>> snapshotResources,
                                            String version,
                                            T group) {
    return respondDeltaTracked(watch, snapshotResources, findRemovedResources(watch, snapshotResources), version, group);
  }

  private ResponseState respondDeltaTracked(DeltaWatch watch,
                                            Map<String, SnapshotResource<?>> snapshotResources,
                                            List<String> removedResources,
                                            String version,
                                            T group) {

    Map<String, SnapshotResource<?>> resources = snapshotResources.entrySet()
        .stream()
        .filter(entry -> {
          if (watch.pendingResources().contains(entry.getKey())) {
            return true;
          }
          String resourceVersion = watch.trackedResources().get(entry.getKey());
          if (resourceVersion == null) {
            // resource is not tracked, should respond it only if watch is wildcard
            return watch.isWildcard();
          }
          return !entry.getValue().version().equals(resourceVersion);
        })
        .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

    return respondDelta(watch, resources, removedResources, version, group);
  }

  private ResponseState respondDelta(DeltaWatch watch,
                                     Map<String, SnapshotResource<?>> resources,
                                     List<String> removedResources,
                                     String version,
                                     T group) {
    if (resources.isEmpty() && removedResources.isEmpty() && !isFirstWildcardRequest(watch)) {
      return ResponseState.UNRESPONDED;
    }

    DeltaResponse response = DeltaResponse.create(
        watch.request(),
        resources,
        removedResources,
        version);


    try {
      watch.respond(response);
      return ResponseState.RESPONDED;
    } catch (WatchCancelledException e) {
      LOGGER.error(
          "failed to respond for {} from node {} with version {} because watch was already cancelled",
          watch.request().getTypeUrl(),
          group,
          version);
    }

    return ResponseState.CANCELLED;
  }

  /**
   * The first wildcard request of a stream is answered even when there is nothing to send. Envoy only completes
   * the initialization of a wildcard type once a response arrives, so a snapshot with no resources of that type
   * would otherwise leave it waiting for its initial fetch timeout, and on a reconnect with up-to-date versions the
   * empty response confirms the stream is in sync. go-control-plane does the same. Later requests on the stream
   * carry the version of the previous response and stay silent when nothing changed.
   */
  private static boolean isFirstWildcardRequest(DeltaWatch watch) {
    return watch.isWildcard() && watch.version().isEmpty();
  }

  private enum ResponseState {
    RESPONDED,
    UNRESPONDED,
    CANCELLED
  }
}
