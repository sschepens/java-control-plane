package io.envoyproxy.controlplane.v3.server;

import io.envoyproxy.controlplane.v3.cache.DeltaResponse;
import io.envoyproxy.controlplane.v3.cache.DeltaWatch;
import io.envoyproxy.controlplane.v3.cache.TrackedResources;
import io.envoyproxy.controlplane.v3.server.exception.RequestException;
import io.envoyproxy.envoy.config.core.v3.Node;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryResponse;
import io.envoyproxy.envoy.service.discovery.v3.Resource;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code DiscoveryRequestStreamObserver} provides the base implementation for XDS stream handling.
 */
public abstract class DeltaDiscoveryRequestStreamObserver implements StreamObserver<DeltaDiscoveryRequest> {
  private static final AtomicLongFieldUpdater<DeltaDiscoveryRequestStreamObserver> streamNonceUpdater =
      AtomicLongFieldUpdater.newUpdater(DeltaDiscoveryRequestStreamObserver.class, "streamNonce");
  private static final Logger LOGGER = LoggerFactory.getLogger(DiscoveryServer.class);

  final long streamId;
  private final String defaultTypeUrl;
  private final StreamObserver<DeltaDiscoveryResponse> responseObserver;
  private final ScheduledExecutorService executor;
  private final DiscoveryServer discoverySever;
  volatile boolean hasClusterChanged;
  private volatile long streamNonce;
  private volatile boolean isClosing;
  private Node node;

  DeltaDiscoveryRequestStreamObserver(String defaultTypeUrl,
                                      StreamObserver<DeltaDiscoveryResponse> responseObserver,
                                      long streamId,
                                      ScheduledExecutorService executor,
                                      DiscoveryServer discoveryServer) {
    this.defaultTypeUrl = defaultTypeUrl;
    this.responseObserver = responseObserver;
    this.streamId = streamId;
    this.executor = executor;
    this.streamNonce = 0;
    this.discoverySever = discoveryServer;
    this.hasClusterChanged = false;
  }

  @Override
  public void onNext(DeltaDiscoveryRequest request) {
    String requestTypeUrl = request.getTypeUrl().isEmpty() ? defaultTypeUrl : request.getTypeUrl();
    if (node == null && request.hasNode()) {
      node = request.getNode();
    }

    final DeltaDiscoveryRequest completeRequest;
    if (!request.hasNode() || request.getTypeUrl().isEmpty()) {
      completeRequest = request.toBuilder()
          .setTypeUrl(requestTypeUrl)
          .setNode(node)
          .build();
    } else {
      completeRequest = request;
    }

    String nonce = completeRequest.getResponseNonce();

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("[{}] request {}[{}] with nonce {} from versions {}",
          streamId,
          requestTypeUrl,
          String.join(", ", completeRequest.getResourceNamesSubscribeList()),
          nonce,
          completeRequest.getInitialResourceVersionsMap());
    }

    try {
      discoverySever.callbacks.forEach(cb -> cb.onStreamDeltaRequest(streamId, completeRequest));
    } catch (RequestException e) {
      closeWithError(e);
      return;
    }

    final String version;
    if (latestVersion(requestTypeUrl) == null) {
      version = "";
    } else {
      version = latestVersion(requestTypeUrl);
    }

    final TrackedResources tracked = trackedResources(requestTypeUrl);
    // The snapshot thread reads this state while it evaluates the open watch, under the same lock. Cancel that
    // watch before touching the state so an evaluation racing with us is discarded instead of responding from a
    // half-updated view, and keep the lock until the replacement watch exists.
    tracked.locked(() -> {
      cancelWatch(requestTypeUrl);

      if (!completeRequest.getResponseNonce().isEmpty()) {
        // envoy is replying to a response we sent, get and clear respective response
        LatestDeltaDiscoveryResponse response = clearResponse(requestTypeUrl, completeRequest.getResponseNonce());
        if (response == null) {
          // Not a response we are waiting for: a duplicate ack, or an ack for a response that was already cleared.
          // Ignore the ack but still apply the subscription changes carried by the request. Throwing here would
          // make grpc close the stream with UNKNOWN.
          LOGGER.warn("[{}] ignoring {} request with unknown nonce {}",
              streamId, requestTypeUrl, completeRequest.getResponseNonce());
        } else if (completeRequest.hasErrorDetail()) {
          // The versions of a rejected response stay recorded as returned, so its resources are not sent again
          // until they change (as go-control-plane does). Resending the same versions would only be rejected again.
          LOGGER.warn("[{}] {} response {} rejected: {}", streamId, requestTypeUrl, response.nonce(),
              completeRequest.getErrorDetail().getMessage());
        }
      }

      // Apply subscription changes after the ACK so explicitly re-requested resources remain pending,
      // and resources unsubscribed in this request are not restored by the ACK.
      updateSubscriptions(tracked,
          request.getResourceNamesSubscribeList(),
          request.getResourceNamesUnsubscribeList());

      // On the first request of a stream envoy lists every resource it is interested in under
      // resource_names_subscribe and, for the ones it already holds, their versions under
      // initial_resource_versions. Apply the versions after the subscriptions so a name with a known
      // version becomes tracked instead of pending: it is then only sent again if the version differs
      // (or reported in removed_resources if it no longer exists), instead of being resent in full.
      // Names subscribed without a version stay pending and are sent as soon as they are available.
      if (request.getInitialResourceVersionsCount() > 0) {
        updateTrackedResources(tracked, request.getInitialResourceVersionsMap());
      }

      if (responseCount(requestTypeUrl) == 0) {
        // we should only create watches when there's no pending ack
        // this tries to ensure we don't have two outstanding responses
        setWatch(requestTypeUrl, discoverySever.configWatcher.createDeltaWatch(
            completeRequest,
            version,
            tracked,
            isWildcard(requestTypeUrl),
            r -> {
              // Runs on the thread that produced the response, inside the lock, before anything else can create
              // a watch for this type.
              recordReturned(tracked, r);
              executor.execute(() -> send(r, requestTypeUrl));
            },
            hasClusterChanged
        ));
      }
    });
  }

  /**
   * Records the resources of a response as held by the client, as soon as the response is produced. The client is
   * expected to accept it; if it rejects it the versions stay recorded, so the same versions are not sent again until
   * they change.
   */
  private static void recordReturned(TrackedResources tracked, DeltaResponse response) {
    response.resources().forEach((name, resource) -> {
      tracked.versions().put(name, resource.version());
      tracked.pending().remove(name);
    });
    response.removedResources().forEach(name -> {
      tracked.versions().remove(name);
      tracked.pending().remove(name);
    });
  }

  private static void updateTrackedResources(TrackedResources tracked, Map<String, String> resourcesVersions) {
    resourcesVersions.forEach((k, v) -> {
      tracked.versions().put(k, v);
      tracked.pending().remove(k);
    });
  }

  private static void updateSubscriptions(TrackedResources tracked,
                                          List<String> resourceNamesSubscribe,
                                          List<String> resourceNamesUnsubscribe) {
    // unsubscribe first
    resourceNamesUnsubscribe.forEach(s -> {
      tracked.versions().remove(s);
      tracked.pending().remove(s);
    });
    tracked.pending().addAll(resourceNamesSubscribe);
  }

  /**
   * Resource versions the client holds for the given type. Only for tests: production callers hold the monitor of
   * {@link #trackedResources(String)}.
   */
  Map<String, String> resourceVersions(String typeUrl) {
    return trackedResources(typeUrl).versions();
  }

  /**
   * Resource names the client is waiting for, for the given type. Only for tests, see {@link #resourceVersions}.
   */
  Set<String> pendingResources(String typeUrl) {
    return trackedResources(typeUrl).pending();
  }

  @Override
  public void onError(Throwable t) {
    if (!Status.fromThrowable(t).getCode().equals(Status.CANCELLED.getCode())) {
      LOGGER.error("[{}] stream closed with error", streamId, t);
    }

    try {
      discoverySever.callbacks.forEach(cb -> cb.onStreamCloseWithError(streamId, defaultTypeUrl, t));
      closeWithError(Status.fromThrowable(t).asException());
    } finally {
      cancel();
    }
  }

  @Override
  public void onCompleted() {
    LOGGER.debug("[{}] stream closed", streamId);

    try {
      discoverySever.callbacks.forEach(cb -> cb.onStreamClose(streamId, defaultTypeUrl));
      synchronized (responseObserver) {
        if (!isClosing) {
          isClosing = true;
          responseObserver.onCompleted();
        }
      }
    } finally {
      cancel();
    }
  }

  void onCancelled() {
    LOGGER.info("[{}] stream cancelled", streamId);
    cancel();
  }

  void closeWithError(Throwable exception) {
    synchronized (responseObserver) {
      if (!isClosing) {
        isClosing = true;
        responseObserver.onError(exception);
      }
    }
    cancel();
  }

  private void send(DeltaResponse response, String typeUrl) {
    try {
      doSend(response, typeUrl);
    } catch (Throwable t) {
      if (t instanceof StatusRuntimeException
          && Status.CANCELLED.getCode().equals(((StatusRuntimeException) t).getStatus().getCode())) {
        // the client went away, the stream is already being torn down
        return;
      }
      failStream(typeUrl, t);
      if (t instanceof Error) {
        throw (Error) t;
      }
    }
  }

  /**
   * Closes the stream after a response could not be built or written. Without this the stream would stay open for
   * that type with no watch, or with a response that is never acked, and the client would silently stop receiving
   * updates until the connection is recycled. Closing it makes the client reconnect and resync right away.
   */
  private void failStream(String typeUrl, Throwable cause) {
    LOGGER.error("[{}] failed to send {} response, closing stream", streamId, typeUrl, cause);
    try {
      discoverySever.callbacks.forEach(cb -> cb.onStreamCloseWithError(streamId, defaultTypeUrl, cause));
    } catch (RuntimeException e) {
      LOGGER.error("[{}] stream close callback failed", streamId, e);
    }
    try {
      closeWithError(Status.INTERNAL
          .withDescription("failed to send " + typeUrl + " response: " + cause.getClass().getName())
          .withCause(cause)
          .asException());
    } catch (RuntimeException e) {
      LOGGER.error("[{}] failed to close stream", streamId, e);
      cancel();
    }
  }

  private void doSend(DeltaResponse response, String typeUrl) {
    String nonce = Long.toString(streamNonceUpdater.getAndIncrement(this));

    DeltaDiscoveryResponse discoveryResponse = DeltaDiscoveryResponse.newBuilder()
        .setSystemVersionInfo(response.version())
        .addAllResources(response.resources()
            .entrySet()
            .stream()
            .map(entry -> Resource.newBuilder()
                .setName(entry.getKey())
                .setResource(discoverySever.protoResourcesSerializer.serialize(entry.getValue().resource()))
                .setVersion(entry.getValue().version())
                .build())
            .collect(Collectors.toList()))
        .addAllRemovedResources(response.removedResources())
        .setTypeUrl(typeUrl)
        .setNonce(nonce)
        .build();

    LOGGER.debug("[{}] response {} with nonce {} version {}", streamId, typeUrl, nonce, response.version());

    discoverySever.callbacks.forEach(cb ->
        cb.onStreamDeltaResponse(streamId, response.request(), discoveryResponse));

    // Store the latest response *before* we send the response. This ensures that by the time the request
    // is processed the map is guaranteed to be updated. Doing it afterwards leads to a race conditions
    // which may see the incoming request arrive before the map is updated, failing the nonce check erroneously.
    setResponse(typeUrl, nonce, LatestDeltaDiscoveryResponse.create(nonce, response.version()));
    setLatestVersion(typeUrl, response.version());
    synchronized (responseObserver) {
      if (!isClosing) {
        responseObserver.onNext(discoveryResponse);
      }
    }
  }

  abstract void cancel();

  abstract boolean ads();

  abstract void setLatestVersion(String typeUrl, String version);

  abstract String latestVersion(String typeUrl);

  abstract void setResponse(String typeUrl, String nonce, LatestDeltaDiscoveryResponse response);

  abstract LatestDeltaDiscoveryResponse clearResponse(String typeUrl, String nonce);

  abstract int responseCount(String typeUrl);

  abstract boolean isWildcard(String typeUrl);

  /**
   * The tracked resources of the given type. Mutated only by this stream and only inside their lock.
   */
  abstract TrackedResources trackedResources(String typeUrl);

  /**
   * Cancels the current watch of the given type, if any.
   */
  abstract void cancelWatch(String typeUrl);

  /**
   * Stores the current watch of the given type.
   */
  abstract void setWatch(String typeUrl, DeltaWatch watch);
}
