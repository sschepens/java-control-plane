package io.envoyproxy.controlplane.v3.cache;

import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryRequest;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * {@code Watch} is a dedicated stream of configuration resources produced by the configuration cache and consumed by
 * the xDS server.
 */
public class DeltaWatch {
  private static final AtomicIntegerFieldUpdater<DeltaWatch> isCancelledUpdater =
      AtomicIntegerFieldUpdater.newUpdater(DeltaWatch.class, "isCancelled");
  private final DeltaDiscoveryRequest request;
  private final Consumer<DeltaResponse> responseConsumer;
  private final Map<String, String> resourceVersions;
  private final Set<String> pendingResources;
  private final TrackedResources trackedResources;
  private final boolean isWildcard;
  private final String version;
  private volatile int isCancelled = 0;
  private Runnable stop;

  /**
   * Construct a watch.
   * @param request          the original request for the watch
   * @param trackedResources the resources tracked by the stream; read through {@link #trackedResources()} and
   *                         {@link #pendingResources()} inside {@link #locked(Supplier)}
   * @param version          indicates the stream current version
   * @param isWildcard       indicates if the stream is in wildcard mode
   * @param responseConsumer handler for outgoing response messages
   */
  public DeltaWatch(DeltaDiscoveryRequest request,
                    TrackedResources trackedResources,
                    String version,
                    boolean isWildcard,
                    Consumer<DeltaResponse> responseConsumer) {
    this.request = request;
    this.resourceVersions = trackedResources.versions();
    this.pendingResources = trackedResources.pending();
    this.trackedResources = trackedResources;
    this.version = version;
    this.isWildcard = isWildcard;
    this.responseConsumer = responseConsumer;
  }

  /**
   * Cancel the watch. A watch must be cancelled in order to complete its resource stream and free resources. Cancel
   * may be called multiple times, with each subsequent call being a no-op.
   */
  public void cancel() {
    if (isCancelledUpdater.compareAndSet(this, 0, 1)) {
      if (stop != null) {
        stop.run();
      }
    }
  }

  /**
   * Returns boolean indicating whether or not the watch has been cancelled.
   */
  public boolean isCancelled() {
    return isCancelledUpdater.get(this) == 1;
  }

  /**
   * Returns the original request for the watch.
   */
  public DeltaDiscoveryRequest request() {
    return request;
  }

  /**
   * Returns the tracked resources for the watch.
   */
  public Map<String, String> trackedResources() {
    return resourceVersions;
  }

  /**
   * Returns the pending resources for the watch.
   */
  public Set<String> pendingResources() {
    return pendingResources;
  }

  /**
   * Runs the given action while holding the lock of the tracked resources, which is shared with the stream that
   * mutates them. {@link #trackedResources()} and {@link #pendingResources()} must only be read inside it.
   */
  public <R> R locked(Supplier<R> action) {
    return trackedResources.locked(action);
  }

  /**
   * Returns the stream current version.
   */
  public String version() {
    return version;
  }

  /**
   * Indicates if the stream is in wildcard mode.
   */
  public boolean isWildcard() {
    return isWildcard;
  }

  /**
   * Sends the given response to the watch's response handler.
   *
   * @param response the response to be handled
   * @throws WatchCancelledException if the watch has already been cancelled
   */
  public void respond(DeltaResponse response) throws WatchCancelledException {
    if (isCancelled()) {
      throw new WatchCancelledException();
    }

    responseConsumer.accept(response);
  }

  /**
   * Sets the callback method to be executed when the watch is cancelled. Even if cancel is executed multiple times, it
   * ensures that this stop callback is only executed once.
   */
  public void setStop(Runnable stop) {
    this.stop = stop;
  }
}
