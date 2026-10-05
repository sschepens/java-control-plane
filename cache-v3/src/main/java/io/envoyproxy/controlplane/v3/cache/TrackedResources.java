package io.envoyproxy.controlplane.v3.cache;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Resource bookkeeping for one resource type on one delta stream: the versions the client is known to hold and the
 * names it has subscribed to but has not received yet.
 *
 * <p>The stream that owns an instance mutates it when it processes a request and, through its response consumer,
 * when a watch produces a response, which may happen on the thread that sets snapshots; the cache reads it from that
 * same thread. Every access to the collections must happen inside {@link #locked(Runnable)} or
 * {@link #locked(Supplier)}: the server uses them while it processes a request (cancelling the current watch before
 * mutating the state that watch reads) and the cache uses them in {@link ConfigWatcher#createDeltaWatch} and while
 * evaluating an open watch against a new snapshot, which is also where the response consumer runs. The lock is
 * reentrant.
 */
public final class TrackedResources {
  private final Map<String, String> versions = new HashMap<>();
  private final Set<String> pending = new HashSet<>();

  /**
   * Returns the versions of the resources the client holds, keyed by resource name. Only use it inside
   * {@link #locked}.
   */
  public Map<String, String> versions() {
    return versions;
  }

  /**
   * Returns the names the client has subscribed to and is still waiting for. Only use it inside {@link #locked}.
   */
  public Set<String> pending() {
    return pending;
  }

  /**
   * Runs the given action while holding this object's lock.
   */
  public synchronized void locked(Runnable action) {
    action.run();
  }

  /**
   * Runs the given action while holding this object's lock and returns its result.
   */
  public synchronized <R> R locked(Supplier<R> action) {
    return action.get();
  }
}
