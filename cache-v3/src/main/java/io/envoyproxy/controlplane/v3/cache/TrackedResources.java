package io.envoyproxy.controlplane.v3.cache;

import java.util.Collection;
import java.util.Collections;
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
 * same thread. Every call must happen inside {@link #locked(Runnable)} or {@link #locked(Supplier)}: the server uses
 * them while it processes a request (cancelling the current watch before mutating the state that watch reads) and the
 * cache uses them in {@link ConfigWatcher#createDeltaWatch} and while evaluating an open watch against a new snapshot,
 * which is also where the response consumer runs. The lock is reentrant.
 */
public final class TrackedResources {
  private final Map<String, String> versions = new HashMap<>();
  private final Set<String> pending = new HashSet<>();
  private final Map<String, String> versionsView = Collections.unmodifiableMap(versions);
  private final Set<String> pendingView = Collections.unmodifiableSet(pending);

  /**
   * Returns a read-only view of the versions the client holds, keyed by resource name.
   */
  public Map<String, String> versions() {
    return versionsView;
  }

  /**
   * Returns a read-only view of the names the client has subscribed to and is still waiting for.
   */
  public Set<String> pending() {
    return pendingView;
  }

  /**
   * The client is interested in the given names; the ones it does not hold yet become pending.
   */
  public void subscribe(Collection<String> names) {
    pending.addAll(names);
  }

  /**
   * The client is no longer interested in the given names: forget their versions and stop waiting for them.
   */
  public void unsubscribe(Collection<String> names) {
    for (String name : names) {
      versions.remove(name);
      pending.remove(name);
    }
  }

  /**
   * The client holds (or has just been sent) the given version of a resource, so it is no longer pending.
   */
  public void track(String name, String version) {
    versions.put(name, version);
    pending.remove(name);
  }

  /**
   * The resource no longer exists for the client: forget its version and stop waiting for it.
   */
  public void untrack(String name) {
    versions.remove(name);
    pending.remove(name);
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
