package io.envoyproxy.controlplane.v3.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import io.envoyproxy.envoy.config.core.v3.Node;
import io.envoyproxy.envoy.config.route.v3.RouteConfiguration;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryRequest;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.Test;

/**
 * The cache evaluates an open delta watch under the lock of the stream's {@link TrackedResources}, so a stream
 * mutating that state never races with the evaluation.
 */
public class SimpleCacheDeltaWatchLockTest {
  private static final String GROUP = "group";
  private static final String ROUTE_NAME = "route0";

  @Test
  public void snapshotEvaluationWaitsForTheStreamMonitorAndSeesTheUpdatedState() throws Exception {
    SimpleCache<String> cache = new SimpleCache<>(node -> GROUP);
    cache.setSnapshot(GROUP, snapshot("v1", "snapshot-v1"));

    TrackedResources tracked = new TrackedResources();
    tracked.track(ROUTE_NAME, "v1");
    List<DeltaResponse> responses = new CopyOnWriteArrayList<>();
    cache.createDeltaWatch(request(), "", tracked, false, responses::add, false);

    assertThat(responses).as("client is up to date, the watch stays open").isEmpty();

    Thread setter = new Thread(() -> cache.setSnapshot(GROUP, snapshot("v2", "snapshot-v2")), "snapshot-setter");
    tracked.locked(() -> {
      setter.start();
      awaitState(setter, Thread.State.BLOCKED);
      assertThat(responses).as("evaluation must wait for the lock").isEmpty();

      // The stream learns, while holding the lock, that the client already has v2.
      tracked.track(ROUTE_NAME, "v2");
    });
    setter.join(5_000);

    assertThat(setter.isAlive()).isFalse();
    assertThat(responses).as("evaluation saw the state after the mutation, so nothing to send").isEmpty();
  }

  @Test
  public void snapshotEvaluationResumesOnceTheMonitorIsReleased() throws Exception {
    SimpleCache<String> cache = new SimpleCache<>(node -> GROUP);
    cache.setSnapshot(GROUP, snapshot("v1", "snapshot-v1"));

    TrackedResources tracked = new TrackedResources();
    tracked.track(ROUTE_NAME, "v1");
    List<DeltaResponse> responses = new CopyOnWriteArrayList<>();
    cache.createDeltaWatch(request(), "", tracked, false, responses::add, false);

    Thread setter = new Thread(() -> cache.setSnapshot(GROUP, snapshot("v2", "snapshot-v2")), "snapshot-setter");
    tracked.locked(() -> {
      setter.start();
      awaitState(setter, Thread.State.BLOCKED);
      assertThat(responses).isEmpty();
    });
    setter.join(5_000);

    assertThat(setter.isAlive()).isFalse();
    assertThat(responses).hasSize(1);
    assertThat(responses.get(0).resources()).containsOnlyKeys(ROUTE_NAME);
    assertThat(responses.get(0).resources().get(ROUTE_NAME).version()).isEqualTo("v2");
  }

  private static void awaitState(Thread thread, Thread.State state) {
    long deadline = System.nanoTime() + 5_000_000_000L;
    while (thread.getState() != state) {
      if (System.nanoTime() > deadline) {
        fail("thread %s did not reach %s, state is %s", thread.getName(), state, thread.getState());
      }
      try {
        Thread.sleep(5);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        fail("interrupted while waiting for %s", thread.getName());
      }
    }
  }

  private static DeltaDiscoveryRequest request() {
    return DeltaDiscoveryRequest.newBuilder()
        .setNode(Node.getDefaultInstance())
        .setTypeUrl(Resources.ROUTE_TYPE_URL)
        .build();
  }

  private static Snapshot snapshot(String resourceVersion, String snapshotVersion) {
    return Snapshot.create(
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.singletonList(SnapshotResource.create(
            RouteConfiguration.newBuilder().setName(ROUTE_NAME).build(), resourceVersion)),
        Collections.emptyList(),
        Collections.emptyList(),
        snapshotVersion);
  }
}
