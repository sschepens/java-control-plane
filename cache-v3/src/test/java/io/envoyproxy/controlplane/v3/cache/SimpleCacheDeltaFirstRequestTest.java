package io.envoyproxy.controlplane.v3.cache;

import static org.assertj.core.api.Assertions.assertThat;

import io.envoyproxy.envoy.config.cluster.v3.Cluster;
import io.envoyproxy.envoy.config.core.v3.Node;
import io.envoyproxy.envoy.config.endpoint.v3.ClusterLoadAssignment;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryRequest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.junit.Test;

/**
 * The first wildcard request of a stream is answered even when there is nothing to send, so Envoy completes the
 * initialization of the type; everything else stays silent until something changes.
 */
public class SimpleCacheDeltaFirstRequestTest {
  private static final String GROUP = "group";
  private static final Node NODE = Node.newBuilder().setId("envoy").build();
  private static final SnapshotResource<Cluster> CLUSTER =
      SnapshotResource.create(Cluster.newBuilder().setName("cluster0").build(), "v1");
  private static final SnapshotResource<ClusterLoadAssignment> ENDPOINT =
      SnapshotResource.create(ClusterLoadAssignment.newBuilder().setClusterName("cluster0").build(), "v1");

  private final SimpleCache<String> cache = new SimpleCache<>(node -> GROUP);
  private final List<DeltaResponse> responses = new ArrayList<>();

  @Test
  public void firstWildcardRequestIsAnsweredEvenWhenTheSnapshotHasNothingForIt() {
    cache.setSnapshot(GROUP, snapshot(List.of(), List.of(), "1"));

    cache.createDeltaWatch(request(Resources.CLUSTER_TYPE_URL, Map.of()), "", new TrackedResources(), true,
        responses::add, false);

    assertThat(responses).hasSize(1);
    assertThat(responses.get(0).resources()).isEmpty();
    assertThat(responses.get(0).removedResources()).isEmpty();
    assertThat(responses.get(0).version()).isEqualTo("1");
    assertThat(cache.statusInfo(GROUP).numDeltaWatches()).isZero();
  }

  @Test
  public void reconnectWithUpToDateVersionsGetsAnEmptyResponseForAWildcardType() {
    cache.setSnapshot(GROUP, snapshot(List.of(CLUSTER), List.of(ENDPOINT), "1"));
    TrackedResources tracked = new TrackedResources();
    tracked.track("cluster0", "v1");

    cache.createDeltaWatch(request(Resources.CLUSTER_TYPE_URL, Map.of("cluster0", "v1")), "", tracked, true,
        responses::add, false);

    assertThat(responses).hasSize(1);
    assertThat(responses.get(0).resources()).isEmpty();
    assertThat(responses.get(0).removedResources()).isEmpty();
    assertThat(responses.get(0).version()).isEqualTo("1");
  }

  @Test
  public void reconnectWithUpToDateVersionsStaysSilentForASubscribedType() {
    cache.setSnapshot(GROUP, snapshot(List.of(CLUSTER), List.of(ENDPOINT), "1"));
    TrackedResources tracked = new TrackedResources();
    tracked.track("cluster0", "v1");

    cache.createDeltaWatch(request(Resources.ENDPOINT_TYPE_URL, Map.of("cluster0", "v1")), "", tracked, false,
        responses::add, false);

    assertThat(responses).isEmpty();
    assertThat(cache.statusInfo(GROUP).numDeltaWatches()).isEqualTo(1);
  }

  @Test
  public void laterRequestsOnTheStreamStaySilentWhenNothingChanged() {
    cache.setSnapshot(GROUP, snapshot(List.of(CLUSTER), List.of(ENDPOINT), "1"));
    TrackedResources tracked = new TrackedResources();
    tracked.track("cluster0", "v1");

    cache.createDeltaWatch(request(Resources.CLUSTER_TYPE_URL, Map.of()), "1", tracked, true, responses::add, false);

    assertThat(responses).isEmpty();
    assertThat(cache.statusInfo(GROUP).numDeltaWatches()).isEqualTo(1);
  }

  @Test
  public void firstWildcardWatchOpenedBeforeAnySnapshotIsAnsweredByAnEmptySnapshot() {
    cache.createDeltaWatch(request(Resources.CLUSTER_TYPE_URL, Map.of()), "", new TrackedResources(), true,
        responses::add, false);
    assertThat(responses).isEmpty();

    cache.setSnapshot(GROUP, snapshot(List.of(), List.of(), "1"));

    assertThat(responses).hasSize(1);
    assertThat(responses.get(0).resources()).isEmpty();
    assertThat(responses.get(0).version()).isEqualTo("1");
  }

  private static DeltaDiscoveryRequest request(String typeUrl, Map<String, String> initialResourceVersions) {
    return DeltaDiscoveryRequest.newBuilder()
        .setNode(NODE)
        .setTypeUrl(typeUrl)
        .putAllInitialResourceVersions(initialResourceVersions)
        .build();
  }

  private static Snapshot snapshot(Collection<SnapshotResource<Cluster>> clusters,
                                   Collection<SnapshotResource<ClusterLoadAssignment>> endpoints,
                                   String version) {
    return Snapshot.create(clusters, endpoints, List.of(), List.of(), List.of(), List.of(), version);
  }
}
