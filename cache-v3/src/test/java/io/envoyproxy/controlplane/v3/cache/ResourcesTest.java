package io.envoyproxy.controlplane.v3.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Any;
import com.google.protobuf.Message;
import com.google.type.Color;
import io.envoyproxy.envoy.config.cluster.v3.Cluster;
import io.envoyproxy.envoy.config.cluster.v3.Cluster.DiscoveryType;
import io.envoyproxy.envoy.config.cluster.v3.Cluster.EdsClusterConfig;
import io.envoyproxy.envoy.config.endpoint.v3.ClusterLoadAssignment;
import io.envoyproxy.envoy.config.listener.v3.Listener;
import io.envoyproxy.envoy.config.route.v3.RouteConfiguration;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.Test;

public class ResourcesTest {

  private static final boolean ADS = ThreadLocalRandom.current().nextBoolean();
  private static final String CLUSTER_NAME = "cluster0";
  private static final String LISTENER_NAME = "listener0";
  private static final String ROUTE_NAME = "route0";

  private static final int ENDPOINT_PORT = ThreadLocalRandom.current().nextInt(10000, 20000);
  private static final int LISTENER_PORT = ThreadLocalRandom.current().nextInt(20000, 30000);

  private static final SnapshotResource<Cluster> CLUSTER = SnapshotResource.create(
      TestResources.createCluster(CLUSTER_NAME),
      "1");
  private static final SnapshotResource<ClusterLoadAssignment> ENDPOINT = SnapshotResource.create(
      TestResources.createEndpoint(CLUSTER_NAME, ENDPOINT_PORT),
      "1");
  private static final SnapshotResource<Listener> LISTENER = SnapshotResource.create(
      TestResources.createListener(ADS, LISTENER_NAME, LISTENER_PORT, ROUTE_NAME),
      "1");
  private static final SnapshotResource<RouteConfiguration> ROUTE = SnapshotResource.create(
      TestResources.createRoute(ROUTE_NAME, CLUSTER_NAME),
      "1");

  @Test
  public void getResourceNameReturnsExpectedNameForValidResourceMessage() {
    ImmutableMap<SnapshotResource<? extends Message>, String> cases = ImmutableMap.of(
        CLUSTER, CLUSTER_NAME,
        ENDPOINT, CLUSTER_NAME,
        LISTENER, LISTENER_NAME,
        ROUTE, ROUTE_NAME);

    cases.forEach((resource, expectedName) ->
        assertThat(Resources.getResourceName(resource.resource())).isEqualTo(expectedName));
  }

  @Test
  public void getResourceNameReturnsEmptyStringForNonResourceMessage() {
    Message message = Color.newBuilder().build();

    assertThat(Resources.getResourceName(message)).isEmpty();
  }

  @Test
  public void getResourceNameAnyThrowsOnBadClass() {
    assertThatThrownBy(() -> Resources.getResourceName(Any.newBuilder().setTypeUrl("garbage").build()))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("cannot unpack");
  }

  @Test
  public void getResourceReferencesReturnsExpectedReferencesForValidResourceMessages() {
    String clusterServiceName = "clusterWithServiceName0";
    Cluster clusterWithServiceName = Cluster.newBuilder()
        .setName(CLUSTER_NAME)
        .setEdsClusterConfig(
            EdsClusterConfig.newBuilder()
                .setServiceName(clusterServiceName))
        .setType(DiscoveryType.EDS)
        .build();

    Map<Collection<SnapshotResource<Message>>, Set<String>> cases =
        ImmutableMap.<Collection<SnapshotResource<Message>>, Set<String>>builder()
            .put((Collection) ImmutableList.of(CLUSTER), ImmutableSet.of(CLUSTER_NAME))
            .put((Collection) ImmutableList.of(SnapshotResource.create(clusterWithServiceName, "1")),
                ImmutableSet.of(clusterServiceName))
            .put((Collection) ImmutableList.of(ENDPOINT), ImmutableSet.of())
            .put((Collection) ImmutableList.of(LISTENER), ImmutableSet.of(ROUTE_NAME))
            .put((Collection) ImmutableList.of(ROUTE), ImmutableSet.of())
            .put((Collection) ImmutableList.of(CLUSTER, ENDPOINT, LISTENER, ROUTE),
                ImmutableSet.of(CLUSTER_NAME, ROUTE_NAME))
            .build();

    cases.forEach((resources, refs) ->
        assertThat(Resources.getResourceReferences(resources)).containsExactlyElementsOf(refs));
  }
}
