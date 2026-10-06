package io.envoyproxy.controlplane.v3.server;

import io.envoyproxy.controlplane.v3.cache.Snapshot;
import io.envoyproxy.controlplane.v3.cache.SnapshotResource;
import io.envoyproxy.controlplane.v3.cache.TestResources;
import io.envoyproxy.envoy.config.cluster.v3.Cluster;
import io.envoyproxy.envoy.config.endpoint.v3.ClusterLoadAssignment;
import io.envoyproxy.envoy.config.listener.v3.Listener;
import io.envoyproxy.envoy.config.route.v3.RouteConfiguration;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

class TestSnapshots {

  static Snapshot createSnapshot(
      boolean ads,
      String clusterName,
      String endpointAddress,
      int endpointPort,
      String listenerName,
      int listenerPort,
      String routeName,
      String version) {

    Cluster cluster = TestResources.createCluster(clusterName);
    ClusterLoadAssignment endpoint = TestResources.createEndpoint(clusterName, endpointAddress, endpointPort);
    Listener listener = TestResources.createListener(ads, listenerName, listenerPort, routeName);
    RouteConfiguration route = TestResources.createRoute(routeName, clusterName);

    return Snapshot.create(
        ImmutableList.of(SnapshotResource.create(cluster, version)),
        ImmutableList.of(SnapshotResource.create(endpoint, version)),
        ImmutableList.of(SnapshotResource.create(listener, version)),
        ImmutableList.of(),
        ImmutableList.of(SnapshotResource.create(route, version)),
        ImmutableList.of(),
        version);
  }

  static Snapshot createSnapshotNoEds(
      boolean ads,
      String clusterName,
      String endpointAddress,
      int endpointPort,
      String listenerName,
      int listenerPort,
      String routeName,
      String version) {

    Cluster cluster = TestResources.createCluster(clusterName, endpointAddress, endpointPort);
    Listener listener = TestResources.createListener(ads, listenerName, listenerPort, routeName);
    RouteConfiguration route = TestResources.createRoute(routeName, clusterName);

    return Snapshot.create(
        ImmutableList.of(SnapshotResource.create(cluster, version)),
        ImmutableList.of(),
        ImmutableList.of(SnapshotResource.create(listener, version)),
        ImmutableList.of(),
        ImmutableList.of(SnapshotResource.create(route, version)),
        ImmutableList.of(),
        version);
  }

  private TestSnapshots() { }
}
