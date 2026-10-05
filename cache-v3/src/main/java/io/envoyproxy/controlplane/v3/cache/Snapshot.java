package io.envoyproxy.controlplane.v3.cache;

import static io.envoyproxy.controlplane.v3.cache.Resources.CLUSTER_TYPE_URL;
import static io.envoyproxy.controlplane.v3.cache.Resources.ENDPOINT_TYPE_URL;
import static io.envoyproxy.controlplane.v3.cache.Resources.LISTENER_TYPE_URL;
import static io.envoyproxy.controlplane.v3.cache.Resources.ROUTE_TYPE_URL;
import static io.envoyproxy.controlplane.v3.cache.Resources.SCOPED_ROUTE_TYPE_URL;
import static io.envoyproxy.controlplane.v3.cache.Resources.SECRET_TYPE_URL;
import static io.envoyproxy.controlplane.v3.cache.Resources.VIRTUAL_HOST_TYPE_URL;

import com.google.auto.value.AutoValue;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Message;
import io.envoyproxy.envoy.config.cluster.v3.Cluster;
import io.envoyproxy.envoy.config.endpoint.v3.ClusterLoadAssignment;
import io.envoyproxy.envoy.config.listener.v3.Listener;
import io.envoyproxy.envoy.config.route.v3.RouteConfiguration;
import io.envoyproxy.envoy.config.route.v3.ScopedRouteConfiguration;
import io.envoyproxy.envoy.config.route.v3.VirtualHost;
import io.envoyproxy.envoy.extensions.transport_sockets.tls.v3.Secret;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * {@code Snapshot} is a data class that contains an internally consistent snapshot of xDS resources. Snapshots should
 * have distinct versions per node group.
 */
@AutoValue
public abstract class Snapshot {

  /**
   * Returns a new {@link Snapshot} instance that is versioned uniformly across all resources.
   *
   * @param clusters  the cluster resources in this snapshot
   * @param endpoints the endpoint resources in this snapshot
   * @param listeners the listener resources in this snapshot
   * @param routes    the route resources in this snapshot
   * @param version   the version associated with all resources in this snapshot
   */
  public static Snapshot create(
      Collection<SnapshotResource<Cluster>> clusters,
      Collection<SnapshotResource<ClusterLoadAssignment>> endpoints,
      Collection<SnapshotResource<Listener>> listeners,
      Collection<SnapshotResource<ScopedRouteConfiguration>> scopedRoutes,
      Collection<SnapshotResource<RouteConfiguration>> routes,
      Collection<SnapshotResource<VirtualHost>> virtualHosts,
      Collection<SnapshotResource<Secret>> secrets,
      String version) {

    return new AutoValue_Snapshot(
        SnapshotResources.create(clusters, version),
        SnapshotResources.create(endpoints, version),
        SnapshotResources.create(listeners, version),
        SnapshotResources.create(scopedRoutes, version),
        SnapshotResources.create(routes, version),
        SnapshotResources.create(virtualHosts, version),
        SnapshotResources.create(secrets, version));
  }

  /**
   * Returns a new {@link Snapshot} instance that has separate versions for each resource type.
   *
   * @param clusters         the cluster resources in this snapshot
   * @param clustersVersion  the version of the cluster resources
   * @param endpoints        the endpoint resources in this snapshot
   * @param endpointsVersion the version of the endpoint resources
   * @param listeners        the listener resources in this snapshot
   * @param listenersVersion the version of the listener resources
   * @param routes           the route resources in this snapshot
   * @param routesVersion    the version of the route resources
   */
  public static Snapshot create(
      Collection<SnapshotResource<Cluster>> clusters,
      String clustersVersion,
      Collection<SnapshotResource<ClusterLoadAssignment>> endpoints,
      String endpointsVersion,
      Collection<SnapshotResource<Listener>> listeners,
      String listenersVersion,
      Collection<SnapshotResource<ScopedRouteConfiguration>> scopedRoutes,
      String scopedRoutesVersion,
      Collection<SnapshotResource<RouteConfiguration>> routes,
      String routesVersion,
      Collection<SnapshotResource<VirtualHost>> virtualHosts,
      String virtualHostsVersion,
      Collection<SnapshotResource<Secret>> secrets,
      String secretsVersion) {

    // TODO(snowp): add a builder alternative
    return new AutoValue_Snapshot(
        SnapshotResources.create(clusters, clustersVersion),
        SnapshotResources.create(endpoints, endpointsVersion),
        SnapshotResources.create(listeners, listenersVersion),
        SnapshotResources.create(scopedRoutes, scopedRoutesVersion),
        SnapshotResources.create(routes, routesVersion),
        SnapshotResources.create(virtualHosts, virtualHostsVersion),
        SnapshotResources.create(secrets, secretsVersion));
  }

  /**
   * Returns a new {@link Snapshot} instance that has separate versions for each resource type.
   *
   * @param clusters                the cluster resources in this snapshot
   * @param clusterVersionResolver  version resolver of the clusters in this snapshot
   * @param endpoints               the endpoint resources in this snapshot
   * @param endpointVersionResolver version resolver of the endpoints in this snapshot
   * @param listeners               the listener resources in this snapshot
   * @param listenerVersionResolver version resolver of listeners in this snapshot
   * @param routes                  the route resources in this snapshot
   * @param routeVersionResolver    version resolver of the routes in this snapshot
   * @param secrets                 the secret resources in this snapshot
   * @param secretVersionResolver   version resolver of the secrets in this snapshot
   */
  public static Snapshot create(
      Collection<SnapshotResource<Cluster>> clusters,
      ResourceVersionResolver clusterVersionResolver,
      Collection<SnapshotResource<ClusterLoadAssignment>> endpoints,
      ResourceVersionResolver endpointVersionResolver,
      Collection<SnapshotResource<Listener>> listeners,
      ResourceVersionResolver listenerVersionResolver,
      Collection<SnapshotResource<ScopedRouteConfiguration>> scopedRoutes,
      ResourceVersionResolver scopedRouteVersionResolver,
      Collection<SnapshotResource<RouteConfiguration>> routes,
      ResourceVersionResolver routeVersionResolver,
      Collection<SnapshotResource<VirtualHost>> virtualHosts,
      ResourceVersionResolver virtualHostVersionResolver,
      Collection<SnapshotResource<Secret>> secrets,
      ResourceVersionResolver secretVersionResolver) {

    return new AutoValue_Snapshot(
        SnapshotResources.create(clusters, clusterVersionResolver),
        SnapshotResources.create(endpoints, endpointVersionResolver),
        SnapshotResources.create(listeners, listenerVersionResolver),
        SnapshotResources.create(scopedRoutes, scopedRouteVersionResolver),
        SnapshotResources.create(routes, routeVersionResolver),
        SnapshotResources.create(virtualHosts, virtualHostVersionResolver),
        SnapshotResources.create(secrets, secretVersionResolver));
  }

  /**
   * Creates an empty snapshot with the given version.
   *
   * @param version the version of the snapshot resources
   */
  public static Snapshot createEmpty(String version) {
    return create(Collections.emptySet(),
        Collections.emptySet(),
        Collections.emptySet(),
        Collections.emptySet(),
        Collections.emptySet(),
        Collections.emptySet(),
        Collections.emptySet(),
        version);
  }

  /**
   * Asserts that all of the given resource names have corresponding values in the given resources collection.
   *
   * @param parentTypeUrl     the type of the parent resources (source of the resource name refs)
   * @param dependencyTypeUrl the type of the given dependent resources
   * @param resourceNames     the set of dependent resource names that must exist
   * @param resources         the collection of resources whose names are being checked
   * @throws SnapshotConsistencyException if a name is given that does not exist in the resources collection
   */
  private static <T extends Message> void ensureAllResourceNamesExist(
      String parentTypeUrl,
      String dependencyTypeUrl,
      Set<String> resourceNames,
      Map<String, SnapshotResource<T>> resources,
      boolean sizeCheck) throws SnapshotConsistencyException {

    if (sizeCheck && resourceNames.size() != resources.size()) {
      throw new SnapshotConsistencyException(
          String.format(
              "Mismatched %s -> %s reference and resource lengths, [%s] != %d",
              parentTypeUrl,
              dependencyTypeUrl,
              String.join(", ", resourceNames),
              resources.size()));
    }

    for (String name : resourceNames) {
      if (!resources.containsKey(name)) {
        throw new SnapshotConsistencyException(
            String.format(
                "%s named '%s', referenced by a %s, not listed in [%s]",
                dependencyTypeUrl,
                name,
                parentTypeUrl,
                String.join(", ", resources.keySet())));
      }
    }
  }

  /**
   * Returns all cluster items in the CDS payload.
   */
  public abstract SnapshotResources<Cluster> clusters();

  /**
   * Returns all endpoint items in the EDS payload.
   */
  public abstract SnapshotResources<ClusterLoadAssignment> endpoints();

  /**
   * Returns all listener items in the LDS payload.
   */
  public abstract SnapshotResources<Listener> listeners();

  /**
   * Returns all scoped route items in the SRDS payload.
   */
  public abstract SnapshotResources<ScopedRouteConfiguration> scopedRoutes();

  /**
   * Returns all route items in the RDS payload.
   */
  public abstract SnapshotResources<RouteConfiguration> routes();

  /**
   * Returns all virtual hosts items in the VHDS payload.
   */
  public abstract SnapshotResources<VirtualHost> virtualHosts();

  /**
   * Returns all secret items in the SDS payload.
   */
  public abstract SnapshotResources<Secret> secrets();

  /**
   * Asserts that all dependent resources are included in the snapshot. All EDS resources are listed by name in CDS
   * resources, and all RDS resources are listed by name in LDS resources.
   *
   * <p>Note that clusters and listeners are requested without name references, so Envoy will accept the snapshot list
   * of clusters as-is, even if it does not match all references found in xDS.
   *
   * @throws SnapshotConsistencyException if the snapshot is not consistent
   */
  public void ensureConsistent() throws SnapshotConsistencyException {
    Set<String> clusterEndpointRefs = Resources.getResourceReferences(clusters().resources().values());

    ensureAllResourceNamesExist(CLUSTER_TYPE_URL, ENDPOINT_TYPE_URL,
        clusterEndpointRefs, endpoints().resources(), true);

    Set<String> listenerRouteRefs = Resources.getResourceReferences(listeners().resources().values());

    ensureAllResourceNamesExist(LISTENER_TYPE_URL, ROUTE_TYPE_URL,
        listenerRouteRefs, routes().resources(), true);

    Set<String> scopedRoutesRefs = Resources.getResourceReferences(scopedRoutes().resources().values());

    ensureAllResourceNamesExist(SCOPED_ROUTE_TYPE_URL, ROUTE_TYPE_URL,
        scopedRoutesRefs, routes().resources(), false);
  }

  /**
   * Returns the resources with the given type.
   *
   * @param typeUrl the URL for the requested resource type
   */
  public Map<String, SnapshotResource<?>> resources(String typeUrl) {
    if (Strings.isNullOrEmpty(typeUrl)) {
      return ImmutableMap.of();
    }

    switch (typeUrl) {
      case CLUSTER_TYPE_URL:
        return (Map) clusters().resources();
      case ENDPOINT_TYPE_URL:
        return (Map) endpoints().resources();
      case LISTENER_TYPE_URL:
        return (Map) listeners().resources();
      case SCOPED_ROUTE_TYPE_URL:
        return (Map) scopedRoutes().resources();
      case ROUTE_TYPE_URL:
        return (Map) routes().resources();
      case VIRTUAL_HOST_TYPE_URL:
        return (Map) virtualHosts().resources();
      case SECRET_TYPE_URL:
        return (Map) secrets().resources();
      default:
        return ImmutableMap.of();
    }
  }

  /**
   * Returns the version in this snapshot for the given resource type.
   *
   * @param typeUrl the URL for the requested resource type
   */
  public String version(String typeUrl) {
    return version(typeUrl, Collections.emptyList());
  }

  /**
   * Returns the version in this snapshot for the given resource type.
   *
   * @param typeUrl the URL for the requested resource type
   * @param resourceNames list of requested resource names,
   *                      used to calculate a version for the given resources
   */
  public String version(String typeUrl, List<String> resourceNames) {
    if (Strings.isNullOrEmpty(typeUrl)) {
      return "";
    }

    switch (typeUrl) {
      case CLUSTER_TYPE_URL:
        return clusters().version(resourceNames);
      case ENDPOINT_TYPE_URL:
        return endpoints().version(resourceNames);
      case LISTENER_TYPE_URL:
        return listeners().version(resourceNames);
      case SCOPED_ROUTE_TYPE_URL:
        return scopedRoutes().version(resourceNames);
      case ROUTE_TYPE_URL:
        return routes().version(resourceNames);
      case VIRTUAL_HOST_TYPE_URL:
        return virtualHosts().version(resourceNames);
      case SECRET_TYPE_URL:
        return secrets().version(resourceNames);
      default:
        return "";
    }
  }
}
