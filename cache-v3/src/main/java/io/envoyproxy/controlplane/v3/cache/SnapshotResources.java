package io.envoyproxy.controlplane.v3.cache;

import com.google.auto.value.AutoValue;
import com.google.protobuf.Message;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

@AutoValue
public abstract class SnapshotResources<T extends Message> {

  /**
   * Returns a new {@link SnapshotResources} instance.
   *
   * @param resources the resources in this collection
   * @param version   the version associated with the resources in this collection
   * @param <T>       the type of resources in this collection
   */
  public static <T extends Message> SnapshotResources<T> create(
      Collection<SnapshotResource<T>> resources,
      String version) {
    return create(resources, version, Resources::getResourceName);
  }

  /**
   * Creates resources with a uniform version and a caller-supplied resource-name extractor.
   */
  public static <T extends Message> SnapshotResources<T> create(
      Collection<SnapshotResource<T>> resources,
      String version,
      Function<T, String> resourceName) {
    return create(resources, (r) -> version, resourceName);
  }

  /**
   * Creates resources with a version resolver and the standard xDS resource names.
   */
  public static <T extends Message> SnapshotResources<T> create(
      Collection<SnapshotResource<T>> resources,
      ResourceVersionResolver versionResolver) {
    return create(resources, versionResolver, Resources::getResourceName);
  }

  /**
   * Creates resources with a version resolver and a caller-supplied resource-name extractor.
   *
   * <p>The result exposes an unmodifiable map. If several resources have the same extracted name,
   * the last resource in iteration order replaces earlier entries.
   */
  public static <T extends Message> SnapshotResources<T> create(
      Collection<SnapshotResource<T>> resources,
      ResourceVersionResolver versionResolver,
      Function<T, String> resourceName) {
    return new AutoValue_SnapshotResources<>(resourcesMap(resources, resourceName), versionResolver);
  }

  private static <T extends Message> Map<String, SnapshotResource<T>> resourcesMap(
      Collection<SnapshotResource<T>> resources,
      Function<T, String> resourceName) {
    Map<String, SnapshotResource<T>> result = new HashMap<>(resources.size());
    for (SnapshotResource<T> resource : resources) {
      result.put(resourceName.apply(resource.resource()), resource);
    }
    return Collections.unmodifiableMap(result);
  }

  /**
   * Returns a map of the resources in this collection, where the key is the name of the resource.
   */
  public abstract Map<String, SnapshotResource<T>> resources();

  /**
   * Returns the version associated with all resources in this collection.
   */
  public String version() {
    return resourceVersionResolver().version();
  }

  /**
   * Returns the version associated with the requested resources in this collection.
   *
   * @param resourceNames list of list of requested resources.
   */
  public String version(List<String> resourceNames) {
    return resourceVersionResolver().version(resourceNames);
  }

  /**
   * Returns the version resolver associated with this resources in this collection.
   */
  public abstract ResourceVersionResolver resourceVersionResolver();
}
