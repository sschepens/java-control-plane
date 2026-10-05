package io.envoyproxy.controlplane.v3.cache;

import com.google.auto.value.AutoValue;
import com.google.protobuf.Any;
import com.google.protobuf.Message;

@AutoValue
public abstract class SnapshotResource<T extends Message> {
  // Best-effort cache of the packed resource. Senders on different executor threads pack the same resource
  // right after a snapshot change, so this deliberately avoids a lock: a concurrent first call may pack twice,
  // a few microseconds of duplicate work, instead of parking a sender on a contended monitor. Any is immutable,
  // so either instance is valid, and the volatile write publishes it safely.
  private volatile Any packed;


  /**
   * Returns a new {@link SnapshotResource} instance.
   *
   * @param resource the resource
   * @param version  the version associated with the resource
   * @param <T>      the type of resource
   */
  public static <T extends Message> SnapshotResource<T> create(T resource, String version) {
    return new AutoValue_SnapshotResource<>(
        resource,
        version
    );
  }

  /**
   * Returns the resource.
   */
  public abstract T resource();

  /**
   * Returns the version associated with the resource.
   */
  public abstract String version();

  /**
   * Returns the resource packed as {@link Any}, ready to be put in a delta response. It is computed on first use
   * and kept for the lifetime of this instance, so a resource is serialized once per snapshot no matter how many
   * streams receive it, except for callers that race on the very first use, which may each pack it once.
   */
  public Any packed() {
    Any result = packed;
    if (result == null) {
      result = Any.pack(resource());
      packed = result;
    }
    return result;
  }
}
