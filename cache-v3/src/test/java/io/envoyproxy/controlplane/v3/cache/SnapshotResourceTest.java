package io.envoyproxy.controlplane.v3.cache;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.Any;
import io.envoyproxy.envoy.config.cluster.v3.Cluster;
import org.junit.Test;

public class SnapshotResourceTest {

  @Test
  public void packedIsCachedAndUnpacksToTheResource() throws Exception {
    Cluster cluster = Cluster.newBuilder().setName("cluster0").build();
    SnapshotResource<Cluster> resource = SnapshotResource.create(cluster, "v1");

    Any first = resource.packed();
    Any second = resource.packed();

    assertThat(second).isSameAs(first);
    assertThat(first.unpack(Cluster.class)).isEqualTo(cluster);
  }

  @Test
  public void packingDoesNotAffectEquality() {
    Cluster cluster = Cluster.newBuilder().setName("cluster0").build();
    SnapshotResource<Cluster> packed = SnapshotResource.create(cluster, "v1");
    SnapshotResource<Cluster> fresh = SnapshotResource.create(cluster, "v1");
    packed.packed();

    assertThat(packed).isEqualTo(fresh);
    assertThat(packed.hashCode()).isEqualTo(fresh.hashCode());
  }
}
