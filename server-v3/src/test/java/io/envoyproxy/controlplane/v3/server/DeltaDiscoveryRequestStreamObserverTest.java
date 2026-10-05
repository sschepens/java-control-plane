package io.envoyproxy.controlplane.v3.server;

import static io.envoyproxy.controlplane.v3.cache.Resources.ROUTE_TYPE_URL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.google.rpc.Status;
import io.envoyproxy.controlplane.v3.cache.SimpleCache;
import io.envoyproxy.controlplane.v3.cache.Snapshot;
import io.envoyproxy.controlplane.v3.cache.SnapshotResource;
import io.envoyproxy.envoy.config.core.v3.Node;
import io.envoyproxy.envoy.config.route.v3.RouteConfiguration;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryRequest;
import io.envoyproxy.envoy.service.discovery.v3.DeltaDiscoveryResponse;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class DeltaDiscoveryRequestStreamObserverTest {
  private static final String RESOURCE_NAME = "route0";
  private static final String MISSING_RESOURCE_NAME = "missing-route";
  private static final String RESOURCE_VERSION = "resource-v1";
  private static final Node NODE = Node.newBuilder().setId("test-node").build();

  @Parameterized.Parameters(name = "ADS={0}")
  public static Collection<Object[]> streamTypes() {
    return Arrays.asList(new Object[][] {{true}, {false}});
  }

  private final boolean ads;
  private final List<DeltaDiscoveryResponse> responses = new ArrayList<>();
  private ScheduledExecutorService executor;
  private DeltaDiscoveryRequestStreamObserver observer;

  public DeltaDiscoveryRequestStreamObserverTest(boolean ads) {
    this.ads = ads;
  }

  @Before
  public void setUp() {
    executor = Executors.newSingleThreadScheduledExecutor();
    SimpleCache<String> cache = new SimpleCache<>(node -> "group");
    cache.setSnapshot("group", Snapshot.create(
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.singletonList(SnapshotResource.create(
            RouteConfiguration.newBuilder().setName(RESOURCE_NAME).build(), RESOURCE_VERSION)),
        Collections.emptyList(),
        Collections.emptyList(),
        "snapshot-v1"));
    DiscoveryServer server = new DiscoveryServer(cache);
    StreamObserver<DeltaDiscoveryResponse> responseObserver = new StreamObserver<DeltaDiscoveryResponse>() {
      @Override
      public void onNext(DeltaDiscoveryResponse response) {
        responses.add(response);
      }

      @Override
      public void onError(Throwable error) {
        fail("unexpected stream error", error);
      }

      @Override
      public void onCompleted() {
      }
    };
    observer = ads
        ? new AdsDeltaDiscoveryRequestStreamObserver(responseObserver, 1, executor, server)
        : new XdsDeltaDiscoveryRequestStreamObserver(ROUTE_TYPE_URL, responseObserver, 1, executor, server);
  }

  @After
  public void tearDown() {
    observer.onCompleted();
    executor.shutdownNow();
  }

  @Test
  public void ackWithResubscribeResendsUnchangedResource() throws Exception {
    send(request().addResourceNamesSubscribe(RESOURCE_NAME));
    assertThat(responses).hasSize(1);
    DeltaDiscoveryResponse first = responses.get(0);

    send(request().setResponseNonce(first.getNonce()).addResourceNamesSubscribe(RESOURCE_NAME));

    assertThat(responses).hasSize(2);
    DeltaDiscoveryResponse resent = responses.get(1);
    assertThat(resent.getResourcesList()).isEqualTo(first.getResourcesList());
    assertThat(resent.getNonce()).isNotEqualTo(first.getNonce());
    send(request().setResponseNonce(resent.getNonce()));
    assertThat(responses).hasSize(2);
  }

  @Test
  public void ackOfMissingResourceClearsPendingRemoval() throws Exception {
    send(request().addResourceNamesSubscribe(MISSING_RESOURCE_NAME));
    assertThat(responses).hasSize(1);
    assertThat(responses.get(0).getResourcesList()).isEmpty();
    assertThat(responses.get(0).getRemovedResourcesList()).containsExactly(MISSING_RESOURCE_NAME);

    send(request().setResponseNonce(responses.get(0).getNonce()));

    assertThat(observer.pendingResources(ROUTE_TYPE_URL)).isEmpty();
    assertThat(observer.responseCount(ROUTE_TYPE_URL)).isZero();
    assertThat(responses).hasSize(1);
    send(request());
    assertThat(responses).hasSize(1);
  }

  @Test
  public void ackOfRemovalWithResubscribeResendsRemovalOnce() throws Exception {
    send(request().addResourceNamesSubscribe(MISSING_RESOURCE_NAME));
    assertThat(responses).hasSize(1);

    send(request().setResponseNonce(responses.get(0).getNonce())
        .addResourceNamesSubscribe(MISSING_RESOURCE_NAME));

    assertThat(responses).hasSize(2);
    assertThat(responses.get(1).getRemovedResourcesList()).containsExactly(MISSING_RESOURCE_NAME);
    send(request().setResponseNonce(responses.get(1).getNonce()));
    assertThat(observer.pendingResources(ROUTE_TYPE_URL)).isEmpty();
    assertThat(responses).hasSize(2);
  }

  @Test
  public void ackWithUnsubscribeDoesNotRestoreTrackedResource() throws Exception {
    send(request().addResourceNamesSubscribe(RESOURCE_NAME));
    assertThat(responses).hasSize(1);

    send(request().setResponseNonce(responses.get(0).getNonce())
        .addResourceNamesUnsubscribe(RESOURCE_NAME));

    assertThat(observer.resourceVersions(ROUTE_TYPE_URL)).isEmpty();
    assertThat(observer.pendingResources(ROUTE_TYPE_URL)).isEmpty();
    assertThat(responses).hasSize(1);
  }

  @Test
  public void nackOfRemovalDoesNotClearPendingResource() throws Exception {
    send(request().addResourceNamesSubscribe(MISSING_RESOURCE_NAME));
    assertThat(responses).hasSize(1);

    send(request().setResponseNonce(responses.get(0).getNonce())
        .setErrorDetail(Status.newBuilder().setCode(3).setMessage("rejected")));

    assertThat(observer.pendingResources(ROUTE_TYPE_URL)).containsExactly(MISSING_RESOURCE_NAME);
    assertThat(responses).hasSize(2);
    assertThat(responses.get(1).getRemovedResourcesList()).containsExactly(MISSING_RESOURCE_NAME);
  }

  private DeltaDiscoveryRequest.Builder request() {
    return DeltaDiscoveryRequest.newBuilder().setNode(NODE).setTypeUrl(ROUTE_TYPE_URL);
  }

  private void send(DeltaDiscoveryRequest.Builder request) throws Exception {
    observer.onNext(request.build());
    // Wait for all responses queued by this request before checking for an extra response.
    executor.submit(() -> { }).get(5, TimeUnit.SECONDS);
  }
}
