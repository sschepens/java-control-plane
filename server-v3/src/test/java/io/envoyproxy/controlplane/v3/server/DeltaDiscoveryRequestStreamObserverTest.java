package io.envoyproxy.controlplane.v3.server;

import static io.envoyproxy.controlplane.v3.cache.Resources.ROUTE_TYPE_URL;
import static org.assertj.core.api.Assertions.assertThat;

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
  private static final String OTHER_RESOURCE_NAME = "route1";
  private static final String OTHER_RESOURCE_VERSION = "other-resource-v1";
  private static final Node NODE = Node.newBuilder().setId("test-node").build();

  @Parameterized.Parameters(name = "ADS={0}")
  public static Collection<Object[]> streamTypes() {
    return Arrays.asList(new Object[][] {{true}, {false}});
  }

  private final boolean ads;
  private final List<DeltaDiscoveryResponse> responses = new ArrayList<>();
  private final List<Throwable> errors = new ArrayList<>();
  private volatile RuntimeException sendFailure;
  private ScheduledExecutorService executor;
  private SimpleCache<String> cache;
  private DeltaDiscoveryRequestStreamObserver observer;

  public DeltaDiscoveryRequestStreamObserverTest(boolean ads) {
    this.ads = ads;
  }

  @Before
  public void setUp() {
    executor = Executors.newSingleThreadScheduledExecutor();
    cache = new SimpleCache<>(node -> "group");
    cache.setSnapshot("group", snapshot(RESOURCE_VERSION, OTHER_RESOURCE_VERSION, "snapshot-v1"));
    observer = newObserver(new DiscoveryServer(cache));
  }

  private DeltaDiscoveryRequestStreamObserver newObserver(DiscoveryServer server) {
    StreamObserver<DeltaDiscoveryResponse> responseObserver = new StreamObserver<DeltaDiscoveryResponse>() {
      @Override
      public void onNext(DeltaDiscoveryResponse response) {
        RuntimeException failure = sendFailure;
        if (failure != null) {
          sendFailure = null;
          throw failure;
        }
        responses.add(response);
      }

      @Override
      public void onError(Throwable error) {
        errors.add(error);
      }

      @Override
      public void onCompleted() {
      }
    };
    return ads
        ? new AdsDeltaDiscoveryRequestStreamObserver(responseObserver, 1, executor, server)
        : new XdsDeltaDiscoveryRequestStreamObserver(ROUTE_TYPE_URL, responseObserver, 1, executor, server);
  }

  @After
  public void tearDown() {
    assertThat(errors).as("unexpected stream errors").isEmpty();
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
  public void nackedResourceIsNotResentUntilItChanges() throws Exception {
    send(request().addResourceNamesSubscribe(RESOURCE_NAME));
    assertThat(responses).hasSize(1);
    assertThat(observer.resourceVersions(ROUTE_TYPE_URL))
        .as("versions are recorded when the response is produced").containsEntry(RESOURCE_NAME, RESOURCE_VERSION);

    send(request().setResponseNonce(responses.get(0).getNonce())
        .setErrorDetail(Status.newBuilder().setCode(3).setMessage("rejected")));

    assertThat(responses).as("the same versions are not resent after a nack").hasSize(1);
    assertThat(observer.responseCount(ROUTE_TYPE_URL)).isZero();

    cache.setSnapshot("group", snapshot("resource-v2", OTHER_RESOURCE_VERSION, "snapshot-v2"));
    flush();

    assertThat(responses).hasSize(2);
    assertThat(responses.get(1).getResources(0).getName()).isEqualTo(RESOURCE_NAME);
    assertThat(responses.get(1).getResources(0).getVersion()).isEqualTo("resource-v2");
  }

  @Test
  public void nackOfRemovalIsNotResent() throws Exception {
    send(request().addResourceNamesSubscribe(MISSING_RESOURCE_NAME));
    assertThat(responses).hasSize(1);

    send(request().setResponseNonce(responses.get(0).getNonce())
        .setErrorDetail(Status.newBuilder().setCode(3).setMessage("rejected")));

    assertThat(observer.pendingResources(ROUTE_TYPE_URL)).isEmpty();
    assertThat(responses).hasSize(1);
  }

  @Test
  public void initialResourceVersionsSuppressResendOfUnchangedResource() throws Exception {
    // first request of a stream: envoy subscribes to everything it wants and reports what it already has
    send(request().addResourceNamesSubscribe(RESOURCE_NAME)
        .putInitialResourceVersions(RESOURCE_NAME, RESOURCE_VERSION));

    assertThat(responses).isEmpty();
    assertThat(observer.resourceVersions(ROUTE_TYPE_URL)).containsEntry(RESOURCE_NAME, RESOURCE_VERSION);
    assertThat(observer.pendingResources(ROUTE_TYPE_URL)).isEmpty();

    // the watch is left open: a later change is delivered as a delta
    cache.setSnapshot("group", snapshot("resource-v2", OTHER_RESOURCE_VERSION, "snapshot-v2"));
    flush();

    assertThat(responses).hasSize(1);
    assertThat(responses.get(0).getResourcesList()).hasSize(1);
    assertThat(responses.get(0).getResources(0).getName()).isEqualTo(RESOURCE_NAME);
    assertThat(responses.get(0).getResources(0).getVersion()).isEqualTo("resource-v2");
  }

  @Test
  public void initialResourceVersionsWithStaleVersionResendsResource() throws Exception {
    send(request().addResourceNamesSubscribe(RESOURCE_NAME)
        .putInitialResourceVersions(RESOURCE_NAME, "stale"));

    assertThat(responses).hasSize(1);
    assertThat(responses.get(0).getResourcesList()).hasSize(1);
    assertThat(responses.get(0).getResources(0).getName()).isEqualTo(RESOURCE_NAME);
    assertThat(responses.get(0).getResources(0).getVersion()).isEqualTo(RESOURCE_VERSION);
    assertThat(responses.get(0).getRemovedResourcesList()).isEmpty();
  }

  @Test
  public void initialResourceVersionsOnlySendSubscribedNamesWithoutAVersion() throws Exception {
    send(request().addResourceNamesSubscribe(RESOURCE_NAME).addResourceNamesSubscribe(OTHER_RESOURCE_NAME)
        .putInitialResourceVersions(RESOURCE_NAME, RESOURCE_VERSION));

    assertThat(responses).hasSize(1);
    assertThat(responses.get(0).getResourcesList()).hasSize(1);
    assertThat(responses.get(0).getResources(0).getName()).isEqualTo(OTHER_RESOURCE_NAME);
    assertThat(responses.get(0).getResources(0).getVersion()).isEqualTo(OTHER_RESOURCE_VERSION);
    assertThat(observer.pendingResources(ROUTE_TYPE_URL)).isEmpty();

    send(request().setResponseNonce(responses.get(0).getNonce()));
    assertThat(observer.pendingResources(ROUTE_TYPE_URL)).isEmpty();
    assertThat(observer.resourceVersions(ROUTE_TYPE_URL))
        .containsEntry(RESOURCE_NAME, RESOURCE_VERSION)
        .containsEntry(OTHER_RESOURCE_NAME, OTHER_RESOURCE_VERSION);
    assertThat(responses).hasSize(1);
  }

  @Test
  public void initialResourceVersionsForMissingResourceReportsRemoval() throws Exception {
    send(request().addResourceNamesSubscribe(RESOURCE_NAME).addResourceNamesSubscribe(MISSING_RESOURCE_NAME)
        .putInitialResourceVersions(RESOURCE_NAME, RESOURCE_VERSION)
        .putInitialResourceVersions(MISSING_RESOURCE_NAME, "gone"));

    assertThat(responses).hasSize(1);
    assertThat(responses.get(0).getResourcesList()).isEmpty();
    assertThat(responses.get(0).getRemovedResourcesList()).containsExactly(MISSING_RESOURCE_NAME);

    send(request().setResponseNonce(responses.get(0).getNonce()));
    assertThat(observer.resourceVersions(ROUTE_TYPE_URL)).containsOnlyKeys(RESOURCE_NAME);
    assertThat(observer.pendingResources(ROUTE_TYPE_URL)).isEmpty();
    assertThat(responses).hasSize(1);
  }

  @Test
  public void unknownNonceIsIgnoredWhileSubscriptionsStillApply() throws Exception {
    send(request().addResourceNamesSubscribe(RESOURCE_NAME));
    assertThat(responses).hasSize(1);
    String realNonce = responses.get(0).getNonce();

    // a bogus ack must not break the stream nor count as the real ack, but its subscription must be kept
    send(request().setResponseNonce("not-a-nonce-we-sent").addResourceNamesSubscribe(OTHER_RESOURCE_NAME));

    assertThat(errors).isEmpty();
    assertThat(responses).hasSize(1);
    assertThat(observer.responseCount(ROUTE_TYPE_URL)).as("the real response is still pending").isEqualTo(1);
    assertThat(observer.pendingResources(ROUTE_TYPE_URL)).containsExactly(OTHER_RESOURCE_NAME);

    // the real ack lets the pending subscription be served
    send(request().setResponseNonce(realNonce));

    assertThat(responses).hasSize(2);
    assertThat(responses.get(1).getResourcesList()).hasSize(1);
    assertThat(responses.get(1).getResources(0).getName()).isEqualTo(OTHER_RESOURCE_NAME);
  }

  @Test
  public void unknownNonceWithNothingPendingIsIgnored() throws Exception {
    send(request().setResponseNonce("stale"));

    assertThat(errors).isEmpty();
    assertThat(responses).isEmpty();
    assertThat(observer.responseCount(ROUTE_TYPE_URL)).isZero();

    send(request().addResourceNamesSubscribe(RESOURCE_NAME));
    assertThat(responses).hasSize(1);
  }

  @Test
  public void failureWhileWritingAResponseClosesTheStream() throws Exception {
    sendFailure = new IllegalStateException("transport exploded");

    send(request().addResourceNamesSubscribe(RESOURCE_NAME));

    assertThat(responses).isEmpty();
    assertThat(errors).hasSize(1);
    io.grpc.Status status = io.grpc.Status.fromThrowable(errors.remove(0));
    assertThat(status.getCode()).isEqualTo(io.grpc.Status.Code.INTERNAL);
    assertThat(status.getDescription()).contains(ROUTE_TYPE_URL).contains("IllegalStateException");

    // the stream is closed: a new snapshot that would normally produce a response writes nothing
    cache.setSnapshot("group", snapshot("resource-v2", OTHER_RESOURCE_VERSION, "snapshot-v2"));
    flush();
    assertThat(responses).isEmpty();
  }

  @Test
  public void failureWhileBuildingAResponseClosesTheStreamAndNotifiesCallbacks() throws Exception {
    List<Long> closedStreams = new ArrayList<>();
    DiscoveryServerCallbacks callbacks = new DiscoveryServerCallbacks() {
      @Override
      public void onStreamDeltaResponse(long streamId, DeltaDiscoveryRequest request,
                                        DeltaDiscoveryResponse response) {
        throw new IllegalStateException("callback exploded");
      }

      @Override
      public void onStreamCloseWithError(long streamId, String typeUrl, Throwable error) {
        closedStreams.add(streamId);
      }
    };
    observer = newObserver(new DiscoveryServer(callbacks, cache));

    send(request().addResourceNamesSubscribe(RESOURCE_NAME));

    assertThat(responses).isEmpty();
    assertThat(observer.responseCount(ROUTE_TYPE_URL)).as("nothing is left pending an ack").isZero();
    assertThat(closedStreams).containsExactly(1L);
    assertThat(errors).hasSize(1);
    assertThat(io.grpc.Status.fromThrowable(errors.remove(0)).getCode()).isEqualTo(io.grpc.Status.Code.INTERNAL);
  }

  @Test
  public void cancelledClientDoesNotCloseTheStreamAgain() throws Exception {
    sendFailure = io.grpc.Status.CANCELLED.asRuntimeException();

    send(request().addResourceNamesSubscribe(RESOURCE_NAME));

    assertThat(responses).isEmpty();
    assertThat(errors).isEmpty();
  }

  private static Snapshot snapshot(String resourceVersion, String otherResourceVersion, String snapshotVersion) {
    return Snapshot.create(
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        Arrays.asList(
            SnapshotResource.create(
                RouteConfiguration.newBuilder().setName(RESOURCE_NAME).build(), resourceVersion),
            SnapshotResource.create(
                RouteConfiguration.newBuilder().setName(OTHER_RESOURCE_NAME).build(), otherResourceVersion)),
        Collections.emptyList(),
        Collections.emptyList(),
        snapshotVersion);
  }

  private DeltaDiscoveryRequest.Builder request() {
    return DeltaDiscoveryRequest.newBuilder().setNode(NODE).setTypeUrl(ROUTE_TYPE_URL);
  }

  private void send(DeltaDiscoveryRequest.Builder request) throws Exception {
    observer.onNext(request.build());
    flush();
  }

  private void flush() throws Exception {
    // Wait for all responses queued so far before checking for an extra response.
    executor.submit(() -> { }).get(5, TimeUnit.SECONDS);
  }
}
