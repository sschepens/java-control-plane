package io.envoyproxy.controlplane.v3.server;

import com.google.auto.value.AutoValue;

/**
 * Class introduces optimization which store only required data during next request.
 */
@AutoValue
public abstract class LatestDeltaDiscoveryResponse {
  static LatestDeltaDiscoveryResponse create(String nonce, String version) {
    return new AutoValue_LatestDeltaDiscoveryResponse(nonce, version);
  }

  abstract String nonce();

  abstract String version();

}
