/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.service.flight;

import java.io.InputStream;
import java.util.Optional;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.server.SabotContext;

/**
 * Utility class for working with FlightClients
 */
public final class FlightClientUtils {
  /**
   * Container class for holding a FlightClient and its associated allocator.
   */
  public static final class FlightClientWrapper implements AutoCloseable {
    private BufferAllocator allocator;
    private FlightClient client;
    private String authMode;
    private CredentialCallOption tokenCallOption;

    public FlightClientWrapper(BufferAllocator allocator, FlightClient client,
                               String authMode) {
      this.allocator = allocator;
      this.client = client;
      this.authMode = authMode;
      this.tokenCallOption = null;
    }

    public BufferAllocator getAllocator() {
      return allocator;
    }

    public FlightClient getClient() {
      return client;
    }

    public String getAuthMode() {
      return authMode;
    }

    public CredentialCallOption getTokenCallOption() {
      return tokenCallOption;
    }

    public void setTokenCredentialCallOption(CredentialCallOption tokenCallOption) {
      this.tokenCallOption = tokenCallOption;
    }

    @Override
    public void close() throws Exception {
      // Note - client must close first as it creates a child allocator from
      // the input allocator.
      AutoCloseables.close(client, allocator);
      client = null;
      allocator = null;
      tokenCallOption = null;
    }
  }

  public static FlightClientWrapper openFlightClient(int port, String user, String password,
                                                     SabotContext context, String authMode) throws Exception {
    final BufferAllocator allocator = context.getAllocator().newChildAllocator("flight-client-allocator", 0, Long.MAX_VALUE);
    final FlightClient.Builder builder = FlightClient.builder()
      .allocator(allocator)
      .location(Location.forGrpcInsecure("localhost", port));

    final FlightClientWrapper wrapper = new FlightClientWrapper(allocator, builder.build(), authMode);

    try {
      if (DremioFlightService.FLIGHT_LEGACY_AUTH_MODE.equals(authMode)) {
        wrapper.client.authenticateBasic(user, password);
      } else if (DremioFlightService.FLIGHT_AUTH2_AUTH_MODE.equals(authMode)) {
        Optional<CredentialCallOption> callOption =  wrapper.client.authenticateBasicToken(user, password);
        callOption.ifPresent(wrapper::setTokenCredentialCallOption);
      } else {
        throw new UnsupportedOperationException(authMode
          + " is not a supported FlightServer Endpoint authentication mode.");
      }
      return wrapper;
    } catch (Exception ex) {
      AutoCloseables.close(wrapper);
      throw ex;
    }
  }

  public static FlightClientWrapper openEncryptedFlightClient(String host, int port, String user,
                                                              String password, InputStream trustedCerts,
                                                              SabotContext context, String authMode) throws Exception {
    final BufferAllocator allocator = context.getAllocator().newChildAllocator("flight-client-allocator", 0, Long.MAX_VALUE);
    final FlightClient.Builder builder = FlightClient.builder()
      .allocator(allocator)
      .useTls()
      .location(Location.forGrpcTls(host, port));

    if (trustedCerts != null) {
      builder.trustedCertificates(trustedCerts);
    }

    final FlightClientWrapper wrapper = new FlightClientWrapper(allocator, builder.build(), authMode);

    try {
      if (DremioFlightService.FLIGHT_LEGACY_AUTH_MODE.equals(authMode)) {
        wrapper.client.authenticateBasic(user, password);
      } else if (DremioFlightService.FLIGHT_AUTH2_AUTH_MODE.equals(authMode)) {
        Optional<CredentialCallOption> callOption = wrapper.client.authenticateBasicToken(user, password);
        callOption.ifPresent(wrapper::setTokenCredentialCallOption);
      } else {
        throw new UnsupportedOperationException(authMode
          + " is not a supported FlightServer Endpoint authentication mode.");
      }
      return wrapper;
    } catch (Exception ex) {
      AutoCloseables.close(wrapper);
      throw ex;
    }
  }
}
