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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.auth.BasicClientAuthHandler;
import org.apache.arrow.flight.auth2.BasicAuthCredentialWriter;
import org.apache.arrow.flight.auth2.ClientBearerHeaderHandler;
import org.apache.arrow.flight.auth2.ClientIncomingAuthHeaderMiddleware;
import org.apache.arrow.flight.client.ClientCookieMiddleware;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.flight.sql.FlightSqlClient;
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
    private final BufferAllocator allocator;
    private final FlightClient client;
    private final FlightSqlClient sqlClient;
    private final String authMode;
    private final Collection<CallOption> options;
    private CredentialCallOption tokenCallOption;

    public FlightClientWrapper(BufferAllocator allocator, FlightClient client,
                               String authMode, Collection<CallOption> options) {
      this.allocator = allocator;
      this.client = client;
      this.sqlClient = new FlightSqlClient(this.client);
      this.authMode = authMode;
      this.tokenCallOption = null;
      this.options = new ArrayList<>(options);
    }

    public FlightClientWrapper(BufferAllocator allocator, FlightClient client,
                               String authMode) {
      this(allocator, client, authMode, Collections.emptyList());
    }

    public CallOption[] getOptions() {
      return options.toArray(new CallOption[0]);
    }

    public void addOptions(Collection<CallOption> callOptions) {
      options.addAll(callOptions);
    }

    public FlightSqlClient getSqlClient() {
      return sqlClient;
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
      AutoCloseables.close(sqlClient, allocator);
      tokenCallOption = null;
    }
  }

  public static FlightClientWrapper openFlightClient(int port, String user, String password,
                                                     SabotContext context, String authMode) throws Exception {
    final BufferAllocator allocator = context.getAllocator().newChildAllocator("flight-client-allocator", 0, Long.MAX_VALUE);
    FlightClient.Builder builder = FlightClient.builder()
      .allocator(allocator)
      .location(Location.forGrpcInsecure("localhost", port));

    final FlightClientWrapper wrapper = new FlightClientWrapper(allocator, builder.build(), authMode);

    try {
      if (DremioFlightService.FLIGHT_LEGACY_AUTH_MODE.equals(authMode)) {
        wrapper.client.authenticateBasic(user, password);
      } else if (DremioFlightService.FLIGHT_AUTH2_AUTH_MODE.equals(authMode)) {
        final ClientIncomingAuthHeaderMiddleware.Factory authFactory =
          new ClientIncomingAuthHeaderMiddleware.Factory(new ClientBearerHeaderHandler());

        builder.intercept(authFactory);
        builder.intercept(new ClientCookieMiddleware.Factory());

        Optional<CredentialCallOption> callOption = Optional.ofNullable(
          getTokenAuthenticate(wrapper.client,
            new CredentialCallOption(new BasicAuthCredentialWriter(user, password)), authFactory));
        callOption.ifPresent(wrapper::setTokenCredentialCallOption);
        callOption.ifPresent(option -> wrapper.addOptions(Collections.singletonList(option)));
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
        final ClientIncomingAuthHeaderMiddleware.Factory authFactory =
          new ClientIncomingAuthHeaderMiddleware.Factory(new ClientBearerHeaderHandler());

        builder.intercept(authFactory);
        builder.intercept(new ClientCookieMiddleware.Factory());

        Optional<CredentialCallOption> callOption = Optional.ofNullable(
          getTokenAuthenticate(wrapper.client,
            new CredentialCallOption(new BasicAuthCredentialWriter(user, password)), authFactory));
        callOption.ifPresent(wrapper::setTokenCredentialCallOption);
        callOption.ifPresent(option -> wrapper.addOptions(Collections.singletonList(option)));
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

  public static FlightClientWrapper openFlightClientWithOptions(int port, String user, String password,
                                                                SabotContext context, String authMode,
                                                                Collection<CallOption> options) throws Exception {
    final BufferAllocator allocator = context.getAllocator().newChildAllocator("flight-client-allocator", 0, Long.MAX_VALUE);
    FlightClient.Builder builder = FlightClient.builder()
      .allocator(allocator)
      .location(Location.forGrpcInsecure("localhost", port));

    final FlightClientWrapper wrapper = new FlightClientWrapper(allocator, builder.build(), authMode, options);

    try {
      if (DremioFlightService.FLIGHT_LEGACY_AUTH_MODE.equals(authMode)) {
        BasicClientAuthHandler authHandler = new BasicClientAuthHandler(user, password);
        wrapper.client.authenticate(authHandler, options.toArray(new CallOption[0]));
      } else if (DremioFlightService.FLIGHT_AUTH2_AUTH_MODE.equals(authMode)) {
        final ClientIncomingAuthHeaderMiddleware.Factory authFactory =
          new ClientIncomingAuthHeaderMiddleware.Factory(new ClientBearerHeaderHandler());

        builder.intercept(authFactory);
        builder.intercept(new ClientCookieMiddleware.Factory());

        Optional<CredentialCallOption> callOption = Optional.ofNullable(
          getTokenAuthenticate(wrapper.client,
            new CredentialCallOption(new BasicAuthCredentialWriter(user, password)),
            authFactory,
            options.toArray(new CallOption[0])));
        callOption.ifPresent(wrapper::setTokenCredentialCallOption);
        callOption.ifPresent(option -> wrapper.addOptions(Collections.singletonList(option)));
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

  private static CredentialCallOption getTokenAuthenticate(final FlightClient client,
                                                           final CredentialCallOption token,
                                                           final ClientIncomingAuthHeaderMiddleware.Factory factory,
                                                           final CallOption... options) {
    final List<CallOption> theseOptions = new ArrayList<>();
    theseOptions.add(token);
    theseOptions.addAll(Arrays.asList(options));
    client.handshake(theseOptions.toArray(new CallOption[0]));
    return factory.getCredentialCallOption();
  }
}
