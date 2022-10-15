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
import com.dremio.service.flight.utils.TestConnectionProperties;

/**
 * Utility class for working with FlightClients
 */
public final class FlightClientUtils {
  /**
   * Container class for holding a FlightClient and its associated allocator.
   */
  public static final class FlightClientWrapper implements AutoCloseable {
    private final BufferAllocator allocator;
    private FlightClient client;
    private FlightSqlClient sqlClient;
    private final String authMode;
    private final Collection<CallOption> options;
    private final TestConnectionProperties clientProperties;
    private CredentialCallOption tokenCallOption;

    public FlightClientWrapper(BufferAllocator allocator, FlightClient client,
                               String authMode, TestConnectionProperties clientProperties) {
      this(allocator, client, authMode, Collections.emptyList(), clientProperties);
    }

    public FlightClientWrapper(BufferAllocator allocator, FlightClient client,
                               String authMode, Collection<CallOption> options,
                               TestConnectionProperties clientProperties) {
      this.allocator = allocator;
      this.client = client;
      this.sqlClient = new FlightSqlClient(this.client);
      this.authMode = authMode;
      this.tokenCallOption = null;
      this.options = new ArrayList<>(options);
      this.clientProperties = clientProperties;
    }

    public void resetClient() throws Exception {
      final FlightClientType clientType = clientProperties.getClientType();
      final String host = clientProperties.getHost();
      final Integer port = clientProperties.getPort();
      final String user = clientProperties.getUser();
      final String password = clientProperties.getPassword();

      switch (clientType) {
        case FlightClient:
          sqlClient.close();

          client = openFlightClient(port, user, password, allocator, authMode).getClient();
          sqlClient = new FlightSqlClient(client);
          break;
        case EncryptedFlightClient:
          sqlClient.close();

          final InputStream trustedCerts = clientProperties.getTrustedCerts();

          client =
            openEncryptedFlightClient(host, port, user, password, trustedCerts, allocator, authMode).getClient();
          sqlClient = new FlightSqlClient(client);
          break;
        case FlightClientWithOptions:
          sqlClient.close();

          client = openFlightClientWithOptions(port, user, password, allocator, authMode, options).getClient();
          sqlClient = new FlightSqlClient(client);
          break;
        default:
          throw new UnsupportedOperationException("Missing resetClient implementation for clientType: " + clientType);
      }
    }

    @Override
    public void close() throws Exception {
      // NOTE: client must close first as it came from a child allocator from the input allocator.
      AutoCloseables.close(sqlClient, allocator);
      tokenCallOption = null;
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

    public enum FlightClientType {
      FlightClient,
      EncryptedFlightClient,
      FlightClientWithOptions
    }
  }

  public static FlightClientWrapper openFlightClient(int port, String user, String password,
                                                     BufferAllocator allocator, String authMode) throws Exception {
    final String host = "localhost";  // TODO: Can this come from dremioConfig.getNode() in BaseFlightQueryTest?
    FlightClient.Builder builder = FlightClient.builder()
      .allocator(allocator)
      .location(Location.forGrpcInsecure(host, port));

    final TestConnectionProperties clientProperties = new TestConnectionProperties(
      FlightClientWrapper.FlightClientType.FlightClient,
      host,
      port,
      user,
      password,
      null);

    final FlightClientWrapper wrapper = new FlightClientWrapper(allocator, builder.build(), authMode, clientProperties);

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
                                                              BufferAllocator allocator, String authMode) throws Exception {
    final FlightClient.Builder builder = FlightClient.builder()
      .allocator(allocator)
      .useTls()
      .location(Location.forGrpcTls(host, port));

    if (trustedCerts != null) {
      builder.trustedCertificates(trustedCerts);
    }

    final TestConnectionProperties clientProperties = new TestConnectionProperties(
      FlightClientWrapper.FlightClientType.EncryptedFlightClient,
      host,
      port,
      user,
      password,
      trustedCerts);

    final FlightClientWrapper wrapper = new FlightClientWrapper(allocator, builder.build(), authMode, clientProperties);

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
                                                                BufferAllocator allocator, String authMode,
                                                                Collection<CallOption> options) throws Exception {
    final String host = "localhost";  // TODO: Can this come from dremioConfig.getNode() in BaseFlightQueryTest?
    FlightClient.Builder builder = FlightClient.builder()
      .allocator(allocator)
      .location(Location.forGrpcInsecure(host, port));

    final TestConnectionProperties clientProperties = new TestConnectionProperties(
      FlightClientWrapper.FlightClientType.FlightClientWithOptions,
      host,
      port,
      user,
      password,
      null);

    final FlightClientWrapper wrapper = new FlightClientWrapper(
      allocator,
      builder.build(),
      authMode,
      options,
      clientProperties);

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
