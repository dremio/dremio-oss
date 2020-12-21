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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.Enumeration;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.auth.BasicServerAuthHandler;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.junit.AfterClass;

import com.dremio.BaseTestQuery;
import com.dremio.common.AutoCloseables;
import com.dremio.config.DremioConfig;
import com.dremio.datastore.adapter.LegacyKVStoreProviderAdapter;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.rpc.ssl.SSLConfigurator;
import com.dremio.exec.work.protector.UserWorker;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValue;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.flight.impl.FlightWorkManager.RunQueryResponseHandlerFactory;
import com.dremio.service.tokens.TokenDetails;
import com.dremio.service.tokens.TokenManager;
import com.dremio.service.users.SimpleUser;
import com.dremio.service.users.SimpleUserService;
import com.dremio.service.users.User;
import com.dremio.service.users.UserService;
import com.dremio.ssl.SSLConfig;
import com.dremio.test.DremioTest;
import com.google.inject.AbstractModule;
import com.google.inject.util.Providers;

/**
 * Base class for testing result set correctness from the Flight endpoint.
 */
public class BaseFlightQueryTest extends BaseTestQuery {

  protected static final String DUMMY_USER = "dummy_user";
  protected static final String DUMMY_PASSWORD = "dummy_password123";
  protected static final String DUMMY_TOKEN = "dummy_token";

  private static LegacyKVStoreProvider kvStore;

  private static DremioFlightService flightService;
  private static FlightClientUtils.FlightClientWrapper flightClientWrapper;
  private static int flightServicePort;
  private static SSLConfig sslConfig;
  private static DremioConfig dremioConfig;
  private static String authMode;

  public static void setupBaseFlightQueryTest(boolean tls, boolean backpressureHandling,
                                              String portPomFileSystemProperty,
                                              RunQueryResponseHandlerFactory runQueryResponseHandlerFactory)
    throws Exception {
    setupBaseFlightQueryTest(
      tls,
      backpressureHandling,
      portPomFileSystemProperty,
      runQueryResponseHandlerFactory,
      DremioFlightService.FLIGHT_AUTH2_AUTH_MODE);
  }

  public static void setupBaseFlightQueryTest(boolean tls, boolean backpressureHandling,
                                              String portPomFileSystemProperty,
                                              RunQueryResponseHandlerFactory runQueryResponseHandlerFactory,
                                              String serverAuthMode) throws Exception {
    flightServicePort = Integer.getInteger(portPomFileSystemProperty, 32010);
    authMode = serverAuthMode;
    setupDefaultTestCluster(backpressureHandling);
    if (tls) {
      createEncryptedFlightService(runQueryResponseHandlerFactory);
      // Encryption tests explicitly start/stop clients.
    } else {
      createFlightService(runQueryResponseHandlerFactory);
    }
  }

  public static void setupDefaultTestCluster(boolean backpressureHandling) throws Exception {
    kvStore = LegacyKVStoreProviderAdapter.inMemory(DremioTest.CLASSPATH_SCAN_RESULT);
    kvStore.start();

    // Create a mock TokenManager that returns a fixed token and user session
    final TokenManager tokenManager = mock(TokenManager.class);
    final TokenDetails fixedTokenDetails = TokenDetails.of(DUMMY_TOKEN, DUMMY_USER,
      System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert(200, TimeUnit.HOURS));
    when(tokenManager.createToken(any(), any())).thenReturn(fixedTokenDetails);
    when(tokenManager.validateToken(any())).thenReturn(fixedTokenDetails);

    // Create a user service with a dummy user.
    final UserService userService = new SimpleUserService(() -> kvStore);
    final User user = SimpleUser.newBuilder()
      .setUserName(DUMMY_USER).setFirstName("Dummy").setLastName("User")
      .setCreatedAt(System.currentTimeMillis()).setEmail("dummy_user@dremio.com").build();
    userService.createUser(user, DUMMY_PASSWORD);

    getSabotContext().getOptionManager().setOption(
      OptionValue.createBoolean(OptionValue.OptionType.SYSTEM, DremioFlightServiceOptions.ENABLE_BACKPRESSURE_HANDLING.getOptionName(), backpressureHandling)
    );

    SABOT_NODE_RULE.register(new AbstractModule() {
      @Override
      protected void configure() {
        bind(UserService.class).toInstance(userService);
        bind(TokenManager.class).toInstance(tokenManager);
      }
    });

    // Update default properties for the UserRPC test client to connect as the dummy user.
    defaultProperties = new Properties();
    defaultProperties.setProperty(UserSession.USER, DUMMY_USER);
    defaultProperties.setProperty(UserSession.PASSWORD, DUMMY_PASSWORD);

    BaseTestQuery.setupDefaultTestCluster();
  }

  public static void createFlightService(RunQueryResponseHandlerFactory runQueryResponseHandlerFactory) throws Exception {
    dremioConfig = DremioConfig.create(null, config)
      .withValue(DremioConfig.FLIGHT_SERVICE_ENABLED_BOOLEAN, true)
      .withValue(DremioConfig.FLIGHT_SERVICE_PORT_INT, flightServicePort)
      .withValue(DremioConfig.FLIGHT_SERVICE_AUTHENTICATION_MODE, authMode);

    flightService = new DremioFlightService(
      Providers.of(dremioConfig),
      Providers.of(getSabotContext().getAllocator()),
      getBindingProvider().provider(UserService.class),
      getBindingProvider().provider(UserWorker.class),
      Providers.of(getSabotContext()),
      getBindingProvider().provider(TokenManager.class),
      getBindingProvider().provider(OptionManager.class),
      runQueryResponseHandlerFactory);

    flightService.start();
    flightClientWrapper =
      FlightClientUtils.openFlightClient(flightServicePort, DUMMY_USER, DUMMY_PASSWORD,
        getSabotContext(), authMode);
  }

  protected void assertThatServerStartedOnPort() {
    assertEquals(flightServicePort, flightService.getFlightServer().getPort());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    AutoCloseables.close(flightClientWrapper, flightService, kvStore);
    flightClientWrapper = null;
    flightService = null;
    kvStore = null;
    closeClient();
    resetNodeCount();
  }

  protected FlightTestBuilder flightTestBuilder() {
    return new FlightTestBuilder(getFlightClientWrapper());
  }

  protected FlightClientUtils.FlightClientWrapper getFlightClientWrapper() {
    if (null == flightClientWrapper) {
      fail("FlightClient is not open, is this an encryption test? Call #openEncryptedFlightClient");
    }
    return flightClientWrapper;
  }

  /**
   * Opens a client which the test is responsible for closing.
   */
  protected static FlightClient openFlightClient(String user, String password, String authMode) throws Exception {
    return FlightClientUtils.openFlightClient(flightServicePort, user, password, getSabotContext(),
      authMode).getClient();
  }

  protected static FlightClient openEncryptedFlightClient(String user, String password,
                                                          InputStream trustedCerts, String authMode) throws Exception {
    return FlightClientUtils.openEncryptedFlightClient(dremioConfig.getThisNode(), flightServicePort, user, password,
      trustedCerts, BaseTestQuery.getSabotContext(), authMode).getClient();
  }

  private static void createEncryptedFlightService(RunQueryResponseHandlerFactory runQueryResponseHandlerFactory) throws Exception {
    dremioConfig = DremioConfig.create(null, BaseTestQuery.config)
      .withValue(DremioFlightService.FLIGHT_SSL_ENABLED, true)
      .withValue(DremioConfig.FLIGHT_SERVICE_ENABLED_BOOLEAN, true)
      .withValue(DremioConfig.FLIGHT_SERVICE_PORT_INT, flightServicePort)
      .withValue(DremioConfig.FLIGHT_SERVICE_AUTHENTICATION_MODE, authMode);

    flightService = new DremioFlightService(
      Providers.of(dremioConfig),
      Providers.of(BaseTestQuery.getSabotContext().getAllocator()),
      BaseTestQuery.getBindingProvider().provider(UserService.class),
      BaseTestQuery.getBindingProvider().provider(UserWorker.class),
      Providers.of(BaseTestQuery.getSabotContext()),
      BaseTestQuery.getBindingProvider().provider(TokenManager.class),
      BaseTestQuery.getBindingProvider().provider(OptionManager.class),
      runQueryResponseHandlerFactory) {
      @Override
      protected BasicServerAuthHandler.BasicAuthValidator createBasicAuthValidator(javax.inject.Provider<UserService> userServiceProvider,
                                                                                   javax.inject.Provider<TokenManager> tokenManagerProvider,
                                                                                   DremioFlightSessionsManager dremioFlightSessionsManager) {
        return new BasicServerAuthHandler.BasicAuthValidator() {
          @Override
          public byte[] getToken(String user, String password) {
            return new byte[0];
          }

          @Override
          public Optional<String> isValid(byte[] bytes) {
            return Optional.of(DUMMY_USER);
          }
        };
      }

      @Override
      protected SSLConfig getSSLConfig(DremioConfig config, SSLConfigurator sslConfigurator) {
        try {
          // Explicitly add "localhost" as a hostname certificate for testing purposes.
          sslConfig = sslConfigurator.getSSLConfig(true,
            config.getThisNode(), InetAddress.getLocalHost().getCanonicalHostName(), "localhost").get();
          return sslConfig;
        } catch (GeneralSecurityException | IOException ex) {
          throw new RuntimeException(ex);
        }
      }
    };

    flightService.start();
  }

  // We must inherit from BaseEnterpriseTestQuery to access enterprise-only properties
  // from DremioConfig.
  protected static InputStream getCertificateStream() throws Exception {
    final KeyStore keyStore = KeyStore.getInstance(sslConfig.getKeyStoreType());
    try (final InputStream keyStoreStream = Files.newInputStream(Paths.get(sslConfig.getKeyStorePath()))) {
      keyStore.load(keyStoreStream, sslConfig.getKeyStorePassword().toCharArray());
    }

    final Enumeration<String> aliases = keyStore.aliases();
    while (aliases.hasMoreElements()) {
      final String alias = aliases.nextElement();
      if (keyStore.isKeyEntry(alias)) {
        final Certificate[] certificates = keyStore.getCertificateChain(alias);
        return toInputStream(certificates);
      }
    }
    throw new RuntimeException("Keystore did not have a private key.");
  }

  private static InputStream toInputStream(Certificate[] certificates) throws IOException {
    try (final StringWriter writer = new StringWriter();
         final JcaPEMWriter pemWriter = new JcaPEMWriter(writer)) {
      for (Certificate certificate : certificates) {
        pemWriter.writeObject(certificate);
      }
      pemWriter.flush();
      return new ByteArrayInputStream(writer.toString().getBytes(StandardCharsets.UTF_8));
    }
  }
}
