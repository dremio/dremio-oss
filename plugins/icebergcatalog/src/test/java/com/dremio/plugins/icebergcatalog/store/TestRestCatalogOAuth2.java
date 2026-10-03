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
package com.dremio.plugins.icebergcatalog.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.core.read.ListAppender;
import com.dremio.BaseTestQuery;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.service.namespace.SourceState;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

/**
 * OAuth2 behavior of the Iceberg REST catalog source against an in-process emulation of an Iceberg
 * REST catalog with an OAuth2 token endpoint ({@code /v1/oauth/tokens}, {@code /v1/config}, {@code
 * /v1/namespaces}). Authentication is done by the Iceberg REST client itself (properties {@code
 * credential}, {@code scope}, {@code oauth2-server-uri}); these tests check what the source makes
 * of it: source state and hints, token refresh, reuse of the authenticated client by health checks,
 * and that secrets and tokens never reach messages or logs.
 */
public class TestRestCatalogOAuth2 extends BaseTestQuery {

  // Dummy, test-only values. They are used to assert that secrets never leak.
  private static final String CLIENT_ID = "test-client-id";
  private static final String CLIENT_SECRET = "test-oauth2-client-secret-0042";
  private static final String TOKEN_PREFIX = "test-issued-access-token-";
  private static final String SCOPE = "PRINCIPAL_ROLE:ALL";
  private static final String WAREHOUSE = "test_wh";
  private static final String TOKEN_EXCHANGE_GRANT =
      "urn:ietf:params:oauth:grant-type:token-exchange";

  private static final AtomicInteger SOURCE_COUNTER = new AtomicInteger();

  private FakeRestCatalogServer server;
  private final List<RestIcebergCatalogPlugin> plugins = new ArrayList<>();
  private final List<ListAppender<ILoggingEvent>> appenders = new ArrayList<>();
  private final Map<Logger, Level> previousLevels = new HashMap<>();

  @Before
  public void startServer() throws IOException {
    server = new FakeRestCatalogServer();
    captureLogs("com.dremio", "org.apache.iceberg", org.slf4j.Logger.ROOT_LOGGER_NAME);
  }

  @After
  public void stopServer() throws Exception {
    try {
      for (RestIcebergCatalogPlugin plugin : plugins) {
        plugin.close();
      }
    } finally {
      server.close();
      for (ListAppender<ILoggingEvent> appender : appenders) {
        for (Logger logger : previousLevels.keySet()) {
          logger.detachAppender(appender);
        }
      }
      for (Map.Entry<Logger, Level> e : previousLevels.entrySet()) {
        e.getKey().setLevel(e.getValue());
      }
    }
    // Every test: no secret and no issued token in any log event (DEBUG included).
    for (ListAppender<ILoggingEvent> appender : appenders) {
      for (ILoggingEvent event : appender.list) {
        assertNoSecretIn("log event", eventText(event));
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------

  private void captureLogs(String... loggerNames) {
    for (String loggerName : loggerNames) {
      Logger logger = (Logger) LoggerFactory.getLogger(loggerName);
      previousLevels.put(logger, logger.getLevel());
      if (!org.slf4j.Logger.ROOT_LOGGER_NAME.equals(loggerName)) {
        logger.setLevel(Level.DEBUG);
      }
      ListAppender<ILoggingEvent> appender = new ListAppender<>();
      appender.start();
      logger.addAppender(appender);
      appenders.add(appender);
    }
  }

  private static String eventText(ILoggingEvent event) {
    StringBuilder sb = new StringBuilder(String.valueOf(event.getFormattedMessage()));
    for (IThrowableProxy t = event.getThrowableProxy(); t != null; t = t.getCause()) {
      sb.append(" | ").append(t.getClassName()).append(": ").append(t.getMessage());
    }
    return sb.toString();
  }

  private static void assertNoSecretIn(String context, String text) {
    if (text == null) {
      return;
    }
    assertFalse(context + " leaked the client secret: " + text, text.contains(CLIENT_SECRET));
    assertFalse(context + " leaked an access token: " + text, text.contains(TOKEN_PREFIX));
    // Markers that log scanners look for (token responses, form parameters, auth headers).
    for (String marker : Arrays.asList("client_secret", "access_token", "Bearer ")) {
      assertFalse(context + " contains '" + marker + "': " + text, text.contains(marker));
    }
  }

  private static void assertNoSecretIn(SourceState state) {
    assertNoSecretIn("suggested user action", state.getSuggestedUserAction());
    for (SourceState.Message message : state.getMessages()) {
      assertNoSecretIn("source state message", message.getMessage());
    }
    assertNoSecretIn("source state", state.toString());
  }

  private RestIcebergCatalogPluginConfig newConfig() {
    RestIcebergCatalogPluginConfig conf = new RestIcebergCatalogPluginConfig();
    conf.restEndpointUri = server.baseUri();
    conf.propertyList =
        new ArrayList<>(
            Arrays.asList(
                new Property("warehouse", WAREHOUSE),
                new Property("scope", SCOPE),
                new Property("oauth2-server-uri", server.baseUri() + "/v1/oauth/tokens")));
    conf.secretPropertyList =
        new ArrayList<>(
            Collections.singletonList(new Property("credential", CLIENT_ID + ":" + CLIENT_SECRET)));
    return conf;
  }

  private static void removeProperty(RestIcebergCatalogPluginConfig conf, String name) {
    conf.propertyList.removeIf(p -> name.equals(p.name));
  }

  private static void setProperty(RestIcebergCatalogPluginConfig conf, String name, String value) {
    removeProperty(conf, name);
    conf.propertyList.add(new Property(name, value));
  }

  private static String newSourceName() {
    return "oauth2src" + SOURCE_COUNTER.incrementAndGet();
  }

  private RestIcebergCatalogPlugin newPlugin(RestIcebergCatalogPluginConfig conf, String name) {
    RestIcebergCatalogPlugin plugin =
        new RestIcebergCatalogPlugin(
            conf, getSabotContext(), name, () -> mock(StoragePluginId.class));
    plugins.add(plugin);
    return plugin;
  }

  private RestIcebergCatalogPlugin startPlugin(RestIcebergCatalogPluginConfig conf, String name)
      throws IOException {
    RestIcebergCatalogPlugin plugin = newPlugin(conf, name);
    plugin.start();
    return plugin;
  }

  private static String actionAndMessages(SourceState state) {
    StringBuilder sb = new StringBuilder(String.valueOf(state.getSuggestedUserAction()));
    for (SourceState.Message message : state.getMessages()) {
      sb.append('\n').append(message.getMessage());
    }
    return sb.toString();
  }

  private static void assertBad(SourceState state, String sourceName, String... expectedParts) {
    String text = actionAndMessages(state);
    assertEquals(text, SourceState.SourceStatus.bad, state.getStatus());
    // The suggested user action is what the source create/update API returns as its error.
    assertTrue(
        state.getSuggestedUserAction(),
        state.getSuggestedUserAction().startsWith("Could not connect to " + sourceName + ". "));
    for (String part : expectedParts) {
      assertTrue(
          "expected '" + part + "' in the suggested user action: " + state.getSuggestedUserAction(),
          state.getSuggestedUserAction().contains(part));
    }
    assertNoSecretIn(state);
  }

  // ---------------------------------------------------------------------------------------------
  // Valid credentials, scope pass-through, client reuse (G-11)
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testValidClientCredentialsAndScopeArePassedThrough() throws Exception {
    RestIcebergCatalogPlugin plugin = startPlugin(newConfig(), newSourceName());

    SourceState state = plugin.getState();

    assertEquals(actionAndMessages(state), SourceState.SourceStatus.good, state.getStatus());
    List<FakeRestCatalogServer.Request> tokenRequests =
        server.requests(r -> r.path.endsWith("/v1/oauth/tokens"));
    assertEquals(1, tokenRequests.size());
    Map<String, String> form = tokenRequests.get(0).form();
    assertEquals("client_credentials", form.get("grant_type"));
    assertEquals(CLIENT_ID, form.get("client_id"));
    assertEquals(SCOPE, form.get("scope"));
    // The configuration request carries the warehouse and the issued token.
    List<FakeRestCatalogServer.Request> configRequests =
        server.requests(r -> r.path.endsWith("/v1/config"));
    assertEquals(1, configRequests.size());
    assertEquals(WAREHOUSE, configRequests.get(0).query().get("warehouse"));
    assertTrue(configRequests.get(0).bearerToken().startsWith(TOKEN_PREFIX));
    assertEquals(1, server.requests(r -> r.path.endsWith("/v1/namespaces")).size());
  }

  @Test
  public void testHealthChecksReuseTheAuthenticatedClient() throws Exception {
    RestIcebergCatalogPlugin plugin = startPlugin(newConfig(), newSourceName());

    for (int i = 0; i < 3; i++) {
      assertEquals(SourceState.SourceStatus.good, plugin.getState().getStatus());
    }

    // One client: one token and one configuration request; each check is one listing request.
    assertEquals(1, server.requests(r -> r.path.endsWith("/v1/oauth/tokens")).size());
    assertEquals(1, server.requests(r -> r.path.endsWith("/v1/config")).size());
    assertEquals(3, server.requests(r -> r.path.endsWith("/v1/namespaces")).size());
  }

  @Test
  public void testTokenIsRefreshedBeforeItExpires() throws Exception {
    // The Iceberg client refreshes a token when 90% of its lifetime has passed (here after 1.8 s,
    // 200 ms before it expires). The emulator accepts tokens up to 2 s past their expiry (and
    // counts them as late), so that a slow or loaded machine does not fail the test; tokens later
    // than that are rejected with HTTP 401.
    server.expiresInSeconds = 2;
    server.expiryGraceMillis = 2000;
    RestIcebergCatalogPlugin plugin = startPlugin(newConfig(), newSourceName());
    assertEquals(SourceState.SourceStatus.good, plugin.getState().getStatus());
    String firstToken =
        server.requests(r -> r.path.endsWith("/v1/namespaces")).get(0).bearerToken();

    // Wait (bounded) for two refreshes: the token in use then is past the first token's lifetime.
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    while (server.requests(r -> r.path.endsWith("/v1/oauth/tokens")).size() < 3
        && System.nanoTime() < deadline) {
      Thread.sleep(100);
    }
    SourceState state = plugin.getState();

    assertEquals(actionAndMessages(state), SourceState.SourceStatus.good, state.getStatus());
    List<FakeRestCatalogServer.Request> listings =
        server.requests(r -> r.path.endsWith("/v1/namespaces"));
    String lastToken = listings.get(listings.size() - 1).bearerToken();
    assertFalse("the token was not refreshed", firstToken.equals(lastToken));
    // Refreshed by the Iceberg client's own OAuth2 session: no new client was built.
    assertEquals(1, server.requests(r -> r.path.endsWith("/v1/config")).size());
    assertEquals(0, server.rejectedExpiredTokens.get());
    assertTrue(
        "expected two token refresh requests",
        server.requests(r -> r.path.endsWith("/v1/oauth/tokens")).size() >= 3);
  }

  @Test
  public void testStaleSessionIsReplacedAfterCatalogRestart() throws Exception {
    RestIcebergCatalogPlugin plugin = startPlugin(newConfig(), newSourceName());
    assertEquals(SourceState.SourceStatus.good, plugin.getState().getStatus());

    // A catalog restart invalidates every issued token (e.g. new signing keys).
    server.revokeAllTokens();
    SourceState state = plugin.getState();

    assertEquals(actionAndMessages(state), SourceState.SourceStatus.good, state.getStatus());
    assertEquals(2, server.requests(r -> r.path.endsWith("/v1/config")).size());

    // The working client replaced the stale one: the next check needs no new client.
    assertEquals(SourceState.SourceStatus.good, plugin.getState().getStatus());
    assertEquals(2, server.requests(r -> r.path.endsWith("/v1/config")).size());
    assertEquals(2, server.requests(r -> r.path.endsWith("/v1/oauth/tokens")).size());
  }

  // ---------------------------------------------------------------------------------------------
  // Failures: state and hint (G-09)
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testWrongClientSecretGivesCredentialHint() throws Exception {
    RestIcebergCatalogPluginConfig conf = newConfig();
    conf.secretPropertyList =
        new ArrayList<>(
            Collections.singletonList(
                new Property("credential", CLIENT_ID + ":" + CLIENT_SECRET + "-wrong")));
    String name = newSourceName();
    RestIcebergCatalogPlugin plugin = startPlugin(conf, name);

    SourceState state = plugin.getState();

    // Like Apache Polaris: HTTP 401 with OAuth2 error unauthorized_client.
    assertBad(state, name, "rejected the credentials", "unauthorized_client", "'credential'");
    assertEquals(0, server.requests(r -> r.path.endsWith("/v1/config")).size());
  }

  @Test
  public void testInvalidClientGivesHttp401Hint() throws Exception {
    server.tokenErrorOverride = "invalid_client";
    String name = newSourceName();
    RestIcebergCatalogPlugin plugin = startPlugin(newConfig(), name);

    assertBad(plugin.getState(), name, "rejected the credentials (HTTP 401)", "'credential'");
  }

  @Test
  public void testUnknownScopeGivesScopeHint() throws Exception {
    RestIcebergCatalogPluginConfig conf = newConfig();
    setProperty(conf, "scope", "PRINCIPAL_ROLE:no_such_role");
    String name = newSourceName();
    RestIcebergCatalogPlugin plugin = startPlugin(conf, name);

    assertBad(plugin.getState(), name, "invalid_scope", "Check the 'scope' catalog property");
    assertEquals(
        "PRINCIPAL_ROLE:no_such_role",
        server.requests(r -> r.path.endsWith("/v1/oauth/tokens")).get(0).form().get("scope"));
  }

  @Test
  public void testMissingScopeGivesScopeHint() throws Exception {
    RestIcebergCatalogPluginConfig conf = newConfig();
    removeProperty(conf, "scope");
    String name = newSourceName();
    RestIcebergCatalogPlugin plugin = startPlugin(conf, name);

    assertBad(plugin.getState(), name, "invalid_scope", "Set the 'scope' catalog property");
  }

  @Test
  public void testMissingWarehouseGivesWarehouseHint() throws Exception {
    RestIcebergCatalogPluginConfig conf = newConfig();
    removeProperty(conf, "warehouse");
    String name = newSourceName();
    RestIcebergCatalogPlugin plugin = startPlugin(conf, name);

    assertBad(plugin.getState(), name, "'warehouse' catalog property is not set");
  }

  @Test
  public void testUnknownWarehouseGivesWarehouseHint() throws Exception {
    RestIcebergCatalogPluginConfig conf = newConfig();
    setProperty(conf, "warehouse", "no_such_wh");
    String name = newSourceName();
    RestIcebergCatalogPlugin plugin = startPlugin(conf, name);

    assertBad(plugin.getState(), name, "warehouse named by the 'warehouse' catalog property");
  }

  @Test
  public void testUnreachableCatalogGivesEndpointHint() throws Exception {
    int closedPort;
    try (ServerSocket probe = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
      closedPort = probe.getLocalPort();
    }
    String base = "http://127.0.0.1:" + closedPort + "/api/catalog";
    RestIcebergCatalogPluginConfig conf = newConfig();
    conf.restEndpointUri = base;
    setProperty(conf, "oauth2-server-uri", base + "/v1/oauth/tokens");
    String name = newSourceName();
    RestIcebergCatalogPlugin plugin = startPlugin(conf, name);

    assertBad(plugin.getState(), name, "Unable to reach the Iceberg REST catalog at " + base);
  }

  @Test
  public void testUnresponsiveCatalogGivesTimeoutHint() throws Exception {
    RestIcebergCatalogPluginConfig conf = newConfig();
    conf.propertyList.add(new Property("rest.client.socket-timeout-ms", "1000"));
    server.hangConfig = true;
    String name = newSourceName();
    RestIcebergCatalogPlugin plugin = startPlugin(conf, name);

    assertBad(plugin.getState(), name, "did not respond in time", "rest.client.socket-timeout-ms");
  }

  @Test
  public void testCatalogThatStopsRespondingIsReportedWithoutBuildingANewClient() throws Exception {
    RestIcebergCatalogPluginConfig conf = newConfig();
    conf.propertyList.add(new Property("rest.client.socket-timeout-ms", "1000"));
    String name = newSourceName();
    RestIcebergCatalogPlugin plugin = startPlugin(conf, name);
    assertEquals(SourceState.SourceStatus.good, plugin.getState().getStatus());

    server.hangNamespaces = true;
    long start = System.nanoTime();
    SourceState state = plugin.getState();
    long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

    assertBad(state, name, "did not respond in time");
    // A new client would time out as well: it is not tried, so the check takes one timeout.
    assertEquals(1, server.requests(r -> r.path.endsWith("/v1/config")).size());
    assertTrue("check took " + elapsedMillis + " ms", elapsedMillis < 5000);
  }

  @Test
  public void testServerErrorThenRecoveryWithoutRecreatingTheSource() throws Exception {
    String name = newSourceName();
    RestIcebergCatalogPlugin plugin = startPlugin(newConfig(), name);
    assertEquals(SourceState.SourceStatus.good, plugin.getState().getStatus());

    server.failCatalogRequests = true;
    assertBad(plugin.getState(), name, "server error (HTTP 5xx)");

    server.failCatalogRequests = false;
    assertEquals(SourceState.SourceStatus.good, plugin.getState().getStatus());
    // A server error is not a stale session: the cached client was kept (no new client built).
    assertEquals(1, server.requests(r -> r.path.endsWith("/v1/config")).size());
    assertEquals(1, server.requests(r -> r.path.endsWith("/v1/oauth/tokens")).size());
  }

  @Test
  public void testNamespaceListingForbidden() throws Exception {
    server.namespacesStatus = 403;
    String name = newSourceName();
    RestIcebergCatalogPlugin plugin = startPlugin(newConfig(), name);

    // Discovering every namespace needs the listing: warn, the connection itself works.
    SourceState state = plugin.getState();
    assertEquals(actionAndMessages(state), SourceState.SourceStatus.warn, state.getStatus());
    assertTrue(state.getSuggestedUserAction(), state.getSuggestedUserAction().contains("403"));
    assertNoSecretIn(state);

    // With allowed namespaces the principal may lack privileges on the catalog root.
    RestIcebergCatalogPluginConfig scoped = newConfig();
    scoped.allowedNamespaces = new ArrayList<>(Collections.singletonList("ns1"));
    RestIcebergCatalogPlugin scopedPlugin = startPlugin(scoped, newSourceName());
    assertEquals(SourceState.SourceStatus.good, scopedPlugin.getState().getStatus());
  }

  @Test
  public void testStaleSessionIsReplacedWhenTheNewClientMayNotListNamespaces() throws Exception {
    RestIcebergCatalogPlugin plugin = startPlugin(newConfig(), newSourceName());
    assertEquals(SourceState.SourceStatus.good, plugin.getState().getStatus());

    server.revokeAllTokens();
    server.namespacesStatus = 403;
    assertEquals(SourceState.SourceStatus.warn, plugin.getState().getStatus());
    assertEquals(2, server.requests(r -> r.path.endsWith("/v1/config")).size());

    // The new client authenticated, so it replaced the stale one.
    assertEquals(SourceState.SourceStatus.warn, plugin.getState().getStatus());
    assertEquals(2, server.requests(r -> r.path.endsWith("/v1/config")).size());
  }

  @Test
  public void testFailedCreateIsReportedByTheNeverStartedInstance() throws Exception {
    RestIcebergCatalogPluginConfig conf = newConfig();
    removeProperty(conf, "warehouse");
    String name = newSourceName();

    // Dremio's create flow: the started instance fails its state check and is closed, then the
    // source's initial instance (never started) is put back and its state becomes the API error.
    RestIcebergCatalogPlugin started = startPlugin(conf, name);
    SourceState failed = started.getState();
    assertBad(failed, name, "'warehouse' catalog property is not set");
    started.close();
    plugins.remove(started);

    RestIcebergCatalogPlugin initial = newPlugin(conf, name);
    assertEquals(failed, initial.getState());

    // A closed instance and an unrelated never-started source keep the generic state.
    assertFalse(
        started.getState().getSuggestedUserAction().contains("'warehouse' catalog property"));
    SourceState other = newPlugin(newConfig(), newSourceName()).getState();
    assertEquals(SourceState.SourceStatus.bad, other.getStatus());
    assertTrue(other.toString(), other.toString().contains("has not been started"));
  }

  @Test
  public void testSuccessfulCheckClearsTheRecordedFailure() throws Exception {
    String name = newSourceName();
    server.failCatalogRequests = true;
    RestIcebergCatalogPlugin plugin = startPlugin(newConfig(), name);
    assertEquals(SourceState.SourceStatus.bad, plugin.getState().getStatus());

    server.failCatalogRequests = false;
    assertEquals(SourceState.SourceStatus.good, plugin.getState().getStatus());

    SourceState initial = newPlugin(newConfig(), name).getState();
    assertTrue(initial.toString(), initial.toString().contains("has not been started"));
  }

  // ---------------------------------------------------------------------------------------------
  // In-process Iceberg REST catalog + OAuth2 token endpoint
  // ---------------------------------------------------------------------------------------------

  /**
   * Emulates the parts of an Iceberg REST catalog with a built-in OAuth2 token endpoint (like
   * Apache Polaris) that a source uses to connect: the client credentials and token exchange
   * grants, the configuration endpoint (which requires a known warehouse) and namespace listing.
   * Tokens expire after {@link #expiresInSeconds}; expired or unknown tokens get HTTP 401.
   */
  private static final class FakeRestCatalogServer implements AutoCloseable {
    private final ServerSocket serverSocket;
    private final ExecutorService pool =
        Executors.newCachedThreadPool(
            r -> {
              Thread t = new Thread(r, "fake-rest-catalog");
              t.setDaemon(true);
              return t;
            });
    private final List<Request> requests = new CopyOnWriteArrayList<>();
    private final Map<String, Long> tokenExpiryMillis = new ConcurrentHashMap<>();
    private final AtomicInteger tokenCounter = new AtomicInteger();
    private final AtomicInteger rejectedExpiredTokens = new AtomicInteger();
    private final AtomicInteger lateTokens = new AtomicInteger();

    private volatile long expiresInSeconds = 3600;

    /** How long after its expiry a token is still accepted (counted in {@link #lateTokens}). */
    private volatile long expiryGraceMillis;

    private volatile String tokenErrorOverride;
    private volatile int namespacesStatus = 200;
    private volatile boolean failCatalogRequests;
    private volatile boolean hangConfig;
    private volatile boolean hangNamespaces;

    FakeRestCatalogServer() throws IOException {
      this.serverSocket = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
      pool.execute(this::acceptLoop);
    }

    String baseUri() {
      return "http://127.0.0.1:" + serverSocket.getLocalPort() + "/api/catalog";
    }

    List<Request> requests(Predicate<Request> filter) {
      List<Request> result = new ArrayList<>();
      for (Request r : requests) {
        if (filter.test(r)) {
          result.add(r);
        }
      }
      return result;
    }

    void revokeAllTokens() {
      tokenExpiryMillis.clear();
    }

    private void acceptLoop() {
      while (!serverSocket.isClosed()) {
        try {
          Socket socket = serverSocket.accept();
          pool.execute(() -> handle(socket));
        } catch (IOException e) {
          // closed
        }
      }
    }

    private void handle(Socket socket) {
      try (Socket s = socket) {
        Request request = Request.read(s.getInputStream());
        if (request == null) {
          return;
        }
        requests.add(request);
        Response response = route(request);
        if (response == null) {
          // Hang: never answer (the client times out).
          Thread.sleep(TimeUnit.SECONDS.toMillis(30));
          return;
        }
        byte[] body = response.body.getBytes(StandardCharsets.UTF_8);
        OutputStream out = s.getOutputStream();
        String head =
            "HTTP/1.1 "
                + response.status
                + " X\r\n"
                + "Content-Type: application/json\r\n"
                + "Content-Length: "
                + body.length
                + "\r\n"
                + "Connection: close\r\n\r\n";
        out.write(head.getBytes(StandardCharsets.US_ASCII));
        out.write(body);
        out.flush();
      } catch (IOException | InterruptedException e) {
        // client went away or server closed
      }
    }

    private Response route(Request request) {
      if (request.path.endsWith("/v1/oauth/tokens")) {
        return token(request.form());
      }
      if (failCatalogRequests) {
        return error(500, "ServiceFailureException", "Internal server error");
      }
      if (!isValid(request.bearerToken())) {
        return error(401, "NotAuthorizedException", "Not authorized: invalid or expired token");
      }
      if (request.path.endsWith("/v1/config")) {
        if (hangConfig) {
          return null;
        }
        String warehouse = request.query().get("warehouse");
        if (warehouse == null) {
          return error(400, "BadRequestException", "Please specify a warehouse");
        }
        if (!WAREHOUSE.equals(warehouse)) {
          return error(404, "NotFoundException", "Unable to find warehouse " + warehouse);
        }
        return new Response(200, "{\"defaults\":{},\"overrides\":{}}");
      }
      if (request.path.endsWith("/v1/namespaces")) {
        if (hangNamespaces) {
          return null;
        }
        if (namespacesStatus == 403) {
          return error(
              403, "ForbiddenException", "Principal is not authorized for op LIST_NAMESPACES");
        }
        return new Response(200, "{\"namespaces\":[[\"ns1\"]]}");
      }
      return error(404, "NotFoundException", "No route for " + request.path);
    }

    private Response token(Map<String, String> form) {
      if (tokenErrorOverride != null) {
        return oauthError(401, tokenErrorOverride, "Client authentication failed");
      }
      String grantType = form.get("grant_type");
      if ("client_credentials".equals(grantType)) {
        if (!CLIENT_ID.equals(form.get("client_id"))
            || !CLIENT_SECRET.equals(form.get("client_secret"))) {
          return oauthError(401, "unauthorized_client", "The client is not authorized");
        }
      } else if (TOKEN_EXCHANGE_GRANT.equals(grantType)) {
        if (!isValid(form.get("subject_token"))) {
          return oauthError(400, "invalid_grant", "The subject token is invalid");
        }
      } else {
        return oauthError(400, "unsupported_grant_type", "Unsupported grant type");
      }
      String scope = form.get("scope");
      if (scope == null || !scope.startsWith("PRINCIPAL_ROLE:") || scope.contains("no_such")) {
        return oauthError(400, "invalid_scope", "The scope is invalid");
      }
      String token = TOKEN_PREFIX + tokenCounter.incrementAndGet();
      tokenExpiryMillis.put(
          token, System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(expiresInSeconds));
      return new Response(
          200,
          "{\"access_token\":\""
              + token
              + "\",\"token_type\":\"bearer\",\"issued_token_type\":"
              + "\"urn:ietf:params:oauth:token-type:access_token\",\"expires_in\":"
              + expiresInSeconds
              + "}");
    }

    private boolean isValid(String token) {
      if (token == null) {
        return false;
      }
      Long expiry = tokenExpiryMillis.get(token);
      if (expiry == null) {
        return false;
      }
      long now = System.currentTimeMillis();
      if (now >= expiry + expiryGraceMillis) {
        rejectedExpiredTokens.incrementAndGet();
        return false;
      }
      if (now >= expiry) {
        lateTokens.incrementAndGet();
      }
      return true;
    }

    private static Response error(int status, String type, String message) {
      return new Response(
          status,
          "{\"error\":{\"message\":\""
              + message
              + "\",\"type\":\""
              + type
              + "\",\"code\":"
              + status
              + "}}");
    }

    private static Response oauthError(int status, String error, String description) {
      return new Response(
          status, "{\"error\":\"" + error + "\",\"error_description\":\"" + description + "\"}");
    }

    @Override
    public void close() throws Exception {
      serverSocket.close();
      pool.shutdownNow();
      pool.awaitTermination(5, TimeUnit.SECONDS);
    }

    private static final class Response {
      private final int status;
      private final String body;

      Response(int status, String body) {
        this.status = status;
        this.body = body;
      }
    }

    /** A received request (method, path, query, headers, body). */
    static final class Request {
      private final String method;
      private final String path;
      private final String rawQuery;
      private final Map<String, String> headers;
      private final String body;

      private Request(
          String method, String path, String rawQuery, Map<String, String> headers, String body) {
        this.method = method;
        this.path = path;
        this.rawQuery = rawQuery;
        this.headers = headers;
        this.body = body;
      }

      String bearerToken() {
        String auth = headers.get("authorization");
        if (auth == null || !auth.toLowerCase(Locale.ROOT).startsWith("bearer ")) {
          return null;
        }
        return auth.substring("bearer ".length()).trim();
      }

      Map<String, String> query() {
        return parseForm(rawQuery);
      }

      Map<String, String> form() {
        return "POST".equals(method) ? parseForm(body) : Collections.emptyMap();
      }

      private static Map<String, String> parseForm(String encoded) {
        Map<String, String> result = new HashMap<>();
        if (encoded == null || encoded.isEmpty()) {
          return result;
        }
        for (String pair : encoded.split("&")) {
          int idx = pair.indexOf('=');
          String key = idx < 0 ? pair : pair.substring(0, idx);
          String value = idx < 0 ? "" : pair.substring(idx + 1);
          result.put(decode(key), decode(value));
        }
        return result;
      }

      private static String decode(String s) {
        try {
          return URLDecoder.decode(s, "UTF-8");
        } catch (java.io.UnsupportedEncodingException e) {
          throw new IllegalStateException(e);
        }
      }

      static Request read(InputStream in) throws IOException {
        ByteArrayOutputStream head = new ByteArrayOutputStream();
        int matched = 0;
        byte[] terminator = {'\r', '\n', '\r', '\n'};
        while (matched < terminator.length) {
          int b = in.read();
          if (b < 0) {
            return null;
          }
          head.write(b);
          matched = (b == terminator[matched]) ? matched + 1 : (b == '\r' ? 1 : 0);
        }
        String[] lines = new String(head.toByteArray(), StandardCharsets.US_ASCII).split("\r\n");
        String[] requestLine = lines[0].split(" ");
        String target = requestLine[1];
        int q = target.indexOf('?');
        String path = q < 0 ? target : target.substring(0, q);
        String rawQuery = q < 0 ? "" : target.substring(q + 1);
        Map<String, String> headers = new HashMap<>();
        for (int i = 1; i < lines.length; i++) {
          int idx = lines[i].indexOf(':');
          if (idx > 0) {
            headers.put(
                lines[i].substring(0, idx).trim().toLowerCase(Locale.ROOT),
                lines[i].substring(idx + 1).trim());
          }
        }
        int length = Integer.parseInt(headers.getOrDefault("content-length", "0"));
        byte[] body = new byte[length];
        int read = 0;
        while (read < length) {
          int n = in.read(body, read, length - read);
          if (n < 0) {
            break;
          }
          read += n;
        }
        return new Request(
            requestLine[0],
            path,
            rawQuery,
            headers,
            new String(body, 0, read, StandardCharsets.UTF_8));
      }
    }
  }
}
