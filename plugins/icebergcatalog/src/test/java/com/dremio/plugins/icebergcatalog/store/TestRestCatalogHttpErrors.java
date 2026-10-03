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

import static com.dremio.exec.catalog.conf.ConnectionConf.USE_EXISTING_SECRET_VALUE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.core.read.ListAppender;
import com.dremio.BaseTestQuery;
import com.dremio.common.exceptions.UserException;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
import com.dremio.service.namespace.SourceState;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.base.Strings;
import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

/**
 * Error handling and secret hygiene of the RESTCATALOG source against a local HTTP(S) server that
 * emulates an Iceberg REST catalog (token endpoint, {@code v1/config}, namespaces and tables).
 *
 * <p>For every failure the tests check that the source state is {@code bad} with a usable message
 * and that no configured secret (OAuth2 client secret, S3 keys, session token) nor the access token
 * issued by the server appears in the state, in thrown exceptions or in any log event captured at
 * DEBUG (Dremio, Iceberg and Apache HttpClient loggers; HttpClient wire/header loggers are kept at
 * INFO, as in the distribution logback.xml).
 *
 * <p>Each scenario logs one {@code P3-MATRIX} line with the observed (already redacted) message.
 */
public class TestRestCatalogHttpErrors extends BaseTestQuery {
  private static final org.slf4j.Logger logger =
      LoggerFactory.getLogger(TestRestCatalogHttpErrors.class);

  // Dummy, test-only values. They are used to assert that secrets never leak.
  private static final String CLIENT_ID = "p3-client-id";
  private static final String CLIENT_SECRET = "p3-client-secret-value-0101";
  private static final String FULL_CREDENTIAL = CLIENT_ID + ":" + CLIENT_SECRET;
  private static final String S3_ACCESS = "p3-s3-access-value-0102";
  private static final String S3_SECRET = "p3-s3-secret-value-0103";
  private static final String S3_SESSION = "p3-s3-session-token-value-0104";

  /** Issued by the fake token endpoint; it is not part of the source config, so never redacted. */
  private static final String ACCESS_TOKEN = "p3-access-token-value-0105";

  private static final List<String> SECRETS =
      Arrays.asList(CLIENT_SECRET, S3_ACCESS, S3_SECRET, S3_SESSION, ACCESS_TOKEN);

  private static final String SOURCE_NAME = "polaris";
  private static final String TOKEN_PATH = "/v1/oauth/tokens";
  private static final String CONFIG_PATH = "/v1/config";
  private static final String TABLE_PATH = "/v1/namespaces/ns1/tables/t1";
  private static final String NAMESPACES_PATH = "/v1/namespaces";

  private StoragePluginId storagePluginId;
  private LogCapture logs;

  @Before
  public void setUp() {
    storagePluginId = mock(StoragePluginId.class);
    logs = new LogCapture();
  }

  @After
  public void tearDown() {
    logs.close();
  }

  // ---------------------------------------------------------------------------------------------
  // getState(): HTTP status of GET v1/config (the token request succeeds)
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testConfig400() throws Exception {
    String msg = configFailure(400, "BadRequestException");
    assertThat(msg).contains("HTTP 400").contains("Malformed request").contains(marker(400));
  }

  @Test
  public void testConfig401() throws Exception {
    String msg = configFailure(401, "NotAuthorizedException");
    assertThat(msg).contains("HTTP 401").contains("Not authorized").contains(marker(401));
  }

  @Test
  public void testConfig403() throws Exception {
    String msg = configFailure(403, "ForbiddenException");
    assertThat(msg).contains("HTTP 403").contains("Forbidden").contains(marker(403));
  }

  @Test
  public void testConfig404() throws Exception {
    // Wrong path prefix in restEndpointUri or unknown warehouse.
    String msg = configFailure(404, "NotFoundException");
    assertThat(msg).contains("Unable to process").contains(marker(404));
  }

  @Test
  public void testConfig409() throws Exception {
    String msg = configFailure(409, "AlreadyExistsException");
    assertThat(msg).contains("Unable to process").contains(marker(409));
  }

  @Test
  public void testConfig500IsNotRetried() throws Exception {
    try (FakeRestCatalogServer server = FakeRestCatalogServer.http()) {
      server.route("GET", CONFIG_PATH, Response.error(500, "ServerError", echoMessage(500)));
      String msg = badStateMessage(server, "500", "rest.client.max-retries", "3");
      assertThat(msg).contains("Server error").contains(marker(500));
      assertEquals("500 is not retried", 1, server.count("GET", CONFIG_PATH));
    }
  }

  @Test
  public void testConfig502HtmlBodyIsRetried() throws Exception {
    // Reverse proxies answer 502 with an HTML page, not an Iceberg ErrorResponse.
    try (FakeRestCatalogServer server = FakeRestCatalogServer.http()) {
      server.route(
          "GET",
          CONFIG_PATH,
          new Response(502, "text/html", "<html><body>502 Bad Gateway</body></html>"));
      String msg = badStateMessage(server, "502 (html)", "rest.client.max-retries", "2");
      assertThat(msg).isNotBlank();
      assertEquals("502 is retried max-retries times", 3, server.count("GET", CONFIG_PATH));
    }
  }

  @Test
  public void testConfig503IsRetried() throws Exception {
    try (FakeRestCatalogServer server = FakeRestCatalogServer.http()) {
      server.route(
          "GET",
          CONFIG_PATH,
          Response.error(503, "ServiceUnavailableException", echoMessage(503))
              .withHeader("Retry-After", "1"));
      String msg = badStateMessage(server, "503", "rest.client.max-retries", "2");
      assertThat(msg).contains("Service unavailable").contains(marker(503));
      assertEquals("503 is retried max-retries times", 3, server.count("GET", CONFIG_PATH));
    }
  }

  @Test
  public void testConfig429HonorsRetryAfterWithDefaultRetries() throws Exception {
    // No rest.client.max-retries: the Iceberg default (5) applies and Retry-After is honored.
    try (FakeRestCatalogServer server = FakeRestCatalogServer.http()) {
      server.route(
          "GET",
          CONFIG_PATH,
          Response.error(429, "TooManyRequestsException", echoMessage(429))
              .withHeader("Retry-After", "1"));
      long start = System.nanoTime();
      String msg = badStateMessage(server, "429 (default retries)");
      long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
      assertThat(msg).contains("Unable to process").contains(marker(429));
      assertEquals("429 is retried 5 times by default", 6, server.count("GET", CONFIG_PATH));
      assertTrue("Retry-After: 1 must be honored, took " + elapsedMs, elapsedMs >= 4500);
      logger.info("P3-MATRIX 429 default retries: 6 requests in {} ms", elapsedMs);
    }
  }

  // ---------------------------------------------------------------------------------------------
  // getState(): failures of the OAuth2 token request
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testToken401InvalidClient() throws Exception {
    try (FakeRestCatalogServer server = FakeRestCatalogServer.http()) {
      server.route(
          "POST", TOKEN_PATH, Response.oauthError(401, "invalid_client", echoMessage(401)));
      String msg = badStateMessage(server, "token 401 invalid_client");
      assertThat(msg).contains("HTTP 401").contains("invalid_client").contains("****");
      assertEquals("no catalog call without a token", 0, server.count("GET", CONFIG_PATH));
    }
  }

  @Test
  public void testToken400InvalidScope() throws Exception {
    try (FakeRestCatalogServer server = FakeRestCatalogServer.http()) {
      server.route("POST", TOKEN_PATH, Response.oauthError(400, "invalid_scope", echoMessage(400)));
      String msg = badStateMessage(server, "token 400 invalid_scope");
      // The OAuth2 error code is more specific than the HTTP status here.
      assertThat(msg).contains("invalid_scope").contains("'scope'").contains("echo ****");
    }
  }

  @Test
  public void testToken503PostIsRetried() throws Exception {
    // The retry strategy retries 429/502/503/504 for every method, including the token POST.
    try (FakeRestCatalogServer server = FakeRestCatalogServer.http()) {
      server.route(
          "POST",
          TOKEN_PATH,
          Response.oauthError(503, "temporarily_unavailable", echoMessage(503))
              .withHeader("Retry-After", "1"));
      String msg = badStateMessage(server, "token 503", "rest.client.max-retries", "1");
      assertThat(msg).isNotBlank();
      assertEquals("token POST is retried", 2, server.count("POST", TOKEN_PATH));
    }
  }

  // ---------------------------------------------------------------------------------------------
  // getState(): network level failures
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testReadTimeoutUsesSocketTimeoutProperty() throws Exception {
    try (FakeRestCatalogServer server = FakeRestCatalogServer.http()) {
      server.route("GET", CONFIG_PATH, Response.config().withDelayMs(5000));
      long start = System.nanoTime();
      String msg = badStateMessage(server, "read timeout", "rest.client.socket-timeout-ms", "500");
      long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
      assertThat(msg).contains("127.0.0.1:" + server.port());
      assertThat(msg.toLowerCase(Locale.ROOT)).contains("timed out");
      assertTrue("socket timeout must apply, took " + elapsedMs, elapsedMs < 4500);
      assertEquals("read timeouts are not retried", 1, server.count("GET", CONFIG_PATH));
    }
  }

  @Test
  public void testConnectionRefused() throws Exception {
    int closedPort;
    try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
      closedPort = socket.getLocalPort();
    }
    String base = "http://127.0.0.1:" + closedPort + "/api/catalog";
    String msg = badStateMessage(base, "connection refused");
    assertThat(msg).contains("Unable to reach").contains("127.0.0.1:" + closedPort);
  }

  @Test
  public void testUntrustedSelfSignedCertificate() throws Exception {
    try (FakeRestCatalogServer server = FakeRestCatalogServer.https(selfSignedContext())) {
      String msg = badStateMessage(server, "TLS self-signed");
      assertThat(msg).isNotBlank();
      // The handshake fails before any HTTP request (and so before the credential) is sent.
      assertEquals(0, server.requests.size());
      awaitPositive("handshake was attempted", server.tlsFailures);
    }
  }

  @Test
  public void testHttpsEndpointOnPlainHttpServer() throws Exception {
    try (FakeRestCatalogServer server = FakeRestCatalogServer.http()) {
      String base = "https://127.0.0.1:" + server.port() + "/api/catalog";
      String msg = badStateMessage(base, "https scheme on http server");
      assertThat(msg).isNotBlank();
      assertEquals(0, server.requests.size());
      awaitPositive("client spoke TLS", server.tlsHellosOnPlainSocket);
    }
  }

  /**
   * HTTP errors the Iceberg client does not classify (it keeps no status code for them) get a hint
   * that lists the likely statuses and the retry property; 503 and TLS failures get their own hint.
   */
  @Test
  public void testUnclassifiedHttpErrorsGetAStatusHint() throws Exception {
    for (int code : new int[] {404, 409, 429, 502}) {
      try (FakeRestCatalogServer server = FakeRestCatalogServer.http()) {
        server.route("GET", CONFIG_PATH, Response.error(code, "Error", echoMessage(code)));
        assertThat(badStateMessage(server, "unclassified " + code, "rest.client.max-retries", "1"))
            .contains("unexpected HTTP error")
            .contains("'rest.client.max-retries'")
            .contains(marker(code))
            .contains("echo ****");
      }
    }
  }

  @Test
  public void testConfig503NamesTheStatusAndTheRetries() throws Exception {
    try (FakeRestCatalogServer server = FakeRestCatalogServer.http()) {
      server.route(
          "GET", CONFIG_PATH, Response.error(503, "ServiceUnavailableException", echoMessage(503)));
      assertThat(badStateMessage(server, "503 hint", "rest.client.max-retries", "1"))
          .contains("HTTP 503")
          .contains("'rest.client.max-retries'")
          .contains(marker(503));
    }
  }

  @Test
  public void testHtmlErrorPageIsStrippedFromTheMessage() throws Exception {
    try (FakeRestCatalogServer server = FakeRestCatalogServer.http()) {
      server.route(
          "GET",
          CONFIG_PATH,
          new Response(
              502,
              "text/html",
              "<html><head><title>502</title></head><body><h1>502 Bad Gateway</h1>"
                  + "</body></html>"));
      String msg = badStateMessage(server, "502 html hint", "rest.client.max-retries", "1");
      assertThat(msg).contains("502 Bad Gateway").doesNotContain("<html>").doesNotContain("<h1>");
    }
  }

  @Test
  public void testTlsFailuresGetATlsHint() throws Exception {
    try (FakeRestCatalogServer server = FakeRestCatalogServer.https(selfSignedContext())) {
      assertThat(badStateMessage(server, "TLS hint self-signed"))
          .contains("TLS connection")
          .contains("certificate")
          .contains("javax.net.ssl.trustStore");
    }
    try (FakeRestCatalogServer server = FakeRestCatalogServer.http()) {
      String base = "https://127.0.0.1:" + server.port() + "/api/catalog";
      assertThat(badStateMessage(base, "TLS hint scheme"))
          .contains("TLS connection")
          .contains("scheme");
    }
  }

  @Test
  public void testLongServerMessagesAreAbbreviated() {
    String detail = "<p>" + Strings.repeat("x", 1000) + "</p>";
    String abbreviated = RestIcebergCatalogPlugin.abbreviateDetail(detail);
    assertThat(abbreviated).startsWith("xxx").endsWith("...").doesNotContain("<p>");
    assertThat(abbreviated.length()).isLessThanOrEqualTo(303);
    assertEquals("a b c", RestIcebergCatalogPlugin.abbreviateDetail("<b>a</b>\n  b\t<i>c</i>"));
  }

  // ---------------------------------------------------------------------------------------------
  // Healthy catalog, failing operations (raw Iceberg exceptions reach the caller)
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testHealthyCatalogIsGoodAndSendsBearerToken() throws Exception {
    try (FakeRestCatalogServer server = FakeRestCatalogServer.http()) {
      RestIcebergCatalogPlugin plugin = startPlugin(server.baseUri());
      try {
        SourceState state = plugin.getState();
        assertEquals(SourceState.SourceStatus.good, state.getStatus());
      } finally {
        plugin.close();
      }
      Request config = server.find("GET", CONFIG_PATH);
      assertNotNull(config);
      assertEquals("Bearer " + ACCESS_TOKEN, config.headers.get("authorization"));
      assertThat(config.path).contains("warehouse=wh");
      Request token = server.find("POST", TOKEN_PATH);
      assertNotNull(token);
      assertThat(token.body).contains("grant_type=client_credentials").contains("scope=");
    }
    logs.assertNoSecrets();
  }

  @Test
  public void testTableExists403IsRaisedWithoutSecrets() throws Exception {
    // The server message echoes the secrets: the mapped error copies it, redacted.
    Throwable thrown = datasetHandleFailure(403, "ForbiddenException", echoMessage(403));
    assertThat(chainText(thrown)).contains(marker(403));
    // Mapped to a permission error with a hint instead of the raw Iceberg exception.
    assertThat(thrown).isInstanceOf(UserException.class);
    assertEquals(ErrorType.PERMISSION, ((UserException) thrown).getErrorType());
    assertThat(thrown.getMessage())
        .contains("denied the request to look up [ns1.t1]")
        .contains("echo ****")
        .contains("privileges");
  }

  @Test
  public void testTableExists401IsRaisedWithoutSecrets() throws Exception {
    Throwable thrown = datasetHandleFailure(401, "NotAuthorizedException", echoMessage(401));
    assertThat(chainText(thrown)).contains(marker(401));
    assertThat(thrown).isInstanceOf(UserException.class);
    assertEquals(ErrorType.PERMISSION, ((UserException) thrown).getErrorType());
    assertThat(thrown.getMessage())
        .contains("rejected the credentials")
        .contains("echo ****")
        .contains("'credential'");
  }

  @Test
  public void testTableExists500IsAConnectionErrorWithHint() throws Exception {
    Throwable thrown = datasetHandleFailure(500, "ServerError", echoMessage(500));
    assertThat(chainText(thrown)).contains(marker(500));
    assertRequestFailed(
        thrown,
        "reported a server error (HTTP 500) for the request to look up [ns1.t1]",
        "echo ****",
        RestCatalogExceptionMapper.RETRY_HINT);
  }

  @Test
  public void testTableExists429IsAConnectionErrorWithHint() throws Exception {
    Throwable thrown = datasetHandleFailure(429, "TooManyRequestsException", echoMessage(429));
    assertThat(chainText(thrown)).contains(marker(429));
    assertRequestFailed(
        thrown,
        "returned an unexpected HTTP error for the request to look up [ns1.t1]",
        "echo ****",
        "429 (Too Many Requests)",
        "'rest.client.max-retries'");
  }

  @Test
  public void testTableExists503IsAConnectionErrorWithHint() throws Exception {
    Throwable thrown = datasetHandleFailure(503, "ServiceUnavailableException", echoMessage(503));
    assertThat(chainText(thrown)).contains(marker(503));
    assertRequestFailed(
        thrown,
        "is unavailable (HTTP 503) for the request to look up [ns1.t1]",
        "echo ****",
        RestCatalogExceptionMapper.RETRIED_NOTE,
        RestCatalogExceptionMapper.RETRY_HINT);
  }

  @Test
  public void testTableExists502HtmlPageIsAConnectionErrorWithHint() throws Exception {
    Throwable thrown =
        datasetHandleFailure(
            "502 (html)",
            new Response(
                502, "text/html", "<html><body>" + marker(502) + " Bad Gateway</body></html>"),
            "rest.client.max-retries",
            "1");
    assertRequestFailed(
        thrown,
        "returned an unexpected HTTP error for the request to look up [ns1.t1]",
        "502/504 from a proxy");
    assertThat(thrown.getMessage()).doesNotContain("<html>");
  }

  @Test
  public void testTableExistsReadTimeoutIsAConnectionErrorWithHint() throws Exception {
    Throwable thrown =
        datasetHandleFailure(
            "read timeout",
            Response.json(200, "{}").withDelayMs(3000),
            "rest.client.socket-timeout-ms",
            "300");
    assertRequestFailed(
        thrown,
        "did not respond in time to the request to look up [ns1.t1]",
        "timed out",
        "'rest.client.socket-timeout-ms'");
  }

  @Test
  public void testTableExistsDroppedConnectionIsAConnectionErrorWithHint() throws Exception {
    Throwable thrown =
        datasetHandleFailure(
            "connection dropped", Response.dropConnection(), "rest.client.max-retries", "1");
    assertRequestFailed(
        thrown,
        "connection to the Iceberg REST catalog failed during the request to look up [ns1.t1]",
        RestCatalogExceptionMapper.CONNECTION_RETRIED_NOTE);
  }

  @Test
  public void testTableExistsOnStoppedCatalogIsAConnectionErrorWithHint() throws Exception {
    RestIcebergCatalogPlugin plugin;
    try (FakeRestCatalogServer server = FakeRestCatalogServer.http()) {
      // Starts against a healthy catalog, which then stops (connection refused).
      plugin = startPlugin(server.baseUri(), "rest.client.max-retries", "1");
    }
    Throwable thrown = null;
    try {
      plugin.getDatasetHandle(new EntityPath(Arrays.asList(SOURCE_NAME, "ns1", "t1")));
      fail("expected a failure for a stopped catalog");
    } catch (RuntimeException e) {
      thrown = e;
    } finally {
      plugin.close();
    }
    logger.info("P3-MATRIX tableExists stopped catalog -> {}", thrown.getMessage());
    assertRequestFailed(
        thrown,
        "Unable to reach the Iceberg REST catalog for the request to look up [ns1.t1]",
        RestCatalogExceptionMapper.UNREACHABLE_HINT);
    logs.assertNoSecrets();
  }

  @Test
  public void testTableExists404IsNotFound() throws Exception {
    try (FakeRestCatalogServer server = FakeRestCatalogServer.http()) {
      server.route(
          "*", TABLE_PATH, Response.error(404, "NoSuchTableException", "Table does not exist"));
      RestIcebergCatalogPlugin plugin = startPlugin(server.baseUri());
      try {
        Optional<DatasetHandle> handle =
            plugin.getDatasetHandle(new EntityPath(Arrays.asList(SOURCE_NAME, "ns1", "t1")));
        assertFalse(handle.isPresent());
      } finally {
        plugin.close();
      }
    }
    logs.assertNoSecrets();
  }

  @Test
  public void testListNamespaces403IsReportedWithoutSecrets() throws Exception {
    try (FakeRestCatalogServer server = FakeRestCatalogServer.http()) {
      server.route(
          "GET", NAMESPACES_PATH, Response.error(403, "ForbiddenException", echoMessage(403)));
      RestIcebergCatalogPlugin plugin = startPlugin(server.baseUri());
      List<DatasetHandle> handles = new ArrayList<>();
      try {
        Iterator<? extends DatasetHandle> it = plugin.listDatasetHandles().iterator();
        while (it.hasNext()) {
          handles.add(it.next());
        }
      } finally {
        plugin.close();
      }
      // The listing does not fail: it is empty, and the denied listing is logged as one WARN line
      // (no stack trace) per walk of the catalog (tables, views) with the redacted server message.
      // The source state reports the denied listing (see TestRestCatalogOAuth2).
      assertThat(handles).isEmpty();
      List<ILoggingEvent> warnings = logs.events(Level.WARN, "Skipping namespace");
      assertThat(warnings).hasSize(2);
      for (ILoggingEvent warning : warnings) {
        assertThat(warning.getThrowableProxy()).isNull();
        assertThat(warning.getFormattedMessage()).contains(marker(403)).contains("echo ****");
      }
      logger.info("P3-MATRIX listNamespaces 403 -> empty listing, WARN log only");
    }
    logs.assertNoSecrets();
  }

  // ---------------------------------------------------------------------------------------------
  // Config masking (what the source GET API and logs would print)
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testConfigToStringDoesNotExposeSecrets() {
    RestIcebergCatalogPluginConfig conf = newConfig("http://127.0.0.1:1/api/catalog");
    assertNoSecret("config toString", conf.toString());
  }

  @Test
  public void testClearedConfigJsonMasksEverySecretProperty() throws Exception {
    RestIcebergCatalogPluginConfig conf = newConfig("http://127.0.0.1:1/api/catalog");
    conf.clearSecrets();
    ObjectMapper mapper = new ObjectMapper();
    mapper.registerSubtypes(new NamedType(RestIcebergCatalogPluginConfig.class, "RESTCATALOG"));
    SourceJson body = new SourceJson(conf);
    String json = mapper.writeValueAsString(body);

    assertNoSecret("source JSON", json);
    assertThat(json).contains("\"type\":\"RESTCATALOG\"");
    // Secret names stay visible, values are the placeholder; plain properties are unchanged.
    for (String key :
        Arrays.asList(
            "credential", "fs.s3a.access.key", "fs.s3a.secret.key", "fs.s3a.session.token")) {
      assertThat(json)
          .contains(
              "{\"name\":\""
                  + key
                  + "\",\"value\":\""
                  + escapeJson(USE_EXISTING_SECRET_VALUE)
                  + "\"}");
    }
    assertThat(json).contains("{\"name\":\"warehouse\",\"value\":\"wh\"}");
  }

  @Test
  public void testStateOfFailedSourceDoesNotExposeSecretsInToString() throws Exception {
    try (FakeRestCatalogServer server = FakeRestCatalogServer.http()) {
      server.route(
          "POST", TOKEN_PATH, Response.oauthError(401, "invalid_client", echoMessage(401)));
      RestIcebergCatalogPlugin plugin = startPlugin(server.baseUri());
      try {
        SourceState state = plugin.getState();
        assertNoSecret("SourceState.toString", state.toString());
      } finally {
        plugin.close();
      }
    }
  }

  /** Jackson view of a source body: ConnectionConf uses an external "type" property. */
  public static final class SourceJson {
    @JsonProperty("config")
    private final ConnectionConf<?, ?> config;

    SourceJson(ConnectionConf<?, ?> config) {
      this.config = config;
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------

  private static String marker(int code) {
    return "p3-marker-" + code;
  }

  /**
   * The fake server records TLS failures on its own thread, which may lag behind the client's
   * failure: wait (bounded) instead of asserting right away.
   */
  private static void awaitPositive(String what, AtomicInteger counter)
      throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (counter.get() == 0 && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    assertTrue(what, counter.get() > 0);
  }

  /** An error message that echoes the submitted secrets, so that redaction is exercised. */
  private static String echoMessage(int code) {
    return marker(code) + " echo " + FULL_CREDENTIAL + " s3=" + S3_SECRET + " key=" + S3_ACCESS;
  }

  private String configFailure(int code, String type) throws Exception {
    try (FakeRestCatalogServer server = FakeRestCatalogServer.http()) {
      server.route("GET", CONFIG_PATH, Response.error(code, type, echoMessage(code)));
      String msg = badStateMessage(server, String.valueOf(code), "rest.client.max-retries", "1");
      // The echoed secrets were masked, not dropped.
      assertThat(msg).contains("echo ****");
      return msg;
    }
  }

  private String badStateMessage(FakeRestCatalogServer server, String scenario, String... props)
      throws Exception {
    return badStateMessage(server.baseUri(), scenario, props);
  }

  /**
   * Starts a plugin against {@code base}, calls getState() and checks the common expectations: bad
   * state, generic suggested action, no secret in the state or in captured logs. Returns the joined
   * state messages.
   */
  private String badStateMessage(String base, String scenario, String... props) throws Exception {
    RestIcebergCatalogPlugin plugin = startPlugin(base, props);
    SourceState state;
    try {
      state = plugin.getState();
    } finally {
      plugin.close();
    }
    assertEquals(scenario, SourceState.SourceStatus.bad, state.getStatus());
    // The suggested user action is what the source create/update API returns as its error.
    assertThat(state.getSuggestedUserAction()).contains("Could not connect to " + SOURCE_NAME);
    assertNoSecret(scenario + " suggested action", state.getSuggestedUserAction());
    assertNoSecret(scenario + " state", state.toString());
    StringBuilder messages = new StringBuilder();
    for (SourceState.Message message : state.getMessages()) {
      assertNoSecret(scenario + " message", message.getMessage());
      messages.append(message.getMessage()).append(' ');
    }
    logs.assertNoSecrets();
    String msg = messages.toString().trim();
    assertThat(msg).startsWith("Failure connecting to source:");
    logger.info("P3-MATRIX {} -> {}", scenario, msg);
    return msg;
  }

  private Throwable datasetHandleFailure(int code, String type, String serverMessage)
      throws Exception {
    return datasetHandleFailure(
        String.valueOf(code),
        Response.error(code, type, serverMessage),
        "rest.client.max-retries",
        "1");
  }

  private Throwable datasetHandleFailure(String scenario, Response response, String... props)
      throws Exception {
    Throwable thrown = null;
    try (FakeRestCatalogServer server = FakeRestCatalogServer.http()) {
      server.route("*", TABLE_PATH, response);
      RestIcebergCatalogPlugin plugin = startPlugin(server.baseUri(), props);
      try {
        plugin.getDatasetHandle(new EntityPath(Arrays.asList(SOURCE_NAME, "ns1", "t1")));
        fail("expected a failure for " + scenario);
      } catch (RuntimeException e) {
        thrown = e;
      } finally {
        plugin.close();
      }
    }
    for (Throwable t = thrown; t != null; t = t.getCause()) {
      assertNoSecret("exception " + t.getClass().getName(), String.valueOf(t.getMessage()));
      assertNoSecret("exception toString", t.toString());
    }
    logs.assertNoSecrets();
    logger.info(
        "P3-MATRIX tableExists {} -> {}: {}",
        scenario,
        thrown.getClass().getName(),
        thrown.getMessage());
    return thrown;
  }

  /** A connection error (not the raw Iceberg exception) whose message has all the fragments. */
  private static void assertRequestFailed(Throwable thrown, String... fragments) {
    assertThat(thrown).isInstanceOf(UserException.class);
    assertEquals(ErrorType.CONNECTION, ((UserException) thrown).getErrorType());
    for (String fragment : fragments) {
      assertThat(thrown.getMessage()).contains(fragment);
    }
  }

  /** Class names and messages of the exception and its causes (raw or mapped to UserException). */
  private static String chainText(Throwable thrown) {
    StringBuilder sb = new StringBuilder();
    for (Throwable t = thrown; t != null; t = t.getCause()) {
      sb.append(t.getClass().getName()).append(": ").append(t.getMessage()).append(" | ");
    }
    return sb.toString();
  }

  private RestIcebergCatalogPlugin startPlugin(String base, String... props) throws IOException {
    RestIcebergCatalogPluginConfig conf = newConfig(base);
    for (int i = 0; i + 1 < props.length; i += 2) {
      conf.propertyList.add(new Property(props[i], props[i + 1]));
    }
    RestIcebergCatalogPlugin plugin =
        new RestIcebergCatalogPlugin(conf, getSabotContext(), SOURCE_NAME, () -> storagePluginId);
    plugin.start();
    return plugin;
  }

  private static RestIcebergCatalogPluginConfig newConfig(String base) {
    RestIcebergCatalogPluginConfig conf = new RestIcebergCatalogPluginConfig();
    conf.restEndpointUri = base;
    conf.propertyList =
        new ArrayList<>(
            Arrays.asList(
                new Property("warehouse", "wh"),
                new Property("scope", "PRINCIPAL_ROLE:ALL"),
                new Property("oauth2-server-uri", base + TOKEN_PATH),
                new Property("fs.s3a.endpoint", "127.0.0.1:9000"),
                new Property("fs.s3a.path.style.access", "true")));
    conf.secretPropertyList =
        new ArrayList<>(
            Arrays.asList(
                new Property("credential", FULL_CREDENTIAL),
                new Property("fs.s3a.access.key", S3_ACCESS),
                new Property("fs.s3a.secret.key", S3_SECRET),
                new Property("fs.s3a.session.token", S3_SESSION)));
    return conf;
  }

  private static void assertNoSecret(String context, String text) {
    if (text == null) {
      return;
    }
    for (String secret : SECRETS) {
      assertFalse(context + " leaked a secret value: " + mask(text), text.contains(secret));
    }
    assertFalse(context + " leaked a client_secret form field", text.contains("client_secret="));
  }

  /** Masks every test secret so a failure message never prints one. */
  private static String mask(String text) {
    String result = text;
    for (String secret : SECRETS) {
      result = result.replace(secret, "<leaked>");
    }
    return result;
  }

  private static String escapeJson(String value) {
    return value.replace("\\", "\\\\").replace("\"", "\\\"");
  }

  /** Self-signed RSA certificate for 127.0.0.1, not trusted by the JVM default trust store. */
  private static SSLContext selfSignedContext() throws Exception {
    KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
    generator.initialize(2048, new SecureRandom());
    KeyPair keyPair = generator.generateKeyPair();
    X500Name subject = new X500Name("CN=127.0.0.1, O=Dremio test (self-signed)");
    long now = System.currentTimeMillis();
    X509Certificate certificate =
        new JcaX509CertificateConverter()
            .getCertificate(
                new JcaX509v3CertificateBuilder(
                        subject,
                        BigInteger.valueOf(now),
                        new Date(now - TimeUnit.DAYS.toMillis(1)),
                        new Date(now + TimeUnit.DAYS.toMillis(1)),
                        subject,
                        keyPair.getPublic())
                    .build(
                        new JcaContentSignerBuilder("SHA256WithRSAEncryption")
                            .build(keyPair.getPrivate())));
    char[] password = "test-only-keystore".toCharArray();
    KeyStore keyStore = KeyStore.getInstance("PKCS12");
    keyStore.load(null, null);
    keyStore.setKeyEntry(
        "server", keyPair.getPrivate(), password, new X509Certificate[] {certificate});
    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    kmf.init(keyStore, password);
    SSLContext context = SSLContext.getInstance("TLS");
    context.init(kmf.getKeyManagers(), null, new SecureRandom());
    return context;
  }

  /**
   * Captures DEBUG events of the Dremio, Iceberg and Apache HttpClient loggers. The HttpClient wire
   * and header loggers are kept at INFO, as in the distribution logback.xml: at DEBUG they print
   * request bodies and Authorization headers by design.
   */
  private static final class LogCapture implements AutoCloseable {
    private static final String[] DEBUG_LOGGERS = {
      "com.dremio.plugins.icebergcatalog", "org.apache.iceberg", "org.apache.hc.client5"
    };
    private static final String[] INFO_LOGGERS = {
      "org.apache.hc.client5.http.wire", "org.apache.hc.client5.http.headers"
    };
    private static final String[] ATTACH_TO = {
      org.slf4j.Logger.ROOT_LOGGER_NAME, "com.dremio", "org.apache.iceberg", "org.apache.hc"
    };

    private final Map<Logger, Level> previousLevels = new LinkedHashMap<>();
    private final Map<Logger, ListAppender<ILoggingEvent>> appenders = new LinkedHashMap<>();

    LogCapture() {
      for (String name : DEBUG_LOGGERS) {
        setLevel(name, Level.DEBUG);
      }
      for (String name : INFO_LOGGERS) {
        setLevel(name, Level.INFO);
      }
      for (String name : ATTACH_TO) {
        Logger target = (Logger) LoggerFactory.getLogger(name);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        target.addAppender(appender);
        appenders.put(target, appender);
      }
    }

    private void setLevel(String name, Level level) {
      Logger target = (Logger) LoggerFactory.getLogger(name);
      previousLevels.put(target, target.getLevel());
      target.setLevel(level);
    }

    List<String> texts() {
      List<String> result = new ArrayList<>();
      for (ListAppender<ILoggingEvent> appender : appenders.values()) {
        for (ILoggingEvent event : new ArrayList<>(appender.list)) {
          result.add(eventText(event));
        }
      }
      return result;
    }

    /**
     * The events of the given level whose text contains {@code text}. Taken from the appender of
     * the "com.dremio" logger only: the other appenders see the same events again.
     */
    List<ILoggingEvent> events(Level level, String text) {
      List<ILoggingEvent> result = new ArrayList<>();
      ListAppender<ILoggingEvent> appender =
          appenders.get((Logger) LoggerFactory.getLogger("com.dremio"));
      for (ILoggingEvent event : new ArrayList<>(appender.list)) {
        if (event.getLevel() == level && eventText(event).contains(text)) {
          result.add(event);
        }
      }
      return result;
    }

    void assertNoSecrets() {
      for (String text : texts()) {
        assertNoSecret("log event", text);
        assertFalse("log event leaked a bearer token", text.contains("Bearer " + ACCESS_TOKEN));
      }
    }

    private static String eventText(ILoggingEvent event) {
      StringBuilder sb =
          new StringBuilder(event.getLoggerName()).append(' ').append(event.getFormattedMessage());
      for (IThrowableProxy t = event.getThrowableProxy(); t != null; t = t.getCause()) {
        sb.append(" | ").append(t.getClassName()).append(": ").append(t.getMessage());
      }
      return sb.toString();
    }

    @Override
    public void close() {
      for (Map.Entry<Logger, ListAppender<ILoggingEvent>> e : appenders.entrySet()) {
        e.getKey().detachAppender(e.getValue());
        e.getValue().stop();
      }
      for (Map.Entry<Logger, Level> e : previousLevels.entrySet()) {
        e.getKey().setLevel(e.getValue());
      }
    }
  }

  /** One recorded request. Header names are lower case. */
  private static final class Request {
    private final String method;
    private final String path;
    private final Map<String, String> headers;
    private final String body;

    Request(String method, String path, Map<String, String> headers, String body) {
      this.method = method;
      this.path = path;
      this.headers = headers;
      this.body = body;
    }

    String pathWithoutQuery() {
      int idx = path.indexOf('?');
      return idx < 0 ? path : path.substring(0, idx);
    }
  }

  /** A canned response. */
  private static final class Response {
    private final int status;
    private final String contentType;
    private final String body;
    private final Map<String, String> headers = new LinkedHashMap<>();
    private long delayMs;

    Response(int status, String contentType, String body) {
      this.status = status;
      this.contentType = contentType;
      this.body = body;
    }

    Response withHeader(String name, String value) {
      headers.put(name, value);
      return this;
    }

    Response withDelayMs(long delay) {
      this.delayMs = delay;
      return this;
    }

    static Response json(int status, String body) {
      return new Response(status, "application/json", body);
    }

    static Response token() {
      return json(
          200,
          "{\"access_token\":\""
              + ACCESS_TOKEN
              + "\",\"token_type\":\"bearer\",\"issued_token_type\":"
              + "\"urn:ietf:params:oauth:token-type:access_token\",\"expires_in\":3600}");
    }

    static Response config() {
      return json(200, "{\"defaults\":{},\"overrides\":{}}");
    }

    /** Iceberg REST ErrorResponse. */
    static Response error(int status, String type, String message) {
      return json(
          status,
          "{\"error\":{\"message\":\""
              + message
              + "\",\"type\":\""
              + type
              + "\",\"code\":"
              + status
              + "}}");
    }

    /** Closes the connection after reading the request, without any response. */
    static Response dropConnection() {
      return new Response(-1, "text/plain", "");
    }

    /** OAuth2 (RFC 6749 section 5.2) error. */
    static Response oauthError(int status, String error, String description) {
      return json(
          status, "{\"error\":\"" + error + "\",\"error_description\":\"" + description + "\"}");
    }
  }

  /**
   * Minimal HTTP/1.1 server on a loopback port (optionally TLS) that emulates an Iceberg REST
   * catalog. Routes match on method ("*" for any) and path suffix (query excluded); the most
   * recently added matching route wins. Every connection serves one request.
   */
  private static final class FakeRestCatalogServer implements AutoCloseable {
    private final ServerSocket serverSocket;
    private final ExecutorService pool = Executors.newCachedThreadPool();
    private final Map<String, Response> routes = Collections.synchronizedMap(new LinkedHashMap<>());
    private final List<Request> requests = new CopyOnWriteArrayList<>();
    private final AtomicInteger tlsFailures = new AtomicInteger();
    private final AtomicInteger tlsHellosOnPlainSocket = new AtomicInteger();

    static FakeRestCatalogServer http() throws IOException {
      return new FakeRestCatalogServer(new ServerSocket(0, 50, InetAddress.getLoopbackAddress()));
    }

    static FakeRestCatalogServer https(SSLContext context) throws IOException {
      return new FakeRestCatalogServer(
          context
              .getServerSocketFactory()
              .createServerSocket(0, 50, InetAddress.getLoopbackAddress()));
    }

    private FakeRestCatalogServer(ServerSocket serverSocket) {
      this.serverSocket = serverSocket;
      route("POST", TOKEN_PATH, Response.token());
      route("GET", CONFIG_PATH, Response.config());
      route("GET", NAMESPACES_PATH, Response.json(200, "{\"namespaces\":[]}"));
      route("GET", "/v1/namespaces/ns1/tables", Response.json(200, "{\"identifiers\":[]}"));
      route("GET", "/v1/namespaces/ns1/views", Response.json(200, "{\"identifiers\":[]}"));
      pool.execute(this::acceptLoop);
    }

    int port() {
      return serverSocket.getLocalPort();
    }

    String baseUri() {
      String scheme = serverSocket instanceof javax.net.ssl.SSLServerSocket ? "https" : "http";
      return scheme + "://127.0.0.1:" + port() + "/api/catalog";
    }

    void route(String method, String pathSuffix, Response response) {
      String key = method + " " + pathSuffix;
      routes.remove(key);
      routes.put(key, response);
    }

    int count(String method, String pathSuffix) {
      int n = 0;
      for (Request r : requests) {
        if (r.method.equals(method) && r.pathWithoutQuery().endsWith(pathSuffix)) {
          n++;
        }
      }
      return n;
    }

    Request find(String method, String pathSuffix) {
      for (Request r : requests) {
        if (r.method.equals(method) && r.pathWithoutQuery().endsWith(pathSuffix)) {
          return r;
        }
      }
      return null;
    }

    private Response match(Request request) {
      List<Map.Entry<String, Response>> entries;
      synchronized (routes) {
        entries = new ArrayList<>(routes.entrySet());
      }
      Collections.reverse(entries);
      for (Map.Entry<String, Response> e : entries) {
        int space = e.getKey().indexOf(' ');
        String method = e.getKey().substring(0, space);
        String suffix = e.getKey().substring(space + 1);
        boolean methodMatches =
            "*".equals(method)
                || method.equals(request.method)
                || ("GET".equals(method) && "HEAD".equals(request.method));
        if (methodMatches && request.pathWithoutQuery().endsWith(suffix)) {
          return e.getValue();
        }
      }
      return Response.error(404, "NoSuchEntityException", "No route for " + request.path);
    }

    private void acceptLoop() {
      while (!serverSocket.isClosed()) {
        try {
          Socket socket = serverSocket.accept();
          pool.execute(() -> serve(socket));
        } catch (IOException | RuntimeException e) {
          // closed
        }
      }
    }

    private void serve(Socket socket) {
      try (Socket s = socket) {
        s.setSoTimeout(10_000);
        if (s instanceof javax.net.ssl.SSLSocket) {
          try {
            // Handshake explicitly: a client that rejects the certificate may end it with an
            // alert, a reset or EOF, and only the alert surfaces as SSLException on read.
            ((javax.net.ssl.SSLSocket) s).startHandshake();
          } catch (IOException e) {
            tlsFailures.incrementAndGet();
            return;
          }
        }
        InputStream in = new BufferedInputStream(s.getInputStream());
        if (!(s instanceof javax.net.ssl.SSLSocket)) {
          in.mark(1);
          if (in.read() == 0x16) {
            // TLS ClientHello on the plain HTTP port: hang up like a real HTTP server would.
            tlsHellosOnPlainSocket.incrementAndGet();
            return;
          }
          in.reset();
        }
        Request request;
        try {
          request = readRequest(in);
        } catch (javax.net.ssl.SSLException e) {
          tlsFailures.incrementAndGet();
          return;
        }
        if (request == null) {
          return;
        }
        requests.add(request);
        Response response = match(request);
        if (response.delayMs > 0) {
          Thread.sleep(response.delayMs);
        }
        if (response.status < 0) {
          return;
        }
        byte[] body =
            "HEAD".equals(request.method)
                ? new byte[0]
                : response.body.getBytes(StandardCharsets.UTF_8);
        StringBuilder head =
            new StringBuilder("HTTP/1.1 ")
                .append(response.status)
                .append(" P3\r\nContent-Type: ")
                .append(response.contentType)
                .append("\r\nContent-Length: ")
                .append(body.length)
                .append("\r\nConnection: close\r\n");
        for (Map.Entry<String, String> h : response.headers.entrySet()) {
          head.append(h.getKey()).append(": ").append(h.getValue()).append("\r\n");
        }
        head.append("\r\n");
        OutputStream out = s.getOutputStream();
        out.write(head.toString().getBytes(StandardCharsets.US_ASCII));
        out.write(body);
        out.flush();
      } catch (IOException e) {
        // client went away (e.g. after its read timeout)
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    private static Request readRequest(InputStream in) throws IOException {
      ByteArrayOutputStream headBytes = new ByteArrayOutputStream();
      int matched = 0;
      byte[] terminator = {'\r', '\n', '\r', '\n'};
      while (matched < terminator.length) {
        int b = in.read();
        if (b < 0) {
          return null;
        }
        headBytes.write(b);
        matched = (b == terminator[matched]) ? matched + 1 : (b == '\r' ? 1 : 0);
      }
      String[] lines = new String(headBytes.toByteArray(), StandardCharsets.US_ASCII).split("\r\n");
      String[] requestLine = lines[0].split(" ");
      Map<String, String> headers = new LinkedHashMap<>();
      for (int i = 1; i < lines.length; i++) {
        int colon = lines[i].indexOf(':');
        if (colon > 0) {
          headers.put(
              lines[i].substring(0, colon).trim().toLowerCase(Locale.ROOT),
              lines[i].substring(colon + 1).trim());
        }
      }
      int contentLength = Integer.parseInt(headers.getOrDefault("content-length", "0"));
      byte[] body = new byte[contentLength];
      int read = 0;
      while (read < contentLength) {
        int n = in.read(body, read, contentLength - read);
        if (n < 0) {
          break;
        }
        read += n;
      }
      return new Request(
          requestLine[0],
          requestLine.length > 1 ? requestLine[1] : "",
          headers,
          new String(body, 0, read, StandardCharsets.UTF_8));
    }

    @Override
    public void close() throws Exception {
      serverSocket.close();
      pool.shutdownNow();
      pool.awaitTermination(10, TimeUnit.SECONDS);
    }
  }
}
