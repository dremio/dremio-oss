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
import com.dremio.common.util.TestTools;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.io.AsyncByteReader;
import com.dremio.plugins.s3.store.S3FileSystem;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.slf4j.LoggerFactory;

/**
 * Secret exposure checks of the RESTCATALOG source for object storage (Phase 4): static S3 keys, S3
 * session tokens and the storage credentials that a REST catalog vends ({@code s3.*} keys of the
 * Iceberg REST {@code loadTable} config).
 *
 * <p>Covers what Phase 3 tests do not: classification and redaction of vended credential keys,
 * {@code SourceConfig}/{@code StoragePluginId} rendering, the Hadoop configuration handed to the
 * file systems, and a storage failure against a fake S3 endpoint (request bytes, exception chain
 * and DEBUG log events).
 */
public class TestRestCatalogSecretExposure extends BaseTestQuery {
  private static final org.slf4j.Logger logger =
      LoggerFactory.getLogger(TestRestCatalogSecretExposure.class);

  @Rule public final TestRule timeoutRule = TestTools.getTimeoutRule(180, TimeUnit.SECONDS);

  // Dummy, test-only values. They are used to assert that secrets never leak.
  private static final String CLIENT_ID = "p4-client-id";
  private static final String CLIENT_SECRET = "p4-client-secret-value-0401";
  private static final String OAUTH_TOKEN = "p4-oauth-token-value-0402";
  private static final String S3_ACCESS = "P4ACCESSKEYVALUE0403";
  private static final String S3_SECRET = "p4-s3-secret-value-0404";
  private static final String S3_SESSION = "p4-s3-session-token-value-0405";
  private static final String VENDED_ACCESS = "P4VENDEDACCESSKEY0406";
  private static final String VENDED_SECRET = "p4-vended-secret-value-0407";
  private static final String VENDED_SESSION = "p4-vended-session-token-value-0408";

  /** Values that must never be shown, logged or sent anywhere except to their own service. */
  private static final List<String> SECRETS =
      Arrays.asList(
          CLIENT_SECRET, OAUTH_TOKEN, S3_SECRET, S3_SESSION, VENDED_SECRET, VENDED_SESSION);

  /**
   * Storage credential keys that an Iceberg REST catalog vends in the {@code loadTable} config or
   * {@code storage-credentials} (Iceberg 1.7 S3/GCS/ADLS FileIO properties; the S3 keys are the
   * ones Apache Polaris 1.1 returns), plus the Hadoop S3A and REST client secrets.
   */
  private static final List<String> SECRET_KEYS =
      Arrays.asList(
          "s3.access-key-id",
          "s3.secret-access-key",
          "s3.session-token",
          "fs.s3a.access.key",
          "fs.s3a.secret.key",
          "fs.s3a.session.token",
          "gcs.oauth2.token",
          "adls.sas-token.account.dfs.core.windows.net",
          "credential",
          "token",
          "header.Authorization");

  /** Non-secret keys that come with vended credentials (Polaris 1.1 loadTable config). */
  private static final List<String> NON_SECRET_VENDED_KEYS =
      Arrays.asList(
          "s3.session-token-expires-at-ms",
          "expiration-time",
          "client.refresh-credentials-endpoint",
          "client.region",
          "s3.endpoint",
          "s3.path-style-access",
          "gcs.oauth2.token-expires-at");

  private StoragePluginId storagePluginId;

  @Before
  public void setUp() {
    storagePluginId = mock(StoragePluginId.class);
  }

  // ---------------------------------------------------------------------------------------------
  // Vended credential keys
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testVendedCredentialKeysAreSensitive() {
    for (String key : SECRET_KEYS) {
      assertTrue(key, RestIcebergCatalogPlugin.isSensitivePropertyKey(key));
      assertTrue(
          key, RestIcebergCatalogPlugin.isSensitivePropertyKey(key.toUpperCase(Locale.ROOT)));
    }
    for (String key : NON_SECRET_VENDED_KEYS) {
      assertFalse(key, RestIcebergCatalogPlugin.isSensitivePropertyKey(key));
    }
  }

  @Test
  public void testVendedKeysConfiguredAsSecretsAreRedactedAndMasked() {
    RestIcebergCatalogPluginConfig conf = newConfig("127.0.0.1:9000");
    conf.secretPropertyList.add(new Property("token", OAUTH_TOKEN));
    conf.secretPropertyList.add(new Property("s3.access-key-id", VENDED_ACCESS));
    conf.secretPropertyList.add(new Property("s3.secret-access-key", VENDED_SECRET));
    conf.secretPropertyList.add(new Property("s3.session-token", VENDED_SESSION));
    RestIcebergCatalogPlugin plugin = newPlugin(conf);

    String message =
        String.format(
            "403 for credential %s:%s token %s keys %s/%s/%s vended %s/%s/%s",
            CLIENT_ID,
            CLIENT_SECRET,
            OAUTH_TOKEN,
            S3_ACCESS,
            S3_SECRET,
            S3_SESSION,
            VENDED_ACCESS,
            VENDED_SECRET,
            VENDED_SESSION);
    String redacted = plugin.redactSecrets(message);
    assertNoSecret("redactSecrets", redacted);
    assertFalse(redacted.contains(S3_ACCESS));
    assertFalse(redacted.contains(VENDED_ACCESS));
    assertNoSecret(
        "describeConnectionFailure",
        plugin.describeConnectionFailure(new IllegalStateException(message)));

    ConnectionConf<?, ?> masked = conf.clone();
    masked.clearSecrets();
    for (Property p : ((RestIcebergCatalogPluginConfig) masked).secretPropertyList) {
      assertEquals(p.name, USE_EXISTING_SECRET_VALUE, p.value);
    }
  }

  @Test
  public void testVendedKeysInPlainPropertiesAreRedactedAndLoggedByNameOnly() throws Exception {
    RestIcebergCatalogPluginConfig conf = newConfig("127.0.0.1:9000");
    conf.propertyList.add(new Property("s3.secret-access-key", VENDED_SECRET));
    conf.propertyList.add(new Property("s3.session-token", VENDED_SESSION));

    try (LogCapture logs = new LogCapture()) {
      RestIcebergCatalogPlugin plugin = newPlugin(conf);
      try {
        plugin.start();
        assertNoSecret(
            "redactSecrets",
            plugin.redactSecrets("echo " + VENDED_SECRET + " and " + VENDED_SESSION));
      } finally {
        plugin.close();
      }
      logs.assertNoSecrets();
      assertTrue(
          "expected a WARN naming the sensitive plain keys",
          logs.texts().stream()
              .anyMatch(t -> t.contains("[s3.secret-access-key, s3.session-token]")));
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Rendering of the source configuration
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testSourceConfigAndPluginIdRenderingDoesNotExposeSecrets() {
    RestIcebergCatalogPluginConfig conf = newConfig("127.0.0.1:9000");
    conf.secretPropertyList.add(new Property("token", OAUTH_TOKEN));
    conf.secretPropertyList.add(new Property("s3.session-token", VENDED_SESSION));
    SourceConfig sourceConfig =
        new SourceConfig()
            .setName("polaris")
            .setType("RESTCATALOG")
            .setConfig(conf.toBytesString());
    StoragePluginId pluginId = new StoragePluginId(sourceConfig, conf, SourceCapabilities.NONE);

    assertNoSecret("config toString", conf.toString());
    assertNoSecret("SourceConfig toString", sourceConfig.toString());
    assertNoSecret("StoragePluginId toString", pluginId.toString());
    assertNoSecret("plugin toString", newPlugin(conf).toString());
  }

  @Test
  public void testMaskedSourceJsonHidesSessionTokensAndVendedKeys() throws Exception {
    RestIcebergCatalogPluginConfig conf = newConfig("127.0.0.1:9000");
    conf.secretPropertyList.add(new Property("token", OAUTH_TOKEN));
    conf.secretPropertyList.add(new Property("s3.secret-access-key", VENDED_SECRET));
    conf.secretPropertyList.add(new Property("s3.session-token", VENDED_SESSION));
    ObjectMapper mapper = new ObjectMapper();
    mapper.registerSubtypes(new NamedType(RestIcebergCatalogPluginConfig.class, "RESTCATALOG"));

    // Positive control: the unmasked JSON (what the KV store keeps, see G-12) has the values.
    String unmasked = mapper.writeValueAsString(new SourceJson(conf));
    assertTrue(unmasked.contains(S3_SESSION));
    assertTrue(unmasked.contains(VENDED_SESSION));

    ConnectionConf<?, ?> masked = conf.clone();
    masked.clearSecrets();
    String json = mapper.writeValueAsString(new SourceJson(masked));
    assertNoSecret("masked source JSON", json);
    assertFalse(json.contains(S3_ACCESS));
    for (Property p : conf.secretPropertyList) {
      assertTrue(
          p.name,
          json.contains(
              "{\"name\":\""
                  + p.name
                  + "\",\"value\":\""
                  + USE_EXISTING_SECRET_VALUE.replace("\\", "\\\\")
                  + "\"}"));
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Hadoop configuration
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testRestClientSecretsNeverReachTheHadoopConfiguration() {
    RestIcebergCatalogPluginConfig conf = newConfig("127.0.0.1:9000");
    conf.secretPropertyList.add(new Property("token", OAUTH_TOKEN));
    conf.secretPropertyList.add(new Property("header.Authorization", "Bearer " + OAUTH_TOKEN));
    RestIcebergCatalogPlugin plugin = newPlugin(conf);

    Configuration fsConf = plugin.getFsConfCopy();
    for (Map.Entry<String, String> entry : fsConf) {
      String value = entry.getValue();
      assertFalse(entry.getKey(), value.contains(CLIENT_SECRET));
      assertFalse(entry.getKey(), value.contains(OAUTH_TOKEN));
    }
    // The S3 keys are storage settings: they are meant to be there.
    assertEquals(S3_SECRET, fsConf.get("fs.s3a.secret.key"));
    assertEquals(S3_SESSION, fsConf.get("fs.s3a.session.token"));
  }

  // ---------------------------------------------------------------------------------------------
  // Storage failure against a fake S3 endpoint
  // ---------------------------------------------------------------------------------------------

  /**
   * Reads through the S3 file system with the source's Hadoop configuration (session credentials,
   * as a vended-credential runtime would set them) from a fake S3 endpoint that denies object
   * access, with both S3 clients: Hadoop S3A (AWS SDK v1, Apache HttpClient 4) and the async reader
   * of data files (AWS SDK v2, Netty). The secret key must never be sent (SigV4 only sends the
   * access key ID, the signature and the session token), and neither the secret key nor the session
   * token may appear in the thrown exceptions or in the log events captured at DEBUG (Dremio,
   * Hadoop S3A, AWS SDK v1/v2, Netty; the header, frame and SigV4 signer loggers stay at INFO, see
   * {@link LogCapture}).
   */
  @Test
  public void testS3AccessDeniedDoesNotExposeSecretKeyOrSessionToken() throws Exception {
    try (FakeS3Server server = new FakeS3Server();
        LogCapture logs = new LogCapture()) {
      RestIcebergCatalogPluginConfig conf = newConfig("127.0.0.1:" + server.port());
      conf.propertyList.removeIf(p -> "fs.s3a.aws.credentials.provider".equals(p.name));
      conf.propertyList.add(
          new Property(
              "fs.s3a.aws.credentials.provider",
              "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"));
      Configuration fsConf = newPlugin(conf).getFsConfCopy();

      List<Throwable> failures = new ArrayList<>();
      try (S3FileSystem fs = new S3FileSystem()) {
        fs.initialize(new URI("dremioS3:///"), fsConf);
        Path object = new Path("/" + FakeS3Server.BUCKET + "/ns1/t1/metadata/v1.metadata.json");
        try {
          fs.getFileStatus(object);
        } catch (Exception e) {
          failures.add(e);
        }
        try (InputStream in = fs.open(object)) {
          logger.debug("unexpected read result {}", in.read());
        } catch (Exception e) {
          failures.add(e);
        }
        try {
          fs.listStatus(new Path("/" + FakeS3Server.BUCKET + "/ns1/"));
        } catch (Exception e) {
          failures.add(e);
        }
        // Data files are read with the AWS SDK v2 async client (Netty).
        try (AsyncByteReader reader = fs.getAsyncByteReader(object, "0", Collections.emptyMap())) {
          logger.debug(
              "unexpected async read result {}",
              reader.readFully(0, 16).get(60, TimeUnit.SECONDS).length);
        } catch (Exception e) {
          failures.add(e);
        }
      }

      assertFalse("the fake S3 endpoint received no request", server.requests().isEmpty());
      boolean sessionTokenSent = false;
      for (String request : server.requests()) {
        assertFalse("S3 secret key sent over the wire", request.contains(S3_SECRET));
        assertFalse("OAuth2 client secret sent to S3", request.contains(CLIENT_SECRET));
        for (String line : request.split("\r\n")) {
          if (line.contains(S3_SESSION)) {
            assertTrue(
                "session token outside its header: " + line,
                line.toLowerCase(Locale.ROOT).startsWith("x-amz-security-token:"));
            sessionTokenSent = true;
          }
        }
      }
      assertTrue("the session credentials were not used", sessionTokenSent);
      assertTrue(
          "no request of the async (Netty) S3 client",
          server.requests().stream().anyMatch(r -> r.contains("http#NettyNio")));

      assertFalse("expected the denied object access to fail", failures.isEmpty());
      for (Throwable failure : failures) {
        for (String text : throwableTexts(failure)) {
          assertNoSecret("storage exception", text);
        }
      }
      logs.assertNoSecrets();
      // Positive control: the S3 clients really logged at DEBUG, so the check above is not vacuous.
      long s3DebugEvents = logs.debugEventCount("org.apache.hadoop.fs.s3a", "com.amazonaws");
      assertTrue("no DEBUG event of the S3 clients was captured", s3DebugEvents > 0);
      long sdkV2DebugEvents = logs.debugEventCount("software.amazon.awssdk");
      assertTrue("no DEBUG event of the AWS SDK v2 was captured", sdkV2DebugEvents > 0);
      logger.info(
          "P4-MATRIX fake S3 403: {} requests, {} failures ({}), {} log events ({} S3A + AWS SDK v1"
              + " DEBUG, {} AWS SDK v1 request DEBUG, {} AWS SDK v2 DEBUG, {} Netty DEBUG), 0"
              + " secrets",
          server.requests().size(),
          failures.size(),
          failures.get(0).getClass().getSimpleName(),
          logs.texts().size(),
          s3DebugEvents,
          logs.debugEventCount("com.amazonaws.request"),
          sdkV2DebugEvents,
          logs.debugEventCount("io.netty"));
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------

  /** Polaris-style source with MinIO-style S3 settings and every secret in the secret list. */
  private static RestIcebergCatalogPluginConfig newConfig(String s3Endpoint) {
    RestIcebergCatalogPluginConfig conf = new RestIcebergCatalogPluginConfig();
    conf.restEndpointUri = "http://127.0.0.1:1/api/catalog";
    conf.propertyList =
        new ArrayList<>(
            Arrays.asList(
                new Property("warehouse", "wh"),
                new Property("scope", "PRINCIPAL_ROLE:ALL"),
                new Property(
                    "fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"),
                new Property("fs.s3a.endpoint", s3Endpoint),
                new Property("fs.s3a.connection.ssl.enabled", "false"),
                new Property("fs.s3a.path.style.access", "true"),
                new Property("dremio.s3.compat", "true"),
                new Property("dremio.s3.region", "us-east-1"),
                new Property("dremio.bucket.discovery.enabled", "false"),
                new Property("fs.s3a.requester.pays.enabled", "false"),
                new Property("fs.s3a.attempts.maximum", "1"),
                new Property("fs.s3a.retry.limit", "1"),
                new Property("fs.s3a.retry.interval", "10ms"),
                new Property("fs.s3a.connection.establish.timeout", "5000"),
                new Property("fs.s3a.connection.timeout", "5000")));
    conf.secretPropertyList =
        new ArrayList<>(
            Arrays.asList(
                new Property("credential", CLIENT_ID + ":" + CLIENT_SECRET),
                new Property("fs.s3a.access.key", S3_ACCESS),
                new Property("fs.s3a.secret.key", S3_SECRET),
                new Property("fs.s3a.session.token", S3_SESSION)));
    return conf;
  }

  private RestIcebergCatalogPlugin newPlugin(RestIcebergCatalogPluginConfig conf) {
    return new RestIcebergCatalogPlugin(conf, getSabotContext(), "polaris", () -> storagePluginId);
  }

  private static void assertNoSecret(String context, String text) {
    if (text == null) {
      return;
    }
    for (int i = 0; i < SECRETS.size(); i++) {
      assertFalse(
          context + " leaked secret #" + i + ": " + mask(text), text.contains(SECRETS.get(i)));
    }
  }

  /** The text with every test secret replaced by its index, safe to show in a failure message. */
  private static String mask(String text) {
    String masked = text;
    for (int i = 0; i < SECRETS.size(); i++) {
      masked = masked.replace(SECRETS.get(i), "<secret#" + i + ">");
    }
    return masked.length() > 2000 ? masked.substring(0, 2000) + "..." : masked;
  }

  /** Message and toString of the throwable, its causes and suppressed exceptions. */
  private static List<String> throwableTexts(Throwable failure) {
    List<String> texts = new ArrayList<>();
    Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
    List<Throwable> pending = new ArrayList<>(Collections.singletonList(failure));
    while (!pending.isEmpty()) {
      Throwable t = pending.remove(pending.size() - 1);
      if (t == null || !seen.add(t)) {
        continue;
      }
      texts.add(String.valueOf(t.getMessage()));
      texts.add(t.toString());
      pending.add(t.getCause());
      pending.addAll(Arrays.asList(t.getSuppressed()));
    }
    return texts;
  }

  /** Source request body, as the REST API serializes it. */
  public static final class SourceJson {
    @JsonProperty("config")
    private final ConnectionConf<?, ?> config;

    SourceJson(ConnectionConf<?, ?> config) {
      this.config = config;
    }
  }

  /**
   * Captures DEBUG events of the Dremio, Iceberg, Hadoop S3A, AWS SDK and Netty loggers. Loggers
   * that print request headers or SigV4 canonical requests at DEBUG are kept at INFO, as the
   * distribution logback.xml pins them (security.md): Apache HttpClient 5 wire/headers (OAuth2
   * {@code Authorization}, token request body) and, for S3, Apache HttpClient 4 wire/headers, the
   * AWS SDK v1/v2 SigV4 signers and the Netty {@code LoggingHandler}/{@code Http2FrameLogger} of
   * the SDK v2 async client, which print the session token ({@code X-Amz-Security-Token}, e.g. of
   * vended credentials). The S3 secret key itself is never sent.
   */
  private static final class LogCapture implements AutoCloseable {
    private static final String[] DEBUG_LOGGERS = {
      "com.dremio.plugins",
      "org.apache.iceberg",
      "org.apache.hadoop.fs.s3a",
      "com.amazonaws",
      "software.amazon.awssdk",
      "io.netty"
    };
    private static final String[] INFO_LOGGERS = {
      "org.apache.hc.client5.http.wire",
      "org.apache.hc.client5.http.headers",
      "org.apache.http.wire",
      "org.apache.http.headers",
      "com.amazonaws.auth.AWS4Signer",
      "software.amazon.awssdk.auth.signer",
      "software.amazon.awssdk.http.auth.aws.internal.signer",
      "io.netty.handler.logging.LoggingHandler",
      "io.netty.handler.codec.http2.Http2FrameLogger"
    };
    private static final String[] ATTACH_TO = {org.slf4j.Logger.ROOT_LOGGER_NAME, "com.dremio"};

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

    /** Number of captured DEBUG events of loggers whose name starts with one of the prefixes. */
    long debugEventCount(String... loggerPrefixes) {
      long count = 0;
      for (ListAppender<ILoggingEvent> appender : appenders.values()) {
        for (ILoggingEvent event : new ArrayList<>(appender.list)) {
          if (event.getLevel() != Level.DEBUG) {
            continue;
          }
          for (String prefix : loggerPrefixes) {
            if (event.getLoggerName().startsWith(prefix)) {
              count++;
              break;
            }
          }
        }
      }
      return count;
    }

    void assertNoSecrets() {
      for (String text : texts()) {
        assertNoSecret("log event", text);
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

  /**
   * Minimal S3 endpoint (path-style, plain HTTP, one request per connection): HEAD on the bucket
   * succeeds, every other request is denied with an S3 {@code AccessDenied} error. Records each raw
   * request (head and body).
   */
  private static final class FakeS3Server implements AutoCloseable {
    static final String BUCKET = "p4bucket";
    private static final String BUCKET_PATH = "/" + BUCKET;
    private static final String BUCKET_DIR_PATH = BUCKET_PATH + "/";
    private static final String ERROR_XML =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>AccessDenied</Code>"
            + "<Message>Access Denied</Message><RequestId>P4D0001</RequestId>"
            + "<HostId>p4d</HostId></Error>";

    private final ServerSocket socket;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final List<String> requests = new CopyOnWriteArrayList<>();

    FakeS3Server() throws IOException {
      socket = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
      executor.execute(this::serve);
    }

    int port() {
      return socket.getLocalPort();
    }

    List<String> requests() {
      return requests;
    }

    private void serve() {
      while (!socket.isClosed()) {
        try (Socket client = socket.accept()) {
          client.setSoTimeout(5000);
          handle(client);
        } catch (IOException e) {
          if (!socket.isClosed()) {
            logger.debug("fake S3 connection failed", e);
          }
        }
      }
    }

    private void handle(Socket client) throws IOException {
      InputStream in = client.getInputStream();
      ByteArrayOutputStream raw = new ByteArrayOutputStream();
      int contentLength = 0;
      try {
        String head = readHead(in, raw);
        for (String line : head.split("\r\n")) {
          if (line.toLowerCase(Locale.ROOT).startsWith("content-length:")) {
            contentLength = Integer.parseInt(line.substring(line.indexOf(':') + 1).trim());
          }
        }
        for (int i = 0; i < contentLength; i++) {
          int b = in.read();
          if (b < 0) {
            break;
          }
          raw.write(b);
        }
      } catch (SocketTimeoutException e) {
        // record what arrived
      }
      String request = new String(raw.toByteArray(), StandardCharsets.ISO_8859_1);
      requests.add(request);
      String[] requestLine = request.split("\r\n", 2)[0].split(" ");
      String method = requestLine.length > 0 ? requestLine[0] : "";
      String path = requestLine.length > 1 ? requestLine[1] : "";
      boolean headBucket =
          "HEAD".equals(method) && (BUCKET_PATH.equals(path) || BUCKET_DIR_PATH.equals(path));
      StringBuilder response = new StringBuilder();
      byte[] body = new byte[0];
      if (headBucket) {
        response.append("HTTP/1.1 200 OK\r\nx-amz-bucket-region: us-east-1\r\n");
      } else {
        response.append("HTTP/1.1 403 Forbidden\r\nx-amz-request-id: P4D0001\r\n");
        if (!"HEAD".equals(method)) {
          body = ERROR_XML.getBytes(StandardCharsets.UTF_8);
          response.append("Content-Type: application/xml\r\n");
        }
      }
      response
          .append("Content-Length: ")
          .append("HEAD".equals(method) ? 0 : body.length)
          .append("\r\nConnection: close\r\n\r\n");
      OutputStream out = client.getOutputStream();
      out.write(response.toString().getBytes(StandardCharsets.ISO_8859_1));
      out.write(body);
      out.flush();
    }

    private static String readHead(InputStream in, ByteArrayOutputStream raw) throws IOException {
      int matched = 0;
      int b;
      while ((b = in.read()) >= 0) {
        raw.write(b);
        if ((matched % 2 == 0 && b == '\r') || (matched % 2 == 1 && b == '\n')) {
          matched++;
          if (matched == 4) {
            break;
          }
        } else {
          matched = b == '\r' ? 1 : 0;
        }
      }
      return new String(raw.toByteArray(), StandardCharsets.ISO_8859_1);
    }

    @Override
    public void close() throws IOException {
      socket.close();
      executor.shutdownNow();
      try {
        executor.awaitTermination(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
