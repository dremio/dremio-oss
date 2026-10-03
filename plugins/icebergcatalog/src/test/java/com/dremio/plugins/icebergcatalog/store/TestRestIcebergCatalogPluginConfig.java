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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.core.read.ListAppender;
import com.dremio.BaseTestQuery;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.ConnectionSchema;
import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.service.namespace.SourceState;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtobufIOUtil;
import io.protostuff.Schema;
import io.protostuff.Tag;
import io.protostuff.runtime.RuntimeSchema;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.rest.RESTCatalog;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.slf4j.LoggerFactory;

public class TestRestIcebergCatalogPluginConfig extends BaseTestQuery {

  private static final String VENDED_HEADER = "header.X-Iceberg-Access-Delegation";
  private static final String VENDED_VALUE = "vended-credentials";

  // Dummy, test-only values. They are used to assert that secrets never leak.
  private static final String TEST_CLIENT_SECRET = "test-client-secret-value-0001";
  private static final String TEST_S3_SECRET = "test-s3-secret-value-0002";
  private static final String TEST_S3_ACCESS = "test-s3-access-value-0003";

  private RestIcebergCatalogPlugin restIcebergCatalogPlugin;
  private StoragePluginId storagePluginId;

  /**
   * The persisted shape of RestIcebergCatalogPluginConfig as of release 25.2.0 (tags 1-5, 10-12, no
   * tag 13). Used to produce/consume "old" protostuff bytes.
   */
  public static class RestCatalogConf2520 {
    @Tag(1)
    public List<Property> propertyList;

    @Tag(2)
    public List<Property> secretPropertyList;

    @Tag(3)
    public boolean enableAsync = true;

    @Tag(4)
    public boolean isCachingEnabled = true;

    @Tag(5)
    public int maxCacheSpacePct = 100;

    @Tag(10)
    public String restEndpointUri;

    @Tag(11)
    public List<String> allowedNamespaces;

    @Tag(12)
    public boolean isRecursiveAllowedNamespaces = true;
  }

  @Before
  public void setup() {
    RestIcebergCatalogPluginConfig pluginConfig = new RestIcebergCatalogPluginConfig();
    pluginConfig.propertyList = new ArrayList<>();
    pluginConfig.propertyList.add(new Property("testPropertyName", "testPropertyValue"));
    pluginConfig.secretPropertyList = new ArrayList<>();
    pluginConfig.secretPropertyList.add(
        new Property("testSecretPropertyName", "testSecretPropertyValue"));

    storagePluginId = mock(StoragePluginId.class);
    restIcebergCatalogPlugin =
        new RestIcebergCatalogPlugin(
            pluginConfig, getSabotContext(), "test", () -> storagePluginId);
  }

  // ---------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------

  private static RestIcebergCatalogPluginConfig newPolarisConfig() {
    RestIcebergCatalogPluginConfig conf = new RestIcebergCatalogPluginConfig();
    conf.restEndpointUri = "http://localhost:8181/api/catalog";
    conf.propertyList =
        new ArrayList<>(
            Arrays.asList(
                new Property("warehouse", "test_catalog"),
                new Property("scope", "PRINCIPAL_ROLE:ALL"),
                new Property(
                    "oauth2-server-uri", "http://localhost:8181/api/catalog/v1/oauth/tokens"),
                new Property("header.X-Custom-Header", "custom-value"),
                new Property("fs.s3a.endpoint", "127.0.0.1:9000"),
                new Property("fs.s3a.path.style.access", "true")));
    conf.secretPropertyList =
        new ArrayList<>(
            Arrays.asList(
                new Property("credential", "test-client-id:" + TEST_CLIENT_SECRET),
                new Property("fs.s3a.access.key", TEST_S3_ACCESS),
                new Property("fs.s3a.secret.key", TEST_S3_SECRET)));
    return conf;
  }

  private RestIcebergCatalogPlugin newPlugin(RestIcebergCatalogPluginConfig conf) {
    return new RestIcebergCatalogPlugin(conf, getSabotContext(), "polaris", () -> storagePluginId);
  }

  /** Builds the catalog via the plugin and returns the properties passed to loadCatalog. */
  @SuppressWarnings("unchecked")
  private static Map<String, String> capturedLoadCatalogProperties(
      RestIcebergCatalogPlugin plugin, Configuration conf) {
    try (MockedStatic<CatalogUtil> mockCatalogUtil = mockStatic(CatalogUtil.class)) {
      mockCatalogUtil
          .when(() -> CatalogUtil.loadCatalog(any(), any(), any(), any()))
          .thenReturn(mock(RESTCatalog.class));
      assertNotNull(plugin.createRestCatalog(conf).get());
      ArgumentCaptor<Map<String, String>> argument = ArgumentCaptor.forClass(Map.class);
      mockCatalogUtil.verify(
          () -> CatalogUtil.loadCatalog(any(), any(), argument.capture(), any()));
      return new HashMap<>(argument.getValue());
    }
  }

  private static ListAppender<ILoggingEvent> attachAppender(Logger logger) {
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.start();
    logger.addAppender(appender);
    return appender;
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
    assertFalse(context + " leaked client secret: " + text, text.contains(TEST_CLIENT_SECRET));
    assertFalse(context + " leaked s3 secret: " + text, text.contains(TEST_S3_SECRET));
    assertFalse(context + " leaked s3 access key: " + text, text.contains(TEST_S3_ACCESS));
  }

  private static Map<String, Integer> tagsByFieldName() {
    Map<String, Integer> tags = new HashMap<>();
    for (Class<?> c = RestIcebergCatalogPluginConfig.class; c != null; c = c.getSuperclass()) {
      for (Field field : c.getDeclaredFields()) {
        Tag tag = field.getAnnotation(Tag.class);
        if (tag != null) {
          tags.put(field.getName(), tag.value());
        }
      }
    }
    return tags;
  }

  private static RestIcebergCatalogPluginConfig readCurrent(byte[] bytes) {
    ConnectionSchema<RestIcebergCatalogPluginConfig> schema =
        ConnectionSchema.getSchema(RestIcebergCatalogPluginConfig.class);
    RestIcebergCatalogPluginConfig conf = schema.newMessage();
    ProtobufIOUtil.mergeFrom(bytes, conf, schema);
    return conf;
  }

  // Minimal protobuf wire-format writer, so 25.2.0 bytes don't depend on any current schema.
  private static void writeVarint(ByteArrayOutputStream out, long value) {
    long v = value;
    while ((v & ~0x7FL) != 0) {
      out.write((int) ((v & 0x7F) | 0x80));
      v >>>= 7;
    }
    out.write((int) v);
  }

  private static void writeKey(ByteArrayOutputStream out, int tag, int wireType) {
    writeVarint(out, ((long) tag << 3) | wireType);
  }

  private static void writeBytes(ByteArrayOutputStream out, int tag, byte[] bytes) {
    writeKey(out, tag, 2);
    writeVarint(out, bytes.length);
    out.write(bytes, 0, bytes.length);
  }

  private static void writeString(ByteArrayOutputStream out, int tag, String value) {
    writeBytes(out, tag, value.getBytes(StandardCharsets.UTF_8));
  }

  private static void writeVarintField(ByteArrayOutputStream out, int tag, long value) {
    writeKey(out, tag, 0);
    writeVarint(out, value);
  }

  private static void writeProperty(ByteArrayOutputStream out, int tag, String name, String value) {
    ByteArrayOutputStream message = new ByteArrayOutputStream();
    writeString(message, 1, name);
    writeString(message, 2, value);
    writeBytes(out, tag, message.toByteArray());
  }

  /**
   * Minimal HTTP server on a loopback port that answers every request with HTTP 401 and a fixed
   * OAuth2 error body. It stays bound for the whole test, so no other process can take the port.
   */
  private static final class Fixed401Server implements AutoCloseable {
    private final ServerSocket serverSocket;
    private final byte[] body;
    private final Thread thread;

    Fixed401Server(String body) throws IOException {
      this.serverSocket = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
      this.body = body.getBytes(StandardCharsets.UTF_8);
      this.thread = new Thread(this::serve, "fixed-401-server");
      this.thread.setDaemon(true);
      this.thread.start();
    }

    int port() {
      return serverSocket.getLocalPort();
    }

    private void serve() {
      while (!serverSocket.isClosed()) {
        try (Socket socket = serverSocket.accept()) {
          drainRequest(socket.getInputStream());
          OutputStream out = socket.getOutputStream();
          String head =
              "HTTP/1.1 401 Unauthorized\r\n"
                  + "Content-Type: application/json\r\n"
                  + "Content-Length: "
                  + body.length
                  + "\r\n"
                  + "Connection: close\r\n\r\n";
          out.write(head.getBytes(StandardCharsets.US_ASCII));
          out.write(body);
          out.flush();
        } catch (IOException e) {
          // Socket closed by close() or by the client: keep serving until closed.
        }
      }
    }

    /** Reads the request line, headers and Content-Length body so the client sees no reset. */
    private static void drainRequest(InputStream in) throws IOException {
      ByteArrayOutputStream headers = new ByteArrayOutputStream();
      int matched = 0;
      byte[] terminator = {'\r', '\n', '\r', '\n'};
      while (matched < terminator.length) {
        int b = in.read();
        if (b < 0) {
          return;
        }
        headers.write(b);
        matched = (b == terminator[matched]) ? matched + 1 : (b == '\r' ? 1 : 0);
      }
      long contentLength = 0;
      for (String line :
          new String(headers.toByteArray(), StandardCharsets.US_ASCII).split("\r\n")) {
        String lower = line.toLowerCase(Locale.ROOT);
        if (lower.startsWith("content-length:")) {
          contentLength = Long.parseLong(lower.substring("content-length:".length()).trim());
        }
      }
      for (long i = 0; i < contentLength && in.read() >= 0; i++) {
        // discard the body
      }
    }

    @Override
    public void close() throws Exception {
      serverSocket.close();
      thread.join(5000);
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Catalog properties passed to loadCatalog (item 6)
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testCreateRestCatalog() throws Exception {
    Configuration conf = new Configuration();
    try (MockedStatic<CatalogUtil> mockCatalogUtil = mockStatic(CatalogUtil.class)) {
      mockCatalogUtil
          .when(() -> CatalogUtil.loadCatalog(any(), any(), any(), any()))
          .thenReturn(mock(RESTCatalog.class));
      CatalogAccessor catalogAccessor = restIcebergCatalogPlugin.createCatalog(conf);
      try {
        catalogAccessor.checkState();
      } catch (Exception e) {
        if (e instanceof IllegalArgumentException) {
          // Ignore it, createRestCatalog() is evaluated lazily through catalogSupplier
          // this checkState() call is to trigger the createRestCatalog(). So later loadCatalog()
          // arguments can be asserted.
        } else {
          throw e;
        }
      }
      ArgumentCaptor<Map<String, String>> argument = ArgumentCaptor.forClass(Map.class);
      mockCatalogUtil.verify(
          () -> CatalogUtil.loadCatalog(any(), any(), argument.capture(), any()));
      Map<String, String> properties = argument.getValue();
      assertTrue(properties.containsKey("testPropertyName"));
      assertTrue(properties.containsKey("testSecretPropertyName"));
    }
  }

  @Test
  public void testLoadCatalogReceivesPolarisProperties() {
    RestIcebergCatalogPluginConfig conf = newPolarisConfig();
    Map<String, String> props = capturedLoadCatalogProperties(newPlugin(conf), new Configuration());

    assertEquals("http://localhost:8181/api/catalog", props.get(CatalogProperties.URI));
    assertEquals(RESTCatalog.class.getName(), props.get(CatalogProperties.CATALOG_IMPL));
    assertEquals("test_catalog", props.get("warehouse"));
    assertEquals("PRINCIPAL_ROLE:ALL", props.get("scope"));
    assertEquals(
        "http://localhost:8181/api/catalog/v1/oauth/tokens", props.get("oauth2-server-uri"));
    assertEquals("custom-value", props.get("header.X-Custom-Header"));
    assertEquals("test-client-id:" + TEST_CLIENT_SECRET, props.get("credential"));
    // Default (flag false): no access delegation header is requested.
    assertFalse(props.containsKey(VENDED_HEADER));
  }

  @Test
  public void testLoadCatalogReceivesVendedHeaderOnlyWhenEnabled() {
    RestIcebergCatalogPluginConfig conf = newPolarisConfig();
    conf.isUsingVendedCredentials = true;
    Configuration hadoopConf = new Configuration();

    Map<String, String> props = capturedLoadCatalogProperties(newPlugin(conf), hadoopConf);

    assertEquals(VENDED_VALUE, props.get(VENDED_HEADER));
    // Catalog-only: the header must not leak into the Hadoop/FS configuration.
    assertNull(hadoopConf.get(VENDED_HEADER));
    // Regular properties still pass through.
    assertEquals("test_catalog", props.get("warehouse"));
    assertEquals("PRINCIPAL_ROLE:ALL", props.get("scope"));
  }

  @Test
  public void testUserProvidedDelegationHeaderWins() {
    RestIcebergCatalogPluginConfig conf = newPolarisConfig();
    conf.isUsingVendedCredentials = true;
    conf.propertyList.add(new Property(VENDED_HEADER, "remote-signing"));

    Map<String, String> props = capturedLoadCatalogProperties(newPlugin(conf), new Configuration());

    assertEquals("remote-signing", props.get(VENDED_HEADER));
  }

  // ---------------------------------------------------------------------------------------------
  // Eager fs.s3a.* propagation at start() (item 7, G-07)
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testStartCopiesS3aPropertiesWithoutContactingCatalog() throws Exception {
    RestIcebergCatalogPluginConfig conf = newPolarisConfig();
    conf.isUsingVendedCredentials = true;
    RestIcebergCatalogPlugin plugin = newPlugin(conf);

    try (MockedStatic<CatalogUtil> mockCatalogUtil = mockStatic(CatalogUtil.class)) {
      plugin.start();
      try {
        mockCatalogUtil.verify(() -> CatalogUtil.loadCatalog(any(), any(), any(), any()), never());

        Configuration fsConf = plugin.getFsConfCopy();
        assertEquals("127.0.0.1:9000", fsConf.get("fs.s3a.endpoint"));
        assertEquals("true", fsConf.get("fs.s3a.path.style.access"));
        assertEquals(TEST_S3_ACCESS, fsConf.get("fs.s3a.access.key"));
        assertEquals(TEST_S3_SECRET, fsConf.get("fs.s3a.secret.key"));
        // The vended-credentials header is a catalog-only property.
        assertNull(fsConf.get(VENDED_HEADER));
        // REST client auth settings never travel with the Hadoop/FileIO configuration.
        assertNull(fsConf.get("credential"));
        assertNull(fsConf.get("scope"));
        assertNull(fsConf.get("oauth2-server-uri"));
        assertNull(fsConf.get("header.X-Custom-Header"));
        assertEquals("test_catalog", fsConf.get("warehouse"));
      } finally {
        plugin.close();
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Null safety (G-14)
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testNullPropertiesAndNullNamesAreSkipped() {
    RestIcebergCatalogPluginConfig conf = new RestIcebergCatalogPluginConfig();
    conf.restEndpointUri = "http://localhost:8181/api/catalog";
    conf.propertyList =
        new ArrayList<>(
            Arrays.asList(
                null, new Property(null, "orphan-value"), new Property("warehouse", "wh")));
    conf.secretPropertyList = new ArrayList<>(Arrays.asList((Property) null));

    RestIcebergCatalogPlugin plugin = newPlugin(conf);
    Configuration hadoopConf = new Configuration();
    Map<String, String> props = plugin.buildCatalogProperties(hadoopConf);

    assertFalse(props.containsKey(null));
    assertFalse(props.containsValue("orphan-value"));
    assertEquals("wh", props.get("warehouse"));
  }

  // ---------------------------------------------------------------------------------------------
  // Secret masking (item 4)
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testClearSecretsMasksOnlySecretPropertyList() {
    RestIcebergCatalogPluginConfig original = newPolarisConfig();
    original.isUsingVendedCredentials = true;
    RestIcebergCatalogPluginConfig masked = (RestIcebergCatalogPluginConfig) original.clone();

    masked.clearSecrets();

    assertEquals(original.secretPropertyList.size(), masked.secretPropertyList.size());
    for (int i = 0; i < masked.secretPropertyList.size(); i++) {
      assertEquals(original.secretPropertyList.get(i).name, masked.secretPropertyList.get(i).name);
      assertEquals(USE_EXISTING_SECRET_VALUE, masked.secretPropertyList.get(i).value);
    }
    // Non-secret fields are untouched.
    assertEquals(original.propertyList, masked.propertyList);
    assertEquals(original.restEndpointUri, masked.restEndpointUri);
    assertTrue(masked.isUsingVendedCredentials);
  }

  @Test
  public void testApplySecretsFromRestoresMaskedValues() {
    RestIcebergCatalogPluginConfig original = newPolarisConfig();
    RestIcebergCatalogPluginConfig masked = (RestIcebergCatalogPluginConfig) original.clone();
    masked.clearSecrets();

    masked.applySecretsFrom(original);

    assertEquals(original.secretPropertyList, masked.secretPropertyList);
    assertEquals(original, masked);
  }

  @Test
  public void testApplySecretsFromKeepsNewlyProvidedValue() {
    RestIcebergCatalogPluginConfig original = newPolarisConfig();
    RestIcebergCatalogPluginConfig updated = (RestIcebergCatalogPluginConfig) original.clone();
    updated.clearSecrets();
    updated.secretPropertyList.set(0, new Property("credential", "test-client-id:rotated"));

    updated.applySecretsFrom(original);

    assertEquals(
        new Property("credential", "test-client-id:rotated"), updated.secretPropertyList.get(0));
    assertEquals(original.secretPropertyList.get(1), updated.secretPropertyList.get(1));
    assertEquals(original.secretPropertyList.get(2), updated.secretPropertyList.get(2));
  }

  @Test
  public void testRenamedMaskedSecretDoesNotBreakPlugin() {
    RestIcebergCatalogPluginConfig original = newPolarisConfig();
    RestIcebergCatalogPluginConfig updated = (RestIcebergCatalogPluginConfig) original.clone();
    updated.clearSecrets();
    // User renamed the key in the UI but kept the masked placeholder value. The kernel cannot
    // resolve it and (as of 26.0.x) inserts a null element into the list.
    updated.secretPropertyList.set(
        0, new Property("credential-renamed", USE_EXISTING_SECRET_VALUE));

    updated.applySecretsFrom(original);

    RestIcebergCatalogPlugin plugin = newPlugin(updated);
    Map<String, String> props = plugin.buildCatalogProperties(new Configuration());
    assertFalse(props.containsKey(null));
    assertFalse(props.containsKey("credential-renamed"));
    assertFalse(props.containsValue(USE_EXISTING_SECRET_VALUE));
    assertEquals(TEST_S3_SECRET, props.get("fs.s3a.secret.key"));
  }

  // ---------------------------------------------------------------------------------------------
  // Protostuff (item 2)
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testVendedCredentialsFieldContract() throws Exception {
    Field field = RestIcebergCatalogPluginConfig.class.getField("isUsingVendedCredentials");
    assertEquals(boolean.class, field.getType());
    assertEquals(13, field.getAnnotation(Tag.class).value());
    DisplayMetadata displayMetadata = field.getAnnotation(DisplayMetadata.class);
    assertNotNull(displayMetadata);
    assertEquals("Use vended credentials", displayMetadata.label());
    assertNotNull(field.getAnnotation(NotMetadataImpacting.class));
    assertFalse(new RestIcebergCatalogPluginConfig().isUsingVendedCredentials);
  }

  @Test
  public void testTogglingVendedCredentialsIsNotMetadataImpacting() {
    RestIcebergCatalogPluginConfig existing = newPolarisConfig();
    RestIcebergCatalogPluginConfig updated = newPolarisConfig();
    updated.isUsingVendedCredentials = true;

    assertNotEquals(existing, updated);
    assertTrue(existing.equalsIgnoringNotMetadataImpacting(updated));
    assertTrue(updated.equalsIgnoringNotMetadataImpacting(existing));

    // A metadata-impacting change is still detected.
    updated.restEndpointUri = "http://other-host:8181/api/catalog";
    assertFalse(existing.equalsIgnoringNotMetadataImpacting(updated));
  }

  /**
   * ManagedStoragePlugin.replacePlugin skips the plugin restart only when ConnectionConf.equals
   * holds. Changing a cache setting alone keeps the metadata (equalsIgnoringNotMetadataImpacting)
   * but is not equal, so the source update starts a new plugin, which re-reads the
   * plugins.restcatalog.* options.
   */
  @Test
  public void testCacheSettingChangeRestartsPluginButKeepsMetadata() {
    RestIcebergCatalogPluginConfig existing = newPolarisConfig();

    RestIcebergCatalogPluginConfig cachingOff = newPolarisConfig();
    cachingOff.isCachingEnabled = false;
    assertNotEquals(existing, cachingOff);
    assertTrue(existing.equalsIgnoringNotMetadataImpacting(cachingOff));

    RestIcebergCatalogPluginConfig halfCache = newPolarisConfig();
    halfCache.maxCacheSpacePct = 50;
    assertNotEquals(existing, halfCache);
    assertTrue(existing.equalsIgnoringNotMetadataImpacting(halfCache));

    // The same settings sent again are equal: no restart.
    assertEquals(existing, newPolarisConfig());
  }

  @Test
  public void testPersistedTagsAreStable() {
    Map<String, Integer> expected = new HashMap<>();
    expected.put("propertyList", 1);
    expected.put("secretPropertyList", 2);
    expected.put("enableAsync", 3);
    expected.put("isCachingEnabled", 4);
    expected.put("maxCacheSpacePct", 5);
    expected.put("restEndpointUri", 10);
    expected.put("allowedNamespaces", 11);
    expected.put("isRecursiveAllowedNamespaces", 12);
    expected.put("isUsingVendedCredentials", 13);
    assertEquals(expected, tagsByFieldName());
  }

  @Test
  public void testProtostuffRoundTripWithVendedCredentials() {
    for (boolean vended : new boolean[] {true, false}) {
      RestIcebergCatalogPluginConfig conf = newPolarisConfig();
      conf.isUsingVendedCredentials = vended;
      conf.allowedNamespaces = new ArrayList<>(Arrays.asList("ns1", "ns2.child"));
      conf.isRecursiveAllowedNamespaces = false;
      conf.maxCacheSpacePct = 42;

      RestIcebergCatalogPluginConfig read = readCurrent(conf.toBytes());

      assertEquals(vended, read.isUsingVendedCredentials);
      assertEquals(conf.restEndpointUri, read.restEndpointUri);
      assertEquals(conf.propertyList, read.propertyList);
      assertEquals(conf.secretPropertyList, read.secretPropertyList);
      assertEquals(conf.allowedNamespaces, read.allowedNamespaces);
      assertFalse(read.isRecursiveAllowedNamespaces);
      assertEquals(42, read.maxCacheSpacePct);
      assertEquals(conf, read);
      assertEquals(conf, conf.clone());
    }
  }

  @Test
  public void testHandEncoded2520BytesDeserializeWithVendedFalse() {
    // Wire bytes laid out exactly like a 25.2.0 RESTCATALOG source (tags 1-5, 10-12).
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writeProperty(out, 1, "warehouse", "legacy_wh");
    writeProperty(out, 1, "scope", "PRINCIPAL_ROLE:ALL");
    writeProperty(out, 2, "credential", "legacy-id:" + TEST_CLIENT_SECRET);
    writeVarintField(out, 3, 0); // enableAsync = false
    writeVarintField(out, 4, 1); // isCachingEnabled = true
    writeVarintField(out, 5, 55); // maxCacheSpacePct
    writeString(out, 10, "http://legacy-host:8181/api/catalog");
    writeString(out, 11, "legacy_ns");
    writeVarintField(out, 12, 0); // isRecursiveAllowedNamespaces = false

    RestIcebergCatalogPluginConfig conf = readCurrent(out.toByteArray());

    assertFalse(conf.isUsingVendedCredentials);
    assertEquals(
        Arrays.asList(
            new Property("warehouse", "legacy_wh"), new Property("scope", "PRINCIPAL_ROLE:ALL")),
        conf.propertyList);
    assertEquals(
        Arrays.asList(new Property("credential", "legacy-id:" + TEST_CLIENT_SECRET)),
        conf.secretPropertyList);
    assertFalse(conf.enableAsync);
    assertTrue(conf.isCachingEnabled);
    assertEquals(55, conf.maxCacheSpacePct);
    assertEquals("http://legacy-host:8181/api/catalog", conf.restEndpointUri);
    assertEquals(Arrays.asList("legacy_ns"), conf.allowedNamespaces);
    assertFalse(conf.isRecursiveAllowedNamespaces);
    assertEquals("RESTCATALOG", conf.getType());
  }

  @Test
  public void testRuntimeSchema2520BytesDeserializeWithVendedFalse() {
    RestCatalogConf2520 legacy = new RestCatalogConf2520();
    legacy.restEndpointUri = "http://legacy-host:8181/api/catalog";
    legacy.propertyList = new ArrayList<>(Arrays.asList(new Property("warehouse", "legacy_wh")));
    legacy.secretPropertyList =
        new ArrayList<>(Arrays.asList(new Property("credential", "legacy-id:legacy-secret")));
    legacy.allowedNamespaces = new ArrayList<>(Arrays.asList("a", "b.c"));
    legacy.isRecursiveAllowedNamespaces = false;
    legacy.enableAsync = false;
    legacy.isCachingEnabled = false;
    legacy.maxCacheSpacePct = 7;
    Schema<RestCatalogConf2520> legacySchema = RuntimeSchema.getSchema(RestCatalogConf2520.class);
    byte[] bytes = ProtobufIOUtil.toByteArray(legacy, legacySchema, LinkedBuffer.allocate());

    RestIcebergCatalogPluginConfig conf = readCurrent(bytes);

    assertFalse(conf.isUsingVendedCredentials);
    assertEquals(legacy.restEndpointUri, conf.restEndpointUri);
    assertEquals(legacy.propertyList, conf.propertyList);
    assertEquals(legacy.secretPropertyList, conf.secretPropertyList);
    assertEquals(legacy.allowedNamespaces, conf.allowedNamespaces);
    assertFalse(conf.isRecursiveAllowedNamespaces);
    assertFalse(conf.enableAsync);
    assertFalse(conf.isCachingEnabled);
    assertEquals(7, conf.maxCacheSpacePct);
  }

  @Test
  public void testCurrentBytesStillReadableWith2520Schema() {
    // Downgrade / mixed-version safety: unknown tag 13 is skipped by the old schema.
    RestIcebergCatalogPluginConfig conf = newPolarisConfig();
    conf.isUsingVendedCredentials = true;
    conf.isRecursiveAllowedNamespaces = false;

    Schema<RestCatalogConf2520> legacySchema = RuntimeSchema.getSchema(RestCatalogConf2520.class);
    RestCatalogConf2520 legacy = legacySchema.newMessage();
    ProtobufIOUtil.mergeFrom(conf.toBytes(), legacy, legacySchema);

    assertEquals(conf.restEndpointUri, legacy.restEndpointUri);
    assertEquals(conf.propertyList, legacy.propertyList);
    assertEquals(conf.secretPropertyList, legacy.secretPropertyList);
    assertFalse(legacy.isRecursiveAllowedNamespaces);
  }

  // ---------------------------------------------------------------------------------------------
  // Logging / error messages never carry secret values (item 9, G-13)
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testIsSensitivePropertyKeyMatchesUiRule() {
    // Same cases as isSensitivePropertyKey in dac/ui/src/utils/sourceUtils-spec.ts.
    for (String key :
        Arrays.asList(
            "credential",
            " Token ",
            "header.Authorization",
            "fs.s3a.access.key",
            "s3.access-key-id",
            "S3.Secret-Access-Key",
            "fs.s3a.bucket.x.secret.key",
            "fs.azure.account.key.acct",
            "gcs.oauth2.token",
            "adls.sas-token.acct",
            "my-client-secret",
            "db.Password",
            "gcs.private-key",
            "header.X-Api-Token",
            "refresh_token")) {
      assertTrue(key, RestIcebergCatalogPlugin.isSensitivePropertyKey(key));
    }
    for (String key :
        Arrays.asList(
            "warehouse",
            "scope",
            "fs.s3a.endpoint",
            "oauth2-server-uri",
            "token-expires-in-ms",
            "fs.s3a.path.style.access",
            "",
            null)) {
      assertFalse(String.valueOf(key), RestIcebergCatalogPlugin.isSensitivePropertyKey(key));
    }
  }

  /**
   * The UI blocks secret keys in the plain property list with its own copy of the rule ({@code
   * isSensitivePropertyKey} in {@code dac/ui/src/utils/sourceUtils.ts}). Fails when the two copies
   * drift apart. Skipped when the UI sources are not available (e.g. a partial checkout).
   */
  @Test
  public void testSensitiveKeyRuleIsInSyncWithUi() throws IOException {
    Path sourceUtils = Paths.get("..", "..", "dac", "ui", "src", "utils", "sourceUtils.ts");
    Assume.assumeTrue(
        "UI sources not available: " + sourceUtils.toAbsolutePath(),
        Files.isRegularFile(sourceUtils));
    String ts = new String(Files.readAllBytes(sourceUtils), StandardCharsets.UTF_8);

    assertEquals(
        new TreeSet<>(RestIcebergCatalogPlugin.SENSITIVE_PROPERTY_KEYS),
        new TreeSet<>(tsStringArray(ts, "SENSITIVE_PROPERTY_KEYS")));
    assertEquals(
        RestIcebergCatalogPlugin.SENSITIVE_KEY_SUBSTRINGS,
        tsStringArray(ts, "SENSITIVE_KEY_SUBSTRINGS"));
    assertEquals(
        RestIcebergCatalogPlugin.SENSITIVE_KEY_SUFFIXES,
        tsStringArray(ts, "SENSITIVE_KEY_SUFFIXES"));
  }

  /** The string elements of {@code const <name> = [...]} in TypeScript source. */
  private static List<String> tsStringArray(String ts, String name) {
    Matcher array =
        Pattern.compile("const " + name + "\\s*=\\s*\\[(.*?)\\]", Pattern.DOTALL).matcher(ts);
    assertTrue("array " + name + " not found in sourceUtils.ts", array.find());
    Matcher element = Pattern.compile("\"([^\"]*)\"").matcher(array.group(1));
    List<String> values = new ArrayList<>();
    while (element.find()) {
      values.add(element.group(1));
    }
    assertFalse("array " + name + " is empty", values.isEmpty());
    return values;
  }

  @Test
  public void testSensitiveKeyInPropertyListLogsWarningWithKeyNameOnly() throws Exception {
    RestIcebergCatalogPluginConfig conf = new RestIcebergCatalogPluginConfig();
    conf.restEndpointUri = "http://localhost:8181/api/catalog";
    conf.propertyList =
        new ArrayList<>(
            Arrays.asList(
                new Property("warehouse", "test_catalog"),
                new Property("fs.s3a.secret.key", TEST_S3_SECRET),
                new Property("credential", "test-client-id:" + TEST_CLIENT_SECRET)));

    Logger dremioLogger = (Logger) LoggerFactory.getLogger("com.dremio");
    Level previousLevel = dremioLogger.getLevel();
    ListAppender<ILoggingEvent> appender = attachAppender(dremioLogger);
    if (!dremioLogger.isEnabledFor(Level.WARN)) {
      dremioLogger.setLevel(Level.WARN);
    }
    RestIcebergCatalogPlugin plugin = null;
    try {
      plugin = newPlugin(conf);
      plugin.start();
    } finally {
      if (plugin != null) {
        plugin.close();
      }
      dremioLogger.detachAppender(appender);
      dremioLogger.setLevel(previousLevel);
    }

    StringBuilder warnings = new StringBuilder();
    for (ILoggingEvent event : appender.list) {
      String text = eventText(event);
      assertNoSecretIn("log event", text);
      if (event.getLevel().isGreaterOrEqual(Level.WARN)) {
        warnings.append(text).append('\n');
      }
    }
    assertTrue(
        "expected a WARN listing exactly the sensitive keys, got: " + warnings,
        warnings.toString().contains("[fs.s3a.secret.key, credential]"));
  }

  @Test
  public void testConnectionFailureDoesNotExposeSecrets() throws Exception {
    String fullCredential = "test-client-id:" + TEST_CLIENT_SECRET;
    // The (fake) authorization server echoes the submitted credential in its error description, so
    // the failure message really carries the secret and redaction is exercised end to end.
    String errorBody =
        "{\"error\":\"invalid_client\",\"error_description\":\"Client authentication failed for "
            + fullCredential
            + " (secret "
            + TEST_CLIENT_SECRET
            + ")\"}";

    Logger pluginLogger =
        (Logger) LoggerFactory.getLogger(RestIcebergCatalogPlugin.class.getName());
    Logger dremioLogger = (Logger) LoggerFactory.getLogger("com.dremio");
    Logger rootLogger = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
    Level previousPluginLevel = pluginLogger.getLevel();
    SourceState state;
    ListAppender<ILoggingEvent> pluginAppender;
    ListAppender<ILoggingEvent> dremioAppender;
    ListAppender<ILoggingEvent> rootAppender;
    try (Fixed401Server server = new Fixed401Server(errorBody)) {
      String base = "http://127.0.0.1:" + server.port() + "/api/catalog";
      RestIcebergCatalogPluginConfig conf = newPolarisConfig();
      conf.restEndpointUri = base;
      conf.propertyList.removeIf(p -> "oauth2-server-uri".equals(p.name));
      conf.propertyList.add(new Property("oauth2-server-uri", base + "/v1/oauth/tokens"));

      // getState() logs the failure at DEBUG: enable it so that the log path is captured.
      pluginLogger.setLevel(Level.DEBUG);
      pluginAppender = attachAppender(pluginLogger);
      dremioAppender = attachAppender(dremioLogger);
      rootAppender = attachAppender(rootLogger);
      RestIcebergCatalogPlugin plugin = newPlugin(conf);
      try {
        plugin.start();
        state = plugin.getState();
      } finally {
        plugin.close();
        pluginLogger.detachAppender(pluginAppender);
        dremioLogger.detachAppender(dremioAppender);
        rootLogger.detachAppender(rootAppender);
        pluginLogger.setLevel(previousPluginLevel);
      }
    }

    assertEquals(SourceState.SourceStatus.bad, state.getStatus());
    assertNoSecretIn("suggested user action", state.getSuggestedUserAction());
    StringBuilder messages = new StringBuilder();
    for (SourceState.Message message : state.getMessages()) {
      assertNoSecretIn("source state message", message.getMessage());
      messages.append(message.getMessage()).append('\n');
    }
    assertNoSecretIn("source state", state.toString());
    // The failure was classified and the echoed secret was masked, not dropped.
    assertTrue(messages.toString(), messages.toString().contains("HTTP 401"));
    assertTrue(
        messages.toString(),
        messages.toString().contains("Client authentication failed for **** (secret ****)"));

    boolean debugLogged = false;
    for (ILoggingEvent event : pluginAppender.list) {
      String text = eventText(event);
      assertNoSecretIn("log event", text);
      if (event.getLevel() == Level.DEBUG && text.contains("HTTP 401")) {
        debugLogged = true;
      }
    }
    assertTrue("expected the DEBUG failure log of getState()", debugLogged);
    for (ILoggingEvent event : dremioAppender.list) {
      assertNoSecretIn("log event", eventText(event));
    }
    for (ILoggingEvent event : rootAppender.list) {
      assertNoSecretIn("log event", eventText(event));
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Failure classification and redaction (describeConnectionFailure / redactSecrets)
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testDescribeNotAuthorizedRedactsFullCredentialAndSecretPart() {
    RestIcebergCatalogPlugin plugin = newPlugin(newPolarisConfig());
    String description =
        plugin.describeConnectionFailure(
            new RuntimeException(
                "wrapper",
                new NotAuthorizedException(
                    "Not authorized: invalid_client: bad %s, secret %s",
                    "test-client-id:" + TEST_CLIENT_SECRET, TEST_CLIENT_SECRET)));

    assertTrue(description, description.contains("rejected the credentials (HTTP 401)"));
    assertTrue(description, description.contains("invalid_client: bad ****, secret ****"));
    assertFalse(description, description.contains("test-client-id:"));
    assertNoSecretIn("description", description);
  }

  @Test
  public void testDescribeForbidden() {
    String description =
        newPlugin(newPolarisConfig())
            .describeConnectionFailure(
                new ForbiddenException("Forbidden: principal using %s", TEST_S3_ACCESS));

    assertTrue(description, description.contains("denied access (HTTP 403)"));
    assertTrue(description, description.contains("principal using ****"));
    assertNoSecretIn("description", description);
  }

  @Test
  public void testDescribeBadRequest() {
    String description =
        newPlugin(newPolarisConfig())
            .describeConnectionFailure(
                new BadRequestException("Malformed request: key %s", TEST_S3_SECRET));

    assertTrue(description, description.contains("rejected the request (HTTP 400)"));
    assertTrue(description, description.contains("'warehouse'"));
    assertTrue(description, description.contains("key ****"));
    assertNoSecretIn("description", description);
  }

  @Test
  public void testDescribeRestExceptionWithNetworkCause() {
    RestIcebergCatalogPluginConfig conf = newPolarisConfig();
    conf.restEndpointUri = "http://user:pw@catalog-host:8181/api/catalog?token=abc#frag";
    String description =
        newPlugin(conf)
            .describeConnectionFailure(
                new RESTException(
                    new ConnectException("Connection refused"),
                    "Error occurred while processing %s request",
                    "POST"));

    assertTrue(
        description,
        description.contains(
            "Unable to reach the Iceberg REST catalog at http://catalog-host:8181/api/catalog."));
    assertTrue(description, description.contains("Details: Connection refused"));
    assertFalse(description, description.contains("user:pw"));
    assertFalse(description, description.contains("token=abc"));
  }

  @Test
  public void testDescribeRestExceptionWithoutNetworkCause() {
    String description =
        newPlugin(newPolarisConfig())
            .describeConnectionFailure(
                new RESTException("Unable to process: %s", "test-client-id:" + TEST_CLIENT_SECRET));

    assertTrue(description, description.contains("returned an unexpected HTTP error"));
    assertTrue(description, description.contains("Unable to process: ****"));
    assertNoSecretIn("description", description);
  }

  @Test
  public void testDescribeOtherFailure() {
    String description =
        newPlugin(newPolarisConfig())
            .describeConnectionFailure(new IllegalStateException("boom " + TEST_S3_SECRET));
    assertTrue(
        description, description.contains("Could not initialize the Iceberg REST catalog client."));
    assertTrue(description, description.contains("Details: boom ****"));
    assertNoSecretIn("description", description);

    String noMessage =
        newPlugin(newPolarisConfig()).describeConnectionFailure(new IllegalStateException());
    assertTrue(noMessage, noMessage.contains("Details: IllegalStateException"));
  }

  @Test
  public void testRedactSecretsIgnoresShortValuesAndMatchesMediumValuesAsWholeTokens() {
    RestIcebergCatalogPluginConfig conf = new RestIcebergCatalogPluginConfig();
    conf.restEndpointUri = "http://localhost:8181/api/catalog";
    conf.propertyList = new ArrayList<>(Arrays.asList(new Property("warehouse", "bucketabc")));
    conf.secretPropertyList =
        new ArrayList<>(
            Arrays.asList(
                new Property("credential", "id:1"),
                new Property("fs.s3a.access.key", "abc"),
                new Property("token", "abcd")));
    RestIcebergCatalogPlugin plugin = newPlugin(conf);

    // "1" and "abc" are too short to redact; "id:1" and "abcd" only as whole tokens.
    assertEquals(
        "HTTP 401 for bucketabc, token ****, xabcdx, credential ****",
        plugin.redactSecrets("HTTP 401 for bucketabc, token abcd, xabcdx, credential id:1"));
  }

  @Test
  public void testRedactSecretsReplacesLongestValueFirst() {
    RestIcebergCatalogPlugin plugin = newPlugin(newPolarisConfig());
    String fullCredential = "test-client-id:" + TEST_CLIENT_SECRET;

    assertEquals(
        "before **** after ****x",
        plugin.redactSecrets("before " + fullCredential + " after " + TEST_CLIENT_SECRET + "x"));
    assertNull(plugin.redactSecrets(null));
  }
}
