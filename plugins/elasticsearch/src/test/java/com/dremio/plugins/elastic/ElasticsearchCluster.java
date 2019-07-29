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
package com.dremio.plugins.elastic;

import static com.dremio.plugins.elastic.ElasticsearchConf.AuthenticationType.ACCESS_KEY;
import static com.dremio.plugins.elastic.ElasticsearchConf.AuthenticationType.EC2_METADATA;
import static com.dremio.plugins.elastic.ElasticsearchConf.AuthenticationType.ES_ACCOUNT;
import static com.dremio.plugins.elastic.ElasticsearchConf.AuthenticationType.NONE;
import static com.dremio.plugins.elastic.ElasticsearchConstants.DEFAULT_READ_TIMEOUT_MILLIS;
import static com.dremio.plugins.elastic.ElasticsearchConstants.DEFAULT_SCROLL_TIMEOUT_MILLIS;
import static java.lang.String.format;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.Key;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.WebTarget;

import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.AlgorithmIdentifier;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.crypto.util.PrivateKeyFactory;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.DefaultDigestAlgorithmIdentifierFinder;
import org.bouncycastle.operator.DefaultSignatureAlgorithmIdentifierFinder;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.bc.BcRSAContentSignerBuilder;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.elasticsearch.Version;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.exec.catalog.conf.AWSAuthenticationType;
import com.dremio.exec.catalog.conf.AuthenticationType;
import com.dremio.exec.catalog.conf.EncryptionValidationMode;
import com.dremio.exec.catalog.conf.Host;
import com.dremio.plugins.elastic.ElasticActions.IndexExists;
import com.dremio.plugins.elastic.ElasticActions.Result;
import com.dremio.plugins.elastic.ElasticActions.SearchBytes;
import com.dremio.plugins.elastic.ElasticConnectionPool.ElasticConnection;
import com.dremio.plugins.elastic.ElasticConnectionPool.TLSValidationMode;
import com.dremio.plugins.elastic.ElasticTestActions.AliasActionDef;
import com.dremio.plugins.elastic.ElasticTestActions.Bulk;
import com.dremio.plugins.elastic.ElasticTestActions.CreateAliases;
import com.dremio.plugins.elastic.ElasticTestActions.CreateIndex;
import com.dremio.plugins.elastic.ElasticTestActions.DeleteIndex;
import com.dremio.plugins.elastic.ElasticTestActions.PutMapping;
import com.dremio.plugins.elastic.util.ProxyServerFactory;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSource;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


/**
 * Utilities for testing against embedded or remote elasticsearch clusters.
 */
public class ElasticsearchCluster implements Closeable {

  public static final boolean USE_EXTERNAL_ES5 = Boolean.getBoolean("dremio.elastic.external");

  private static final String BULK_INDEX_TEXT = "{ \"index\" : { \"_index\" : \"%s\", \"_type\" : \"%s\" } }";

  private static final JsonParser PARSER = new JsonParser();
  public static final int ELASTICSEARCH_PORT = 9200;

  private static final Logger logger = LoggerFactory.getLogger(ElasticsearchCluster.class);

  private static final String CLUSTER_NAME = "dremio-test-cluster";
  private static final String NODE_NAME_PREFIX = "elastic-test-node-";

  private final String host;
  private final Integer port;
  private final String username;
  private final String password;
  private final String accessKey;
  private final String accessSecret;
  private final boolean overwriteRegion;
  private final String regionName;
  private final ElasticsearchConf.AuthenticationType authenticationType;
  /* Authentication type used to access elasticsearch for the unit tests.
   * ES_ACCOUNT: provide user/password as credentials to access regular elasticsearch.
   * ACCESS_KEY: provide access key/secret and region as credentials to access Amazon Elasticsearch Service.
   * EC2_METADATA: provide region and use the EC2 metadata to get credentials to access Amazon Elasticsearch Service.
   * NONE: access elasticsearch anonymously.
   */
  private final String testAuthenticationType;

  private final Random random;

  private Server proxy;
  private boolean remote = false;
  private boolean deleteExisting = false;
  private Version version;
  private final boolean scriptsEnabled;
  private final boolean showIDColumn;
  private final boolean sslEnabled;
  private final boolean useWhiteList;

  private ElasticConnectionPool pool;
  private ElasticConnection connection;
  private WebTarget webTarget;

  private int sslPort;
  private Client client;
  private int scrollSize;

  public ElasticsearchCluster(int scrollSize, Random random, boolean scriptsEnabled, boolean showIDColumn, boolean publishHost, boolean sslEnabled) throws IOException {
    this(scrollSize, random, scriptsEnabled, showIDColumn, publishHost, sslEnabled, null);
  }

  /**
   * @param sslEnabled only compatible with size == 1
   * @throws IOException
   */
  public ElasticsearchCluster(int scrollSize, Random random, boolean scriptsEnabled, boolean showIDColumn, boolean publishHost, boolean sslEnabled, Integer presetSSLPort) throws IOException {
    this.scrollSize = scrollSize;
    this.random = random;
    this.scriptsEnabled = scriptsEnabled;
    this.showIDColumn = showIDColumn;

    this.testAuthenticationType = System.getProperty("dremio.elastic.authentication.type", "NONE").toUpperCase();
    switch (testAuthenticationType) {
      case "ES_ACCOUNT":
        this.authenticationType = ES_ACCOUNT;
        this.sslEnabled = sslEnabled;
        this.useWhiteList = false;
        break;
      case "ACCESS_KEY":
        this.authenticationType = ACCESS_KEY;
        this.sslEnabled = true;
        this.useWhiteList = true;
        break;
      case "EC2_METADATA":
        this.authenticationType = EC2_METADATA;
        this.sslEnabled = true;
        this.useWhiteList = true;
        break;
      default:
        this.authenticationType = NONE;
        this.sslEnabled = sslEnabled;
        this.useWhiteList = false;
        break;
    }
    this.host = System.getProperty("dremio.elastic.host", "127.0.0.1");
    this.port = Integer.valueOf(System.getProperty("dremio.elastic.port", Integer.toString(sslEnabled ? sslPort : ELASTICSEARCH_PORT)));
    this.username = System.getProperty("dremio.elastic.username", "");
    this.password = System.getProperty("dremio.elastic.password", "");
    this.accessKey = System.getProperty("dremio.elastic.access.key", "");
    this.accessSecret = System.getProperty("dremio.elastic.access.secret", "");
    this.overwriteRegion = "true".equals(System.getProperty("dremio.elastic.region.overwrite", "false"));
    this.regionName = System.getProperty("dremio.elastic.region.name", "");
    initClient();
  }

  private void initClient() throws IOException {
    List<Host> hosts = ImmutableList.of(new Host(host, port));

    this.pool = new ElasticConnectionPool(hosts, sslEnabled ? TLSValidationMode.UNSECURE : TLSValidationMode.OFF, new ElasticsearchAuthentication(hosts, authenticationType,
      username, password, accessKey, accessSecret, regionName), 10000, useWhiteList);
    pool.connect();
    connection = pool.getRandomConnection();
    webTarget = connection.getTarget();
  }

  private int setupSSLProxy(File keystoreFile, String password, String targetHost, int targetPort, Integer presetSSLPort) {
    proxy = ProxyServerFactory.of(format("http://%s:%d", targetHost, targetPort), presetSSLPort != null ? presetSSLPort : 0, keystoreFile, password);

    try {
      proxy.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    int port = ((ServerConnector) proxy.getConnectors()[0]).getLocalPort();
    logger.info("Proxy started on https://localhost:" + port);
    return port;
  }

  private static void genCertificate(File keystoreFile, String password) {
    logger.info("generate certificate at " + keystoreFile.getAbsolutePath());
    try {
      KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
      if (keystoreFile.exists()) {
        try (FileInputStream fis = new FileInputStream(keystoreFile)) {
          keyStore.load(fis, password.toCharArray());
          Key key = keyStore.getKey("proxy", password.toCharArray());
          if (key != null) {
            // key exists, no need to generate one
            logger.info("reusing existing certificate at " + keystoreFile.getAbsolutePath());
            return;
          }
        } catch (Exception e) {
          logger.warn("Ignoring old keystore file " + keystoreFile.getAbsolutePath(), e);
        }
      }
      keystoreFile.delete();

      keyStore.load(null, password.toCharArray());

      KeyPairGenerator keyPairGenerator = newKeyPairGenerator("RSA", 2048);

      KeyPair keyPair = keyPairGenerator.generateKeyPair();

      Certificate cert = genSelfSignedCert(keyPair, "SHA256WithRSAEncryption");

      keyStore.setKeyEntry("proxy", keyPair.getPrivate(), password.toCharArray(), new Certificate[]{ cert } );
      keystoreFile.getParentFile().mkdirs();
      try (FileOutputStream fos = new FileOutputStream(keystoreFile);) {
        keyStore.store(fos, password.toCharArray());
      }
    } catch (KeyStoreException | NoSuchAlgorithmException | CertificateException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  public String mapping(String resource1, String resource2) {
    return "";
  }

  private static Certificate genSelfSignedCert(KeyPair keyPair, String signAlgo) throws CertificateException {
    X500Name issuer = new X500Name("CN=localhost, OU=test, O=Dremio, L=Mountain View, ST=CA, C=US");
    X500Name subject = issuer; // self signed
    BigInteger serial = BigInteger.valueOf(new Random().nextInt());
    Date notBefore = new Date(System.currentTimeMillis() - (24 * 3600 * 1000));
    Date notAfter = new Date(System.currentTimeMillis() + (24 * 3600 * 1000));
    SubjectPublicKeyInfo pubkeyInfo = SubjectPublicKeyInfo.getInstance(keyPair.getPublic().getEncoded());
    X509v3CertificateBuilder certBuilder = new X509v3CertificateBuilder(issuer, serial, notBefore, notAfter, subject, pubkeyInfo);
    ContentSigner signer = newSigner(keyPair.getPrivate(), signAlgo);
    X509CertificateHolder certHolder = certBuilder.build(signer);

    Certificate cert = new JcaX509CertificateConverter().getCertificate(certHolder);
    return cert;
  }

  private static KeyPairGenerator newKeyPairGenerator(String algorithmIdentifier,
      int bitCount) {
    try {
      KeyPairGenerator kpg = KeyPairGenerator.getInstance(algorithmIdentifier);
      kpg.initialize(bitCount, new SecureRandom());
      return kpg;
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  private static ContentSigner newSigner(PrivateKey privateKey, String algo) {
    try {
        AlgorithmIdentifier sigAlgId = new DefaultSignatureAlgorithmIdentifierFinder().find(algo);
        AlgorithmIdentifier digAlgId = new DefaultDigestAlgorithmIdentifierFinder().find(sigAlgId);

        return new BcRSAContentSignerBuilder(sigAlgId, digAlgId)
                .build(PrivateKeyFactory.createKey(privateKey.getEncoded()));
    } catch (OperatorCreationException | IOException e) {
        throw new RuntimeException(e);
    }
}

  /**
   * Creates a storage plugin config with values suitable for creating
   * connections to the embedded elasticsearch cluster.
   */
  public BaseElasticStoragePluginConfig config(boolean allowPushdownAnalyzedOrNormalizedFields) {
    if ((this.authenticationType == ES_ACCOUNT) || (this.authenticationType == NONE)) {
      AuthenticationType authenticationType;
      if (this.authenticationType == ES_ACCOUNT) {
        authenticationType = AuthenticationType.MASTER;
      } else {
        authenticationType = AuthenticationType.ANONYMOUS;
      }
      ElasticStoragePluginConfig config = new ElasticStoragePluginConfig(
        ImmutableList.<Host>of(new Host(host, port)),
        username, /* username */
        password, /* password */
        authenticationType,
        scriptsEnabled, /* Scripts enabled */
        false, /* Show Hidden Indices */
        sslEnabled,
        showIDColumn,
        DEFAULT_READ_TIMEOUT_MILLIS,
        DEFAULT_SCROLL_TIMEOUT_MILLIS,
        true, /* use painless */
        useWhiteList, /* use whitelist */
        scrollSize,
        allowPushdownAnalyzedOrNormalizedFields, /* allow group by on normalized fields */
        false, /* warn on row count mismatch */
        EncryptionValidationMode.NO_VALIDATION
      );
      return config;
    } else {
      AWSAuthenticationType authenticationType;
      if (this.authenticationType == ACCESS_KEY) {
        authenticationType = AWSAuthenticationType.ACCESS_KEY;
      } else {
        authenticationType = AWSAuthenticationType.EC2_METADATA;
      }
      AmazonElasticStoragePluginConfig config = new AmazonElasticStoragePluginConfig(
        host, /* Amazon Elasticsearch Service hostname */
        port, /* Amazon Elasticsearch Service port */
        accessKey, /* AWS access key */
        accessSecret, /* AWS access secret */
        overwriteRegion, /* overwrite region */
        regionName, /* region name */
        authenticationType,
        scriptsEnabled, /* Scripts enabled */
        false, /* Show Hidden Indices */
        showIDColumn,
        DEFAULT_READ_TIMEOUT_MILLIS,
        DEFAULT_SCROLL_TIMEOUT_MILLIS,
        true, /* use painless */
        scrollSize,
        allowPushdownAnalyzedOrNormalizedFields, /* allow group by on normalized fields */
        false, /* warn on row count mismatch */
        EncryptionValidationMode.NO_VALIDATION
      );
      return config;
    }
  }

  public BaseElasticStoragePluginConfig config() {
    return config(false);
  }

  /**
   * Features unavailable in lower versions are disabled at one cutoff version. To allow
   * tests to be run against different versions the current version can be checked within
   * a test to customize if we test for a particular pushdown. Tests should always
   * check for correct query results, even if we cannot guarentee that a particular pushdown
   * will always be used.
    */
  public boolean newFeaturesEnabled() {
    // only elastic 5+ now.
    return true;
  }

  /**
   * Shuts down the embedded cluster.
   */
  @Override
  public void close() throws IOException {
    if(pool != null){
      pool.close();
    }
    if(client != null){
      client.close();
    }
  }

    /* ***** Data Utilities ***** */

  public static final EnumSet<ElasticsearchType> ALL_TYPES = EnumSet.allOf(ElasticsearchType.class);

  public static final EnumSet<ElasticsearchType> PRIMITIVE_TYPES = EnumSet.of(
          ElasticsearchType.INTEGER,
          ElasticsearchType.LONG,
          ElasticsearchType.FLOAT,
          ElasticsearchType.DOUBLE,
          ElasticsearchType.BOOLEAN,
          ElasticsearchType.TEXT);

  public static final EnumSet<ElasticsearchType> NESTED_TYPES = EnumSet.of(
          ElasticsearchType.NESTED,
          ElasticsearchType.OBJECT);

  /**
   * Wipes cluster clean of all indices.
   */
  public void wipe() {
    try {
      new DeleteIndex("*").getResult(webTarget);
    } catch (Exception e) {
      logger.warn("--> failed to wipe test indices");
    }
  }

  /**
   * Deletes the given index.
   */
  public void wipe(String... indices) {
    for (String index : indices) {
      try {
        new DeleteIndex(index).getResult(pool.getRandomConnection().getTarget());
      } catch (Exception e) {
        logger.warn("--> failed to delete index: {}", index);
      }
    }
  }

  public void load(String schema, String table, String mapping) throws IOException, URISyntaxException {
    mappingFromFile(schema, table, mapping);
  }

  public void load(String schema, String table, String mapping, String data) throws IOException, URISyntaxException {
    mappingFromFile(schema, table, mapping);
    dataFromFile(schema, table, data);
  }

  public void mappingFromFile(String schema, String table, String file) throws IOException {

    StringBuilder sb = new StringBuilder();

    try (BufferedReader reader = new BufferedReader(
            new InputStreamReader(getClass().getResourceAsStream(file)))) {

      for (String line; (line = reader.readLine()) != null; ) {
        sb.append(line);
      }
    }

    String _mapping = sb.toString().replaceAll("<TABLE_NAME>", table);
    schema(1, 0, schema);

    PutMapping putMapping = new PutMapping(schema, table).setMapping(_mapping);

    connection.execute(putMapping);
  }

  public void load(String schema, String table, Path path) throws IOException, URISyntaxException {
    if (path == null) {
      return;
    }
    File file = path.toFile();
    Bulk bulk = new Bulk();
    int i = 0;
    try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
      for (String line; (line = reader.readLine()) != null; ) {
        bulk.add(String.format(BULK_INDEX_TEXT, schema, table));
        bulk.add(line);
        i++;
      }
    }
    Result response = bulk.getResult(webTarget);
    if (response.getAsJsonObject().get("errors").getAsBoolean()) {
      fail(response.toString());
    }

    logger.info("--> indexed [{}] test documents", i);
  }

  public void dataFromFile(String schema, String table, String dir) throws IOException, URISyntaxException {
    Path path = Paths.get(getClass().getResource(dir).toURI());
    File file = path.toFile();
    File[] files;
    if (file.isDirectory()) {
      files = file.listFiles();
    } else {
      files = new File[]{file};
    }

    if (files == null || files.length == 0) {
      return;
    }

    Bulk bulk = new Bulk();

    for (File data : files) {

      StringBuilder sb = new StringBuilder();
      try (BufferedReader reader = new BufferedReader(new FileReader(data))) {
        for (String line; (line = reader.readLine()) != null; ) {
          sb.append(line);
        }
      }

      bulk.add(String.format(BULK_INDEX_TEXT, schema, table));
      bulk.add(sb.toString());
    }

    Result response = bulk.getResult(webTarget);
    if (response.getAsJsonObject().get("errors").getAsBoolean()) {
      fail(response.toString());
    }

    logger.info("--> indexed [{}] test documents", files.length);
  }

  public void load(String schema, String table, ColumnData[] data) throws IOException {

    schema(1, 0, schema);

    PutMapping putMapping = new PutMapping(schema, table);

    XContentBuilder json = XContentFactory.jsonBuilder();
    json.startObject().startObject(table).startObject("properties");

    for (ColumnData datum : data) {
      json.startObject(datum.name);
      json.field("type", datum.type.name().toLowerCase(Locale.ENGLISH));
      if (datum.attributes != null) {
        for (Map.Entry<String, String> entry : datum.attributes.entrySet()) {
          json.field(entry.getKey(), entry.getValue());
        }
      }
      json.endObject();
    }

    json.endObject().endObject().endObject();
    putMapping.setMapping(json.string());
    connection.execute(putMapping);

    int max = 0;
    for (ColumnData datum : data) {
      if (datum.rows != null) {
        max = Math.max(max, datum.rows.length);
      }
    }

    if (max == 0) {
      return;
    }

    Bulk bulk = new Bulk();

    for (int i = 0; i < max; i++) {

      json = XContentFactory.jsonBuilder().startObject();

      for (ColumnData datum : data) {
        if (datum.rows != null && i < datum.rows.length && datum.rows[i] != null) {
          Object[] row = datum.rows[i];
          if (row.length == 1) {
            json.field(datum.name, row[0]);
          } else {
            json.field(datum.name, row);
          }
        }
      }

      bulk.add(String.format(BULK_INDEX_TEXT, schema, table));
      bulk.add(json.endObject().string());
    }

    Result response = bulk.getResult(webTarget);
    if (response.getAsJsonObject().get("errors").getAsBoolean()) {
      fail(response.toString());
    }

    // ensure that we wait for any new shards to be initialized.
    logger.info("--> indexed [{}] test documents", data.length);
  }

  /**
   * Creates a table in the given schema with the given name with fields for
   * all supported data types.
   */
  public void table(String schema, String... tables) throws IOException {
    for (String table : tables) {
      table(1, 0, schema, table, ALL_TYPES);
    }
  }

  /**
   * Creates a table in the given schema with the given name with fields for
   * all supported data types.
   */
  public void table(int shards, int replicas, String schema, String... tables) throws IOException {
    for (String table : tables) {
      table(shards, replicas, schema, table, ALL_TYPES);
    }
  }

  /**
   * Creates a table in the given schema with the given name with fields for
   * the given data types.
   */
  public void table(String schema, String table, EnumSet<ElasticsearchType> types)
          throws IOException {
    table(1, 0, schema, table, types);
  }

  /**
   * Creates a table in the given schema with the given name with fields for
   * the given data types.
   */
  public void table(int shards, int replicas, String schema, String table, EnumSet<ElasticsearchType> types)
          throws IOException {

    schema(shards, replicas, schema);

    PutMapping putMapping = new PutMapping(schema, table);
    XContentBuilder json = XContentFactory.jsonBuilder();
    json.startObject().startObject(table).startObject("properties");

    for (ElasticsearchType type : types) {
      switch (type) {
        case INTEGER:
        case LONG:
        case FLOAT:
        case DOUBLE:
        case BOOLEAN:
        case TEXT:
          String name = type.name().toLowerCase(Locale.ENGLISH);
          json.startObject(name + "_field");
          json.field("type", name);
          json.endObject();
          break;
        case NESTED:
          json.startObject("person").field("type", "nested").startObject("properties");
          json.startObject("first_name").field("type", "text").endObject();
          json.startObject("last_name").field("type", "text").endObject();
          json.startObject("ssn").field("type", "integer").endObject();

          json.startObject("address").field("type", "nested").startObject("properties");
          json.startObject("street_line_1").field("type", "text").field("index", "false")
                  .endObject();
          json.startObject("street_line_2").field("type", "text").field("index", "false")
                  .endObject();
          json.startObject("city").field("type", "text").field("index", "false").endObject();
          json.startObject("state").field("type", "text").field("index", "false").endObject();
          json.startObject("zipcode").field("type", "integer").endObject();
          json.startObject("coordinates").field("type", "geo_point").endObject();
          json.endObject();
          json.endObject();

          json.startObject("relative").field("type", "nested").startObject("properties");
          json.startObject("first_name").field("type", "text").endObject();
          json.startObject("last_name").field("type", "text").endObject();
          json.endObject();
          json.endObject();
          json.endObject();

          json.endObject();
          break;
        case OBJECT:
          json.startObject("person2").field("type", "object").startObject("properties");
          json.startObject("first_name").field("type", "text").endObject();
          json.startObject("last_name").field("type", "text").endObject();
          json.startObject("ssn").field("type", "integer").endObject();
          json.endObject().endObject();
          break;
        case GEO_POINT:
          json.startObject("geo_location_1").field("type", "geo_point").endObject();
          break;
        case GEO_SHAPE:
          break;
      }
    }

    json.endObject().endObject().endObject();

    putMapping.setMapping(json.string());
    connection.execute(putMapping);
  }

  private static int[] randomIntArray(int size, Random random) {
    int ia[] = new int[size];
    for (int i = 0; i < size; i++) {
      ia[i] = random.nextInt();
    }
    return ia;
  }

  private static float[] randomFloatArray(int size, Random random) {
    float fa[] = new float[size];
    for (int i = 0; i < size; i++) {
      fa[i] = random.nextFloat();
    }
    return fa;
  }

  public static class ColumnData {

    final String name;
    final Object[][] rows;
    final ElasticsearchType type;
    final Map<String, String> attributes;

    public ColumnData(String name, ElasticsearchType type, Object[][] rows) {
      this(name, type, null, rows);
    }

    public ColumnData(String name, ElasticsearchType type, Map<String, String> attributes, Object[][] rows) {
      this.name = name;
      this.type = type;
      this.attributes = attributes;
      this.rows = rows;
    }

    public int size() {
      return rows != null ? rows.length : 0;
    }
  }

  public void schema(String... schemas) {
    schema(1, 0, schemas);
  }

  /**
   * Creates schemas with the given name(s).
   */
  public void schema(int shards, int replicas, String... schemas) {
    for (String schema : schemas) {

      if (deleteExisting) {
        wipe(schema);
      }

      IndexExists indexExists = new IndexExists();
      indexExists.addIndex(schema);
      Result result = indexExists.getResult(webTarget);

      if (result.success()) {
        continue;
      }

      CreateIndex createIndex = new CreateIndex(schema, shards, replicas);

      createIndex.getResult(webTarget);
    }
  }

  /**
   * Creates an alias to the given schema(s).
   */
  public void alias(String alias, String... schemas) {
    CreateAliases createAliases = new CreateAliases();
    for (String schema : schemas) {
      createAliases.addAlias(schema, alias);
    }
    createAliases.getResult(webTarget);
  }

  public void aliasWithFilter(String alias, String filter, String... schemas) {
    CreateAliases createAliases = new CreateAliases();
    for (String schema : schemas) {
      createAliases.addAlias(schema, alias, filter);
    }
    createAliases.getResult(webTarget);
  }

  public void alias(List<AliasActionDef> aliasActions) {
    CreateAliases createAliases = new CreateAliases();
    for (AliasActionDef aliasAction : aliasActions) {
      switch (aliasAction.actionType) {
      case ADD:
        createAliases.addAlias(aliasAction.index, aliasAction.alias);
        break;
      case REMOVE:
        createAliases.removeAlias(aliasAction.index, aliasAction.alias);
        break;
      }
    }

    createAliases.getResult(webTarget);
  }

  /**
   * Populates the given index with test data.
   */
  public void populate(String schema, String table, int rows) throws IOException {
    populate(schema, table, rows, ALL_TYPES);
  }

  /**
   * Populates the given index with test data.
   */
  public void populate(String schema, String table, int rows, EnumSet<ElasticsearchType> types) throws IOException {
    populate(1, 0, schema, table, rows, null, types);
  }

  /**
   * Populates the given index with test data.
   */
  public void populate(int shards, int replicas, String schema, String table, int rows,
                       EnumSet<ElasticsearchType> types) throws IOException {
    populate(shards, replicas, schema, table, rows, null, types);
  }

  /**
   * Populates the given index with test data.
   */
  public void populate(int shards, int replicas, String schema, String table, int rows,
                       Map<ElasticsearchType, Tuple<Boolean, Integer>> arrayRandomness,
                       EnumSet<ElasticsearchType> types) throws IOException {

    populate(shards, replicas, schema, table, rows, arrayRandomness, types, false);
  }

  /**
   * Populates the given index with test data.
   */
  public void populate(int shards, int replicas, String schema, String table, int rows,
                       Map<ElasticsearchType, Tuple<Boolean, Integer>> arrayRandomness,
                       EnumSet<ElasticsearchType> types, boolean variations) throws IOException {

    table(shards, replicas, schema, table, types);

    Bulk bulk = new Bulk();

    for (int i = 0; i < rows; i++) {

      bulk.add(String.format(BULK_INDEX_TEXT, schema, table));
      XContentBuilder json = XContentFactory.jsonBuilder().startObject();
      Tuple<Boolean, Integer> tuple;

      for (ElasticsearchType type : types) {
        switch (type) {
          case INTEGER:
          case LONG:
            tuple = arrayRandomness != null ? arrayRandomness.get(type) : null;
            if (tuple != null && tuple.v1()) {
              int size = tuple.v2();
              // Send variable data representations to elastic: [ 2, "2", 1.5 ]
              if (variations) {
                Object oa[] = new Object[size];
                for (int ii = 0; ii < size; ii++) {
                  if (random.nextBoolean()) {
                    oa[ii] = Integer.toString(random.nextInt());
                  } else if (random.nextBoolean()) {
                    oa[ii] = random.nextFloat();
                  } else {
                    oa[ii] = random.nextInt();
                  }
                }
                json.field(type.name().toLowerCase(Locale.ENGLISH) + "_field", oa);
              } else {
                json.field(type.name().toLowerCase(Locale.ENGLISH) + "_field", randomIntArray(size,
                        random));
              }
              json.field(type.name().toLowerCase(Locale.ENGLISH) + "_field", randomIntArray(size,
                      random));
            } else {
              if (variations && random.nextBoolean()) {
                json.field(type.name().toLowerCase(Locale.ENGLISH) + "_field", Integer.toString(i));
              } else {
                json.field(type.name().toLowerCase(Locale.ENGLISH) + "_field", i);
              }
            }
            break;
          case FLOAT:
            tuple = arrayRandomness != null ? arrayRandomness.get(type) : null;
            if (tuple != null && tuple.v1()) {
              int size = tuple.v2();

              // Send variable data representations to elastic: [ 2, "2", 1.5 ]
              if (variations) {
                Object oa[] = new Object[size];
                for (int ii = 0; ii < size; ii++) {
                  if (random.nextBoolean()) {
                    oa[ii] = Float.toString(random.nextFloat());
                  } else if (random.nextBoolean()) {
                    oa[ii] = random.nextFloat();
                  } else if (random.nextBoolean()) {
                    oa[ii] = random.nextDouble();
                  } else {
                    oa[ii] = random.nextInt();
                  }
                }
                json.field(type.name().toLowerCase(Locale.ENGLISH) + "_field", oa);
              } else {
                json.field(type.name().toLowerCase(Locale.ENGLISH) + "_field", randomFloatArray(size,
                        random));
              }
            } else {
              if (variations && random.nextBoolean()) {
                json.field(type.name().toLowerCase(Locale.ENGLISH) + "_field", Float.toString(i));
              } else {
                json.field(type.name().toLowerCase(Locale.ENGLISH) + "_field", (float) i);
              }
            }
            break;
          case DOUBLE:
            tuple = arrayRandomness != null ? arrayRandomness.get(type) : null;
            if (tuple != null && tuple.v1()) {
              double[] doubles = new double[tuple.v2()];
              for (int j = 0; j < doubles.length; j++) {
                doubles[j] = i + j + tuple.v2();
              }
              json.field(type.name().toLowerCase(Locale.ENGLISH) + "_field", doubles);
            } else {
              if (variations && random.nextBoolean()) {
                json.field(type.name().toLowerCase(Locale.ENGLISH) + "_field", Double.toString(
                        i));
              } else {
                json.field(type.name().toLowerCase(Locale.ENGLISH) + "_field", (double) i);
              }

            }
            break;
          case BOOLEAN:
            if (variations && random.nextBoolean()) {
              json.field(type.name().toLowerCase(Locale.ENGLISH) + "_field", random.nextBoolean() ?
                      "true" : "false");
            } else {
              json.field(type.name().toLowerCase(Locale.ENGLISH) + "_field", true);
            }

            break;
          case TEXT:
            tuple = arrayRandomness != null ? arrayRandomness.get(type) : null;
            if (tuple != null && tuple.v1()) {
              String[] strings = new String[tuple.v2()];
              for (int j = 0; j < strings.length; j++) {
                strings[j] = "string_value_" + Integer.toString(i) + Integer.toString(j) + tuple.v2();
              }
              json.field(type.name().toLowerCase(Locale.ENGLISH) + "_field", strings);
            } else {
              json.field(type.name().toLowerCase(Locale.ENGLISH) + "_field", "string_value_" + Integer
                      .toString(i));
            }
            break;
          case NESTED:
            tuple = arrayRandomness != null ? arrayRandomness.get(type) : null;
            if (tuple != null && tuple.v1()) {
              json.startArray("person");
              for (int j = 0; j < 5; j++) {
                //json.startObject("person");
                json.startObject();
                json.field("first_name", "my_first_name_" + j + "_" + i);
                json.field("last_name", "my_last_name_" + j + "_" + i);
                json.field("ssn", 1234 + j + i);
                json.endObject();
                //json.endObject();
              }
              json.endArray();
            } else {
              json.startObject("person");
              json.field("first_name", "my_first_name_" + i);
              json.field("last_name", "my_last_name_" + i);
              json.field("ssn", 1234 + i);

              json.startObject("address");
              json.field("street_line_1", i + " main st.");
              json.field("street_line_2", "#" + i);
              if (i % 2 == 0) {
                json.field("city", "seattle");
              } else {
                json.field("city", "oxford");
              }
              json.field("zipcode", i);

              Map<String, Double> coordinates = new HashMap<>(2);
              coordinates.put("lat", Math.acos(random.nextDouble() * 2 - 1));
              coordinates.put("lon", random.nextDouble() * Math.PI * 2);
              json.field("coordinates", coordinates);

              json.endObject();

              json.startObject("relative");
              json.field("first_name", "relatives_first_name_" + i);
              json.field("last_name", "relatives_last_name_" + i);
              json.endObject();

              json.endObject();
            }
            break;
          case OBJECT:
            json.startObject("person2");
            json.field("first_name", "my_first_name_" + i);
            json.field("last_name", "my_last_name_" + i);
            json.field("ssn", 1234 + i);
            json.endObject();
            break;
          case GEO_POINT:
            json.startObject("geo_location_1");
            json.field("lat", Math.acos(random.nextDouble() * 2 - 1));
            json.field("lon", random.nextDouble() * Math.PI * 2);
            json.endObject();
            break;
          case GEO_SHAPE:
            // XXX - Implement
            break;
          default:
            break;
        }
      }

      bulk.add(json.endObject().string());
    }

    Result response = bulk.getResult(webTarget);
    if (response.getAsJsonObject().get("errors").getAsBoolean()) {
      logger.error("Failed to index test data:\n{}", response.toString());
      fail();
    }

    logger.info("--> indexed [{}] test documents", rows);
  }

  public static class SearchResults {

    public final long count;
    public final String results;

    public SearchResults(long count, String results) {
      this.count = count;
      this.results = results;
    }

    @Override
    public String toString() {
      return new StringBuilder()
              .append("\nTotal Hit Count: ").append(count)
              .append("\nHits:\n").append(results).toString();
    }
  }

  public static String queryString(String query) {
    return String.format("{ \"query\" : %s } ", query);
  }

  public SearchResults search(String schema, String table, QueryBuilder queryBuilder) throws IOException {

    byte[] response = connection.execute(new SearchBytes()
        .setQuery(String.format("{\"query\": %s }", queryBuilder.toString()))
        .setResource(String.format("%s/%s", schema, table))
        .setParameter("size", "1000")
        );

    JsonObject hits = asJsonObject(response).get("hits").getAsJsonObject();
    return new SearchResults(hits.get("total").getAsInt(), hits.get("hits").toString());
  }

  public static JsonObject asJsonObject(byte[] bytes) throws IOException {
    return PARSER.parse(ByteSource.wrap(bytes).asCharSource(Charsets.UTF_8).openStream()).getAsJsonObject();
  }

  public SearchResults search(String schema, String table) throws IOException {
    return search(schema, table, QueryBuilders.matchAllQuery());
  }

  public com.dremio.plugins.Version getMinVersionInCluster() {
    return pool.getMinVersionInCluster();
  }

  /**
   * to spin off a local cluster
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    ElasticsearchCluster c = new ElasticsearchCluster(4000, new Random(), true, false, false, true, 4443);
    System.out.println(c);
    ColumnData[] data = ElasticBaseTestQuery.getBusinessData();
    c.load("foo", "bar", data);
    Thread.sleep(5000000L);
    c.close();
  }



}
