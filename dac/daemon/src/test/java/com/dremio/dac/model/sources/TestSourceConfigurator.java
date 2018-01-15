/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.dac.model.sources;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.dremio.common.store.StoragePluginConfig;
import com.dremio.dac.proto.model.source.ClassPathConfig;
import com.dremio.dac.proto.model.source.DB2Config;
import com.dremio.dac.proto.model.source.ElasticConfig;
import com.dremio.dac.proto.model.source.HBaseConfig;
import com.dremio.dac.proto.model.source.HdfsConfig;
import com.dremio.dac.proto.model.source.HiveConfig;
import com.dremio.dac.proto.model.source.Host;
import com.dremio.dac.proto.model.source.MSSQLConfig;
import com.dremio.dac.proto.model.source.MapRFSConfig;
import com.dremio.dac.proto.model.source.MongoConfig;
import com.dremio.dac.proto.model.source.MySQLConfig;
import com.dremio.dac.proto.model.source.NASConfig;
import com.dremio.dac.proto.model.source.OracleConfig;
import com.dremio.dac.proto.model.source.PostgresConfig;
import com.dremio.dac.proto.model.source.Property;
import com.dremio.dac.proto.model.source.RedshiftConfig;
import com.dremio.dac.proto.model.source.S3Config;
import com.dremio.dac.server.ClassPathSourceConfigurator;
import com.dremio.dac.server.NASSourceConfigurator;
import com.dremio.dac.sources.DB2SourceConfigurator;
import com.dremio.dac.sources.ElasticSourceConfigurator;
import com.dremio.dac.sources.HBaseSourceConfigurator;
import com.dremio.dac.sources.HDFSSourceConfigurator;
import com.dremio.dac.sources.HiveSourceConfigurator;
import com.dremio.dac.sources.MSSQLSourceConfigurator;
import com.dremio.dac.sources.MYSQLSourceConfigurator;
import com.dremio.dac.sources.MaprFsSourceConfigurator;
import com.dremio.dac.sources.MongoSourceConfigurator;
import com.dremio.dac.sources.OracleSourceConfigurator;
import com.dremio.dac.sources.PostgresSourceConfigurator;
import com.dremio.dac.sources.RedshiftSourceConfigurator;
import com.dremio.dac.sources.S3SourceConfigurator;
import com.dremio.exec.store.dfs.FileSystemConfig;
import com.dremio.exec.store.hbase.HBaseStoragePluginConfig;
import com.dremio.exec.store.hive.HiveStoragePluginConfig;
import com.dremio.exec.store.jdbc.JdbcStorageConfig;
import com.dremio.plugins.elastic.ElasticsearchConstants;
import com.dremio.plugins.elastic.ElasticsearchStoragePluginConfig;
import com.dremio.plugins.mongo.MongoStoragePluginConfig;
import com.google.common.collect.Lists;

/**
 * tests the source configurator
 *
 */
public class TestSourceConfigurator {

  @Test
  public void testMongoDefaults() {
    MongoConfig c = newMongoConfig();

    StoragePluginConfig pluginConfig = new MongoSourceConfigurator().configure(c);

    MongoStoragePluginConfig mspc = (MongoStoragePluginConfig)pluginConfig;
    assertEquals("mongodb://hostname1:1234/?", mspc.getConnection());
    assertEquals("default secondaryReadsOnly", false, mspc.isSecondaryReadsOnly());
    assertEquals("default subpartition size", 0, mspc.getSubpartitionSize());
    assertEquals("default timeout", 2000, mspc.getAuthTimeoutMillis());
  }

  /**
   * set required fields
   * @return
   */
  private MongoConfig newMongoConfig() {
    MongoConfig c = new MongoConfig();
    c.setHostList(asList(new Host().setHostname("hostname1").setPort(1234)));
    c.setUseSsl(false);
    c.setPropertyList(Collections.<Property>emptyList());
    return c;
  }

  @Test
  public void testMongoConf() {
    MongoConfig c = newMongoConfig();
    c.setAuthenticationTimeoutMillis(123);
    c.setSecondaryReadsOnly(true);
    c.setSubpartitionSize(12345);

    StoragePluginConfig pluginConfig = new MongoSourceConfigurator().configure(c);

    MongoStoragePluginConfig mspc = (MongoStoragePluginConfig)pluginConfig;
    assertEquals("default secondaryReadsOnly", true, mspc.isSecondaryReadsOnly());
    assertEquals("default subpartition size", 12345, mspc.getSubpartitionSize());
    assertEquals("default timeout", 123, mspc.getAuthTimeoutMillis());
  }


  @Test
  public void testElastic() {
    ElasticConfig c = new ElasticConfig();
    c.setHostList(asList(new Host().setHostname("hostname1").setPort(1234)));
    c.setScriptsEnabled(false);
    c.setSslEnabled(true);
    c.setScrollTimeoutMillis(ElasticsearchConstants.DEFAULT_SCROLL_TIMEOUT_MILLIS);
    c.setReadTimeoutMillis(ElasticsearchConstants.DEFAULT_READ_TIMEOUT_MILLIS);

    StoragePluginConfig pluginConfig = new ElasticSourceConfigurator().configure(c);

    ElasticsearchStoragePluginConfig espc = (ElasticsearchStoragePluginConfig)pluginConfig;
    assertEquals("hostname1:1234", espc.getHosts());
    assertEquals(4000, espc.getBatchSize());
    assertEquals(ElasticsearchConstants.DEFAULT_SCROLL_TIMEOUT_MILLIS, espc.getScrollTimeoutMillis());
    assertEquals(ElasticsearchConstants.DEFAULT_READ_TIMEOUT_MILLIS, espc.getReadTimeoutMillis());
    assertEquals(false, espc.isEnableScripts());
    assertEquals(true, espc.isEnableSSL());
  }

  @Test
  public void testHDFS() {
    HdfsConfig c = new HdfsConfig();

    c.setHostname("hostname1");

    StoragePluginConfig pluginConfig = new HDFSSourceConfigurator().configure(c);

    FileSystemConfig fsspc = (FileSystemConfig)pluginConfig;
    assertEquals("hdfs://hostname1:9000", fsspc.getConnection());
    assertFalse(fsspc.isImpersonationEnabled());
    assertEquals(null, fsspc.getConfig());
    assertEquals(8, fsspc.getFormats().size());

  }

  @Test
  public void testHDFSWithImpersonation() {
    HdfsConfig c = new HdfsConfig();

    c.setHostname("hostname1");
    c.setEnableImpersonation(true);

    StoragePluginConfig pluginConfig = new HDFSSourceConfigurator().configure(c);

    FileSystemConfig fsspc = (FileSystemConfig)pluginConfig;
    assertEquals("hdfs://hostname1:9000", fsspc.getConnection());
    assertTrue(fsspc.isImpersonationEnabled());
    assertEquals(null, fsspc.getConfig());
    assertEquals(8, fsspc.getFormats().size());
  }

  @Test
  public void testMaprFs() {
    MapRFSConfig config = new MapRFSConfig();

    config.setClusterName("my.cluster.com");
    StoragePluginConfig pluginConfig = new MaprFsSourceConfigurator().configure(config);
    FileSystemConfig fsspc = (FileSystemConfig)pluginConfig;
    assertEquals("maprfs://my.cluster.com", fsspc.getConnection());
    assertFalse(fsspc.isImpersonationEnabled());
    assertEquals(null, fsspc.getConfig());
    assertEquals(8, fsspc.getFormats().size());
  }

  @Test
  public void testMaprFsWithImpersonation() {
    MapRFSConfig config = new MapRFSConfig();

    config.setClusterName("my.cluster.com");
    config.setEnableImpersonation(true);
    StoragePluginConfig pluginConfig = new MaprFsSourceConfigurator().configure(config);
    FileSystemConfig fsspc = (FileSystemConfig)pluginConfig;
    assertEquals("maprfs://my.cluster.com", fsspc.getConnection());
    assertTrue(fsspc.isImpersonationEnabled());
    assertEquals(null, fsspc.getConfig());
    assertEquals(8, fsspc.getFormats().size());
  }

  @Test
  public void testHBase() {
    HBaseConfig config = new HBaseConfig();

    config.setZkQuorum("host1,host2");
    config.setPort(2181);

    List<Property> props = Lists.newArrayList();
    props.add(new Property().setName("prop1").setValue("value1"));
    props.add(new Property().setName("prop2").setValue("value2"));

    config.setPropertyList(props);

    HBaseStoragePluginConfig pluginConfig = (HBaseStoragePluginConfig) new HBaseSourceConfigurator()
      .configure(config);
    assertFalse(pluginConfig.isSizeCalculatorEnabled());

    final Map<String, String> propsMap = pluginConfig.getConfig();

    assertEquals(4, propsMap.size());
    assertEquals("host1,host2", propsMap.get("hbase.zookeeper.quorum"));
    assertEquals("2181", propsMap.get("hbase.zookeeper.property.clientPort"));
    assertEquals("value1", propsMap.get("prop1"));
    assertEquals("value2", propsMap.get("prop2"));
  }

  @Test
  public void testHive() {
    HiveConfig c = new HiveConfig();

    c.setHostname("hiveMetaStoreHost");

    List<Property> props = Lists.newArrayList();
    props.add(new Property().setName("prop1").setValue("value1"));
    props.add(new Property().setName("prop2").setValue("value2"));

    c.setPropertyList(props);

    HiveStoragePluginConfig pluginConfig = (HiveStoragePluginConfig) new HiveSourceConfigurator().configure(c);

    Map<String, String> configOverride = pluginConfig.config;

    assertEquals("thrift://hiveMetaStoreHost:9083", configOverride.get("hive.metastore.uris"));
    assertEquals("value1", configOverride.get("prop1"));
    assertEquals("value2", configOverride.get("prop2"));
  }

  @Test
  public void testHiveWithKerberos() {
    HiveConfig c = new HiveConfig();

    c.setHostname("hiveMetaStoreHost");
    c.setPort(10000);

    List<Property> props = Lists.newArrayList();
    props.add(new Property().setName("prop1").setValue("value1"));

    c.setPropertyList(props);

    c.setEnableSasl(true);
    c.setKerberosPrincipal("hive-metastore/_HOST@EXAMPLE.COM");

    HiveStoragePluginConfig pluginConfig = (HiveStoragePluginConfig) new HiveSourceConfigurator().configure(c);

    Map<String, String> configOverride = pluginConfig.config;

    assertEquals("thrift://hiveMetaStoreHost:10000", configOverride.get("hive.metastore.uris"));
    assertEquals("true", configOverride.get("hive.metastore.sasl.enabled"));
    assertEquals("hive-metastore/_HOST@EXAMPLE.COM", configOverride.get("hive.metastore.kerberos.principal"));
    assertEquals("value1", configOverride.get("prop1"));
  }

  @Test
  public void testMSSQL() {
    MSSQLConfig c = new MSSQLConfig();
    c.setHostname("hostname1");
    c.setPort("1234");

    StoragePluginConfig pluginConfig = new MSSQLSourceConfigurator().configure(c);

    JdbcStorageConfig jdbcspc = (JdbcStorageConfig)pluginConfig;
    assertEquals("com.microsoft.sqlserver.jdbc.SQLServerDriver", jdbcspc.getDriver());
    assertEquals("jdbc:sqlserver://hostname1:1234", jdbcspc.getUrl());
    assertEquals(null, jdbcspc.getUsername());
    assertEquals(null, jdbcspc.getPassword());
  }

  @Test
  public void testMSSQLAuthDbAndPort() {
    final MSSQLConfig mssqlConfig = new MSSQLConfig();
    mssqlConfig.setHostname("hostname1");
    mssqlConfig.setPort("1234");
    mssqlConfig.setDatabase("auth_test");

    final JdbcStorageConfig pluginConfig = (JdbcStorageConfig) new MSSQLSourceConfigurator().configure(mssqlConfig);

    assertEquals("jdbc:sqlserver://hostname1:1234;databaseName=auth_test", pluginConfig.getUrl());
  }

  @Test
  public void testMSSQLNoAuthDbNoPort() {
    final MSSQLConfig mssqlConfig = new MSSQLConfig();
    mssqlConfig.setHostname("hostname1");

    final JdbcStorageConfig pluginConfig = (JdbcStorageConfig) new MSSQLSourceConfigurator().configure(mssqlConfig);

    assertEquals("jdbc:sqlserver://hostname1", pluginConfig.getUrl());
  }

  @Test
  public void testMSSQLAuthDbNoPort() {
    final MSSQLConfig mssqlConfig = new MSSQLConfig();
    mssqlConfig.setHostname("hostname1");
    mssqlConfig.setDatabase("auth_test");

    final JdbcStorageConfig pluginConfig = (JdbcStorageConfig) new MSSQLSourceConfigurator().configure(mssqlConfig);

    assertEquals("jdbc:sqlserver://hostname1;databaseName=auth_test", pluginConfig.getUrl());
  }

  @Test
  public void testMySQL() {
    MySQLConfig c = new MySQLConfig();
    c.setHostname("hostname1");
    c.setPort("1234");

    StoragePluginConfig pluginConfig = new MYSQLSourceConfigurator().configure(c);

    JdbcStorageConfig jdbcspc = (JdbcStorageConfig)pluginConfig;
    assertEquals("org.mariadb.jdbc.Driver", jdbcspc.getDriver());
    assertEquals("jdbc:mariadb://hostname1:1234", jdbcspc.getUrl());
    assertEquals(null, jdbcspc.getUsername());
    assertEquals(null, jdbcspc.getPassword());
  }

  @Test
  public void testNAS() {
    NASConfig c = new NASConfig();
    c.setPath("myPath");

    StoragePluginConfig pluginConfig = new NASSourceConfigurator().configure(c);

    FileSystemConfig fsspc = (FileSystemConfig)pluginConfig;
    assertEquals("file:///", fsspc.getConnection());
    assertEquals(null, fsspc.getConfig());
    assertEquals(8, fsspc.getFormats().size());
  }

  @Test
  public void testClassPath() {
    ClassPathConfig c = new ClassPathConfig();
    c.setPath("myPath");

    StoragePluginConfig pluginConfig = new ClassPathSourceConfigurator().configure(c);

    FileSystemConfig fsspc = (FileSystemConfig)pluginConfig;
    assertEquals("classpath:///", fsspc.getConnection());
    assertEquals(null, fsspc.getConfig());
    assertEquals(8, fsspc.getFormats().size());
  }

  @Test
  public void testOracle() {
    OracleConfig c = new OracleConfig();
    c.setHostname("hostname1");
    c.setPort("1234");
    c.setUsername("username");
    c.setPassword("password");
    c.setInstance("instance");

    StoragePluginConfig pluginConfig = new OracleSourceConfigurator().configure(c);

    JdbcStorageConfig jdbcspc = (JdbcStorageConfig)pluginConfig;
    assertEquals("oracle.jdbc.OracleDriver", jdbcspc.getDriver());
    assertEquals("jdbc:oracle:thin:username/password@hostname1:1234/instance", jdbcspc.getUrl());
    assertEquals("username", jdbcspc.getUsername());
    assertEquals("password", jdbcspc.getPassword());
  }

  @Test
  public void testPostgres() {
    PostgresConfig c = new PostgresConfig();
    c.setHostname("hostname1");
    c.setPort("1234");
    c.setDatabaseName("db");

    StoragePluginConfig pluginConfig = new PostgresSourceConfigurator().configure(c);

    JdbcStorageConfig jdbcspc = (JdbcStorageConfig)pluginConfig;
    assertEquals("org.postgresql.Driver", jdbcspc.getDriver());
    assertEquals("jdbc:postgresql://hostname1:1234/db?OpenSourceSubProtocolOverride=true", jdbcspc.getUrl());
    assertEquals(null, jdbcspc.getUsername());
    assertEquals(null, jdbcspc.getPassword());
  }

  @Test
  public void testS3() {
    S3Config c = new S3Config();
    c.setExternalBucketList(asList("bucket1"));
    c.setPropertyList(Collections.<Property>emptyList());
    c.setSecure(false);
    c.setAccessKey("accessKey");
    c.setAccessSecret("accessSecret");

    StoragePluginConfig pluginConfig = new S3SourceConfigurator().configure(c);

    FileSystemConfig fsspc = (FileSystemConfig)pluginConfig;
    assertNull(fsspc.getConnection());
    Map<String, String> config = new HashMap<>();
    config.put("fs.s3a.connection.maximum", "100");
    config.put("fs.s3a.access.key", "accessKey");
    config.put("fs.s3a.secret.key", "accessSecret");
    config.put("fs.s3a.connection.ssl.enabled", "false");

//    assertEquals(config, fsspc.getConfig());
    assertEquals(8, fsspc.getFormats().size());
  }

  @Test
  public void testDB2() {
    DB2Config config = new DB2Config();
    config.setHostname("hostname1");
    config.setPort("1234");
    config.setDatabaseName("mydb");

    StoragePluginConfig pluginConfig = new DB2SourceConfigurator().configure(config);

    JdbcStorageConfig jdbcConfig = (JdbcStorageConfig) pluginConfig;
    assertEquals("com.ibm.db2.jcc.DB2Driver", jdbcConfig.getDriver());
    assertEquals("jdbc:db2://hostname1:1234/mydb", jdbcConfig.getUrl());
    assertEquals(null, jdbcConfig.getUsername());
    assertEquals(null, jdbcConfig.getPassword());
  }

  @Test
  public void testRedshift() {
    RedshiftConfig c = new RedshiftConfig();
    c.setConnectionString("jdbc:redshift://hostname1:1234/db");

    StoragePluginConfig pluginConfig = new RedshiftSourceConfigurator().configure(c);

    JdbcStorageConfig jdbcspc = (JdbcStorageConfig)pluginConfig;
    assertEquals("com.amazon.redshift.jdbc4.Driver", jdbcspc.getDriver());
    assertEquals("jdbc:redshift://hostname1:1234/db", jdbcspc.getUrl());
    assertEquals(null, jdbcspc.getUsername());
    assertEquals(null, jdbcspc.getPassword());
  }
}

