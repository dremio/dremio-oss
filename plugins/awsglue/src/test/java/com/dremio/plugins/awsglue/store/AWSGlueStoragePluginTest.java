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
package com.dremio.plugins.awsglue.store;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dremio.BaseTestQuery;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.SecretRef;
import com.dremio.exec.store.CatalogService;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.google.common.base.Preconditions;
import io.findify.s3mock.S3Mock;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** AWSGlueStoragePluginTest */
public class AWSGlueStoragePluginTest extends BaseTestQuery {

  @ClassRule public static final TemporaryFolder folder = new TemporaryFolder();

  private static int port;
  private static S3Mock s3Mock;

  @BeforeClass
  public static void setupDefaultTestCluster() throws Exception {
    BaseTestQuery.setupDefaultTestCluster();

    setupS3Mock();

    setupBucketAndFile();

    addGlueTestPlugin("testglue", getCatalogService());
  }

  @AfterClass
  public static void teardownDefaultTestCluster() throws Exception {
    if (s3Mock != null) {
      s3Mock.shutdown();
      s3Mock = null;
    }
  }

  @Test
  public void testParquet() throws Exception {
    testBuilder()
        .unOrdered()
        .sqlQuery("SELECT min(n_nationkey) FROM \"testglue\".\"default\".nation_table LIMIT 1")
        .baselineColumns("EXPR$0")
        .baselineValues(0L)
        .go();
  }

  @Test
  public void testCSV() throws Exception {
    testBuilder()
        .unOrdered()
        .sqlQuery("SELECT * FROM \"testglue\".\"default\".csv_table")
        .baselineColumns("col1", "col2")
        .baselineValues(1, 1)
        .baselineValues(2, 2)
        .baselineValues(3, 3)
        .go();

    testBuilder()
        .unOrdered()
        .sqlQuery("SELECT col2 FROM \"testglue\".\"default\".csv_table where col1 = 1")
        .baselineColumns("col2")
        .baselineValues(1)
        .go();
  }

  @Test
  public void testCSVWithOpenCSVSerde() throws Exception {
    testBuilder()
        .unOrdered()
        .sqlQuery("SELECT * FROM \"testglue\".\"default\".csv_table_2")
        .baselineColumns("col1", "col2")
        .baselineValues("1", "1")
        .baselineValues("2", "2")
        .baselineValues("3", "3")
        .go();

    testBuilder()
        .unOrdered()
        .sqlQuery("select col2 FROM \"testglue\".\"default\".csv_table_2 where col1 like '1'")
        .baselineColumns("col2")
        .baselineValues("1")
        .go();
  }

  // json table - unsupported
  @Test
  public void testUnsupportedFormat() {
    assertThatThrownBy(
            () ->
                testBuilder()
                    .unOrdered()
                    .sqlQuery("SELECT * FROM \"testglue\".\"default\".json_table")
                    .baselineColumns("col1", "col2")
                    .baselineValues(1, 1)
                    .baselineValues(2, 2)
                    .baselineValues(3, 3)
                    .go())
        .hasMessageContaining(
            "UNSUPPORTED_OPERATION ERROR: AWS Glue table [testglue.default.json_table] uses an unsupported file format");
  }

  // empty input format
  @Test
  public void testNoInputFormat() throws Exception {
    assertThatThrownBy(
            () ->
                testBuilder()
                    .unOrdered()
                    .sqlQuery("SELECT * FROM \"testglue\".\"default\".no_inputformat_table")
                    .baselineColumns("col1", "col2")
                    .baselineValues(1, 1)
                    .baselineValues(2, 2)
                    .baselineValues(3, 3)
                    .go())
        .hasMessageContaining("DATA_READ ERROR: Unable to get Hive table InputFormat class.");
  }

  @Test
  public void testCreateLongPathShouldThrow() throws Exception {
    assertThatThrownBy(
            () -> test("CREATE TABLE \"testglue\".\"default\".long.path.should.throw (c1 int)"))
        .hasMessageContaining(
            "Dataset path '[testglue, default, long, path, should, throw]' is invalid");
  }

  @Test
  public void testCreateAsLongPathShouldThrow() throws Exception {
    assertThatThrownBy(
            () ->
                test(
                    "CREATE TABLE \"testglue\".\"default\".long.ctas.path.should.throw (c1 int) as values (1), (2)"))
        .hasMessageContaining(
            "Dataset path '[testglue, default, long, ctas, path, should, throw]' is invalid");
  }

  private static void setupS3Mock() {
    Preconditions.checkState(s3Mock == null);
    s3Mock =
        new S3Mock.Builder()
            .withPort(0)
            .withFileBackend(folder.getRoot().getAbsolutePath())
            .build();
    port = s3Mock.start().localAddress().getPort();
  }

  private static void setupBucketAndFile() throws IOException {
    File bucket = folder.newFolder("qa1.dremio.com");

    // parquet
    File destination = new File(bucket.toPath().toString() + "/test");
    destination.mkdir();
    File file =
        new File(
            AWSGlueStoragePluginTest.class
                .getClassLoader()
                .getResource("nation.parquet")
                .getFile());
    Files.copy(
        file.toPath(),
        Paths.get(destination.toPath().toString(), "nation.parquet"),
        StandardCopyOption.REPLACE_EXISTING);

    // csv
    destination = new File(bucket.toPath().toString() + "/csvtest");
    destination.mkdir();
    file =
        new File(
            AWSGlueStoragePluginTest.class.getClassLoader().getResource("simpleint.csv").getFile());
    Files.copy(
        file.toPath(),
        Paths.get(destination.toPath().toString(), "simpleint.csv"),
        StandardCopyOption.REPLACE_EXISTING);
  }

  public static void addGlueTestPlugin(final String pluginName, final CatalogService catalogService)
      throws Exception {
    SourceConfig sc = new SourceConfig();
    sc.setName(pluginName);
    AWSGluePluginConfig conf = new AWSGluePluginConfig();
    final List<Property> finalProperties = new ArrayList<>();
    finalProperties.add(
        new Property(
            HiveConf.ConfVars.METASTORE_CLIENT_FACTORY_CLASS.varname,
            "com.amazonaws.glue.catalog.metastore.MockAWSGlueDataCatalogHiveClientFactory"));

    finalProperties.add(new Property("fs.s3a.bucket.qa1.dremio.com." + "access.key", "test"));
    finalProperties.add(new Property("fs.s3a.bucket.qa1.dremio.com.secret.key", "test"));
    finalProperties.add(
        new Property(
            "fs.s3a.bucket.qa1.dremio.com.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a" + ".SimpleAWSCredentialsProvider"));
    finalProperties.add(new Property("fs.s3a.bucket.qa1.dremio.com.endpoint", "localhost:" + port));
    finalProperties.add(new Property("fs.s3a.bucket.qa1.dremio.com.path.style.access", "true"));
    finalProperties.add(
        new Property("fs.s3a.bucket.qa1.dremio.com.connection.ssl.enabled", "false"));
    finalProperties.add(new Property("fs.s3a.change.detection.version.required", "false"));

    File file =
        new File(
            AWSGlueStoragePluginTest.class
                .getClassLoader()
                .getResource("catalog_store.json")
                .getFile());
    String content = new String(Files.readAllBytes(file.toPath()));
    finalProperties.add(new Property("awsglue.catalog.store.content", content));
    conf.enableAsync = false;

    conf.propertyList = finalProperties;
    conf.accessKey = "test";
    conf.accessSecret = SecretRef.of("test");
    sc.setType(conf.getType());
    sc.setConfig(conf.toBytesString());
    sc.setMetadataPolicy(CatalogService.DEFAULT_METADATA_POLICY);
    catalogService.getSystemUserCatalog().createSource(sc);
  }
}
