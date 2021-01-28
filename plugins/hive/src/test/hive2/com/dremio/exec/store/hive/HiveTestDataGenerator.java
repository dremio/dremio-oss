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
package com.dremio.exec.store.hive;

import static com.dremio.BaseTestQuery.getTempDir;
import static com.dremio.exec.hive.HiveTestUtilities.executeQuery;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.serdeConstants;

import com.dremio.BaseTestQuery;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.CatalogServiceImpl;
import com.dremio.exec.catalog.ManagedStoragePlugin;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.impersonation.hive.BaseTestHiveImpersonation;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;

public class HiveTestDataGenerator {
  public static final String HIVE_TEST_PLUGIN_NAME = "hive";
  public static final String HIVE_TEST_PLUGIN_NAME_WITH_WHITESPACE = "hive plugin name with whitespace";
  private static volatile HiveTestDataGenerator instance;

  private final String dbDir;
  private final String whDir;
  private final Map<String, String> config;
  private final int port;   // Metastore server port

  public static synchronized HiveTestDataGenerator getInstance() throws Exception {
    return getInstance(null);
  }

  public static synchronized HiveTestDataGenerator getInstance(Map<String, String> cfg) throws Exception {
    if (instance == null || (cfg != null && cfg.size() > 0)) {
      HiveTestDataGenerator localInstance = new HiveTestDataGenerator(cfg);
      localInstance.generateTestData();
      instance = localInstance;
    }
    return instance;
  }

  public static synchronized HiveTestDataGenerator newInstanceWithoutTestData(Map<String, String> cfg) throws Exception {
    return new HiveTestDataGenerator(cfg);
  }

  private HiveTestDataGenerator(Map<String, String> cfg) throws Exception {
    this.dbDir = getTempDir("metastore_db");
    this.whDir =  getTempDir("warehouse");

    final HiveConf conf = new HiveConf();
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.METASTORE_SCHEMA_VERIFICATION, false);
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.METASTORE_AUTO_CREATE_ALL, true);
    HiveConf.setVar(conf, HiveConf.ConfVars.METASTOREWAREHOUSE, whDir);
    HiveConf.setVar(conf, HiveConf.ConfVars.METASTORECONNECTURLKEY, String.format("jdbc:derby:;databaseName=%s;create=true", dbDir));
    if (cfg != null) {
      cfg.forEach(conf::set);
    }
    this.config = Maps.newHashMap();
    config.put(FileSystem.FS_DEFAULT_NAME_KEY, "file:///");
    port = MetaStoreUtils.startMetaStore(conf);
  }

  public String getWhDir() {
    return whDir;
  }

  /**
   * Add Hive test storage plugin to the given plugin registry.
   *
   * @throws Exception
   */
  public void addHiveTestPlugin(final String pluginName, final CatalogService pluginRegistry) throws Exception {
    SourceConfig sc = new SourceConfig();
    sc.setName(pluginName);
    Hive2StoragePluginConfig conf = BaseTestHiveImpersonation.createHiveStoragePlugin(config);
    conf.hostname = "localhost";
    conf.port = port;
    sc.setType(conf.getType());
    sc.setConfig(conf.toBytesString());
    sc.setMetadataPolicy(CatalogService.DEFAULT_METADATA_POLICY);
    ((CatalogServiceImpl) pluginRegistry).getSystemUserCatalog().createSource(sc);
  }

  /**
   * Update the current HiveStoragePlugin in given plugin registry with given <i>configOverride</i>.
   *
   * @param configOverride
   * @throws Exception if fails to update or no Hive plugin currently exists in given plugin registry.
   */
  public void updatePluginConfig(final CatalogService pluginRegistry, Map<String, String> configOverride)
    throws Exception {
    StoragePlugin storagePlugin = pluginRegistry.getSource(HIVE_TEST_PLUGIN_NAME);
    if (storagePlugin == null) {
      throw new Exception("Hive test storage plugin doesn't exist. Add a plugin using addHiveTestPlugin()");
    }

    ManagedStoragePlugin msp = ((CatalogServiceImpl) pluginRegistry).getManagedSource(HIVE_TEST_PLUGIN_NAME);
    SourceConfig newSC = msp.getId().getClonedConfig();
    Hive2StoragePluginConfig conf = (Hive2StoragePluginConfig) msp.getId().<Hive2StoragePluginConfig>getConnectionConf().clone();

    List<Property> updated = new ArrayList<>();
    for (Entry<String, String> prop : configOverride.entrySet()) {
      updated.add(new Property(prop.getKey(), prop.getValue()));
    }

    for (Property p : conf.propertyList) {
      if (!configOverride.containsKey(p.name)) {
        updated.add(p);
      }
    }

    conf.propertyList = updated;
    newSC.setConfig(conf.toBytesString());
    ((CatalogServiceImpl) pluginRegistry).getSystemUserCatalog().updateSource(newSC);
  }

  /**
   * Delete the Hive test plugin from registry.
   */
  public void deleteHiveTestPlugin(final String pluginName, final CatalogService pluginRegistry) {
    CatalogServiceImpl impl = (CatalogServiceImpl) pluginRegistry;
    ManagedStoragePlugin msp = impl.getManagedSource(pluginName);
    impl.getSystemUserCatalog().deleteSource(msp.getId().getConfig());
  }

  public void executeDDL(String query) throws IOException {
    final HiveConf conf = newHiveConf();
    runDDL(query, conf);
  }

  public void executeDDL(String query, Map<String,String> confOverrides) throws IOException {
    final HiveConf conf = newHiveConf();
    for(Map.Entry<String,String> entry : confOverrides.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }
    runDDL(query, conf);
  }

  private void runDDL(String query, HiveConf conf) throws IOException {
    try (DriverState driverState = new DriverState(conf)) {
      executeQuery(driverState.driver, query);
    }
  }

  private HiveConf newHiveConf() {
    HiveConf conf = new HiveConf(SessionState.class);

    HiveConf.setVar(conf, ConfVars.METASTOREURIS, "thrift://localhost:" + port);
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "file:///");
    HiveConf.setVar(conf, ConfVars.METASTOREWAREHOUSE, whDir);
    conf.set("mapred.job.tracker", "local");
    HiveConf.setVar(conf, ConfVars.SCRATCHDIR, getTempDir("scratch_dir"));
    HiveConf.setVar(conf, ConfVars.LOCALSCRATCHDIR, getTempDir("local_scratch_dir"));
    HiveConf.setVar(conf, ConfVars.DYNAMICPARTITIONINGMODE, "nonstrict");
    HiveConf.setBoolVar(conf, ConfVars.HIVE_CBO_ENABLED, false);
    return conf;

  }

  private static class DriverState implements AutoCloseable {
    private final Driver driver;
    private final SessionState sessionState;

    DriverState(HiveConf conf) {
      this.driver = new Driver(conf);
      this.sessionState = new SessionState(conf);
      SessionState.start(sessionState);
    }

    @Override
    public void close() throws IOException {
      sessionState.close();
    }
  }

  private void logVersion(Driver hiveDriver) throws Exception {
    hiveDriver.run("SELECT VERSION()");
    hiveDriver.resetFetch();
    hiveDriver.setMaxRows(1);
    List<String> result = new ArrayList<>();
    hiveDriver.getResults(result);

    for (String values : result) {
      System.out.println("Test Hive instance version: " + values);
    }
  }

  public void generateTestData(java.util.function.Function<Driver, Void> generatorFunction) throws Exception {
    try (DriverState driverState = new DriverState(newHiveConf())) {
      generatorFunction.apply(driverState.driver);
    }
  }

  public void generateTestData() throws Exception {
    try (DriverState driverState = new DriverState(newHiveConf())) {
      Driver hiveDriver = driverState.driver;
      logVersion(hiveDriver);

      // generate (key, value) test data
      String testDataFile = generateTestDataFile(5, "dremio-hive-test");

      // Create a (key, value) schema table with Text SerDe which is available in hive-serdes.jar
      executeQuery(hiveDriver, "CREATE TABLE IF NOT EXISTS default.kv(key INT, value STRING) " +
          "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE");
      executeQuery(hiveDriver, "LOAD DATA LOCAL INPATH '" + testDataFile + "' OVERWRITE INTO TABLE default.kv");

      // Create a (key, value) schema table in non-default database with RegexSerDe which is available in hive-contrib.jar
      // Table with RegExSerde is expected to have columns of STRING type only.
      executeQuery(hiveDriver, "CREATE DATABASE IF NOT EXISTS db1");
      executeQuery(hiveDriver, "CREATE TABLE db1.kv_db1(key STRING, value STRING) " +
          "ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.RegexSerDe' " +
          "WITH SERDEPROPERTIES (" +
          "  \"input.regex\" = \"([0-9]*), (.*_[0-9]*)\", " +
          "  \"output.format.string\" = \"%1$s, %2$s\"" +
          ") ");
      executeQuery(hiveDriver, "INSERT INTO TABLE db1.kv_db1 SELECT * FROM default.kv");

      // Create an Avro format based table backed by schema in a separate file
      final String avroCreateQuery = String.format("CREATE TABLE db1.avro " +
              "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' " +
              "STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' " +
              "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat' " +
              "TBLPROPERTIES ('avro.schema.url'='file:///%s')",
          BaseTestQuery.getPhysicalFileFromResource("avro_test_schema.json").replace('\\', '/'));

      executeQuery(hiveDriver, avroCreateQuery);
      executeQuery(hiveDriver, "INSERT INTO TABLE db1.avro SELECT * FROM default.kv");

      executeQuery(hiveDriver, "USE default");

      // create a table with no data
      executeQuery(hiveDriver, "CREATE TABLE IF NOT EXISTS empty_table(a INT, b STRING)");

      // create empty partitioned table
      executeQuery(hiveDriver, "CREATE TABLE IF NOT EXISTS partitioned_empty_table(a INT, b STRING) PARTITIONED BY (p INT)");
      // delete the table location of empty table
      File emptyTableLocation = new File(whDir, "empty_table");
      if (emptyTableLocation.exists()) {
        FileUtils.forceDelete(emptyTableLocation);
      }

      // create Parquet format based table
      final File regionDir = new File(BaseTestQuery.getTempDir("region"));
      regionDir.mkdirs();
      final URL url = Resources.getResource("region.parquet");
      if (url == null) {
        throw new IOException(String.format("Unable to find path %s.", "region.parquet"));
      }

      final File file = new File(regionDir, "region.parquet");
      file.deleteOnExit();
      regionDir.deleteOnExit();
      Files.write(Paths.get(file.toURI()), Resources.toByteArray(url));

      final String parquetUpperSchemaTable = "create external table parquet_region(R_REGIONKEY bigint, R_NAME string, R_COMMENT string) " +
          "stored as parquet location '" + file.getParent() + "'";
      executeQuery(hiveDriver, parquetUpperSchemaTable);

      final String parquetschemalearntest = "create external table parquetschemalearntest(R_REGIONKEY bigint) " +
          "stored as parquet location '" + file.getParent() + "'";
      executeQuery(hiveDriver, parquetschemalearntest);

      // create Parquet format based table, with two files.
      final File regionWithTwoDir = new File(getWhDir(), "parquet_with_two_files");
      regionWithTwoDir.mkdirs();
      final File file1Of2 = new File(regionWithTwoDir, "region1.parquet");
      file1Of2.deleteOnExit();
      Files.write(Paths.get(file1Of2.toURI()), Resources.toByteArray(url));
      final File file2Of2 = new File(regionWithTwoDir, "region2.parquet");
      file2Of2.deleteOnExit();
      Files.write(Paths.get(file2Of2.toURI()), Resources.toByteArray(url));
      regionWithTwoDir.deleteOnExit();
      final String parquetWithTwoFilesTable = "create external table parquet_with_two_files(R_REGIONKEY bigint, R_NAME string, R_COMMENT string) " +
          "stored as parquet location '" + file1Of2.getParent() + "'";
      executeQuery(hiveDriver, parquetWithTwoFilesTable);

      final String orcRegionTable = "create table orc_region stored as orc as SELECT * FROM parquet_region";
      executeQuery(hiveDriver, orcRegionTable);

      byte[] fileData = Files.readAllBytes(new File(getWhDir(), "orc_region/000000_0").toPath());

      // create ORC format based table, with two files.
      final File regionOrcWithTwoDir = new File(getWhDir(), "orc_with_two_files");
      regionOrcWithTwoDir.mkdirs();
      final File fileOrc1Of2 = new File(regionOrcWithTwoDir, "region1.orc");
      fileOrc1Of2.deleteOnExit();
      Files.write(Paths.get(fileOrc1Of2.toURI()), fileData);
      final File fileOrc2Of2 = new File(regionOrcWithTwoDir, "region2.orc");
      fileOrc2Of2.deleteOnExit();
      Files.write(Paths.get(fileOrc2Of2.toURI()), fileData);
      regionOrcWithTwoDir.deleteOnExit();
      final String orcWithTwoFilesTable = "create external table orc_with_two_files(R_REGIONKEY bigint, R_NAME string, R_COMMENT string) " +
          "stored as orc location '" + fileOrc1Of2.getParent() + "'";
      executeQuery(hiveDriver, orcWithTwoFilesTable);

      final String orcStringsTable = "create table orc_strings (key int, country_char25 CHAR(25), " +
          "country_string string, country_varchar VARCHAR(1000), continent_char25 CHAR(25)) stored as orc";
      final String insert1 = "insert into orc_strings values (1, 'INDIA', 'CHINA', 'NEPAL', 'ASIA')";
      final String insert2 = "insert into orc_strings values (2, 'INDONESIA', 'THAILAND', 'SINGAPORE', 'ASIA')";
      final String insert3 = "insert into orc_strings values (3, 'FRANCE', 'ITALY', 'ROMANIA', 'EUROPE')";
      executeQuery(hiveDriver, orcStringsTable);
      executeQuery(hiveDriver, insert1);
      executeQuery(hiveDriver, insert2);
      executeQuery(hiveDriver, insert3);

      // create a table that has all Hive types. This is to test how hive tables metadata is populated in
      // Dremio's INFORMATION_SCHEMA.
      executeQuery(hiveDriver,
          "CREATE TABLE IF NOT EXISTS infoschematest(" +
              "booleanType BOOLEAN, " +
              "tinyintType TINYINT, " +
              "smallintType SMALLINT, " +
              "intType INT, " +
              "bigintType BIGINT, " +
              "floatType FLOAT, " +
              "doubleType DOUBLE, " +
              "dateType DATE, " +
              "timestampType TIMESTAMP, " +
              "binaryType BINARY, " +
              "decimalType DECIMAL(38, 2), " +
              "stringType STRING, " +
              "varCharType VARCHAR(20), " +
              "listType ARRAY<STRING>, " +
              "mapType MAP<STRING,INT>, " +
              "structType STRUCT<sint:INT,sboolean:BOOLEAN,sstring:STRING>, " +
              "uniontypeType UNIONTYPE<int, double, array<string>>, " +
              "charType CHAR(10))"
      );

      String dummy = generateTestDataFile(5000, "dummy");
      executeQuery(hiveDriver, "CREATE TABLE IF NOT EXISTS default.dummy(key INT, value STRING) " +
          "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE");
      executeQuery(hiveDriver, "LOAD DATA LOCAL INPATH '" + dummy + "' OVERWRITE INTO TABLE default.dummy");
      executeQuery(hiveDriver, "CREATE TABLE parquet_timestamp_nulls(a TIMESTAMP, b STRING) stored as parquet");
      executeQuery(hiveDriver, "INSERT INTO TABLE parquet_timestamp_nulls SELECT null, 'a' FROM default.dummy");

      // create a Hive view to test how its metadata is populated in Dremio's INFORMATION_SCHEMA
      //See DX-8078
//    executeQuery(hiveDriver, "CREATE VIEW IF NOT EXISTS hiveview AS SELECT * FROM kv");

      executeQuery(hiveDriver, "CREATE TABLE IF NOT EXISTS " +
          "partition_pruning_test_loadtable(a DATE, b TIMESTAMP, c INT, d INT, e INT) " +
          "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE");
      executeQuery(hiveDriver,
          String.format("LOAD DATA LOCAL INPATH '%s' INTO TABLE partition_pruning_test_loadtable",
              generateTestDataFileForPartitionInput()));

      // create partitioned hive table to test partition pruning
      executeQuery(hiveDriver,
          "CREATE TABLE IF NOT EXISTS partition_pruning_test(a DATE, b TIMESTAMP) " +
              "partitioned by (c INT, d INT, e INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE");
      executeQuery(hiveDriver, "INSERT OVERWRITE TABLE partition_pruning_test PARTITION(c, d, e) " +
          "SELECT a, b, c, d, e FROM partition_pruning_test_loadtable");

      executeQuery(hiveDriver,
          "CREATE TABLE IF NOT EXISTS partition_with_few_schemas(a DATE, b TIMESTAMP) " +
              "partitioned by (c INT, d INT, e INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE");
      executeQuery(hiveDriver, "INSERT OVERWRITE TABLE partition_with_few_schemas PARTITION(c, d, e) " +
          "SELECT a, b, c, d, e FROM partition_pruning_test_loadtable");
      executeQuery(hiveDriver, "alter table partition_with_few_schemas partition(c=1, d=1, e=1) change a a1 INT");
      executeQuery(hiveDriver, "alter table partition_with_few_schemas partition(c=1, d=1, e=2) change a a1 INT");
      executeQuery(hiveDriver, "alter table partition_with_few_schemas partition(c=2, d=2, e=2) change a a1 INT");

      // Add a partition with custom location
      executeQuery(hiveDriver,
          String.format("ALTER TABLE partition_pruning_test ADD PARTITION (c=99, d=98, e=97) LOCATION '%s'",
              getTempDir("part1")));
      executeQuery(hiveDriver,
          String.format("INSERT INTO TABLE partition_pruning_test PARTITION(c=99, d=98, e=97) " +
                  "SELECT '%s', '%s' FROM kv LIMIT 1",
              new Date(System.currentTimeMillis()).toString(), new Timestamp(System.currentTimeMillis()).toString()));

      executeQuery(hiveDriver, "DROP TABLE partition_pruning_test_loadtable");

      // Create a partitioned parquet table (DRILL-3938)
      executeQuery(hiveDriver,
          "CREATE TABLE kv_parquet(key INT, value STRING) PARTITIONED BY (part1 int) STORED AS PARQUET");
      executeQuery(hiveDriver, "INSERT INTO TABLE kv_parquet PARTITION(part1) SELECT key, value, key FROM default.kv");
      executeQuery(hiveDriver, "ALTER TABLE kv_parquet ADD COLUMNS (newcol string)");

      executeQuery(hiveDriver,
          "CREATE TABLE kv_mixedschema(key INT, value STRING) PARTITIONED BY (part int) STORED AS ORC");
      executeQuery(hiveDriver, "INSERT INTO TABLE kv_mixedschema PARTITION(part=1) SELECT key, value FROM default.kv LIMIT 2");
      executeQuery(hiveDriver, "ALTER TABLE kv_mixedschema change column key key string");
      executeQuery(hiveDriver, "INSERT INTO TABLE kv_mixedschema PARTITION(part=2) " +
          "SELECT key, value FROM default.kv ORDER BY key DESC LIMIT 2");

      executeQuery(hiveDriver, "CREATE TABLE sorted_parquet(id int, key int) clustered by (id) sorted by (key) into 10 buckets stored as Parquet");

      executeQuery(hiveDriver, "INSERT INTO TABLE sorted_parquet select key as id, key as key from kv_parquet distribute by id sort by key");

      // Create a StorageHandler based table (DRILL-3739)
      executeQuery(hiveDriver, "CREATE TABLE kv_sh(key INT, value STRING) STORED BY " +
          "'org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler'");
      // Insert fails if the table directory already exists for tables with DefaultStorageHandlers. Its a known
      // issue in Hive. So delete the table directory created as part of the CREATE TABLE
      FileUtils.deleteQuietly(new File(whDir, "kv_sh"));
      //executeQuery(hiveDriver, "INSERT OVERWRITE TABLE kv_sh SELECT * FROM kv");

      // Create text tables with skip header and footer table property
      executeQuery(hiveDriver, "create database if not exists skipper");
      executeQuery(hiveDriver, createTableWithHeaderFooterProperties("skipper.kv_text_small", "textfile", "1", "1"));
      executeQuery(hiveDriver, generateTestDataWithHeadersAndFooters("skipper.kv_text_small", 5, 1, 1));

      executeQuery(hiveDriver, createTableWithHeaderFooterProperties("skipper.kv_text_large", "textfile", "2", "2"));
      executeQuery(hiveDriver, generateTestDataWithHeadersAndFooters("skipper.kv_text_large", 5000, 2, 2));

      executeQuery(hiveDriver, createTableWithHeaderFooterProperties("skipper.kv_incorrect_skip_header", "textfile", "A", "1"));
      executeQuery(hiveDriver, generateTestDataWithHeadersAndFooters("skipper.kv_incorrect_skip_header", 5, 1, 1));

      executeQuery(hiveDriver, createTableWithHeaderFooterProperties("skipper.kv_incorrect_skip_footer", "textfile", "1", "A"));
      executeQuery(hiveDriver, generateTestDataWithHeadersAndFooters("skipper.kv_incorrect_skip_footer", 5, 1, 1));

      // Create rcfile table with skip header and footer table property
      executeQuery(hiveDriver, createTableWithHeaderFooterProperties("skipper.kv_rcfile_large", "rcfile", "1", "1"));
      executeQuery(hiveDriver, "insert into table skipper.kv_rcfile_large select * from skipper.kv_text_large");

      // Create parquet table with skip header and footer table property
      executeQuery(hiveDriver, createTableWithHeaderFooterProperties("skipper.kv_parquet_large", "parquet", "1", "1"));
      executeQuery(hiveDriver, "insert into table skipper.kv_parquet_large select * from skipper.kv_text_large");

      // Create sequencefile table with skip header and footer table property
      executeQuery(hiveDriver, createTableWithHeaderFooterProperties("skipper.kv_sequencefile_large", "sequencefile", "1", "1"));
      executeQuery(hiveDriver, "insert into table skipper.kv_sequencefile_large select * from skipper.kv_text_large");

      // Create a table based on json file
      final URL simpleJson = Resources.getResource("simple.json");
      executeQuery(hiveDriver, "create table default.simple_json(json string)");
      executeQuery(hiveDriver, "load data local inpath '" + simpleJson + "' into table default.simple_json");

      final URL multiRGParquet = Resources.getResource("multiple_rowgroups.parquet");
      executeQuery(hiveDriver, "CREATE TABLE parquet_mult_rowgroups(c1 int) stored as parquet");
      executeQuery(hiveDriver,
          "LOAD DATA LOCAL INPATH '" + multiRGParquet + "' into table default.parquet_mult_rowgroups");

      final URL impalaParquetFile = Resources.getResource("impala_alltypes.parquet");
      executeQuery(hiveDriver, "CREATE TABLE db1.impala_parquet(" +
          "id int, " +
          "bool_col boolean, " +
          "tinyint_col int, " +
          "smallint_col int, " +
          "int_col int, " +
          "bigint_col bigint, " +
          "float_col float, " +
          "double_col double, " +
          "date_string_col string, " +
          "string_col string, " +
          "timestamp_col timestamp) stored as parquet");
      executeQuery(hiveDriver,
          "LOAD DATA LOCAL INPATH '" + impalaParquetFile + "' into table db1.impala_parquet");

      createOrcStringTableWithComplexTypes(hiveDriver);
      createDecimalConversionTable(hiveDriver, "decimal_conversion_test_orc", "orc");
      createDecimalConversionTable(hiveDriver, "decimal_conversion_test_parquet", "parquet");

      createExtTableWithMoreColumnsThanOriginal(hiveDriver, "orc_more_columns");

      // create a Hive table that has columns with data types which are supported for reading in Dremio.
      createAllTypesTextTable(hiveDriver, "readtest");
      createAllTypesTable(hiveDriver, "parquet", "readtest");
      createAllTypesTable(hiveDriver, "orc", "readtest");
      createTimestampToStringTable(hiveDriver, "timestamptostring");
      createDoubleToStringTable(hiveDriver, "doubletostring");

      createFieldSizeLimitTables(hiveDriver, "field_size_limit_test");

      createParquetSchemaChangeTestTable(hiveDriver, "parqschematest_table");
      createParquetDecimalSchemaChangeTestTable(hiveDriver, "parqdecunion_table");
      createParquetDecimalSchemaChangeFilterTestTable(hiveDriver, "parqdecimalschemachange_table");

      createParquetVarcharTable(hiveDriver, "parq_varchar", url);
      createParquetVarcharTableNoTruncationReqd(hiveDriver, "parq_varchar_no_trunc", url);
      createParquetCharTable(hiveDriver, "parq_char", url);
      createParquetVarcharWithMoreTypesTable(hiveDriver, "parq_varchar_more_types");
      createParquetVarcharWithComplexTypeTable(hiveDriver, "parq_varchar_complex");
      createPartitionTruncatedVarchar(hiveDriver, "parquet_fixed_length_varchar_partition");
      createPartitionTruncatedChar(hiveDriver, "parquet_fixed_length_char_partition");

      createSimpleListWithNullsHiveTables(hiveDriver);
      createNestedListWithNullsHiveTables(hiveDriver);
      createNestedStructWithNullsHiveTables(hiveDriver);
      createParuqetComplexFilterTestTable(hiveDriver);
      createComplexTypesTextTable(hiveDriver, "orccomplex");
      createComplexTypesTable(hiveDriver, "orc", "orccomplex");
      createComplexVarcharHiveTables(hiveDriver);

      createListTypesTextTable(hiveDriver, "orclist");
      createListTypesTable(hiveDriver, "orc", "orclist");

      createStructTypesTextTable(hiveDriver, "orcstruct");
      createStructTypesTable(hiveDriver, "orc", "orcstruct");

      createUnionTypesTextTable(hiveDriver, "orcunion");
      createUnionTypesTable(hiveDriver, "orc", "orcunion");

      createMapTypesTextTable(hiveDriver, "orcmap");
      createMapTypesTable(hiveDriver, "orc", "orcmap");

      createORCDecimalCompareTestTable(hiveDriver, "orcdecimalcompare");
      createMixedPartitionTypeTable(hiveDriver, "parquet_mixed_partition_type");
      createPartitionDecimalOverflow(hiveDriver, "parquet_decimal_partition_overflow");
      createTextTableWithDateColumn(hiveDriver, "text_date");
      createOrcTableWithDateColumn(hiveDriver, "orc_date", "text_date");
      createOrcTableWithAncientDates(hiveDriver, "orc_date_table");
      createDecimalParquetTableWithDecimalColumnMismatch(hiveDriver, "parquet_varchar_to_decimal_with_filter");
      createTableWithList(hiveDriver, "complex_types_direct_list");
      createTableWithStruct(hiveDriver, "complex_types_direct_struct");
      createTableNestedList(hiveDriver, "complex_types_nested_list");
      createTableNestedStruct(hiveDriver, "complex_types_nested_struct");
      createTableWithUnsupportedComplexTypes(hiveDriver, "complex_types_map");
      createTableNestedWithUnsupportedComplexTypes(hiveDriver, "complex_types_nested_map");
      createTableForUnsupportedMapTypeInParquet(hiveDriver, "complex_types_map_support_parquet");
      createTableMixedCaseColumnsWithComplexTypes(hiveDriver, "complex_types_case_test");
      createTableFlagTestColumns(hiveDriver, "complex_types_flag_test");
      createTableForPartitionValueFormatException(hiveDriver, "partition_format_exception");
      createParquetTableWithDeeplyNestedColumns(hiveDriver);
      createParquetStructExtraField(hiveDriver);
      createNullORCStructTable(hiveDriver);
      createORCPartitionSchemaTestTable(hiveDriver);

      // This test requires a systemop alteration. Refresh metadata on hive seems to timeout the test preventing re-use of an existing table. Hence, creating a new table.
      createParquetDecimalSchemaChangeFilterTestTable(hiveDriver, "test_nonvc_parqdecimalschemachange_table");

      createParquetStructWithCoercionTestTable(hiveDriver);
    }
  }

  private File getTempFile() throws Exception {
    return getTempFile("dremio-hive-test");
  }

  private File getTempFile(String fileName) throws Exception {
    return java.nio.file.Files.createTempFile(fileName, ".txt").toFile();
  }

  private String generateTestDataFile(int rows, String name) throws Exception {
    final File file = getTempFile(name);
    PrintWriter printWriter = new PrintWriter(file);
    for (int i = 1; i <= rows; i++) {
      printWriter.println(String.format("%d, key_%d", i, i));
    }
    printWriter.close();

    return file.getPath();
  }

  private String generateTestDataFileForPartitionInput() throws Exception {
    final File file = getTempFile();

    PrintWriter printWriter = new PrintWriter(file);

    String partValues[] = {"1", "2", "null"};

    for (int c = 0; c < partValues.length; c++) {
      for (int d = 0; d < partValues.length; d++) {
        for (int e = 0; e < partValues.length; e++) {
          for (int i = 1; i <= 5; i++) {
            Date date = new Date(System.currentTimeMillis());
            Timestamp ts = new Timestamp(System.currentTimeMillis());
            printWriter.printf("%s,%s,%s,%s,%s",
              date.toString(), ts.toString(), partValues[c], partValues[d], partValues[e]);
            printWriter.println();
          }
        }
      }
    }

    printWriter.close();

    return file.getPath();
  }

  private void createOrcStringTableWithComplexTypes(final Driver hiveDriver) {
    final String orcStringsTable = "create table orc_strings_complex (list_col array<int>, " +
      "struct_col struct<f1: int>, map_col map<int, int>, key int, country_char25 CHAR(25), " +
      "country_string string, country_varchar VARCHAR(1000), continent_char25 CHAR(25)) stored as orc";
    final String insert1 = "insert into orc_strings_complex select array(1), named_struct('f1', 1), map(1,1), 1, 'INDIA', 'CHINA', 'NEPAL', 'ASIA'";
    final String insert2 = "insert into orc_strings_complex select array(1), named_struct('f1', 1), map(1,1), 2, 'INDONESIA', 'THAILAND', 'SINGAPORE', 'ASIA'";
    final String insert3 = "insert into orc_strings_complex select array(1), named_struct('f1', 1), map(1,1), 3, 'FRANCE', 'ITALY', 'ROMANIA', 'EUROPE'";
    executeQuery(hiveDriver, orcStringsTable);
    executeQuery(hiveDriver, insert1);
    executeQuery(hiveDriver, insert2);
    executeQuery(hiveDriver, insert3);
  }

  private void createFieldSizeLimitTables(final Driver hiveDriver, final String table) throws Exception {
    final int unsupportedCellSize = Math.toIntExact(ExecConstants.LIMIT_FIELD_SIZE_BYTES.getDefault().getNumVal()) + 1;
    String createOrcTableCmd = "CREATE TABLE " + table + "_orc" + " (col1 string, col2 varchar(" + Integer.toString(unsupportedCellSize) + "), col3 binary) STORED AS ORC";
    String createTextTableCmd = "CREATE TABLE " + table + " (col1 string, col2 varchar(" + Integer.toString(unsupportedCellSize) + "), col3 binary)";

    String stringVal = StringUtils.repeat("a", unsupportedCellSize);
    String insertStrData = "INSERT INTO TABLE " + table + " VALUES('" + stringVal + "', '" + stringVal + "', '" + stringVal + "')";
    String insertStrDataInOrc = "INSERT INTO TABLE " + table + "_orc" + " VALUES('" + stringVal + "', '" + stringVal + "', '" + stringVal + "')";
    executeQuery(hiveDriver, createTextTableCmd);
    executeQuery(hiveDriver, createOrcTableCmd);
    executeQuery(hiveDriver, insertStrData);
    executeQuery(hiveDriver, insertStrDataInOrc);
  }

  private void createParquetSchemaChangeTestTable(final Driver hiveDriver, final String table) throws Exception {
    String createParqetTableCmd = "CREATE TABLE " + table + " (col1 int) STORED AS PARQUET";
    String insertDataCmd = "INSERT INTO " + table + " VALUES (1)";
    String alterTableCmd = "ALTER TABLE " + table + " CHANGE col1 col1 double";
    executeQuery(hiveDriver, createParqetTableCmd);
    executeQuery(hiveDriver, insertDataCmd);
    executeQuery(hiveDriver, alterTableCmd);
  }

  private void createParquetDecimalSchemaChangeTestTable(final Driver hiveDriver, final String table)
    throws Exception {
    String createParqetTableCmd = "CREATE TABLE " + table + " (col1 decimal(5,2)) STORED AS " +
      "PARQUET";
    String insertDataCmd = "INSERT INTO " + table + " VALUES (123.45)";
    String insertDataCmd2 = "INSERT INTO " + table + " VALUES (-543.21)";
    String alterTableCmd = "ALTER TABLE " + table + " CHANGE col1 col1 decimal(3,0)";
    executeQuery(hiveDriver, createParqetTableCmd);
    executeQuery(hiveDriver, insertDataCmd);
    executeQuery(hiveDriver, insertDataCmd2);
    executeQuery(hiveDriver, alterTableCmd);
  }

  private void createParquetDecimalSchemaChangeFilterTestTable(final Driver hiveDriver, final String table) {
    String createParqetTableCmd = "CREATE TABLE " + table + " (schema_mismatch_col decimal(8,5), value_in_parquet decimal(8,5)) STORED AS " +
      "PARQUET";
    String insertDataCmd = "INSERT INTO " + table + " VALUES (123.12341, 123.12341)";
    String insertDataCmd0 = "INSERT INTO " + table + " VALUES (123.13341, 123.13341)";
    String insertDataCmd1 = "INSERT INTO " + table + " VALUES (123.12000, 123.12000)";
    String insertDataCmd2 = "INSERT INTO " + table + " VALUES (123.12100, 123.12100)";
    String insertDataCmd3 = "INSERT INTO " + table + " VALUES (543.21568, 543.21568)";
    String alterTableCmd = "ALTER TABLE " + table + " CHANGE schema_mismatch_col schema_mismatch_col decimal(5,2)";
    executeQuery(hiveDriver, createParqetTableCmd);
    executeQuery(hiveDriver, insertDataCmd);
    executeQuery(hiveDriver, insertDataCmd0);
    executeQuery(hiveDriver, insertDataCmd1);
    executeQuery(hiveDriver, insertDataCmd2);
    executeQuery(hiveDriver, insertDataCmd3);
    executeQuery(hiveDriver, alterTableCmd);
  }

  private void createPartitionDecimalOverflow(final Driver hiveDriver, final String table) throws Exception {
    String createParqetTableCmd = "CREATE TABLE " + table + " (col1 int) partitioned by (col2 decimal(20, 2)) STORED AS PARQUET";
    String insertData = "INSERT INTO TABLE " + table + " PARTITION(col2=123456789101214161.12) VALUES(202)";

    String createParquetExtTable = "CREATE EXTERNAL TABLE " + table + "_ext" + " (col1 int) partitioned by (col2 decimal(15,3))" +
      " STORED AS PARQUET LOCATION 'file://" + this.getWhDir() + "/" + table + "'";
    String insertData1 = "INSERT INTO TABLE " + table + "_ext" + " PARTITION(col2=123456789.120) VALUES(2202)";
    String insertData2 = "INSERT INTO TABLE " + table + "_ext" + " PARTITION(col2=123456789101.123) VALUES(234)";
    String insertData3 = "INSERT INTO TABLE " + table + "_ext" + " PARTITION(col2=123456789101.123) VALUES(154)";
    String insertData4 = "INSERT INTO TABLE " + table + "_ext" + " PARTITION(col2=123456789102.123) VALUES(184)";
    String insertData5 = "INSERT INTO TABLE " + table + "_ext" + " PARTITION(col2=15.300) VALUES(153)";
    String partitionRepair = "msck repair table " + table + "_ext";

    executeQuery(hiveDriver, createParqetTableCmd);
    executeQuery(hiveDriver, insertData);
    executeQuery(hiveDriver, createParquetExtTable);
    executeQuery(hiveDriver, insertData1);
    executeQuery(hiveDriver, insertData2);
    executeQuery(hiveDriver, insertData3);
    executeQuery(hiveDriver, insertData4);
    executeQuery(hiveDriver, insertData4);
    executeQuery(hiveDriver, insertData5);
    executeQuery(hiveDriver, partitionRepair);
  }

  private void createParquetVarcharTable(final Driver hiveDriver, final String table, final URL resourceUrl) throws Exception {
    File parquetDir = new File(getWhDir(), "parq_varchar");
    parquetDir.mkdirs();
    File parquetFile = new File(parquetDir, "region_varchar.parquet");
    parquetFile.deleteOnExit();
    Files.write(Paths.get(parquetFile.toURI()), Resources.toByteArray(resourceUrl));
    parquetDir.deleteOnExit();
    String fixedLenVarchar = "create external table " + table + " (R_NAME varchar(6)) stored as parquet location '" +
      parquetFile.getParent() + "'";
    executeQuery(hiveDriver, fixedLenVarchar);
  }

  private void createParquetVarcharTableNoTruncationReqd(final Driver hiveDriver, final String table, final URL resourceUrl) throws Exception {
    File parquetDir = new File(getWhDir(), "parq_varchar");
    parquetDir.mkdirs();
    File parquetFile = new File(parquetDir, "region_varchar.parquet");
    parquetFile.deleteOnExit();
    Files.write(Paths.get(parquetFile.toURI()), Resources.toByteArray(resourceUrl));
    parquetDir.deleteOnExit();
    String fixedLenVarchar = "create external table " + table + " (R_NAME varchar(50)) stored as parquet location '" +
      parquetFile.getParent() + "'";
    executeQuery(hiveDriver, fixedLenVarchar);
  }

  private void createParquetCharTable(final Driver hiveDriver, final String table, final URL resourceUrl) throws Exception {
    File parquetDir = new File(getWhDir(), "parq_char");
    parquetDir.mkdirs();
    File parquetFile = new File(parquetDir, "region_char.parquet");
    parquetFile.deleteOnExit();
    Files.write(Paths.get(parquetFile.toURI()), Resources.toByteArray(resourceUrl));
    parquetDir.deleteOnExit();
    String fixedLenChar = "create external table " + table + " (R_NAME char(6)) stored as parquet location '" +
      parquetFile.getParent() + "'";
    executeQuery(hiveDriver, fixedLenChar);
  }

  private void createParquetVarcharWithMoreTypesTable(final Driver hiveDriver, final String table) throws Exception {
    String createParqetTableCmd = "CREATE TABLE " + table +
      " (A int, Country string, B int, Capital string, C int, Lang string)" +
      " STORED AS PARQUET";

    String insertData = "INSERT INTO TABLE " + table + " SELECT" +
      " 1, 'United Kingdom', 2, 'London', 3, 'English'";

    String createParquetExtTable = "CREATE EXTERNAL TABLE " + table + "_ext" +
      " (A int, Country varchar(50), B int, Capital string, C int, Lang varchar(3))" +
      " STORED AS PARQUET LOCATION 'file://" + this.getWhDir() + "/" + table + "'";

    executeQuery(hiveDriver, createParqetTableCmd);
    executeQuery(hiveDriver, insertData);
    executeQuery(hiveDriver, createParquetExtTable);
  }

  private void createParquetVarcharWithComplexTypeTable(final Driver hiveDriver, final String table) throws Exception {
    String createParqetTableCmd = "CREATE TABLE " + table +
      " (Country string, A struct<B:int, C:string>, D array<int>, E int, F map<int, string>, Capital string)" +
      " STORED AS PARQUET";

    String insertData = "INSERT INTO TABLE " + table + " SELECT" +
      " 'United Kingdom', named_struct('B', 3, 'C', 'test3'),array(1,2,3), 1, map(1, 'value1', 2, 'value2'), 'London'";

    String createParquetExtTable = "CREATE EXTERNAL TABLE " + table + "_ext" +
      " (Country varchar(3), A struct<B:int, C:string>, D array<int>, E int, F map<int, string>, Capital varchar(3))" +
      " STORED AS PARQUET LOCATION 'file://" + this.getWhDir() + "/" + table + "'";

    executeQuery(hiveDriver, createParqetTableCmd);
    executeQuery(hiveDriver, insertData);
    executeQuery(hiveDriver, createParquetExtTable);
  }

  private void createPartitionTruncatedVarchar(final Driver hiveDriver, final String table) throws Exception {
    String createParqetTableCmd = "CREATE TABLE " + table + " (col1 int) partitioned by (col2 varchar(8)) STORED AS PARQUET";
    String insertData = "INSERT INTO TABLE " + table + " PARTITION(col2='abcdefgh') VALUES(100)";

    String createParquetExtTable = "CREATE EXTERNAL TABLE " + table + "_ext" + " (col1 int) partitioned by (col2 varchar(4))" +
      " STORED AS PARQUET LOCATION 'file://" + this.getWhDir() + "/" + table + "'";

    String partitionRepair = "msck repair table " + table + "_ext";

    executeQuery(hiveDriver, createParqetTableCmd);
    executeQuery(hiveDriver, insertData);
    executeQuery(hiveDriver, createParquetExtTable);
    executeQuery(hiveDriver, partitionRepair);
  }

  private void createPartitionTruncatedChar(final Driver hiveDriver, final String table) throws Exception {
    String createParqetTableCmd = "CREATE TABLE " + table + " (col1 int) partitioned by (col2 char(8)) STORED AS PARQUET";
    String insertData = "INSERT INTO TABLE " + table + " PARTITION(col2='abcdefgh') VALUES(100)";

    String createParquetExtTable = "CREATE EXTERNAL TABLE " + table + "_ext" + " (col1 int) partitioned by (col2 char(4))" +
      " STORED AS PARQUET LOCATION 'file://" + this.getWhDir() + "/" + table + "'";

    String partitionRepair = "msck repair table " + table + "_ext";

    executeQuery(hiveDriver, createParqetTableCmd);
    executeQuery(hiveDriver, insertData);
    executeQuery(hiveDriver, createParquetExtTable);
    executeQuery(hiveDriver, partitionRepair);
  }

  private void createSimpleListWithNullsHiveTables(final Driver hiveDriver) throws Exception {
    final File simpleListWithNullsParquetDir = new File(BaseTestQuery.getTempDir("simplelistwithnullsparquet"));
    simpleListWithNullsParquetDir.mkdirs();
    final URL simpleListWithNullsParquetUrl = Resources.getResource("simple_list_with_nulls_test.parquet");
    if (simpleListWithNullsParquetUrl == null) {
      throw new IOException(String.format("Unable to find path %s.", "simple_list_with_nulls_test.parquet"));
    }

    // parquet file has following columns with one valid row
    // length of array value of each column is 4, and 3rd element is null
    // tinyintcol array<tinyint>
    // smallintcol array<smallint>
    // intcol array<int>
    // bigintcol array<bigint>
    // floatcol array<float>
    // doublcol array<double>
    // decimalcol array<decimal(10,3)>
    // charcol array<char(16)>
    // varcharcol array<varchar(16)>
    // stringcol array<string>
    // datecol array<date>
    // timestampcol array<timestamp>
    final File simpleListWithNullsParquetFile = new File(simpleListWithNullsParquetDir, "simple_list_with_nulls_test.parquet");
    simpleListWithNullsParquetFile.deleteOnExit();
    simpleListWithNullsParquetDir.deleteOnExit();
    Files.write(Paths.get(simpleListWithNullsParquetFile.toURI()), Resources.toByteArray(simpleListWithNullsParquetUrl));

    // create good table with matching names, types and positions
    final String basicAllMatchingTest = "create external table array_simple_with_nulls_test_ext1(" +
      "tinyintcol array<tinyint>, "+
      "smallintcol array<smallint>, "+
      "intcol array<int>, "+
      "bigintcol array<bigint>, "+
      "floatcol array<float>, "+
      "doublecol array<double>, "+
      "decimalcol array<decimal(10,3)>, "+
      "charcol array<char(16)>, "+
      "varcharcol array<varchar(16)>, "+
      "stringcol array<string>, " +
      "datecol array<date>, " +
      "timestampcol array<timestamp> ) " +
      "stored as parquet location '" + simpleListWithNullsParquetFile.getParent() + "'";
    executeQuery(hiveDriver, basicAllMatchingTest);

    // create good table with matching names, types and positions
    final String upPromoteWithNulls = "create external table array_simple_with_nulls_test_ext2(" +
      "tinyintcol array<bigint>, "+
      "smallintcol array<bigint>, "+
      "intcol array<bigint>, "+
      "bigintcol array<bigint>, "+
      "floatcol array<double>, "+
      "doublecol array<double>, "+
      "decimalcol array<decimal(3,1)>, "+
      "charcol array<char(16)>, "+
      "varcharcol array<varchar(16)>, "+
      "stringcol array<string>, " +
      "datecol array<date>, " +
      "timestampcol array<timestamp> ) " +
      "stored as parquet location '" + simpleListWithNullsParquetFile.getParent() + "'";
    executeQuery(hiveDriver, upPromoteWithNulls);
  }

  private void createNestedListWithNullsHiveTables(final Driver hiveDriver) throws Exception {
    final File nestedListWithNullsParquetDir = new File(BaseTestQuery.getTempDir("nestedlistwithnullsparquet"));
    nestedListWithNullsParquetDir.mkdirs();
    final URL nestedListWithNullsParquetUrl = Resources.getResource("list_list_null_test.parquet");
    if (nestedListWithNullsParquetUrl == null) {
      throw new IOException(String.format("Unable to find path %s.", "list_list_null_test.parquet"));
    }

    // parquet file has following columns with one valid row
    // col1 array<array<array<string> with value [[['a', null], null], null]
    final File nestedListWithNullsParquetFile = new File(nestedListWithNullsParquetDir, "list_list_null_test.parquet");
    nestedListWithNullsParquetFile.deleteOnExit();
    nestedListWithNullsParquetDir.deleteOnExit();
    Files.write(Paths.get(nestedListWithNullsParquetFile.toURI()), Resources.toByteArray(nestedListWithNullsParquetUrl));

    final String nestedListTest = "create external table array_nested_with_nulls_test_ext1(" +
      "col1 array<array<array<string>>>)" +
      "stored as parquet location '" + nestedListWithNullsParquetFile.getParent() + "'";
    executeQuery(hiveDriver, nestedListTest);
  }

  private void createNestedStructWithNullsHiveTables(final Driver hiveDriver) throws Exception {
    final File nestedStructWithNullsParquetDir = new File(BaseTestQuery.getTempDir("nestedstructwithnullsparquet"));
    nestedStructWithNullsParquetDir.mkdirs();
    final URL nestedStructWithNullsParquetUrl = Resources.getResource("list_struct_null_test.parquet");
    if (nestedStructWithNullsParquetUrl == null) {
      throw new IOException(String.format("Unable to find path %s.", "list_struct_null_test.parquet"));
    }

    // parquet file has following columns with one valid row
    // col1 array<struct<f1:array<string> with value [{['a', null]}, null]
    final File nestedStructWithNullsParquetFile = new File(nestedStructWithNullsParquetDir, "list_struct_null_test.parquet");
    nestedStructWithNullsParquetFile.deleteOnExit();
    nestedStructWithNullsParquetDir.deleteOnExit();
    Files.write(Paths.get(nestedStructWithNullsParquetFile.toURI()), Resources.toByteArray(nestedStructWithNullsParquetUrl));

    final String nestedStructTest = "create external table array_struct_with_nulls_test_ext1(" +
      "col1 array<struct<f1:array<string>>>)" +
      "stored as parquet location '" + nestedStructWithNullsParquetFile.getParent() + "'";
    executeQuery(hiveDriver, nestedStructTest);
  }

  private void createParuqetComplexFilterTestTable(final Driver hiveDriver)  throws Exception {
    final File complexFilterTest = new File(BaseTestQuery.getTempDir("parquetcomplexfiltertest"));
    complexFilterTest.mkdirs();
    final URL complexFilterTestParquetUrl = Resources.getResource("filter_tests.parquet");
    if (complexFilterTestParquetUrl == null) {
      throw new IOException(String.format("Unable to find path %s.", "filter_tests.parquet"));
    }

    final File complexFilterTestParquetFile = new File(complexFilterTest, "filter_tests.parquet");
    complexFilterTestParquetFile.deleteOnExit();
    complexFilterTest.deleteOnExit();
    Files.write(Paths.get(complexFilterTestParquetFile.toURI()), Resources.toByteArray(complexFilterTestParquetUrl));

    // parquet file has following columns with one valid row
    // intcol int
    // floatcol float
    // decimalcol decimal(10,5)
    // varcharcol varchar(16)
    // listcol array<int>
    // structcol struct<f1:int>

    // create good table with matching types
    final String basicAllMatchingTest = "create external table filter_simple_test_ext1(" +
      "intcol int," +
      "floatcol float," +
      "decimalcol decimal(10,5)," +
      "varcharcol varchar(16)," +
      "listcol array<int>," +
      "structcol struct<f1:int>) " +
      "stored as parquet location '" + complexFilterTestParquetFile.getParent() + "'";
    executeQuery(hiveDriver, basicAllMatchingTest);

    // create table with type coercions
    final String tableWithCoercions = "create external table filter_simple_test_ext2(" +
      "intcol bigint," +
      "floatcol double," +
      "decimalcol decimal(3,2)," +
      "varcharcol varchar(10)," +
      "listcol array<bigint>," +
      "structcol struct<f1:bigint>) " +
      "stored as parquet location '" + complexFilterTestParquetFile.getParent() + "'";
    executeQuery(hiveDriver, tableWithCoercions);
  }

  private void createParquetTableWithDeeplyNestedColumns(final Driver hiveDriver)  throws Exception {
    final File complexNestedLevels = new File(BaseTestQuery.getTempDir("parquetcomplexnestedlevels"));
    complexNestedLevels.mkdirs();
    final URL complexNestedLevelsTestParquetUrl = Resources.getResource("deeply_nested_list.parquet");
    if (complexNestedLevelsTestParquetUrl == null) {
      throw new IOException(String.format("Unable to find path %s.", "deeply_nested_list.parquet"));
    }

    final File complexNestedLevelsTestParquetFile = new File(complexNestedLevels, "deeply_nested_list.parquet");
    complexNestedLevelsTestParquetFile.deleteOnExit();
    complexNestedLevels.deleteOnExit();
    Files.write(Paths.get(complexNestedLevelsTestParquetFile.toURI()), Resources.toByteArray(complexNestedLevelsTestParquetUrl));

    // parquet file has two rows of data for 4 columns
    // col1 is primitive, col2 has depth 30, col3 has depth 32 and col4 has depth 1
    // col2 and col3 have 128 elements in their lists and col4 has 136 items
    final String nestedLevelsTest = "create external table deeply_nested_list_test(" +
      "col1 int," +
      "col2 array<array<array<array<array<array" +
      "<array<array<array<array<array<array<array<" +
      "array<array<array<array<array<array<array<array" +
      "<array<array<array<array<array<array<array" +
      "<array<array<string>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>," +
      "col3 array<array<array<array<array<array<array<array" +
      "<array<array<array<array<array<array<array<array" +
      "<array<array<array<array<array<array<array<" +
      "array<array<array<array<array<array<" +
      "array<array<array<string>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>," +
      "col4 array<string>) " +
      "stored as parquet location '" + complexNestedLevelsTestParquetFile.getParent() + "'";
    executeQuery(hiveDriver, nestedLevelsTest);

    // create an empty table with one column using struct and arrays with depth 20
    final String nestedLevelsStructTest = "create table deeply_nested_struct_test(" +
      "col1 struct<f1:struct<f1:struct<f1:struct<f1:struct<" +
      "f1:struct<f1:struct<f1:struct<f1:struct<f1:struct<" +
      "f1:struct<f1:struct<f1:struct<f1:struct<f1:struct<" +
      "f1:struct<f1:array<struct<f1:array<struct<f1:string>>>>>>>>>>>>>>>>>>>>) " +
      "stored as parquet";
    executeQuery(hiveDriver, nestedLevelsStructTest);
  }

  private void createComplexVarcharHiveTables(final Driver hiveDriver) throws Exception {
    final File complexVarcharParquetDir = new File(BaseTestQuery.getTempDir("complexvarcharparquet"));
    complexVarcharParquetDir.mkdirs();
    final URL complexVarcharParquetUrl = Resources.getResource("varchar_complex.parquet");
    if (complexVarcharParquetUrl == null) {
      throw new IOException(String.format("Unable to find path %s.", "varchar_complex.parquet"));
    }

    // parquet file has following columns with one valid row, all varchars have 'abcdefghijklmno' as value
    // col1 varchar(15)
    // col2 struct<f1:varchar(15)>
    // col3 struct<f1:struct<subf1:varchar(15)>>
    // col4 struct<f1:array<varchar(15)>>
    // col5 array<struct<f1:varchar(15)>>
    // col6 array<varchar(15)>
    // col7 array<array<array<array<varchar(15)>>>>
    final File complexVarcharParquetFile = new File(complexVarcharParquetDir, "varchar_complex.parquet");
    complexVarcharParquetFile.deleteOnExit();
    complexVarcharParquetDir.deleteOnExit();
    Files.write(Paths.get(complexVarcharParquetFile.toURI()), Resources.toByteArray(complexVarcharParquetUrl));

    // create a table with matching names and types with different varchar lengths
    final String varcharTruncationTest =
      "create external table varchar_truncation_test_ext1("
            + "col1 varchar(2), "
            + "col2 struct<f1:varchar(4)>, "
            + "col3 struct<f1:struct<subf1:varchar(6)>>, "
            + "col4 struct<f1:array<varchar(8)>>, "
            + "col5 array<struct<f1:varchar(10)>>, "
            + "col6 array<varchar(12)>, "
            + "col7 array<array<array<array<varchar(14)>>>> ) "
            + "stored as parquet location '"
            + complexVarcharParquetFile.getParent()
            + "'";
    executeQuery(hiveDriver, varcharTruncationTest);
  }

  private void createComplexTypesTextTable(final Driver hiveDriver, final String table) throws Exception {
    String testDataFile = generateComplexTypesDataFile();
    executeQuery(hiveDriver,
      "CREATE TABLE IF NOT EXISTS " + table + " (" +
        " rownum int," +
        " list_field array<int>, " +
        " struct_field struct<name:string, age:int>, " +
        " struct_list_field struct<type:string, value:array<string>>, " +
        " list_struct_field array<struct<name:string, age:int>>, " +
        " map_field map<string, int>, " +
        " map_struct_field map<string, struct<type:string>> " +
        ") ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' COLLECTION ITEMS TERMINATED BY ','" +
        " MAP KEYS TERMINATED BY ':'");
    executeQuery(hiveDriver,
      String.format("LOAD DATA LOCAL INPATH '%s' INTO TABLE default." + table, testDataFile));
  }

  private void createComplexTypesTable(final Driver hiveDriver, final String format, final String table) throws Exception {
    executeQuery(hiveDriver,
      "CREATE TABLE " + table + format + "(" +
        " rownum int," +
        " list_field array<int>, " +
        " struct_field struct<name:string, age:int>, " +
        " struct_list_field struct<type:string, value:array<string>>, " +
        " list_struct_field array<struct<name:string, age:int>>, " +
        " map_field map<string, int>, " +
        " map_struct_field map<string, struct<type:string>> " +
        ") ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' COLLECTION ITEMS TERMINATED BY ','" +
        " MAP KEYS TERMINATED BY ':' STORED AS " + format);
    executeQuery(hiveDriver, "INSERT OVERWRITE TABLE " + table + format + " SELECT * FROM " + table);
  }

  private void createListTypesTextTable(final Driver hiveDriver, final String table) throws Exception {
    String testDataFile = generateListTypesDataFile();
    executeQuery(hiveDriver,
      "CREATE TABLE IF NOT EXISTS " + table + " (" +
        " rownum int," +
        " tinyint_field array<tinyint>, " +
        " smallint_field array<smallint>, " +
        " int_field array<int>, " +
        " bigint_field array<bigint>, " +
        " float_field array<float>, " +
        " double_field array<double>, " +
        " timestamp_field array<timestamp>, " +
        " date_field array<date>, " +
        " string_field array<string>, " +
        " boolean_field array<boolean>, " +
        " binary_field array<binary> " +
        ") ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' COLLECTION ITEMS TERMINATED BY ','");
    // Load data into table 'readtest'
    executeQuery(hiveDriver,
      String.format("LOAD DATA LOCAL INPATH '%s' INTO TABLE default." + table, testDataFile));
  }

  private void createListTypesTable(final Driver hiveDriver, final String format, final String table) throws Exception {
    executeQuery(hiveDriver,
      "CREATE TABLE " + table + format + "(" +
        " rownum int," +
        " tinyint_field array<tinyint>, " +
        " smallint_field array<smallint>, " +
        " int_field array<int>, " +
        " bigint_field array<bigint>, " +
        " float_field array<float>, " +
        " double_field array<double>, " +
        " timestamp_field array<timestamp>, " +
        " date_field array<date>, " +
        " string_field array<string>, " +
        " boolean_field array<boolean>, " +
        " binary_field array<binary> " +
        ") ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' COLLECTION ITEMS TERMINATED BY ',' STORED AS " + format);
    executeQuery(hiveDriver, "INSERT OVERWRITE TABLE " + table + format + " SELECT * FROM " + table);
  }

  private void createMapTypesTextTable(final Driver hiveDriver, final String table) throws Exception {
    String testDataFile = generateMapTypesDataFile();
    executeQuery(hiveDriver,
      "CREATE TABLE IF NOT EXISTS " + table + " (" +
        " rownum int, " +
        " map_field map<int, int>" +
        ") ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' COLLECTION ITEMS TERMINATED BY ','" +
        "  MAP KEYS TERMINATED BY ':'");
    executeQuery(hiveDriver,
      String.format("LOAD DATA LOCAL INPATH '%s' INTO TABLE default." + table, testDataFile));
  }

  private void createMapTypesTable(final Driver hiveDriver, final String format, final String table) throws Exception {
    executeQuery(hiveDriver,
      "CREATE TABLE " + table + format + "(" +
        " rownum int, " +
        " map_field map<int, int>" +
        ") ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' COLLECTION ITEMS TERMINATED BY ',' " +
        "  MAP KEYS TERMINATED BY ':'" +
        " STORED AS " + format);
    executeQuery(hiveDriver, "INSERT OVERWRITE TABLE " + table + format + " SELECT * FROM " + table);
  }

  private void createORCDecimalCompareTestTable(final Driver hiveDriver, final String table) throws Exception {
    String createTableCmd = "CREATE TABLE " + table + " (col1 float, col2 double, col3 decimal(1,1)) stored as orc";
    String insertCmd = "INSERT INTO " + table + " values (0.1, 0.1, 0.1), (-0.1, -0.1, -0.1)";
    executeQuery(hiveDriver, createTableCmd);
    executeQuery(hiveDriver, insertCmd);
  }

  private void createStructTypesTextTable(final Driver hiveDriver, final String table) throws Exception {
    String testDataFile = generateStructTypesDataFile();
    executeQuery(hiveDriver,
      "CREATE TABLE IF NOT EXISTS " + table + " (" +
        " rownum int," +
        " struct_field struct<" +
        " tinyint_field: tinyint, " +
        " smallint_field: smallint, " +
        " int_field: int, " +
        " bigint_field: bigint, " +
        " float_field: float, " +
        " double_field: double, " +
        " string_field: string> " +
        ") ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' COLLECTION ITEMS TERMINATED BY ','");
    executeQuery(hiveDriver,
      String.format("LOAD DATA LOCAL INPATH '%s' INTO TABLE default." + table, testDataFile));
  }

  private void createStructTypesTable(final Driver hiveDriver, final String format, final String table) throws Exception {
    executeQuery(hiveDriver,
      "CREATE TABLE " + table + format + "(" +
        " rownum int," +
        " struct_field struct<" +
        " tinyint_field: tinyint, " +
        " smallint_field: smallint, " +
        " int_field: int, " +
        " bigint_field: bigint, " +
        " float_field: float, " +
        " double_field: double, " +
        " string_field: string> " +
        ") ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' COLLECTION ITEMS TERMINATED BY ',' STORED AS " + format);
    executeQuery(hiveDriver, "INSERT OVERWRITE TABLE " + table + format + " SELECT * FROM " + table);
  }

  private void createUnionTypesTextTable(final Driver hiveDriver, final String table) throws Exception {
    String testIntDataFile = generateUnionIntTypesDataFile();
    String testDoubleDataFile = generateUnionDoubleTypesDataFile();
    String testStringDataFile = generateUnionStringTypesDataFile();
    executeQuery(hiveDriver,
      "CREATE TABLE IF NOT EXISTS " + table + " (" +
        " rownum int," +
        " union_field uniontype<int, double, string>" +
        ") ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' COLLECTION ITEMS TERMINATED BY ','");
    executeQuery(hiveDriver,
      "CREATE TABLE IF NOT EXISTS " + table + "_int_input" + " (" +
        " rownum int, int_field int" +
        ") ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' COLLECTION ITEMS TERMINATED BY ','");
    executeQuery(hiveDriver,
      "CREATE TABLE IF NOT EXISTS " + table + "_double_input" + " (" +
        " rownum int, double_field double" +
        ") ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' COLLECTION ITEMS TERMINATED BY ','");
    executeQuery(hiveDriver,
      "CREATE TABLE IF NOT EXISTS " + table + "_string_input" + " (" +
        " rownum int, string_field string" +
        ") ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' COLLECTION ITEMS TERMINATED BY ','");
    executeQuery(hiveDriver,
      String.format("LOAD DATA LOCAL INPATH '%s' INTO TABLE default." + table + "_int_input", testIntDataFile));
    executeQuery(hiveDriver,
      String.format("LOAD DATA LOCAL INPATH '%s' INTO TABLE default." + table + "_double_input", testDoubleDataFile));
    executeQuery(hiveDriver,
      String.format("LOAD DATA LOCAL INPATH '%s' INTO TABLE default." + table + "_string_input", testStringDataFile));
    executeQuery(hiveDriver,
      String.format("INSERT INTO TABLE " + table + " SELECT rownum, CREATE_UNION(0, int_field, 0.0, \"\") FROM " + table + "_int_input"));
    executeQuery(hiveDriver,
      String.format("INSERT INTO TABLE " + table + " SELECT rownum, CREATE_UNION(1, 0, double_field, \"\") FROM " + table + "_double_input"));
    executeQuery(hiveDriver,
      String.format("INSERT INTO TABLE " + table + " SELECT rownum, CREATE_UNION(2, 0, 0.0, string_field) FROM " + table + "_string_input"));
  }

  private void createUnionTypesTable(final Driver hiveDriver, final String format, final String table) throws Exception {
    executeQuery(hiveDriver,
      "CREATE TABLE " + table + format + "(" +
        " rownum int," +
        " union_field uniontype<int, double, string>" +
        ") ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' COLLECTION ITEMS TERMINATED BY ',' STORED AS " + format);
    executeQuery(hiveDriver, "INSERT OVERWRITE TABLE " + table + format + " SELECT * FROM " + table);
  }

  private void createAllTypesTextTable(final Driver hiveDriver, final String table) throws Exception {
    String testDataFile = generateAllTypesDataFile();
    executeQuery(hiveDriver,
      "CREATE TABLE IF NOT EXISTS " + table + " (" +
        "  binary_field BINARY," +
        "  boolean_field BOOLEAN," +
        "  tinyint_field TINYINT," +
        "  decimal0_field DECIMAL," +
        "  decimal9_field DECIMAL(6, 2)," +
        "  decimal18_field DECIMAL(15, 5)," +
        "  decimal28_field DECIMAL(23, 1)," +
        "  decimal38_field DECIMAL(30, 3)," +
        "  double_field DOUBLE," +
        "  float_field FLOAT," +
        "  int_field INT," +
        "  bigint_field BIGINT," +
        "  smallint_field SMALLINT," +
        "  string_field STRING," +
        "  varchar_field VARCHAR(50)," +
        "  timestamp_field TIMESTAMP," +
        "  date_field DATE," +
        "  char_field CHAR(10)" +
        ") PARTITIONED BY (" +
        // There is a regression in Hive 1.2.1 in binary type partition columns. Disable for now.
        // "  binary_part BINARY," +
        "  boolean_part BOOLEAN," +
        "  tinyint_part TINYINT," +
        "  decimal0_part DECIMAL," +
        "  decimal9_part DECIMAL(6, 2)," +
        "  decimal18_part DECIMAL(15, 5)," +
        "  decimal28_part DECIMAL(23, 1)," +
        "  decimal38_part DECIMAL(30, 3)," +
        "  double_part DOUBLE," +
        "  float_part FLOAT," +
        "  int_part INT," +
        "  bigint_part BIGINT," +
        "  smallint_part SMALLINT," +
        "  string_part STRING," +
        "  varchar_part VARCHAR(50)," +
        "  timestamp_part TIMESTAMP," +
        "  date_part DATE," +
        "  char_part CHAR(10)" +
        ") ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' " +
        "TBLPROPERTIES ('serialization.null.format'='') "
    );

    // Add a partition to table 'readtest'
    executeQuery(hiveDriver,
      "ALTER TABLE " + table + " ADD IF NOT EXISTS PARTITION ( " +
        // There is a regression in Hive 1.2.1 in binary type partition columns. Disable for now.
        // "  binary_part='binary', " +
        "  boolean_part='true', " +
        "  tinyint_part='64', " +
        "  decimal0_part='36.9', " +
        "  decimal9_part='36.9', " +
        "  decimal18_part='3289379872.945645', " +
        "  decimal28_part='39579334534534.35345', " +
        "  decimal38_part='363945093845093890.9', " +
        "  double_part='8.345', " +
        "  float_part='4.67', " +
        "  int_part='123456', " +
        "  bigint_part='234235', " +
        "  smallint_part='3455', " +
        "  string_part='string', " +
        "  varchar_part='varchar', " +
        "  timestamp_part='2013-07-05 17:01:00', " +
        "  date_part='2013-07-05', " +
        "  char_part='char')"
    );

    // Add a second partition to table 'readtest' which contains the same values as the first partition except
    // for tinyint_part partition column
    executeQuery(hiveDriver,
      "ALTER TABLE " + table + " ADD IF NOT EXISTS PARTITION ( " +
        // There is a regression in Hive 1.2.1 in binary type partition columns. Disable for now.
        // "  binary_part='binary', " +
        "  boolean_part='true', " +
        "  tinyint_part='65', " +
        "  decimal0_part='36.9', " +
        "  decimal9_part='36.9', " +
        "  decimal18_part='3289379872.945645', " +
        "  decimal28_part='39579334534534.35345', " +
        "  decimal38_part='363945093845093890.9', " +
        "  double_part='8.345', " +
        "  float_part='4.67', " +
        "  int_part='123456', " +
        "  bigint_part='234235', " +
        "  smallint_part='3455', " +
        "  string_part='string', " +
        "  varchar_part='varchar', " +
        "  timestamp_part='2013-07-05 17:01:00', " +
        "  date_part='2013-07-05', " +
        "  char_part='char')"
    );

    // Load data into table 'readtest'
    executeQuery(hiveDriver,
      String.format("LOAD DATA LOCAL INPATH '%s' INTO TABLE default." + table + " PARTITION (" +
        // There is a regression in Hive 1.2.1 in binary type partition columns. Disable for now.
        // "  binary_part='binary', " +
        "  boolean_part='true', " +
        "  tinyint_part='64', " +
        "  decimal0_part='36.9', " +
        "  decimal9_part='36.9', " +
        "  decimal18_part='3289379872.945645', " +
        "  decimal28_part='39579334534534.35345', " +
        "  decimal38_part='363945093845093890.9', " +
        "  double_part='8.345', " +
        "  float_part='4.67', " +
        "  int_part='123456', " +
        "  bigint_part='234235', " +
        "  smallint_part='3455', " +
        "  string_part='string', " +
        "  varchar_part='varchar', " +
        "  timestamp_part='2013-07-05 17:01:00', " +
        "  date_part='2013-07-05'," +
        "  char_part='char'" +
        ")", testDataFile));
  }

  private void createDecimalConversionTable(Driver hiveDriver, String table, String tableFormat) throws Exception {
    String datatable = "CREATE TABLE IF NOT EXISTS " + table + " (col1 decimal(30, 9), col2 decimal(23, 6), col3 decimal(16, 2)) STORED AS " + tableFormat;
    executeQuery(hiveDriver, datatable);
    String intsert_datatable = "INSERT INTO " + table + " VALUES (111111111111111111111.111111111, 22222222222222222.222222, 333.0)";
    executeQuery(hiveDriver, intsert_datatable);

    // decimal to string
    String exttable = table + "_ext";
    String ext_table = "CREATE EXTERNAL TABLE IF NOT EXISTS " + exttable +
      " (col1 decimal(25,2), col2 string, col3 varchar(32))" + "STORED AS " + tableFormat + " LOCATION 'file://" + this.getWhDir() + "/" + table + "'";
    executeQuery(hiveDriver, ext_table);

    // decimal to decimal with different precision and scale
    String exttable2 = table + "_ext_2";
    String ext_table2 = "CREATE EXTERNAL TABLE IF NOT EXISTS " + exttable2 +
      " (col1 decimal(2,2), col2 decimal(16,7), col3 decimal(4,1))" + "STORED AS " + tableFormat + " LOCATION 'file://" + this.getWhDir() + "/" + table + "'";
    executeQuery(hiveDriver, ext_table2);

    String tablerev = table + "_rev";
    String datatablerev = "CREATE TABLE IF NOT EXISTS " + tablerev + " (col1 int, col2 string, col3 double) STORED AS " + tableFormat;
    executeQuery(hiveDriver, datatablerev);
    String intsert_datatable_rev = "INSERT INTO " + tablerev + " VALUES (1234, 1234567, 1234567.123)";
    executeQuery(hiveDriver, intsert_datatable_rev);

    // string to decimal
    String exttable_rev = tablerev + "_ext";
    String ext_table_rev = "CREATE EXTERNAL TABLE IF NOT EXISTS " + exttable_rev +
      " (col1 decimal(3,2), col2 decimal(5,2), col3 decimal(5,2))" + "STORED AS " + tableFormat + " LOCATION 'file://" + this.getWhDir() + "/" + tablerev + "'";
    executeQuery(hiveDriver, ext_table_rev);

    //decimal to decimal tests
    String decimaltodecimal = table + "_decimal";
    String decimaltodecimal_table = "CREATE TABLE IF NOT EXISTS " + decimaltodecimal +
      " (col1 decimal(30, 10), col2 decimal(30, 10), col3 decimal(30, 10), " +
      " col4 decimal(30, 10), col5 decimal(30, 10), col6 decimal(30, 10), " +
      " col7 decimal(30, 10), col8 decimal(30, 10), col9 decimal(30, 10) ) STORED AS " + tableFormat;
    executeQuery(hiveDriver, decimaltodecimal_table);
    String intsert_decimaltodecimal_table = "INSERT INTO " + decimaltodecimal + " VALUES (" +
      "12345678912345678912.1234567891, 12345678912345678912.1234567891, 12345678912345678912.1234567891, " +
      " 12345678912345678912.1234567891, 12345678912345678912.1234567891, 12345678912345678912.1234567891, " +
      " 12345678912345678912.1234567891, 12345678912345678912.1234567891, 12345678912345678912.1234567891 )";
    executeQuery(hiveDriver, intsert_decimaltodecimal_table);

    String ext_decimaltodecimal_table = "CREATE EXTERNAL TABLE IF NOT EXISTS " + decimaltodecimal + "_ext" +
      " (col1 decimal(30, 10), col2 decimal(30, 15), col3 decimal(30, 5), " +
      " col4 decimal(35, 10), col5 decimal(35, 15), col6 decimal(35, 5), " +
      " col7 decimal(25, 10), col8 decimal(25, 15), col9 decimal(25, 5) ) STORED AS " + tableFormat +
      " LOCATION 'file://" + this.getWhDir() + "/" + decimaltodecimal + "'";
    executeQuery(hiveDriver, ext_decimaltodecimal_table);

    String ext_drop_col3 = "CREATE EXTERNAL TABLE IF NOT EXISTS " + table + "_no_col3_ext" +
      " (col1 decimal(30, 9), col2 decimal(23, 6))" + " STORED AS " + tableFormat + " LOCATION 'file://" + this.getWhDir() + "/" + table + "'";
    executeQuery(hiveDriver, ext_drop_col3);
  }

  private void createTimestampToStringTable(Driver hiveDriver, String table) throws Exception {
    String testDataFile = generateTimestampsDataFile();
    String datatabletxt = "CREATE TABLE IF NOT EXISTS " + table + " (col1 timestamp)";
    String datatableorc = "CREATE TABLE IF NOT EXISTS " + table + "_orc" + " (col1 timestamp) STORED AS ORC";
    executeQuery(hiveDriver, datatabletxt);
    executeQuery(hiveDriver, datatableorc);

    String insert_datatable = String.format("LOAD DATA LOCAL INPATH '%s' INTO TABLE default." + table, testDataFile);
    executeQuery(hiveDriver, insert_datatable);
    String insert_datatableorc = "INSERT OVERWRITE TABLE " + table + "_orc" + " SELECT * FROM " + table;
    executeQuery(hiveDriver, insert_datatableorc);
    String exttable = table + "_orc_ext";
    String ext_table = "CREATE EXTERNAL TABLE IF NOT EXISTS " + exttable +
      " (col1 string)" + "STORED AS ORC LOCATION 'file://" + this.getWhDir() + "/" + table + "_orc" + "'";
    executeQuery(hiveDriver, ext_table);
  }

  private void createDoubleToStringTable(Driver hiveDriver, String table) throws Exception {
    String testDataFile = generateDoubleDataFile();
    String datatabletxt = "CREATE TABLE IF NOT EXISTS " + table + " (col1 double)";
    String datatableorc = "CREATE TABLE IF NOT EXISTS " + table + "_orc" + " (col1 double) STORED AS ORC";
    executeQuery(hiveDriver, datatabletxt);
    executeQuery(hiveDriver, datatableorc);

    String insert_datatable = String.format("LOAD DATA LOCAL INPATH '%s' INTO TABLE default." + table, testDataFile);
    executeQuery(hiveDriver, insert_datatable);
    String insert_datatableorc = "INSERT OVERWRITE TABLE " + table + "_orc" + " SELECT * FROM " + table;
    executeQuery(hiveDriver, insert_datatableorc);
    String exttable = table + "_orc_ext";
    String ext_table = "CREATE EXTERNAL TABLE IF NOT EXISTS " + exttable +
      " (col1 string)" + "STORED AS ORC LOCATION 'file://" + this.getWhDir() + "/" + table + "_orc" + "'";
    executeQuery(hiveDriver, ext_table);
  }

  private void createExtTableWithMoreColumnsThanOriginal(Driver hiveDriver, String table) throws Exception {
    String datatable = "CREATE TABLE IF NOT EXISTS " + table + " (col1 int, col2 int) STORED AS ORC";
    executeQuery(hiveDriver, datatable);
    String insert_datatable = "INSERT INTO " + table + " VALUES (1,2)";
    executeQuery(hiveDriver, insert_datatable);
    String exttable = table + "_ext";
    String ext_table = "CREATE EXTERNAL TABLE IF NOT EXISTS " + exttable +
      " (col1 int, col2 int, col3 int, col4 int)" + "STORED AS ORC LOCATION 'file://" + this.getWhDir() + "/" + table + "'";
    executeQuery(hiveDriver, ext_table);
  }

  private void createAllTypesTable(Driver hiveDriver, String format, String source) throws Exception {
    executeQuery(hiveDriver,
      "CREATE TABLE readtest_" + format + "(" +
        "  binary_field BINARY, " +
        "  boolean_field BOOLEAN, " +
        "  tinyint_field TINYINT," +
        "  decimal0_field DECIMAL," +
        "  decimal9_field DECIMAL(6, 2)," +
        "  decimal18_field DECIMAL(15, 5)," +
        "  decimal28_field DECIMAL(23, 1)," +
        "  decimal38_field DECIMAL(30, 3)," +
        "  double_field DOUBLE," +
        "  float_field FLOAT," +
        "  int_field INT," +
        "  bigint_field BIGINT," +
        "  smallint_field SMALLINT," +
        "  string_field STRING," +
        "  varchar_field VARCHAR(50)," +
        "  timestamp_field TIMESTAMP," +
        "  date_field DATE," +
        "  char_field CHAR(10)" +
        ") PARTITIONED BY (" +
        // There is a regression in Hive 1.2.1 in binary type partition columns. Disable for now.
        // "  binary_part BINARY," +
        "  boolean_part BOOLEAN," +
        "  tinyint_part TINYINT," +
        "  decimal0_part DECIMAL," +
        "  decimal9_part DECIMAL(6, 2)," +
        "  decimal18_part DECIMAL(15, 5)," +
        "  decimal28_part DECIMAL(23, 1)," +
        "  decimal38_part DECIMAL(30, 3)," +
        "  double_part DOUBLE," +
        "  float_part FLOAT," +
        "  int_part INT," +
        "  bigint_part BIGINT," +
        "  smallint_part SMALLINT," +
        "  string_part STRING," +
        "  varchar_part VARCHAR(50)," +
        "  timestamp_part TIMESTAMP," +
        "  date_part DATE," +
        "  char_part CHAR(10)" +
        ") STORED AS " + format
    );

    executeQuery(hiveDriver, "INSERT OVERWRITE TABLE readtest_" + format +
      " PARTITION (" +
      // There is a regression in Hive 1.2.1 in binary type partition columns. Disable for now.
      // "  binary_part='binary', " +
      "  boolean_part='true', " +
      "  tinyint_part='64', " +
      "  decimal0_part='36.9', " +
      "  decimal9_part='36.9', " +
      "  decimal18_part='3289379872.945645', " +
      "  decimal28_part='39579334534534.35345', " +
      "  decimal38_part='363945093845093890.9', " +
      "  double_part='8.345', " +
      "  float_part='4.67', " +
      "  int_part='123456', " +
      "  bigint_part='234235', " +
      "  smallint_part='3455', " +
      "  string_part='string', " +
      "  varchar_part='varchar', " +
      "  timestamp_part='2013-07-05 17:01:00', " +
      "  date_part='2013-07-05', " +
      "  char_part='char'" +
      ") " +
      " SELECT " +
      "  binary_field," +
      "  boolean_field," +
      "  tinyint_field," +
      "  decimal0_field," +
      "  decimal9_field," +
      "  decimal18_field," +
      "  decimal28_field," +
      "  decimal38_field," +
      "  double_field," +
      "  float_field," +
      "  int_field," +
      "  bigint_field," +
      "  smallint_field," +
      "  string_field," +
      "  varchar_field," +
      "  timestamp_field," +
      "  date_field," +
      "  char_field" +
      " FROM " + source + " WHERE tinyint_part = 64");

    // Add a second partition to table 'readtest' which contains the same values as the first partition except
    // for tinyint_part partition column
    executeQuery(hiveDriver,
      "ALTER TABLE readtest_" + format + " ADD PARTITION ( " +
        // There is a regression in Hive 1.2.1 in binary type partition columns. Disable for now.
        // "  binary_part='binary', " +
        "  boolean_part='true', " +
        "  tinyint_part='65', " +
        "  decimal0_part='36.9', " +
        "  decimal9_part='36.9', " +
        "  decimal18_part='3289379872.945645', " +
        "  decimal28_part='39579334534534.35345', " +
        "  decimal38_part='363945093845093890.9', " +
        "  double_part='8.345', " +
        "  float_part='4.67', " +
        "  int_part='123456', " +
        "  bigint_part='234235', " +
        "  smallint_part='3455', " +
        "  string_part='string', " +
        "  varchar_part='varchar', " +
        "  timestamp_part='2013-07-05 17:01:00', " +
        "  date_part='2013-07-05', " +
        "  char_part='char')"
    );
  }

  private String generateTimestampsDataFile() throws Exception {
    File file = getTempFile();

    PrintWriter printWriter = new PrintWriter(file);
    int ROWS = 35000;
    for (int i = 0; i < ROWS; ++i) {
      printWriter.println("2013-07-05 17:01:" + Integer.toString(i % 60));
    }
    printWriter.close();

    return file.getPath();
  }

  private String generateDoubleDataFile() throws Exception {
    File file = getTempFile();

    PrintWriter printWriter = new PrintWriter(file);
    int ROWS = 35000;
    printWriter.println(Double.toString(1));
    for (int i = 0; i < ROWS; ++i) {
      printWriter.println(Double.toString(9_223_372_036_854_700_000L + i));
    }
    printWriter.close();

    return file.getPath();
  }

  private String generateAllTypesDataFile() throws Exception {
    File file = getTempFile();

    PrintWriter printWriter = new PrintWriter(file);
    printWriter.println("YmluYXJ5ZmllbGQ=,false,34,65.99,2347.923,2758725827.9999,29375892739852.7689," +
      "89853749534593985.7834783,8.345,4.67,123456,234235,3455,stringfield,varcharfield," +
      "2013-07-05 17:01:00,2013-07-05,charfield");
    printWriter.println(",,,,,,,,,,,,,,,,");
    printWriter.close();

    return file.getPath();
  }

  private String generateComplexTypesDataFile() throws Exception {
    File file = getTempFile();
    PrintWriter printWriter = new PrintWriter(file);
    int ROW_COUNT = 5000;
    for (int row = 0; row < ROW_COUNT; ++row) {
      String rownum = Integer.toString(row);
      String listvalue = Integer.toString(row) + "," + Integer.toString(row + 1) + "," +
        Integer.toString(row + 2) + "," + Integer.toString(row + 3) + "," +
        Integer.toString(row + 4);
      String structvalue = "name" + Integer.toString(row) + "," + Integer.toString(row);
      String structlistvalue = "type" + Integer.toString(row) + "," +
        "elem" + Integer.toString(row) + "," + "elem" + Integer.toString(row + 1) +
        "elem" + Integer.toString(row + 2) + "elem" + Integer.toString(row + 3) +
        "elem" + Integer.toString(row + 4);
      String listSturctValue = "name" + Integer.toString(row) + ":" + Integer.toString(row) +
        "," + "name" + Integer.toString(row + 1) + ":" + Integer.toString(row + 1);
      String mapValue = "name" + Integer.toString(row) + ":" + Integer.toString(row) +
        "," + "name" + Integer.toString(row + 1) + ":" + Integer.toString(row + 1) +
        ((row % 2 == 0) ? ("," + "name" + Integer.toString(row + 2) + ":" + Integer.toString(row + 2)) : "");
      String mapStructValue = "key" + Integer.toString(row) + ":" + "struct" + Integer.toString(row);

      printWriter.println(rownum + "\t" + listvalue + "\t" +
        structvalue + "\t" + structlistvalue + "\t" + listSturctValue + "\t" +
        mapValue + "\t" + mapStructValue);
    }
    printWriter.close();

    return file.getPath();
  }

  private String generateListTypesDataFile() throws Exception {
    File file = getTempFile();

    PrintWriter printWriter = new PrintWriter(file);
    int ROW_COUNT = 5000;
    for (int row = 0; row < ROW_COUNT; ++row) {
      String rownum = Integer.toString(row);
      String tinyint_field = Integer.toString(1) + "," +
        Integer.toString(2) + "," + Integer.toString(3) + "," +
        Integer.toString(4) + "," + Integer.toString(5);
      String smallint_field = Integer.toString(1024) + "," +
        Integer.toString(1025) + "," + Integer.toString(1026);
      String int_field = Integer.toString(row) + "," +
        Integer.toString(row + 1) + "," + Integer.toString(row + 2);
      String bigint_field = Long.toString(90000000000L) + "," +
        Long.toString(90000000001L) + "," + Long.toString(90000000002L);
      String float_field = Float.toString(row) + "," +
        Float.toString(row + 1) + "," + Float.toString(row + 2);
      String double_field = Double.toString(row) + "," +
        Double.toString(row + 1) + "," + Double.toString(row + 2) + "," +
        Double.toString(row + 3) + "," + Double.toString(row + 4);
      String timestamp_field = "2019-02-18" + "," + "2019-02-19" + "," + "2019-02-20";
      String date_field = "2019-02-18" + "," + "2019-02-19" + "," + "2019-02-20";
      String string_field = Integer.toString(row) + "," +
        Integer.toString(row + 1) + "," + Integer.toString(row + 2) + "," +
        Integer.toString(row + 3) + "," + Integer.toString(row + 4);
      String boolean_field = "true" + "," + "false" + "," + "true";
      String binary_field = "000" + "," + "001" + "," + "010";

      if (row % 7 == 0) {
        printWriter.println(rownum + "\t" + "\\N" +
          "\t" + "\\N" + "\t" + "\\N" + "\t" + "\\N" + "\t" + "\\N" +
          "\t" + "\\N" + "\t" + "\\N" + "\t" + "\\N" + "\t" + "\\N" +
          "\t" + "\\N" + "\t" + "\\N");
      } else {
        printWriter.println(rownum + "\t" + tinyint_field + "\t" +
          smallint_field + "\t" + int_field + "\t" + bigint_field + "\t" + float_field +
          "\t" + double_field + "\t" + timestamp_field + "\t" + date_field + "\t" +
          string_field + "\t" + boolean_field + "\t" + binary_field);
      }
    }
    printWriter.close();

    return file.getPath();
  }

  private String generateUnionIntTypesDataFile() throws Exception {
    File file = getTempFile();

    PrintWriter printWriter = new PrintWriter(file);
    int ROW_COUNT = 5000;
    for (int row = 0; row < ROW_COUNT; row = row + 3) {
      String rownum = Integer.toString(row);
      String int_field = Integer.toString(row);
      printWriter.println(rownum + "\t" + int_field);
    }
    printWriter.close();

    return file.getPath();
  }

  private String generateUnionDoubleTypesDataFile() throws Exception {
    File file = getTempFile();

    PrintWriter printWriter = new PrintWriter(file);
    int ROW_COUNT = 5000;
    for (int row = 1; row < ROW_COUNT; row = row + 3) {
      String rownum = Integer.toString(row);
      String double_field = Integer.toString(row) + "." + Integer.toString(row);
      printWriter.println(rownum + "\t" + double_field);
    }
    printWriter.close();
    return file.getPath();
  }

  private String generateUnionStringTypesDataFile() throws Exception {
    File file = getTempFile();

    PrintWriter printWriter = new PrintWriter(file);
    int ROW_COUNT = 5000;
    for (int row = 2; row < ROW_COUNT; row = row + 3) {
      String rownum = Integer.toString(row);
      String string_field = Integer.toString(row) + "." + Integer.toString(row);
      printWriter.println(rownum + "\t" + string_field);
    }
    printWriter.close();
    return file.getPath();
  }

  private String generateMapTypesDataFile() throws Exception {
    File file = getTempFile();

    PrintWriter printWriter = new PrintWriter(file);
    int ROW_COUNT = 5000;
    for (int row = 0; row < ROW_COUNT; ++row) {
      String rownum = Integer.toString(row);
      String mapfield = Integer.toString(row) + ":" + Integer.toString(row);

      printWriter.println(rownum + "\t" + mapfield);
    }
    printWriter.close();

    return file.getPath();
  }

  private String generateStructTypesDataFile() throws Exception {
    File file = getTempFile();

    PrintWriter printWriter = new PrintWriter(file);
    int ROW_COUNT = 5000;
    for (int row = 0; row < ROW_COUNT; ++row) {
      String rownum = Integer.toString(row);
      String tinyint_field = Integer.toString(1);
      String smallint_field = Integer.toString(1024);
      String int_field = Integer.toString(row);
      String bigint_field = Long.toString(90000000000L);
      String float_field = Float.toString(row);
      String double_field = Double.toString(row);
      String string_field = Integer.toString(row);

      printWriter.println(rownum + "\t" + tinyint_field + "," +
        smallint_field + "," + int_field + "," + bigint_field + "," +
        float_field + "," + double_field + "," + string_field);
    }
    printWriter.close();

    return file.getPath();
  }

  private String createTableWithHeaderFooterProperties(String tableName, String format, String headerValue, String footerValue) {
    return String.format("create table %s (key int, value string) stored as %s tblproperties('%s'='%s', '%s'='%s')",
      tableName, format, serdeConstants.HEADER_COUNT, headerValue, serdeConstants.FOOTER_COUNT, footerValue);
  }

  private String generateTestDataWithHeadersAndFooters(String tableName, int rowCount, int headerLines, int footerLines) {
    StringBuilder sb = new StringBuilder();
    sb.append("insert into table ").append(tableName).append(" (key, value) values ");
    int length = sb.length();
    sb.append(StringUtils.repeat("('key_header', 'value_header')", ",", headerLines));
    for (int i = 1; i <= rowCount; i++) {
      sb.append(",(").append(i).append(",").append("'key_").append(i).append("')");
    }
    if (headerLines <= 0) {
      sb.deleteCharAt(length);
    }
    sb.append(StringUtils.repeat(",('key_footer', 'value_footer')", footerLines));

    return sb.toString();
  }

  private void createMixedPartitionTypeTable(Driver hiveDriver, String table) throws Exception {
    String createTable = "CREATE TABLE " + table + " (col1 int) PARTITIONED BY (col2 int)";
    String insertTextRow1 = "insert into " + table + " partition (col2=1) values(1)";
    String insertTextRow2 = "insert into " + table + " partition (col2=2) values(2)";
    String alterPartitionType = "alter table " + table + " set fileformat " +
      "inputformat 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' " +
      "outputformat 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' " +
      "serde 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'";
    String insertParquetRow1 = "insert into " + table + " partition (col2=3) values(3)";

    executeQuery(hiveDriver, createTable);
    executeQuery(hiveDriver, insertTextRow1);
    executeQuery(hiveDriver, insertTextRow2);
    executeQuery(hiveDriver, alterPartitionType);
    executeQuery(hiveDriver, insertParquetRow1);

    String mixTableWithDecimalField = table + "_with_decimal";

    createTable = "CREATE TABLE " + mixTableWithDecimalField + " (col1 decimal(5,1)) PARTITIONED BY (col2 int)";
    insertTextRow1 = "insert into " + mixTableWithDecimalField + " partition (col2=1) values(1234.5)";
    insertTextRow2 = "insert into " + mixTableWithDecimalField + " partition (col2=2) values(4.5)";
    alterPartitionType = "alter table " + mixTableWithDecimalField + " set fileformat " +
      "inputformat 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' " +
      "outputformat 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' " +
      "serde 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'";
    insertParquetRow1 = "insert into " + mixTableWithDecimalField + " partition (col2=3) values(1234.5)";
    String alterColumn = "alter table " + mixTableWithDecimalField + " change column col1 col1 decimal(3,2)";
    String insertParquetRow2 = "insert into " + mixTableWithDecimalField + " partition (col2=4) values(3.4)";

    executeQuery(hiveDriver, createTable);
    executeQuery(hiveDriver, insertTextRow1);
    executeQuery(hiveDriver, insertTextRow2);
    executeQuery(hiveDriver, alterPartitionType);
    executeQuery(hiveDriver, insertParquetRow1);
    executeQuery(hiveDriver, alterColumn);
    executeQuery(hiveDriver, insertParquetRow2);
  }

  private void createTextTableWithDateColumn(Driver driver, String table) {
    String createCmd = "CREATE TABLE " + table + " (int_col int, date_col date)";
    String insertCmd1 = "INSERT INTO " + table + " VALUES(1, '0001-01-01')";
    String insertCmd2 = "INSERT INTO " + table + " VALUES(2, '1299-01-01')";
    String insertCmd3 = "INSERT INTO " + table + " VALUES(3, '1499-01-01')";
    String insertCmd4 = "INSERT INTO " + table + " VALUES(4, '1582-01-01')";
    String insertCmd5 = "INSERT INTO " + table + " VALUES(5, '1699-01-01')";
    executeQuery(driver, createCmd);
    executeQuery(driver, insertCmd1);
    executeQuery(driver, insertCmd2);
    executeQuery(driver, insertCmd3);
    executeQuery(driver, insertCmd4);
    executeQuery(driver, insertCmd5);
  }

  private void createOrcTableWithDateColumn(Driver driver, String orcTable, String textTable) {
    String createCmd = "CREATE TABLE " + orcTable + " (int_col int, date_col date) stored as orc";
    String insertCmd = "INSERT INTO orc_date SELECT * FROM " + textTable;
    executeQuery(driver, createCmd);
    executeQuery(driver, insertCmd);
  }

  private void createOrcTableWithAncientDates(Driver hiveDriver, String tableName) {
    String createCmd = "CREATE TABLE " + tableName + " (date_col date)" +
      "ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'" +
      "STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'" +
      "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'";

    String insertCmd1 = "INSERT INTO " + tableName + " values('0001-01-01')";
    String insertCmd2 = "INSERT INTO " + tableName + " values('0110-05-12')";
    String insertCmd3 = "INSERT INTO " + tableName + " values('1105-10-06')";
    String insertCmd4 = "INSERT INTO " + tableName + " values('1301-01-01')";
    String insertCmd5 = "INSERT INTO " + tableName + " values('1476-05-31')";
    String insertCmd6 = "INSERT INTO " + tableName + " values('1582-10-01')";
    String insertCmd7 = "INSERT INTO " + tableName + " values('1790-07-17')";
    String insertCmd8 = "INSERT INTO " + tableName + " values('2015-01-01')";

    executeQuery(hiveDriver, createCmd);
    executeQuery(hiveDriver, insertCmd1);
    executeQuery(hiveDriver, insertCmd2);
    executeQuery(hiveDriver, insertCmd3);
    executeQuery(hiveDriver, insertCmd4);
    executeQuery(hiveDriver, insertCmd5);
    executeQuery(hiveDriver, insertCmd6);
    executeQuery(hiveDriver, insertCmd7);
    executeQuery(hiveDriver, insertCmd8);
  }

  private void createDecimalParquetTableWithDecimalColumnMismatch(Driver hiveDriver, String table) throws Exception {
    String createTable = "CREATE TABLE " + table + " (int_col int, name varchar(4)) stored as parquet";
    String insert1 = "INSERT INTO " + table + " VALUES(1, '0.12')";
    String insert2 = "INSERT INTO " + table + " VALUES(2, '0.11')";
    String insert3 = "INSERT INTO " + table + " VALUES(3, '0.13')";
    String insert4 = "INSERT INTO " + table + " VALUES(4, '0.10')";
    String ext_table = "CREATE EXTERNAL TABLE IF NOT EXISTS " + table +
      "_ext (int_col int, name decimal(3,2)) STORED AS PARQUET LOCATION 'file://" + this.getWhDir() + "/" + table + "'";
    executeQuery(hiveDriver, createTable);
    executeQuery(hiveDriver, insert1);
    executeQuery(hiveDriver, insert2);
    executeQuery(hiveDriver, insert3);
    executeQuery(hiveDriver, insert4);
    executeQuery(hiveDriver, ext_table);
  }

  private void createTableWithList(Driver hiveDriver, String table) {
    String createTable = "create table " + table + " (str_list array<string>, int_list array<int>, bool_list array<boolean>) stored as parquet";
    String insert = "INSERT INTO table " + table + " SELECT array('str1', 'str2', 'str3', 'str4') as str_list, array(1,2,3,4) as int_list, array(true, false) as bool_list";
    executeQuery(hiveDriver, createTable);
    executeQuery(hiveDriver, insert);
  }

  private void createTableWithStruct(Driver hiveDriver, String table) {
    String createTable = "create table " + table + " (mixed_struct struct<salary:double, country:string, citizen:boolean, phone:int>) stored as parquet";
    String insert = "insert into " + table + " select named_struct(\"salary\",cast(100.1 as double), \"country\",\"IN\", \"citizen\",false , \"phone\", 12341234) as mixed_struct";
    executeQuery(hiveDriver, createTable);
    executeQuery(hiveDriver, insert);
  }

  private void createTableNestedList(Driver hiveDriver, String table) {
    String createTable = "create table complex_types_nested_list " +
      "(str_list array<array<array<string>>>, " +
      "int_list array<array<array<int>>>, " +
      "bool_list array<array<boolean>>, " +
      "struct_list struct<directarr:array<string>, nestedarr:array<array<string>>>) stored as parquet";
    String insert = "insert into " + table + " select " +
      "array(array(array('str1', 'str2', 'str3', 'str4'))) as str_list, " +
      "array(array(array(1,2,3,4))) as int_list, " +
      "array(array(true, false)) as bool_list, " +
      "named_struct(\"directarr\", array('struct_str1', 'struct_str2'), \"nestedarr\", array(array('nested_struct_str1', 'nested_struct_str1'))) as struct_list";
    executeQuery(hiveDriver, createTable);
    executeQuery(hiveDriver, insert);
  }

  private void createTableNestedStruct(Driver hiveDriver, String table) {
    String createTable = "create table " + table + " (" +
      "struct_indirect struct<name:struct<firstname:string, lastname:string>, " +
                             "address:struct<city:struct<country:string, district:string>, name:string>>, " +
      "list_indirect array<struct<arrstr:string>>) stored as parquet";
    String insert = "insert into " + table + " select " +
      "named_struct(\"name\", named_struct(\"firstname\", \"john\", \"lastname\", \"doe\"), " +
      "             \"address\", named_struct(\"city\", named_struct(\"country\", \"IN\", \"district\", \"Hyderabad\"), \"name\", \"Hyderabad\")) as struct_indirect, " +
      "array(named_struct(\"arrstr\", \"arrstr1\")) as list_indirect";
    executeQuery(hiveDriver, createTable);
    executeQuery(hiveDriver, insert);
  }

  private void createTableWithUnsupportedComplexTypes(Driver hiveDriver, String table) {
    String createTable = "create table " + table + " (name_col string, map_col map<string, string>) stored as parquet";
    String insert = "insert into " + table + " select \"name\", map(\"k1\", \"v1\") as map_col";

    executeQuery(hiveDriver, createTable);
    executeQuery(hiveDriver, insert);
  }

  private void createTableNestedWithUnsupportedComplexTypes(Driver hiveDriver, String table) {
    String createTable = "create table " + table + " (name_col string, " +
      "map_list_col array<map<string, string>>, " +
      "map_struct_col struct<map_col:map<string, string>>) stored as parquet";
    String insert = "insert into " + table + " select \"name\", " +
      "array(map(\"k1\", \"v1\")) as map_list_col, " +
      "named_struct(\"map_col\", map(\"sk1\", \"sv1\")) as map_struct_col";

    executeQuery(hiveDriver, createTable);
    executeQuery(hiveDriver, insert);
  }

  private void createTableForUnsupportedMapTypeInParquet(Driver hiveDriver, String table) {
    String createTable = "create table " + table + " (col1 map<int, int>, " +
      " col2 array<map<int, int>>, " +
      " col3 array<array<array<map<int, int>>>>, " +
      " col4 struct<f1: map<int, int>>, " +
      " col5 struct<f1: array<map<int, int>>>, " +
      " col6 struct<f1: array<array<array<map<int, int>>>>>, " +
      " col7 struct<f1: struct<f2: array<array<array<map<int, int>>>>>>, " +
      " col8 struct<crossdomainid: string, deviceuseragentid: string, sourcelist: map<string, string>>)" +
      " stored as parquet";
    String insert = "insert into " + table + " select map(1,1), " +
      " array(map(1,1)), " +
      " array(array(array(map(1,1)))), " +
      " named_struct('f1',map(1,1)), " +
      " named_struct('f1',array(map(1,1))), " +
      " named_struct('f1',array(array(array(map(1,1))))), " +
      " named_struct('f1',named_struct('f2',array(array(array(map(1,1)))))), " +
      " named_struct('crossdomainid','abc','deviceuseragentid','edf','sourcelist',map('a','b'))";

    executeQuery(hiveDriver, createTable);
    executeQuery(hiveDriver, insert);
  }

  private void createTableMixedCaseColumnsWithComplexTypes(Driver hiveDriver, String table) throws IOException {
    final File casetestDir = new File(BaseTestQuery.getTempDir("casetest"));
    casetestDir.mkdirs();
    final File parquetFile = new File(casetestDir, "casetestdata.parquet");
    parquetFile.deleteOnExit();
    casetestDir.deleteOnExit();
    final URL url = Resources.getResource("casetestdata.parquet");
    Files.write(Paths.get(parquetFile.toURI()), Resources.toByteArray(url));

    String caseTestTable = "create table " + table +
      " (upcase_col string, " +
      "lwcase_col string, " +
      "mixedcase_col string, " +
      "upcase_struct_col struct<upcase_sub1:string, lowcase_sub2:string, mixedcase_sub3:string>, " +
      "lwcase_struct_col struct<upcase_sub1:string, lowcase_sub2:string, mixedcase_sub3:string>) " +
      "stored as parquet location 'file://" + parquetFile.getParent() + "'";
    executeQuery(hiveDriver, caseTestTable);
  }

  private void createTableFlagTestColumns(Driver hiveDriver, String table) {
    String createTable = "create table " + table + " (primary_col string, complex_col struct<subcol:string>) stored as parquet";
    String insertTable = "insert into " + table + " select 'primary_col_val', named_struct('subcol', 'subcol_val')";
    executeQuery(hiveDriver, createTable);
    executeQuery(hiveDriver, insertTable);
  }

  private void createTableForPartitionValueFormatException(Driver hiveDriver, String table) throws IOException {
    String createTextTable = "create table " + table + "_text" + " (col1 int, col2 double)";
    String insertTextTable = "insert into " + table + "_text"  + " values(1, -0.18)";

    executeQuery(hiveDriver, createTextTable);
    executeQuery(hiveDriver, insertTextTable);

    String createOrcTable = "create table " + table + "_orc" + " (col1 int)" + " partitioned by (col2 bigint) stored as orc";
    String insertOrcTable = "insert into " + table + "_orc"  + " partition(col2)  select * from " + table + "_text";

    executeQuery(hiveDriver, createOrcTable);
    executeQuery(hiveDriver, insertOrcTable);
  }

  private void createParquetStructExtraField(final Driver hiveDriver) throws Exception {
    final File structWithExtraField = new File(BaseTestQuery.getTempDir("struct_extra_test"));
    structWithExtraField.mkdirs();
    final URL structWithExtraFieldParquetUrl = Resources.getResource("struct_extra_test.parquet");
    if (structWithExtraFieldParquetUrl == null) {
      throw new IOException(String.format("Unable to find path %s.", "struct_extra_test.parquet"));
    }

    // parquet file has following columns with one valid row
    // col1 int, col2 array<struct<f1:int, f2:struct<f_f_1>>> with value 1, [{f1:1, f2:{2}}]
    final File structWithExtraFieldFile = new File(structWithExtraField, "struct_extra_test.parquet");
    structWithExtraFieldFile.deleteOnExit();
    structWithExtraField.deleteOnExit();
    Files.write(Paths.get(structWithExtraFieldFile.toURI()), Resources.toByteArray(structWithExtraFieldParquetUrl));

    final String structExtraFieldTest = "create external table struct_extra_test_ext(" +
      "col1 int, col2 array<struct<f1:int, f2:struct<f_f_1:int, f_f_2:int>>>)" +
      "stored as parquet location '" + structWithExtraFieldFile.getParent() + "'";
    executeQuery(hiveDriver, structExtraFieldTest);
  }

  private void createNullORCStructTable(final Driver hiveDriver) throws Exception {
    final File structWithNullsOrc = new File(BaseTestQuery.getTempDir("struct_with_nulls_orc_test"));
    structWithNullsOrc.mkdirs();
    final URL structWithNullsOrcUrl = Resources.getResource("struct_with_nulls.orc");
    if (structWithNullsOrcUrl == null) {
      throw new IOException(String.format("Unable to find path %s.", "struct_with_nulls.orc"));
    }

    final File structWithNullsOrcFile = new File(structWithNullsOrc, "struct_with_nulls.orc");
    structWithNullsOrcFile.deleteOnExit();
    structWithNullsOrc.deleteOnExit();
    Files.write(Paths.get(structWithNullsOrcFile.toURI()), Resources.toByteArray(structWithNullsOrcUrl));

    executeQuery(hiveDriver,
      "CREATE EXTERNAL TABLE orcnullstruct (" +
        " id int," +
        " emp_name string," +
        " city struct<f1:string>, " +
        " prm_borr struct<FRST_NM:string," +
        "LAST_NM:string,MID_NM:string,APPLNT_TYPE_CD:string," +
        "EMAIL_ADDR_TXT:string,BIRTH_DT:date,AGE_NBR:int," +
        "APPLNT_ID:int,POSITN_NBR:int> " +
        ") STORED AS ORC location '" + structWithNullsOrcFile.getParent() + "'");
  }

  private void createORCPartitionSchemaTestTable(final Driver hiveDriver) {
    executeQuery(hiveDriver, "create table orc_part_test (col1 decimal(5,3)) partitioned by (partcol1 int) stored as orc");
    executeQuery(hiveDriver, "insert into orc_part_test partition (partcol1=1) values (12.345)");
    executeQuery(hiveDriver, "alter table orc_part_test change col1 col1 decimal(3,1)");
    executeQuery(hiveDriver, "insert into orc_part_test partition (partcol1=1) values (12.9)");
    executeQuery(hiveDriver, "insert into orc_part_test partition (partcol1=2) values (23.9)");
    executeQuery(hiveDriver, "alter table orc_part_test change col1 col1 decimal(5,3)");
    executeQuery(hiveDriver, "insert into orc_part_test partition (partcol1=2) values (23.987)");
  }

  private void createParquetStructWithCoercionTestTable(final Driver hiveDriver) throws Exception {
    final File structDir = new File(BaseTestQuery.getTempDir("struct_coercion_filter"));
    structDir.mkdirs();
    final URL structUrl = Resources.getResource("multifieldstruct.parquet");
    if (structUrl == null) {
      throw new IOException(String.format("Unable to find path %s.", "multifieldstruct.parquet"));
    }

    // parquet file schema:
    // f0 struct<f1: int, f2: float, f3: decimal (10, 5), f4: string>, f1 int
    final File structFile = new File(structDir, "multifieldstruct.parquet");
    structFile.deleteOnExit();
    structDir.deleteOnExit();
    Files.write(Paths.get(structFile.toURI()), Resources.toByteArray(structUrl));

    final String structExtraFieldTest = "create external table struct_coercion_filter(" +
      "f0 struct<f1:bigint, f2:double, f3: decimal(7, 2), f4: varchar(2)>, f1 bigint) " +
      "stored as parquet location '" + structFile.getParent() + "'";
    executeQuery(hiveDriver, structExtraFieldTest);
  }
}
