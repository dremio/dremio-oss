/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.serdeConstants;

import com.dremio.BaseTestQuery;
import com.dremio.exec.catalog.CatalogServiceImpl;
import com.dremio.exec.catalog.ManagedStoragePlugin;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.impersonation.hive.BaseTestHiveImpersonation;
import com.dremio.exec.store.CatalogService;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;

public class HiveTestDataGenerator {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HiveTestDataGenerator.class);

  private static final String HIVE_TEST_PLUGIN_NAME = "hive";
  private static HiveTestDataGenerator instance;

  private final String dbDir;
  private final String whDir;
  private final Map<String, String> config;

  public static synchronized HiveTestDataGenerator getInstance() throws Exception {
    if (instance == null) {
      final String dbDir = getTempDir("metastore_db");
      final String whDir = getTempDir("warehouse");

      instance = new HiveTestDataGenerator(dbDir, whDir);
      instance.generateTestData();
    }

    return instance;
  }

  private HiveTestDataGenerator(final String dbDir, final String whDir) {
    this.dbDir = dbDir;
    this.whDir = whDir;

    config = Maps.newHashMap();
    config.put("hive.metastore.uris", "");
    config.put("javax.jdo.option.ConnectionURL", String.format("jdbc:derby:;databaseName=%s;create=true", dbDir));
    config.put("hive.metastore.warehouse.dir", whDir);
    config.put(FileSystem.FS_DEFAULT_NAME_KEY, "file:///");
  }

  public String getWhDir() {
    return whDir;
  }

  /**
   * Add Hive test storage plugin to the given plugin registry.
   * @throws Exception
   */
  public void addHiveTestPlugin(final CatalogService pluginRegistry) throws Exception {
    SourceConfig sc = new SourceConfig();
    sc.setName(HIVE_TEST_PLUGIN_NAME);
    HiveStoragePluginConfig conf = BaseTestHiveImpersonation.createHiveStoragePlugin(config);
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
    HiveStoragePlugin storagePlugin = (HiveStoragePlugin) pluginRegistry.getSource(HIVE_TEST_PLUGIN_NAME);
    if (storagePlugin == null) {
      throw new Exception(
          "Hive test storage plugin doesn't exist. Add a plugin using addHiveTestPlugin()");
    }

    ManagedStoragePlugin msp = ((CatalogServiceImpl) pluginRegistry).getManagedSource(HIVE_TEST_PLUGIN_NAME);
    SourceConfig newSC = msp.getId().getClonedConfig();
    HiveStoragePluginConfig conf = msp.getId().<HiveStoragePluginConfig>getConnectionConf().clone();

    List<Property> updated = new ArrayList<>();
    for(Entry<String, String> prop : configOverride.entrySet()) {
      updated.add(new Property(prop.getKey(), prop.getValue()));
    }

    for(Property p : conf.propertyList) {
      if(!configOverride.containsKey(p.name)) {
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
  public void deleteHiveTestPlugin(final CatalogService pluginRegistry) {
    CatalogServiceImpl impl = (CatalogServiceImpl) pluginRegistry;
    ManagedStoragePlugin msp = impl.getManagedSource(HIVE_TEST_PLUGIN_NAME);
    impl.getSystemUserCatalog().deleteSource(msp.getId().getConfig());
  }

  public void executeDDL(String query) throws IOException {
    final HiveConf conf = newHiveConf();
    final SessionState ss = new SessionState(conf);
    try {
      SessionState.start(ss);
      final Driver hiveDriver = getHiveDriver(conf);
      executeQuery(hiveDriver, query);
    } finally {
      ss.close();
    }
  }

  private HiveConf newHiveConf() {
    HiveConf conf = new HiveConf(SessionState.class);

    conf.set(ConfVars.METASTORECONNECTURLKEY.varname, String.format("jdbc:derby:;databaseName=%s;create=true", dbDir));
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "file:///");
    conf.set(ConfVars.METASTOREWAREHOUSE.varname, whDir);
    conf.set("mapred.job.tracker", "local");
    conf.set(ConfVars.SCRATCHDIR.varname,  getTempDir("scratch_dir"));
    conf.set(ConfVars.LOCALSCRATCHDIR.varname, getTempDir("local_scratch_dir"));
    conf.set(ConfVars.DYNAMICPARTITIONINGMODE.varname, "nonstrict");
    conf.set(ConfVars.METASTORE_SCHEMA_VERIFICATION.varname, "false");
    conf.set(ConfVars.METASTORE_AUTO_CREATE_ALL.varname, "true");
    conf.set(ConfVars.HIVE_CBO_ENABLED.varname, "false");

    return conf;

  }

  private Driver getHiveDriver(HiveConf conf) {
    return new Driver(conf);
  }

  private void generateTestData() throws Exception {
    HiveConf conf = newHiveConf();
    SessionState ss = new SessionState(conf);
    SessionState.start(ss);
    Driver hiveDriver = getHiveDriver(conf);

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
    Files.copy(Paths.get(url.toURI()), Paths.get(file.toURI()));

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

    final String[][] typeconversinoTables = {
      {"tinyint", "", "90"},
      {"smallint", "", "90"},
      {"int", "", "90"},
      {"bigint", "", "90"},
      {"float", "", "90.0"},
      {"double", "", "90.0"},
      {"decimal", "", "90"},
      {"string", "", "90"},
      {"varchar", "(1024)", "90"},
      {"timestamp", "", "'2019-03-14 11:17:31.119021'"},
      {"date", "", "'2019-03-14'"}
    };
    for (int i=0; i<typeconversinoTables.length; ++i) {
      createTypeConversionSourceTable(hiveDriver, typeconversinoTables[i][0],
        typeconversinoTables[i][1], typeconversinoTables[i][2]);
    }
    final String[][] typeconversinoDestTables = {
      //tinyint
      {"tinyint", "", "smallint", ""},
      {"tinyint", "", "int", ""},
      {"tinyint", "", "bigint", ""},
      {"tinyint", "", "float", ""},
      {"tinyint", "", "double", ""},
      {"tinyint", "", "decimal", ""},
      {"tinyint", "", "string", ""},
      {"tinyint", "", "varchar", "(1024)"},
      //smallint
      {"smallint", "", "int", ""},
      {"smallint", "", "bigint", ""},
      {"smallint", "", "float", ""},
      {"smallint", "", "double", ""},
      {"smallint", "", "decimal", ""},
      {"smallint", "", "string", ""},
      {"smallint", "", "varchar", "(1024)"},
      //int
      {"int", "", "bigint", ""},
      {"int", "", "float", ""},
      {"int", "", "double", ""},
      {"int", "", "decimal", ""},
      {"int", "", "string", ""},
      {"int", "", "varchar", "(1024)"},
      //bigint
      {"bigint", "", "float", ""},
      {"bigint", "", "double", ""},
      {"bigint", "", "decimal", ""},
      {"bigint", "", "string", ""},
      {"bigint", "", "varchar", "(1024)"},
      //float
      {"float", "", "double", ""},
      {"float", "", "decimal", ""},
      {"float", "", "string", ""},
      {"float", "", "varchar", "(1024)"},
      //double
      {"double", "", "decimal", ""},
      {"double", "", "string", ""},
      {"double", "", "varchar", "(1024)"},
      //decimal
      {"decimal", "", "string", ""},
      {"decimal", "", "varchar", "(1024)"},
      //string
      {"string", "", "double", ""},
      {"string", "", "decimal", ""},
      {"string", "", "varchar", "(1024)"},
      //varchar
      {"varchar", "", "double", ""},
      {"varchar", "", "decimal", ""},
      {"varchar", "", "string", ""},
      //timestamp
      {"timestamp", "", "string", ""},
      {"timestamp", "", "varchar", "(1024)"},
      //date
      {"date", "", "string", ""},
      {"date", "", "varchar", "(1024)"}
    };
    for (int i=0; i<typeconversinoDestTables.length; ++i) {
      createTypeConversionDestinationTable(hiveDriver,
        typeconversinoDestTables[i][0],
        typeconversinoDestTables[i][1],
        typeconversinoDestTables[i][2],
        typeconversinoDestTables[i][3]);
    }

    createDecimalConversionTable(hiveDriver, "decimal_conversion_test_orc");
    createExtTableWithMoreColumnsThanOriginal(hiveDriver, "orc_more_columns");

    // create a Hive table that has columns with data types which are supported for reading in Dremio.
    createAllTypesTextTable(hiveDriver, "readtest");
    createAllTypesTable(hiveDriver, "parquet", "readtest");
    createAllTypesTable(hiveDriver, "orc", "readtest");

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
        "CREATE TABLE IF NOT EXISTS partition_pruning_test(a DATE, b TIMESTAMP) "+
        "partitioned by (c INT, d INT, e INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE");
    executeQuery(hiveDriver, "INSERT OVERWRITE TABLE partition_pruning_test PARTITION(c, d, e) " +
        "SELECT a, b, c, d, e FROM partition_pruning_test_loadtable");

    executeQuery(hiveDriver,
      "CREATE TABLE IF NOT EXISTS partition_with_few_schemas(a DATE, b TIMESTAMP) "+
        "partitioned by (c INT, d INT, e INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE");
    executeQuery(hiveDriver, "INSERT OVERWRITE TABLE partition_with_few_schemas PARTITION(c, d, e) " +
      "SELECT a, b, c, d, e FROM partition_pruning_test_loadtable");
    executeQuery(hiveDriver,"alter table partition_with_few_schemas partition(c=1, d=1, e=1) change a a1 INT");
    executeQuery(hiveDriver,"alter table partition_with_few_schemas partition(c=1, d=1, e=2) change a a1 INT");
    executeQuery(hiveDriver,"alter table partition_with_few_schemas partition(c=2, d=2, e=2) change a a1 INT");

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

    ss.close();
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
    for (int i=1; i<=rows; i++) {
      printWriter.println (String.format("%d, key_%d", i, i));
    }
    printWriter.close();

    return file.getPath();
  }

  private String generateTestDataFileForPartitionInput() throws Exception {
    final File file = getTempFile();

    PrintWriter printWriter = new PrintWriter(file);

    String partValues[] = {"1", "2", "null"};

    for(int c = 0; c < partValues.length; c++) {
      for(int d = 0; d < partValues.length; d++) {
        for(int e = 0; e < partValues.length; e++) {
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
  private void createTypeConversionSourceTable(final Driver hiveDriver, final String source, final String sourcetypeargs, final String value) throws Exception {
    String table = source + "_orc";
    String datatable = "CREATE TABLE IF NOT EXISTS " + table + " (col1 " + source + sourcetypeargs + ") STORED AS ORC";
    executeQuery(hiveDriver, datatable);
    String intsert_datatable = "INSERT INTO " + table + " VALUES (" + value + ")";
    executeQuery(hiveDriver, intsert_datatable);
  }
  private void createTypeConversionDestinationTable(final Driver hiveDriver, final String source,
                                                    final String sourcetypeargs,
                                                    final String destination,
                                                    final String desttypeargs) throws Exception {
    String table = source + "_to_" + destination + "_orc_ext";
    String sourcetable = source + "_orc";
    String ext_table = "CREATE EXTERNAL TABLE IF NOT EXISTS " + table +
      " (col1 "+ destination + desttypeargs +") STORED AS ORC LOCATION 'FILE://" + this.getWhDir() + "/" + sourcetable + "'";
    executeQuery(hiveDriver, ext_table);
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

  private void createDecimalConversionTable(Driver hiveDriver, String table) throws Exception {
    String datatable = "CREATE TABLE IF NOT EXISTS " + table + " (col1 decimal(30, 9), col2 decimal(23, 6), col3 decimal(16, 2)) STORED AS ORC";
    executeQuery(hiveDriver, datatable);
    String intsert_datatable = "INSERT INTO " + table + " VALUES (111111111111111111111.111111111, 22222222222222222.222222, 333.0)";
    executeQuery(hiveDriver, intsert_datatable);
    String exttable = table + "_ext";
    String ext_table = "CREATE EXTERNAL TABLE IF NOT EXISTS " + exttable +
      " (col1 decimal(25,2), col2 string, col3 varchar(32))" + "STORED AS ORC LOCATION 'FILE://" + this.getWhDir() + "/" + table + "'";
    executeQuery(hiveDriver, ext_table);
    String exttable2 = table + "_ext_2";
    String ext_table2 = "CREATE EXTERNAL TABLE IF NOT EXISTS " + exttable2 +
      " (col1 decimal(2,2), col2 decimal(16,7), col3 decimal(4,1))" + "STORED AS ORC LOCATION 'FILE://" + this.getWhDir() + "/" + table + "'";
    executeQuery(hiveDriver, ext_table2);

    String tablerev = table + "_rev";
    String datatablerev = "CREATE TABLE IF NOT EXISTS " + tablerev + " (col1 int, col2 string, col3 double) STORED AS ORC";
    executeQuery(hiveDriver, datatablerev);
    String intsert_datatable_rev = "INSERT INTO " + tablerev + " VALUES (1234, 1234567, 1234567.123)";
    executeQuery(hiveDriver, intsert_datatable_rev);
    String exttable_rev = tablerev + "_ext";
    String ext_table_rev = "CREATE EXTERNAL TABLE IF NOT EXISTS " + exttable_rev +
      " (col1 decimal(3,2), col2 decimal(5,2), col3 decimal(5,2))" + "STORED AS ORC LOCATION 'FILE://" + this.getWhDir() + "/" + tablerev + "'";
    executeQuery(hiveDriver, ext_table_rev);

  }

  private void createExtTableWithMoreColumnsThanOriginal(Driver hiveDriver, String table) throws Exception {
    String datatable = "CREATE TABLE IF NOT EXISTS " + table + " (col1 int, col2 int) STORED AS ORC";
    executeQuery(hiveDriver, datatable);
    String insert_datatable = "INSERT INTO " + table + " VALUES (1,2)";
    executeQuery(hiveDriver, insert_datatable);
    String exttable = table + "_ext";
    String ext_table = "CREATE EXTERNAL TABLE IF NOT EXISTS " + exttable +
      " (col1 int, col2 int, col3 int)" + "STORED AS ORC LOCATION 'FILE://" + this.getWhDir() + "/" + table + "'";
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

  private String createTableWithHeaderFooterProperties(String tableName, String format, String headerValue, String footerValue) {
    return String.format("create table %s (key int, value string) stored as %s tblproperties('%s'='%s', '%s'='%s')",
        tableName, format, serdeConstants.HEADER_COUNT, headerValue, serdeConstants.FOOTER_COUNT, footerValue);
  }

  private String generateTestDataWithHeadersAndFooters(String tableName, int rowCount, int headerLines, int footerLines) {
    StringBuilder sb = new StringBuilder();
    sb.append("insert into table ").append(tableName).append(" (key, value) values ");
    int length = sb.length();
    sb.append(StringUtils.repeat("('key_header', 'value_header')", ",", headerLines));
    for (int i  = 1; i <= rowCount; i++) {
        sb.append(",(").append(i).append(",").append("'key_").append(i).append("')");
    }
    if (headerLines <= 0) {
      sb.deleteCharAt(length);
    }
    sb.append(StringUtils.repeat(",('key_footer', 'value_footer')", footerLines));

    return sb.toString();
  }
}
