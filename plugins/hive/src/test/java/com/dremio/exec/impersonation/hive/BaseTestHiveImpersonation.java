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
package com.dremio.exec.impersonation.hive;

import static org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.shims.ShimLoader;

import com.dremio.TestBuilder;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.CatalogServiceImpl;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.dotfile.DotFileType;
import com.dremio.exec.impersonation.BaseTestImpersonation;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.hive.HiveStoragePluginConfig;
import com.dremio.service.namespace.source.proto.SourceConfig;

public class BaseTestHiveImpersonation extends BaseTestImpersonation {
  protected static final String hivePluginName = "hive";

  protected static HiveConf hiveConf;
  protected static String whDir;

  protected static String studentData;
  protected static String voterData;

  protected static final String studentDef = "CREATE TABLE %s.%s" +
      "(rownum int, name string, age int, gpa float, studentnum bigint) " +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE";
  protected static final String voterDef = "CREATE TABLE %s.%s" +
      "(voter_id int,name varchar(30), age tinyint, registration string, " +
      "contributions double,voterzone smallint,create_time timestamp) " +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE";
  protected static final String partitionStudentDef = "CREATE TABLE %s.%s" +
      "(rownum INT, name STRING, gpa FLOAT, studentnum BIGINT) " +
      "partitioned by (age INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE";

  protected static void prepHiveConfAndData() throws Exception {
    hiveConf = new HiveConf();

    // Configure metastore persistence db location on local filesystem
    final String dbUrl = String.format("jdbc:derby:;databaseName=%s;create=true",  getTempDir("metastore_db"));
    hiveConf.set(ConfVars.METASTORECONNECTURLKEY.varname, dbUrl);

    hiveConf.set(ConfVars.SCRATCHDIR.varname, "file:///" + getTempDir("scratch_dir"));
    hiveConf.set(ConfVars.LOCALSCRATCHDIR.varname, getTempDir("local_scratch_dir"));
    hiveConf.set(ConfVars.METASTORE_SCHEMA_VERIFICATION.varname, "false");
    hiveConf.set(ConfVars.METASTORE_AUTO_CREATE_ALL.varname, "true");
    hiveConf.set(ConfVars.HIVE_CBO_ENABLED.varname, "false");

    // Set MiniDFS conf in HiveConf
    hiveConf.set(FS_DEFAULT_NAME_KEY, dfsConf.get(FS_DEFAULT_NAME_KEY));

    whDir = hiveConf.get(ConfVars.METASTOREWAREHOUSE.varname);
    FileSystem.mkdirs(fs, new Path(whDir), new FsPermission((short) 0777));

    studentData = getPhysicalFileFromResource("student.txt");
    voterData = getPhysicalFileFromResource("voter.txt");
  }

  protected static void startHiveMetaStore() throws Exception {
    final int port = MetaStoreUtils.findFreePort();

    hiveConf.set(METASTOREURIS.varname, "thrift://localhost:" + port);

    MetaStoreUtils.startMetaStore(port, ShimLoader.getHadoopThriftAuthBridge(), hiveConf);
  }

  public static HiveStoragePluginConfig createHiveStoragePlugin(final Map<String, String> hiveConfig) throws Exception {
    HiveStoragePluginConfig pluginConfig = new HiveStoragePluginConfig();
    pluginConfig.hostname = "dummy";
    pluginConfig.propertyList = new ArrayList<>();
    for(Entry<String, String> e : hiveConfig.entrySet()) {
      pluginConfig.propertyList.add(new Property(e.getKey(), e.getValue()));
    }
    return pluginConfig;
  }

  protected static Path getWhPathForHiveObject(final String dbName, final String tableName) {
    if (dbName == null) {
      return new Path(whDir);
    }

    if (tableName == null) {
      return new Path(whDir, dbName + ".db");
    }

    return new Path(new Path(whDir, dbName + ".db"), tableName);
  }

  protected static void addHiveStoragePlugin(final Map<String, String> hiveConfig) throws Exception {
    SourceConfig sc = new SourceConfig();
    sc.setName(hivePluginName);
    HiveStoragePluginConfig conf = createHiveStoragePlugin(hiveConfig);
    sc.setType(conf.getType());
    sc.setConfig(conf.toBytesString());
    sc.setMetadataPolicy(CatalogService.DEFAULT_METADATA_POLICY);
    ((CatalogServiceImpl) getSabotContext().getCatalogService()).getSystemUserCatalog().createSource(sc);
  }

  protected void showTablesHelper(final String db, List<String> expectedTables) throws Exception {
    final String dbQualified = hivePluginName + "." + db;
    final TestBuilder testBuilder = testBuilder()
        .sqlQuery("SHOW TABLES IN " + dbQualified)
        .unOrdered()
        .baselineColumns("TABLE_SCHEMA", "TABLE_NAME");

    if (expectedTables.size() == 0) {
      testBuilder.expectsEmptyResultSet();
    } else {
      for (String tbl : expectedTables) {
        testBuilder.baselineValues(dbQualified, tbl);
      }
    }

    testBuilder.go();
  }

  protected static void createView(final String viewOwner, final String viewGroup, final String viewName,
                                 final String viewDef) throws Exception {
    updateClient(viewOwner);
    test(String.format("ALTER SESSION SET \"%s\"='%o';", ExecConstants.NEW_VIEW_DEFAULT_PERMS_KEY, (short) 0750));
    test("CREATE VIEW %s.%s AS %s", MINIDFS_STORAGE_PLUGIN_NAME, viewName, viewDef);
    final Path viewFilePath = new Path("/", viewName + DotFileType.VIEW.getEnding());
    fs.setOwner(viewFilePath, viewOwner, viewGroup);
  }

  public static void stopHiveMetaStore() throws Exception {
    // Unfortunately Hive metastore doesn't provide an API to shut it down. It will be exited as part of the test JVM
    // exit. As each metastore server instance is using its own resources and not sharing it with other metastore
    // server instances this should be ok.
  }
}
