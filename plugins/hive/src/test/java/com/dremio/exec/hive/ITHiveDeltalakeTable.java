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
package com.dremio.exec.hive;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.exec.ExecConstants;

/**
 * Test connection to external Deltalake Table
 */
@Ignore("DX-60788")
public class ITHiveDeltalakeTable extends LazyDataGeneratingHiveTestBase {

  @BeforeClass
  public static void setup() {
    setSystemOption(ExecConstants.ENABLE_DELTALAKE_HIVE_SUPPORT, "true");
  }

  @AfterClass
  public static void cleanup() {
    setSystemOption(ExecConstants.ENABLE_DELTALAKE_HIVE_SUPPORT,
      ExecConstants.ENABLE_DELTALAKE_HIVE_SUPPORT.getDefault().getBoolVal().toString());
  }

  private static String resolveResource(String path) throws Exception {
    File f = new File("src/test/resources/" + path);
    return f.getAbsolutePath();
  }

  private static void executeDDL(String query) throws Exception {
    final HiveConf conf = dataGenerator.newHiveConf();
    String jarPath =  resolveResource("deltalake/delta-hive-assembly_2.13-0.5.0.jar");
    conf.setAuxJars(jarPath);
    try (HiveTestUtilities.DriverState driverState = new HiveTestUtilities.DriverState(conf)) {
      driverState.sessionState.loadAuxJars();
      HiveTestUtilities.executeQuery(driverState.driver, query);
    }
  }

  private static AutoCloseable withDeltaTable(String name, String schema, String path) throws Exception {
    String tablePath = resolveResource(path);
    String createQuery = String.format(
      "CREATE EXTERNAL TABLE %s %s STORED BY 'io.delta.hive.DeltaStorageHandler' LOCATION '%s'",
      name, schema, tablePath);
    executeDDL(createQuery);

    return () -> {
      String dropQuery = String.format("DROP TABLE IF EXISTS %s", name);
      executeDDL(dropQuery);
    };
  }

  @Test
  public void testSelectQuery() throws Exception {
    try (AutoCloseable ac = withDeltaTable("delta_extra", "(col1 INT, col2 STRING, extraCol INT)", "deltalake/delta_extra")) {
      String query = "SELECT * FROM hive.delta_extra";
      testBuilder().sqlQuery(query)
        .unOrdered()
        .baselineColumns("col1", "col2", "extraCol")
        .baselineValues(1, "abc", 2)
        .baselineValues(3, "xyz", 4)
        .baselineValues(5, "lmn", 6)
        .go();
    }
  }

  @Test
  public void testSelectQueryFailure() throws Exception {
    try (AutoCloseable ac = withSystemOption(ExecConstants.ENABLE_DELTALAKE_HIVE_SUPPORT, false);
         AutoCloseable ac2 = withDeltaTable("delta_extra", "(col1 INT, col2 STRING, extraCol INT)", "deltalake/delta_extra")) {
      String query = "SELECT * FROM hive.delta_extra";
      assertThatThrownBy(() -> test(query))
        .isInstanceOf(UserRemoteException.class)
        .hasMessageContaining("DATA_READ ERROR: Error in loading storage handler.io.delta.hive.DeltaStorageHandler");
    }
  }

  @Test
  public void testSelectPartitionsQuery() throws Exception {
    try (AutoCloseable ac = withDeltaTable("delta_parts", "(number INT, partitionKey INT)", "deltalake/delta_parts")) {
      String query = "SELECT * FROM hive.delta_parts";
      testBuilder().sqlQuery(query)
        .unOrdered()
        .baselineColumns("number", "partitionKey")
        .baselineValues(1, 10)
        .baselineValues(2, 20)
        .go();
    }
  }
}
