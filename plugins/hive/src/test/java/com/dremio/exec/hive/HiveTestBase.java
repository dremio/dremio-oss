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

import java.sql.DriverManager;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;

import com.dremio.PlanTestBase;
import com.dremio.common.util.TestTools;
import com.dremio.exec.GuavaPatcherRunner;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.hive.HiveTestDataGenerator;

/**
 * Base class for Hive test. Takes care of generating and adding Hive test plugin before tests and deleting the
 * plugin after tests.
 */
@RunWith(GuavaPatcherRunner.class)
public class HiveTestBase extends PlanTestBase {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HiveTestBase.class);
  @ClassRule
  public static final TestRule CLASS_TIMEOUT = TestTools.getTimeoutRule(100000, TimeUnit.SECONDS);
  @Rule
  public final TestRule TIMEOUT = TestTools.getTimeoutRule(100, TimeUnit.SECONDS);

  protected static HiveTestDataGenerator dataGenerator;

  @BeforeClass
  public static void generateHive() throws Exception{
    SabotContext sabotContext = getSabotContext();
    Objects.requireNonNull(sabotContext);

    dataGenerator = HiveTestDataGenerator.getInstance();
    Objects.requireNonNull(dataGenerator);
    dataGenerator.addHiveTestPlugin(HiveTestDataGenerator.HIVE_TEST_PLUGIN_NAME, getSabotContext().getCatalogService());
    dataGenerator.addHiveTestPlugin(HiveTestDataGenerator.HIVE_TEST_PLUGIN_NAME_WITH_WHITESPACE, getSabotContext().getCatalogService());
  }

  @AfterClass
  public static void cleanupHiveTestData() {
    if (dataGenerator != null) {
      dataGenerator.deleteHiveTestPlugin(HiveTestDataGenerator.HIVE_TEST_PLUGIN_NAME, getSabotContext().getCatalogService());
      dataGenerator.deleteHiveTestPlugin(HiveTestDataGenerator.HIVE_TEST_PLUGIN_NAME_WITH_WHITESPACE, getSabotContext().getCatalogService());
    }
  }

  public static String createDerbyDB(String hiveMetastorePath) throws InterruptedException {
    String dbDir = "";
    String dbUrl;
    final int maxRetries = 10;
    for (int i = 0; i < maxRetries; i++) {
      try {
        dbUrl = String.format("jdbc:derby:;databaseName=%s;create=true", dbDir = getTempDir(hiveMetastorePath));
        // Set login timeout to 90 seconds
        DriverManager.setLoginTimeout(90);
        // Create the database for metastore in derby
        DriverManager.getConnection(dbUrl);
        logger.info("Create derby db successfully.");
        break;
      } catch (Exception e) {
        if (i == maxRetries -1) {
          throw new RuntimeException(String.format("Failed to create derby db after %d retries.", maxRetries), e);
        }
        logger.info("Failed to create derby db, will retry after 10 seconds.", e);
        Thread.sleep(10_000);
      }
    }
    return dbDir;
  }
}
