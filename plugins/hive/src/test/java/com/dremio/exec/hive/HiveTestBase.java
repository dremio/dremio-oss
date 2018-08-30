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
package com.dremio.exec.hive;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.dremio.PlanTestBase;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.hive.HivePluginOptions;
import com.dremio.exec.store.hive.HiveTestDataGenerator;

/**
 * Base class for Hive test. Takes care of generating and adding Hive test plugin before tests and deleting the
 * plugin after tests.
 */
public class HiveTestBase extends PlanTestBase {
  protected static HiveTestDataGenerator hiveTest;

  @BeforeClass
  public static void generateHive() throws Exception{
    hiveTest = HiveTestDataGenerator.getInstance();
    if (hiveTest == null) {
      throw new RuntimeException("hiveTest null!!!");
    }
    SabotContext sabotContext = getSabotContext();
    if (sabotContext == null) {
      throw new RuntimeException("context null!!!");
    }
    hiveTest.addHiveTestPlugin(getSabotContext().getCatalogService());
    test(String.format("alter session set \"%s\" = false", HivePluginOptions.HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS));
  }

  @AfterClass
  public static void cleanupHiveTestData() throws Exception{
    test(String.format("alter session set \"%s\" = true", HivePluginOptions.HIVE_OPTIMIZE_SCAN_WITH_NATIVE_READERS));
    if (hiveTest != null) {
      hiveTest.deleteHiveTestPlugin(getSabotContext().getCatalogService());
    }
  }
}
