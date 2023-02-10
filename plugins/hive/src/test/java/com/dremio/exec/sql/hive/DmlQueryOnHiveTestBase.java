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
package com.dremio.exec.sql.hive;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.hive.LazyDataGeneratingHiveTestBase;
import com.dremio.exec.store.hive.HiveConfFactory;
import com.google.common.collect.ImmutableMap;

/**
 * Enables ICEBERG-Related support options for a local Hive-based source.
 */
public class DmlQueryOnHiveTestBase extends LazyDataGeneratingHiveTestBase {

  @BeforeClass
  public static void beforeClass() throws Exception {
    setSystemOption(ExecConstants.ENABLE_ICEBERG_ADVANCED_DML, "true");
    setSystemOption(ExecConstants.ENABLE_ICEBERG_OPTIMIZE, "true");
    setSystemOption(ExecConstants.ENABLE_ICEBERG_VACUUM, "true");

    dataGenerator.updatePluginConfig((getSabotContext().getCatalogService()),
      ImmutableMap.of(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, "file:///" + dataGenerator.getWhDir() + "/",
        HiveConfFactory.ENABLE_DML_TESTS_WITHOUT_LOCKING, "true"));
  }

  @AfterClass
  public static void afterClass() {
    setSystemOption(ExecConstants.ENABLE_ICEBERG_ADVANCED_DML,
      ExecConstants.ENABLE_ICEBERG_ADVANCED_DML.getDefault().getBoolVal().toString());
    setSystemOption(ExecConstants.ENABLE_ICEBERG_OPTIMIZE,
      ExecConstants.ENABLE_ICEBERG_OPTIMIZE.getDefault().getBoolVal().toString());
    setSystemOption(ExecConstants.ENABLE_ICEBERG_VACUUM,
      ExecConstants.ENABLE_ICEBERG_VACUUM.getDefault().getBoolVal().toString());
  }
}
