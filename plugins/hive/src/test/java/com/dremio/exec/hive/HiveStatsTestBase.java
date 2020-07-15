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

import com.dremio.common.util.TestTools;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.exec.GuavaPatcherRunner;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.hive.HiveTestDataGenerator;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.google.common.base.Preconditions;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Base class for Hive query profile stats test.
 * Takes care of generating and adding Hive test plugin before tests and deleting the plugin after tests.
 */
@RunWith(GuavaPatcherRunner.class)
public abstract class HiveStatsTestBase extends BaseTestServer {
  @ClassRule
  public static final TestRule CLASS_TIMEOUT = TestTools.getTimeoutRule(100000, TimeUnit.SECONDS);
  private static HiveTestDataGenerator hiveTest;

  @BeforeClass
  public static void setup() throws Exception {
    hiveTest = HiveTestDataGenerator.getInstance();
    Preconditions.checkNotNull(hiveTest, "HiveTestDataGenerator not initialized");
    SabotContext sabotContext = getSabotContext();
    Preconditions.checkNotNull(sabotContext, "SabotContext not initialized");
    hiveTest.addHiveTestPlugin(HiveTestDataGenerator.HIVE_TEST_PLUGIN_NAME, getSabotContext().getCatalogService());
    hiveTest.addHiveTestPlugin(HiveTestDataGenerator.HIVE_TEST_PLUGIN_NAME_WITH_WHITESPACE, getSabotContext().getCatalogService());
  }

  @AfterClass
  public static void shutdown() throws Exception {
    if (hiveTest != null) {
      hiveTest.deleteHiveTestPlugin(HiveTestDataGenerator.HIVE_TEST_PLUGIN_NAME, getSabotContext().getCatalogService());
      hiveTest.deleteHiveTestPlugin(HiveTestDataGenerator.HIVE_TEST_PLUGIN_NAME_WITH_WHITESPACE, getSabotContext().getCatalogService());
    }
  }

  protected UserBitShared.OperatorProfile getHiveReaderProfile(final String query) throws Exception {
    final UserBitShared.QueryProfile queryProfile = getQueryProfile(
      JobRequest.newBuilder()
        .setSqlQuery(new SqlQuery(query, DEFAULT_USERNAME))
        .setQueryType(QueryType.UI_INTERNAL_RUN)
        .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
        .setDatasetVersion(DatasetVersion.NONE)
        .build()
    );
    final List<UserBitShared.OperatorProfile> operatorProfileList = queryProfile.getFragmentProfileList()
      .get(0)
      .getMinorFragmentProfileList().get(0)
      .getOperatorProfileList();
    return operatorProfileList.stream()
      .filter(profile -> profile.getOperatorType() ==
        UserBitShared.CoreOperatorType.HIVE_SUB_SCAN_VALUE)
      .findFirst().get();
  }

  protected long getMetricValue(final UserBitShared.OperatorProfile hiveReaderProfile, final ScanOperator.Metric metric) {
    return hiveReaderProfile
      .getMetricList().stream()
      .filter(m -> m.getMetricId() == metric.metricId())
      .findFirst()
      .get()
      .getLongValue();
  }
}
