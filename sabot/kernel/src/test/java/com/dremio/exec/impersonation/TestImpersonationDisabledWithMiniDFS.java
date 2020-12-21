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
package com.dremio.exec.impersonation;

import static com.dremio.common.TestProfileHelper.assumeNonMaprProfile;
import static com.dremio.common.TestProfileHelper.isMaprProfile;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.exec.catalog.CatalogServiceImpl;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.users.SystemUser;

/**
 * Note to future devs, please do not put random tests here. Make sure that they actually require
 * access to a DFS instead of the local filesystem implementation used by default in the rest of
 * the tests. Running this mini cluster is slow and it is best for these tests to only cover
 * necessary cases.
 */
public class TestImpersonationDisabledWithMiniDFS extends BaseTestImpersonation {

  @BeforeClass
  public static void setup() throws Exception {
    assumeNonMaprProfile();
    startMiniDfsCluster(TestImpersonationDisabledWithMiniDFS.class.getSimpleName());
    addMiniDfsBasedStorage( /*impersonationEnabled=*/false);
    createTestData();
  }

  @AfterClass
  public static void removeMiniDfsBasedStorage() throws Exception {
    /*
     JUnit assume() call results in AssumptionViolatedException, which is handled by JUnit with a goal to ignore
     the test having the assume() call. Multiple assume() calls, or other exceptions coupled with a single assume()
     call, result in multiple exceptions, which aren't handled by JUnit, leading to test deemed to be failed.
     We thus use isMaprProfile() check instead of assumeNonMaprProfile() here.
     */
    if (isMaprProfile()) {
      return;
    }

    SourceConfig config = getSabotContext().getNamespaceService(SystemUser.SYSTEM_USERNAME).getSource(new NamespaceKey(MINIDFS_STORAGE_PLUGIN_NAME));
    ((CatalogServiceImpl) getSabotContext().getCatalogService()).getSystemUserCatalog().deleteSource(config);
    stopMiniDfsCluster();
  }

  private static void createTestData() throws Exception {
    // Create test table in minidfs.tmp schema for use in test queries
    test(String.format("CREATE TABLE %s.dfsRegion AS SELECT * FROM cp.\"region.json\"",
        MINIDFS_STORAGE_PLUGIN_NAME));

    test("alter session set \"store.format\"='csv'");
    test(String.format("CREATE TABLE %s.dfsRegionCsv AS SELECT * FROM cp.\"region.json\"",
        MINIDFS_STORAGE_PLUGIN_NAME));

    // generate a large enough file that the DFS will not fulfill requests to read a
    // page of data all at once, see notes above testReadLargeParquetFileFromDFS()
    test("alter session set \"store.format\"='parquet'");
    test(String.format(
        "CREATE TABLE %s.large_employee AS " +
            "(SELECT employee_id, full_name FROM cp.\"/employee.json\") " +
            "UNION ALL (SELECT employee_id, full_name FROM cp.\"/employee.json\")" +
            "UNION ALL (SELECT employee_id, full_name FROM cp.\"/employee.json\")" +
            "UNION ALL (SELECT employee_id, full_name FROM cp.\"/employee.json\")" +
            "UNION ALL (SELECT employee_id, full_name FROM cp.\"/employee.json\")" +
            "UNION ALL (SELECT employee_id, full_name FROM cp.\"/employee.json\")" +
            "UNION ALL (SELECT employee_id, full_name FROM cp.\"/employee.json\")" +
        "UNION ALL (SELECT employee_id, full_name FROM cp.\"/employee.json\")",
        MINIDFS_STORAGE_PLUGIN_NAME));
  }

  /**
   * When working on merging the Drill fork of parquet a bug was found that only manifested when
   * run on a cluster. It appears that the local implementation of the Hadoop FileSystem API
   * never fails to provide all of the bytes that are requested in a single read. The API is
   * designed to allow for a subset of the requested bytes be returned, and a client can decide
   * if they want to do processing on teh subset that are available now before requesting the rest.
   *
   * For parquet's block compression of page data, we need all of the bytes. This test is here as
   * a sanitycheck  to make sure we don't accidentally introduce an issue where a subset of the bytes
   * are read and would otherwise require testing on a cluster for the full contract of the read method
   * we are using to be exercised.
   */
  @Test
  public void testReadLargeParquetFileFromDFS() throws Exception {
    test(String.format("USE %s", MINIDFS_STORAGE_PLUGIN_NAME));
    test("SELECT * FROM \"large_employee\"");
  }

  @Test // DRILL-3037
  public void testSimpleQuery() throws Exception {
    final String query = "SELECT sales_city, sales_country FROM dfsRegion ORDER BY region_id DESC LIMIT 2";

    testBuilder()
        .optionSettingQueriesForTestQuery(String.format("USE %s", MINIDFS_STORAGE_PLUGIN_NAME))
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("sales_city", "sales_country")
        .baselineValues("Santa Fe", "Mexico")
        .baselineValues("Santa Anita", "Mexico")
        .go();
  }

  @Test
  public void testWithTableOptions() throws Exception {
    test(String.format("select * from TABLE(%s.dfsRegionCsv(type => 'TeXT', fieldDelimiter => ',')) ",
        MINIDFS_STORAGE_PLUGIN_NAME));
  }
}
