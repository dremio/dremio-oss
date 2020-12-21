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
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.config.DremioConfig;
import com.dremio.exec.catalog.CatalogServiceImpl;
import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
import com.dremio.exec.store.dfs.WorkspaceConfig;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.users.SystemUser;
import com.dremio.test.TemporarySystemProperties;
import com.google.common.collect.Maps;

/**
 * Test queries involving direct impersonation and multilevel impersonation including join queries where each side is
 * a nested view.
 */
public class TestImpersonationQueries extends BaseTestImpersonation {
  @ClassRule
  public static TemporarySystemProperties properties = new TemporarySystemProperties();

  @BeforeClass
  public static void setup() throws Exception {
    assumeNonMaprProfile();

    properties.set(DremioConfig.LEGACY_STORE_VIEWS_ENABLED, "true");
    startMiniDfsCluster(TestImpersonationQueries.class.getSimpleName());
    createTestWorkspaces();
    addMiniDfsBasedStorage(/*impersonationEnabled=*/true);
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
    // Create test tables/views

    // Create copy of "lineitem" table in /user/user0_1 owned by user0_1:group0_1 with permissions 750. Only user0_1
    // has access to data in created "lineitem" table.
    createTestTable(org1Users[0], org1Groups[0], "lineitem");

    // Create copy of "orders" table in /user/user0_2 owned by user0_2:group0_2 with permissions 750. Only user0_2
    // has access to data in created "orders" table.
    createTestTable(org2Users[0], org2Groups[0], "orders");

    createNestedTestViewsOnLineItem();
    createNestedTestViewsOnOrders();
  }

  private static Map<String, WorkspaceConfig> createTestWorkspaces() throws Exception {
    // Create "/tmp" folder and set permissions to "777"
    final Path tmpPath = new Path("/tmp");
    fs.delete(tmpPath, true);
    FileSystem.mkdirs(fs, tmpPath, new FsPermission((short)0777));

    Map<String, WorkspaceConfig> workspaces = Maps.newHashMap();

    // create user directory (ex. "/user/user0_1", with ownership "user0_1:group0_1" and perms 755) for every user.
    for (int i = 0; i < org1Users.length; i++) {
      final String user = org1Users[i];
      final String group = org1Groups[i];
      createAndAddWorkspace(user, getUserHome(user), (short)0755, user, group, workspaces);
    }

    // create user directory (ex. "/user/user0_2", with ownership "user0_2:group0_2" and perms 755) for every user.
    for (int i = 0; i < org2Users.length; i++) {
      final String user = org2Users[i];
      final String group = org2Groups[i];
      createAndAddWorkspace(user, getUserHome(user), (short)0755, user, group, workspaces);
    }

    return workspaces;
  }

  private static void createTestTable(String user, String group, String tableName) throws Exception {
    updateClient(user);
//    test("USE " + getWSSchema(user));
    test(String.format("CREATE TABLE %s.\"/user/%s/%s\" as SELECT * FROM cp.\"tpch/%s.parquet\";", MINIDFS_STORAGE_PLUGIN_NAME, user, tableName, tableName));

    // Change the ownership and permissions manually. Currently there is no option to specify the default permissions
    // and ownership for new tables.
    final Path tablePath = new Path(getUserHome(user), tableName);

    fs.setOwner(tablePath, user, group);
    fs.setPermission(tablePath, new FsPermission((short)0750));
  }

  private static void createNestedTestViewsOnLineItem() throws Exception {
    // Input table "lineitem"
    // /user/user0_1     lineitem      750    user0_1:group0_1

    // Create a view on top of lineitem table
    // /user/user1_1    u1_lineitem    750    user1_1:group1_1
    createView(org1Users[1], org1Groups[1], (short)0750, "u1_lineitem", getUserHome(org1Users[0]) + "/lineitem");

    // Create a view on top of u1_lineitem view
    // /user/user2_1    u2_lineitem    750    user2_1:group2_1
    createView(org1Users[2], org1Groups[2], (short) 0750, "u2_lineitem", getUserHome(org1Users[1]) + "/u1_lineitem");

    // Create a view on top of u2_lineitem view
    // /user/user2_1    u22_lineitem    750    user2_1:group2_1
    createView(org1Users[2], org1Groups[2], (short) 0750, "u22_lineitem", getUserHome(org1Users[2]) + "/u2_lineitem");

    // Create a view on top of u22_lineitem view
    // /user/user3_1    u3_lineitem    750    user3_1:group3_1
    createView(org1Users[3], org1Groups[3], (short)0750, "u3_lineitem", getUserHome(org1Users[2]) +  "/u22_lineitem");

    // Create a view on top of u3_lineitem view
    // /user/user4_1    u4_lineitem    755    user4_1:group4_1
    createView(org1Users[4], org1Groups[4], (short) 0755, "u4_lineitem", getUserHome(org1Users[3]) + "/u3_lineitem");
  }

  private static void createNestedTestViewsOnOrders() throws Exception {
    // Input table "orders"
    // /user/user0_2     orders      750    user0_2:group0_2

    // Create a view on top of orders table
    // /user/user1_2    u1_orders    750    user1_2:group1_2
    createView(org2Users[1], org2Groups[1], (short)0750, "u1_orders", getUserHome(org2Users[0]) + "/orders");

    // Create a view on top of u1_orders view
    // /user/user2_2    u2_orders    750    user2_2:group2_2
    createView(org2Users[2], org2Groups[2], (short)0750, "u2_orders", getUserHome(org2Users[1]) + "/u1_orders");

    // Create a view on top of u2_orders view
    // /user/user2_2    u22_orders    750    user2_2:group2_2
    createView(org2Users[2], org2Groups[2], (short)0750, "u22_orders", getUserHome(org2Users[2]) + "/u2_orders");

    // Create a view on top of u22_orders view (permissions of this view (755) are different from permissions of the
    // corresponding view in "lineitem" nested views to have a join query of "lineitem" and "orders" nested views)
    // /user/user3_2    u3_orders    750    user3_2:group3_2
    createView(org2Users[3], org2Groups[3], (short)0755, "u3_orders", getUserHome(org2Users[2]) + "/u22_orders");

    // Create a view on top of u3_orders view
    // /user/user4_2    u4_orders    755    user4_2:group4_2
    createView(org2Users[4], org2Groups[4], (short)0755, "u4_orders", getUserHome(org2Users[3]) + "/u3_orders");
  }

  private String fullPath(String user, String table) {
    return String.format("%s.\"%s/%s\"", MINIDFS_STORAGE_PLUGIN_NAME, getUserHome(user), table);
  }

  @Test
  public void testDirectImpersonation_HasUserReadPermissions() throws Exception {
    // Table lineitem is owned by "user0_1:group0_1" with permissions 750. Try to read the table as "user0_1". We
    // shouldn't expect any errors.
    updateClient(org1Users[0]);
    test(String.format("SELECT * FROM %s ORDER BY l_orderkey LIMIT 1", fullPath(org1Users[0], "lineitem")));
  }

  @Test
  public void testCreateTableDirectImpersonation_HasUserReadPermissions() throws Exception {
    // Table lineitem is owned by "user0_1:group0_1" with permissions 750. Try to read the table as "user0_1"
    // and create a table under its home directory, which should succeed. All the created files should also
    // belong to user0_1
    updateClient(org1Users[0]);
    test(String.format("CREATE TABLE %s AS SELECT * FROM %s ORDER BY l_orderkey LIMIT 1",
        fullPath(org1Users[0], "copy_lineitem"),
        fullPath(org1Users[0], "lineitem")));

    final Path target = new Path(getUserHome(org1Users[0]), "copy_lineitem");
    assertEquals(org1Users[0], fs.getFileStatus(target).getOwner());
    FileStatus[] statuses = fs.listStatus(target);
    for(FileStatus status: statuses) {
      assertEquals(org1Users[0], status.getOwner());
    }
  }

  @Test
  public void testCreateTableDirectImpersonation_NoWritePermission() throws Exception {
    // Table lineitem is owned by "user0_1:group0_1" with permissions 750. Try to read the table as "user0_1"
    // and create a table under user2_1, which should fail
    try {
      updateClient(org1Users[0]);
      test(String.format("CREATE TABLE %s AS SELECT * FROM %s ORDER BY l_orderkey LIMIT 1",
          fullPath(org1Users[2], "copy_lineitem"),
          fullPath(org1Users[0], "lineitem")));
      fail("query is expected to fail");
    } catch(UserRemoteException e) {
      assertEquals(ErrorType.SYSTEM, e.getErrorType());
      assertThat(e.getMessage(), containsString("Permission denied"));
    }
  }
  @Test
  public void testDirectImpersonation_HasGroupReadPermissions() throws Exception {
    // Table lineitem is owned by "user0_1:group0_1" with permissions 750. Try to read the table as "user1_1". We
    // shouldn't expect any errors as "user1_1" is part of the "group0_1"
    updateClient(org1Users[1]);
    test(String.format("SELECT * FROM %s ORDER BY l_orderkey LIMIT 1", fullPath(org1Users[0], "lineitem")));
  }

  @Test
  public void testDirectImpersonation_NoReadPermissions() throws Exception {
    try {
      // Table lineitem is owned by "user0_1:group0_1" with permissions 750. Now try to read the table as "user2_1". We
      // should expect a permission denied error as "user2_1" is not part of the "group0_1"
      updateClient(org1Users[2]);
      test(String.format("SELECT * FROM %s ORDER BY l_orderkey LIMIT 1", fullPath(org1Users[0], "lineitem")));
      fail("query is expected to fail");
    } catch(UserRemoteException e) {
      assertEquals(ErrorType.PERMISSION, e.getErrorType());
      assertThat(e.getMessage(), containsString("PERMISSION ERROR: Access denied reading dataset miniDfsPlugin.\"/user/user0_1/lineitem\""));
    }
  }

  @Test
  public void testMultiLevelImpersonation1() throws Exception {
    updateClient(org1Users[4]);
    test(String.format("SELECT * from %s LIMIT 1;", fullPath(org1Users[4], "u4_lineitem")));
  }

  @Test
  public void testMultiLevelImpersonation2() throws Exception {
    updateClient(org1Users[5]);
    test(String.format("SELECT * from %s LIMIT 1;", fullPath(org1Users[4], "u4_lineitem")));
  }

  @Test
  public void testMultiLevelImpersonationJoin1() throws Exception {
    updateClient(org1Users[4]);
    test(String.format("SELECT * from %s l JOIN %s o ON l.l_orderkey = o.o_orderkey LIMIT 1;",
      fullPath(org1Users[4], "u4_lineitem"), fullPath(org2Users[3], "u3_orders")));
  }

  @Test
  public void testMultiLevelImpersonationJoin2() throws Exception {
    updateClient(org1Users[4]);
    test(String.format("SELECT * from %s l JOIN %s o ON l.l_orderkey = o.o_orderkey LIMIT 1;",
        fullPath(org1Users[4], "u4_lineitem"), fullPath(org2Users[4], "u4_orders")));
  }
}
