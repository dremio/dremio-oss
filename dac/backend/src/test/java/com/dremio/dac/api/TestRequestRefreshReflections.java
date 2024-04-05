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

package com.dremio.dac.api;

import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.service.accelerator.BaseTestReflection;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.reflection.proto.RefreshRequest;
import com.dremio.service.reflection.store.RefreshRequestsStore;
import com.google.common.collect.ImmutableList;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.Response;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Tests for requesting refresh reflections for given dataset or reflection. */
public class TestRequestRefreshReflections extends BaseTestReflection {
  private static final String REFLECTIONS_PATH = "/reflection/";
  private static final String CATALOG_PATH = "/catalog/";
  private final RefreshRequestsStore refreshRequestsStore =
      new RefreshRequestsStore(p(LegacyKVStoreProvider.class));
  private final String empTableName = "emp";
  private final String deptTableName = "dept";
  private final String empViewName = "emp_vw";
  private final String empJoinDeptViewName = "emp_join_dept_vw";
  private DatasetConfig datasetConfigEmp;
  private DatasetConfig datasetConfigDept;
  private DatasetConfig datasetConfigEmpView;
  private DatasetConfig datasetConfigEmpJoinDeptView;

  private void runSQL(String sql) {
    submitJobAndWaitUntilCompletion(
        JobRequest.newBuilder().setSqlQuery(getQueryFromSQL(sql.toString())).build());
  }

  private void createTestTables() throws Exception {
    runSQL(
        String.format(
            "CREATE TABLE %s.%s (id BIGINT, name VARCHAR, dept INT, salary DOUBLE)",
            TEMP_SCHEMA, empTableName));
    runSQL(
        String.format("CREATE TABLE %s.%s (id BIGINT, name VARCHAR)", TEMP_SCHEMA, deptTableName));
    runSQL(
        String.format(
            "CREATE VIEW %s.%s AS SELECT * FROM %s.%s",
            TEST_SPACE, empViewName, TEMP_SCHEMA, empTableName));
    runSQL(
        String.format(
            "CREATE VIEW %s.%s AS SELECT e.id id, e.name name, d.name dept, e.salary salary FROM %s.%s e JOIN %s.%s d on e.dept = d.id",
            TEST_SPACE,
            empJoinDeptViewName,
            TEMP_SCHEMA,
            empTableName,
            TEMP_SCHEMA,
            deptTableName));
    datasetConfigEmp =
        CatalogUtil.getDatasetConfig(
            cat(), new NamespaceKey(ImmutableList.of(TEMP_SCHEMA, empTableName)));
    datasetConfigDept =
        CatalogUtil.getDatasetConfig(
            cat(), new NamespaceKey(ImmutableList.of(TEMP_SCHEMA, deptTableName)));
    datasetConfigEmpView =
        CatalogUtil.getDatasetConfig(
            cat(), new NamespaceKey(ImmutableList.of(TEST_SPACE, empViewName)));
    datasetConfigEmpJoinDeptView =
        CatalogUtil.getDatasetConfig(
            cat(), new NamespaceKey(ImmutableList.of(TEST_SPACE, empJoinDeptViewName)));
  }

  private void dropTestTables() throws Exception {
    runSQL(String.format("DROP VIEW IF EXISTS %s.%s", TEST_SPACE, empViewName));
    runSQL(String.format("DROP VIEW IF EXISTS %s.%s", TEST_SPACE, empJoinDeptViewName));
    runSQL(String.format("DROP TABLE IF EXISTS %s.%s", TEMP_SCHEMA, empTableName));
    runSQL(String.format("DROP TABLE IF EXISTS %s.%s", TEMP_SCHEMA, deptTableName));
  }

  @Before
  public void prepare() throws Exception {
    createTestTables();
  }

  @After
  public void cleanUp() throws Exception {
    dropTestTables();
  }

  /** Verifies requesting refresh reflections for PDS using REST API. */
  @Test
  public void testRequestRefreshForPdsRest() throws Exception {
    runRequestRefreshForPdsRest();
  }

  /** Verifies requesting refresh reflections for PDS using SQL. */
  @Test
  public void testRequestRefreshForPdsSql() throws Exception {
    UserRemoteException ex =
        assertThrows(UserRemoteException.class, () -> runRequestRefreshForPdsSql());
    assertEquals("REFRESH REFLECTIONS command is not supported.", ex.getOriginalMessage());
  }

  /** Verifies requesting refresh reflections for VDS using REST API. */
  @Test
  public void testRequestRefreshForViewRest() throws Exception {
    runRequestRefreshForViewRest();
  }

  /** Verifies requesting refresh reflections for VDS using SQL. */
  @Test
  public void testRequestRefreshForViewSql() throws Exception {
    UserRemoteException ex =
        assertThrows(UserRemoteException.class, () -> runRequestRefreshForViewSql());
    assertEquals("REFRESH REFLECTIONS command is not supported.", ex.getOriginalMessage());
  }

  /** Verifies requesting refresh reflections for VDS with join using REST API. */
  @Test
  public void testRequestRefreshForViewWithJoinRest() throws Exception {
    runRequestRefreshForViewWithJoinRest();
  }

  /** Verifies requesting refresh reflections for VDS with join using SQL. */
  @Test
  public void testRequestRefreshForViewWithJoinSql() throws Exception {
    UserRemoteException ex =
        assertThrows(UserRemoteException.class, () -> runRequestRefreshForViewWithJoinSql());
    assertEquals("REFRESH REFLECTIONS command is not supported.", ex.getOriginalMessage());
  }

  /** Verifies requesting refresh reflections for reflection on PDS using REST API. */
  @Test
  public void testRequestRefreshForReflectionOnPdsRest() throws Exception {
    String exp = "HTTP 404 Not Found";
    runRequestRefreshForReflectionOnPdsRest(NOT_FOUND, exp);
  }

  /** Verifies requesting refresh reflections for reflection on VDS using REST API. */
  @Test
  public void testRequestRefreshForReflectionOnViewRest() throws Exception {
    String exp = "HTTP 404 Not Found";
    runRequestRefreshForReflectionOnViewRest(NOT_FOUND, exp);
  }

  /** Verifies requesting refresh reflections for reflection on VDS with join using REST API. */
  @Test
  public void testRequestRefreshForReflectionOnViewWithJoinRest() throws Exception {
    String exp = "HTTP 404 Not Found";
    runRequestRefreshForReflectionOnViewWithJoinRest(NOT_FOUND, exp);
  }

  private void runRequestRefreshForPdsRest() {
    RefreshRequest refreshRequest;
    refreshRequest = refreshRequestsStore.get(datasetConfigEmp.getId().getId());
    assertNull(refreshRequest);

    Invocation invocation =
        getBuilder(
                getPublicAPI(3)
                    .path(CATALOG_PATH)
                    .path(datasetConfigEmp.getId().getId())
                    .path("refresh"))
            .buildPost(null);

    // Test
    expectSuccess(invocation);
    // Verify
    refreshRequest = refreshRequestsStore.get(datasetConfigEmp.getId().getId());
    assertNotNull(refreshRequest);
    assertEquals(datasetConfigEmp.getId().getId(), refreshRequest.getDatasetId());
  }

  private void runRequestRefreshForPdsSql() {
    // Test
    runSQL(String.format("ALTER DATASET %S.%S REFRESH REFLECTIONS", TEMP_SCHEMA, empTableName));
  }

  private void runRequestRefreshForViewRest() {
    RefreshRequest refreshRequest;
    refreshRequest = refreshRequestsStore.get(datasetConfigEmp.getId().getId());
    assertNull(refreshRequest);
    refreshRequest = refreshRequestsStore.get(datasetConfigEmpView.getId().getId());
    assertNull(refreshRequest);

    Invocation invocation =
        getBuilder(
                getPublicAPI(3)
                    .path(CATALOG_PATH)
                    .path(datasetConfigEmpView.getId().getId())
                    .path("refresh"))
            .buildPost(null);

    // Test
    expectSuccess(invocation);

    // Verify
    refreshRequest = refreshRequestsStore.get(datasetConfigEmp.getId().getId());
    assertNull(refreshRequest);
    refreshRequest = refreshRequestsStore.get(datasetConfigEmpView.getId().getId());
    assertNotNull(refreshRequest);
    assertEquals(datasetConfigEmpView.getId().getId(), refreshRequest.getDatasetId());
  }

  private void runRequestRefreshForViewSql() {
    // Test
    runSQL(String.format("ALTER DATASET %S.%S REFRESH REFLECTIONS", TEST_SPACE, empViewName));
  }

  private void runRequestRefreshForViewWithJoinRest() {
    RefreshRequest refreshRequest;
    refreshRequest = refreshRequestsStore.get(datasetConfigEmp.getId().getId());
    assertNull(refreshRequest);
    refreshRequest = refreshRequestsStore.get(datasetConfigDept.getId().getId());
    assertNull(refreshRequest);
    refreshRequest = refreshRequestsStore.get(datasetConfigEmpJoinDeptView.getId().getId());
    assertNull(refreshRequest);

    Invocation invocation =
        getBuilder(
                getPublicAPI(3)
                    .path(CATALOG_PATH)
                    .path(datasetConfigEmpJoinDeptView.getId().getId())
                    .path("refresh"))
            .buildPost(null);

    // Test
    expectSuccess(invocation);

    // Verify
    refreshRequest = refreshRequestsStore.get(datasetConfigEmp.getId().getId());
    assertNull(refreshRequest);
    refreshRequest = refreshRequestsStore.get(datasetConfigDept.getId().getId());
    assertNull(refreshRequest);
    refreshRequest = refreshRequestsStore.get(datasetConfigEmpJoinDeptView.getId().getId());
    assertNotNull(refreshRequest);
    assertEquals(datasetConfigEmpJoinDeptView.getId().getId(), refreshRequest.getDatasetId());
  }

  private void runRequestRefreshForViewWithJoinSql() {
    // Test
    runSQL(
        String.format("ALTER DATASET %S.%S REFRESH REFLECTIONS", TEST_SPACE, empJoinDeptViewName));
  }

  private void runRequestRefreshForReflectionOnPdsRest(
      Response.StatusType status, String expResponse) throws Exception {

    Invocation invocation =
        getBuilder(getPublicAPI(3).path(REFLECTIONS_PATH).path("reflection_1").path("refresh"))
            .buildPost(null);

    // Test / Verify
    Response response = expectStatus(status, invocation);
    assertContains(expResponse, response.readEntity(String.class));
  }

  private void runRequestRefreshForReflectionOnViewRest(
      Response.StatusType status, String expResponse) throws Exception {

    Invocation invocation =
        getBuilder(getPublicAPI(3).path(REFLECTIONS_PATH).path("reflection_1").path("refresh"))
            .buildPost(null);

    // Test / Verify
    Response response = expectStatus(status, invocation);
    assertContains(expResponse, response.readEntity(String.class));
  }

  private void runRequestRefreshForReflectionOnViewWithJoinRest(
      Response.StatusType status, String expResponse) throws Exception {

    Invocation invocation =
        getBuilder(getPublicAPI(3).path(REFLECTIONS_PATH).path("reflection_1").path("refresh"))
            .buildPost(null);

    // Test / Verify
    Response response = expectStatus(status, invocation);
    assertContains(expResponse, response.readEntity(String.class));
  }
}
