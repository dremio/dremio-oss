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
package com.dremio.exec.planner.sql;

import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.DATAPLANE_PLUGIN_NAME;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.DEFAULT_BRANCH_NAME;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createBranchAtBranchQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createEmptyTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createViewQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueBranchName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueViewName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.insertTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.joinedTableKey;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.tablePathWithFolders;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.useBranchQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.useContextQuery;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.dremio.catalog.model.VersionContext;
import com.dremio.exec.catalog.dataplane.ITBaseTestVersioned;
import com.dremio.exec.proto.UserBitShared;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.tools.compatibility.internal.OlderNessieServersExtension;

@ExtendWith(OlderNessieServersExtension.class)
public class TestContextSettingsInPlanProfile extends ITBaseTestVersioned {
  private BufferAllocator allocator;

  @BeforeEach
  public void prepare() throws Exception {
    allocator = getRootAllocator().newChildAllocator(getClass().getName(), 0, Long.MAX_VALUE);
  }

  @AfterEach
  public void clear() {
    allocator.close();
  }

  @Test
  public void testVersionContextSetToMainBranch() throws Exception {
    final String table1 = generateUniqueTableName();
    final List<String> table1Path = tablePathWithFolders(table1);
    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, table1Path, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    String sessionId = runQueryAndGetSessionId(createEmptyTableQuery(table1Path));
    runQuery(insertTableQuery(table1Path));
    runQueryInSession(useBranchQuery(DEFAULT_BRANCH_NAME), sessionId);
    runQueryInSession(useContextQuery(Collections.singletonList(DATAPLANE_PLUGIN_NAME)), sessionId);

    final String query =
        String.format(
            "Select * from %s.%s AT BRANCH \"main\"",
            DATAPLANE_PLUGIN_NAME, joinedTableKey(table1Path));
    final UserBitShared.QueryProfile profile =
        getQueryProfile(createJobRequestFromSqlAndSessionId(query, sessionId));

    assertEquals(DATAPLANE_PLUGIN_NAME, profile.getContextInfo().getSchemaPathContext());
    assertEquals(
        VersionContext.ofBranch(DEFAULT_BRANCH_NAME).toString(),
        profile.getContextInfo().getSourceVersionSettingList().get(0).getVersionContext());
  }

  @Disabled // TODO(DX-83489)
  @Test
  public void testPathContextSetToOneFolder() throws Exception {
    final String table1 = generateUniqueTableName();
    final List<String> table1Path = tablePathWithFolders(table1);
    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, table1Path, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    String sessionId = runQueryAndGetSessionId(createEmptyTableQuery(table1Path));
    runQuery(insertTableQuery(table1Path));
    runQueryInSession(useBranchQuery(DEFAULT_BRANCH_NAME), sessionId);
    runQueryInSession(
        useContextQuery(Arrays.asList(DATAPLANE_PLUGIN_NAME, table1Path.get(0))), sessionId);

    final String query =
        String.format(
            "Select * from %s.%s AT BRANCH \"main\"",
            DATAPLANE_PLUGIN_NAME, joinedTableKey(table1Path));
    final UserBitShared.QueryProfile profile =
        getQueryProfile(createJobRequestFromSqlAndSessionId(query, sessionId));

    assertEquals(DATAPLANE_PLUGIN_NAME, profile.getContextInfo().getSchemaPathContext());
    assertEquals(
        VersionContext.ofBranch(DEFAULT_BRANCH_NAME).toString(),
        profile.getContextInfo().getSourceVersionSettingList().get(0).getVersionContext());
  }

  @Disabled // TODO(DX-83489)
  @Test
  public void testPathContextSetToTwoFolders() throws Exception {
    final String table1 = generateUniqueTableName();
    final List<String> table1Path = tablePathWithFolders(table1);
    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, table1Path, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    String sessionId = runQueryAndGetSessionId(createEmptyTableQuery(table1Path));
    runQuery(insertTableQuery(table1Path));
    runQueryInSession(useBranchQuery(DEFAULT_BRANCH_NAME), sessionId);
    runQueryInSession(useContextQuery(Collections.singletonList(DATAPLANE_PLUGIN_NAME)), sessionId);

    final String query =
        String.format(
            "Select * from %s.%s AT BRANCH \"main\"",
            DATAPLANE_PLUGIN_NAME, joinedTableKey(table1Path));
    final UserBitShared.QueryProfile profile =
        getQueryProfile(createJobRequestFromSqlAndSessionId(query, sessionId));

    assertEquals(DATAPLANE_PLUGIN_NAME, profile.getContextInfo().getSchemaPathContext());
    assertEquals(
        VersionContext.ofBranch(DEFAULT_BRANCH_NAME).toString(),
        profile.getContextInfo().getSourceVersionSettingList().get(0).getVersionContext());
  }

  @Test
  public void testVersionContextSetToDifferentBranch() throws Exception {
    final String table1 = generateUniqueTableName();
    final List<String> table1Path = tablePathWithFolders(table1);
    final String branchName = generateUniqueBranchName();
    final String sessionId =
        runQueryAndGetSessionId(createBranchAtBranchQuery(branchName, DEFAULT_BRANCH_NAME));
    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, table1Path, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    runQuery(createEmptyTableQuery(table1Path));
    runQuery(insertTableQuery(table1Path));
    runQueryInSession(useBranchQuery(branchName), sessionId);
    runQueryInSession(useContextQuery(Collections.singletonList(DATAPLANE_PLUGIN_NAME)), sessionId);

    final String query =
        String.format(
            "Select * from %s.%s AT BRANCH \"main\"",
            DATAPLANE_PLUGIN_NAME, joinedTableKey(table1Path));
    final UserBitShared.QueryProfile profile =
        getQueryProfile(createJobRequestFromSqlAndSessionId(query, sessionId));

    assertEquals(DATAPLANE_PLUGIN_NAME, profile.getContextInfo().getSchemaPathContext());
    assertEquals(
        VersionContext.ofBranch(branchName).toString(),
        profile.getContextInfo().getSourceVersionSettingList().get(0).getVersionContext());
  }

  @Test
  public void testVersionContextNotSet() throws Exception {
    final String table1 = generateUniqueTableName();
    final List<String> table1Path = tablePathWithFolders(table1);
    final String branchName = generateUniqueBranchName();
    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, table1Path, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    final String sessionId = runQueryAndGetSessionId(createEmptyTableQuery(table1Path));
    runQuery(insertTableQuery(table1Path));
    runQueryInSession(useContextQuery(Collections.singletonList(DATAPLANE_PLUGIN_NAME)), sessionId);

    final String query =
        String.format(
            "Select * from %s.%s AT BRANCH \"main\"",
            DATAPLANE_PLUGIN_NAME, joinedTableKey(table1Path));
    final UserBitShared.QueryProfile profile =
        getQueryProfile(createJobRequestFromSqlAndSessionId(query, sessionId));

    assertEquals(DATAPLANE_PLUGIN_NAME, profile.getContextInfo().getSchemaPathContext());
    assertThat(profile.getContextInfo().getSourceVersionSettingList().isEmpty()).isTrue();
  }

  @Test
  public void testViewQuery() throws Exception {
    final String table1 = generateUniqueTableName();
    final List<String> table1Path = tablePathWithFolders(table1);
    final String branchName = generateUniqueBranchName();
    final String sessionId =
        runQueryAndGetSessionId(createBranchAtBranchQuery(branchName, DEFAULT_BRANCH_NAME));
    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, table1Path, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    runQuery(createEmptyTableQuery(table1Path));
    final String viewName = generateUniqueViewName();
    List<String> viewPath = tablePathWithFolders(viewName);
    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, viewPath, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    runQuery(createViewQuery(viewPath, table1Path));
    runQuery(insertTableQuery(table1Path));

    runQueryInSession(useBranchQuery(branchName), sessionId);
    runQueryInSession(useContextQuery(Collections.singletonList(DATAPLANE_PLUGIN_NAME)), sessionId);

    final String query =
        String.format(
            "Select * from %s.%s AT BRANCH \"main\"",
            DATAPLANE_PLUGIN_NAME, joinedTableKey(viewPath));
    final UserBitShared.QueryProfile profile =
        getQueryProfile(createJobRequestFromSqlAndSessionId(query, sessionId));

    assertEquals(DATAPLANE_PLUGIN_NAME, profile.getContextInfo().getSchemaPathContext());
    assertEquals(
        VersionContext.ofBranch(branchName).toString(),
        profile.getContextInfo().getSourceVersionSettingList().get(0).getVersionContext());
  }

  @Test
  public void testViewQueryWithAT() throws Exception {
    final String table1 = generateUniqueTableName();
    final List<String> table1Path = tablePathWithFolders(table1);
    final String branchName = generateUniqueBranchName();
    final String sessionId =
        runQueryAndGetSessionId(createBranchAtBranchQuery(branchName, DEFAULT_BRANCH_NAME));
    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, table1Path, VersionContext.ofBranch(DEFAULT_BRANCH_NAME), null);
    runQuery(createEmptyTableQuery(table1Path));
    final String viewName = generateUniqueViewName();
    List<String> viewPath = tablePathWithFolders(viewName);
    createFoldersForTablePath(
        DATAPLANE_PLUGIN_NAME, viewPath, VersionContext.ofBranch(branchName), null);
    String createViewQuery =
        String.format(
            "CREATE VIEW %s.%s AT BRANCH %s AS SELECT * FROM %s.%s AT BRANCH %s",
            DATAPLANE_PLUGIN_NAME,
            joinedTableKey(viewPath),
            branchName,
            DATAPLANE_PLUGIN_NAME,
            joinedTableKey(table1Path),
            DEFAULT_BRANCH_NAME);
    runQuery(createViewQuery);
    runQuery(insertTableQuery(table1Path));

    runQueryInSession(useBranchQuery(branchName), sessionId);
    runQueryInSession(useContextQuery(Collections.singletonList(DATAPLANE_PLUGIN_NAME)), sessionId);

    final String query =
        String.format(
            "Select * from %s.%s AT BRANCH %s",
            DATAPLANE_PLUGIN_NAME, joinedTableKey(viewPath), branchName);
    final UserBitShared.QueryProfile profile =
        getQueryProfile(createJobRequestFromSqlAndSessionId(query, sessionId));

    assertEquals(DATAPLANE_PLUGIN_NAME, profile.getContextInfo().getSchemaPathContext());
    assertEquals(
        VersionContext.ofBranch(branchName).toString(),
        profile.getContextInfo().getSourceVersionSettingList().get(0).getVersionContext());
  }
}
