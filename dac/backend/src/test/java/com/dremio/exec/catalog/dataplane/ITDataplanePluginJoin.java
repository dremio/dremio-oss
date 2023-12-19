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
package com.dremio.exec.catalog.dataplane;

import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.DATAPLANE_PLUGIN_NAME;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.DEFAULT_BRANCH_NAME;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createBranchAtBranchQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createEmptyTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueBranchName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.insertTableQuery;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.joinConditionWithFullyQualifiedTableName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.joinConditionWithTableName;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.joinTablesQueryWithAtBranchSyntax;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.joinTablesQueryWithAtBranchSyntaxAndExpression;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.joinTablesQueryWithAtBranchSyntaxLeftSide;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.joinTablesQueryWithAtBranchSyntaxRightSide;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.joinedTableKey;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.tablePathWithFolders;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.useBranchQuery;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.dremio.catalog.model.VersionContext;

public class ITDataplanePluginJoin extends ITDataplanePluginTestSetup {

  @Test
  public void testJoinWithFullyQualifiedTableNamesInOnCondition() throws Exception {
    // Operations on the main branch
    final String tableNameInMainBranch = generateUniqueTableName();
    final List<String> tablePathMainBranch = tablePathWithFolders(tableNameInMainBranch);
    final String temporaryBranchName = generateUniqueBranchName();
    final String tableNameInTempBranch = generateUniqueTableName();
    final List<String> tablePathTempBranch = tablePathWithFolders(tableNameInTempBranch);

    createFolders(tablePathMainBranch, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePathMainBranch));
    runSQL(insertTableQuery(tablePathMainBranch));

    // Fork a branch from main
    runSQL(createBranchAtBranchQuery(temporaryBranchName, DEFAULT_BRANCH_NAME));

    // Operations on the temporary branch forked from main
    runSQL(useBranchQuery(temporaryBranchName));
    createFolders(tablePathTempBranch, VersionContext.ofBranch(temporaryBranchName));
    runSQL(createEmptyTableQuery(tablePathTempBranch));
    runSQL(insertTableQuery(tablePathTempBranch));

    // Act on Join now switching the context to main branch
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    String table1 = joinedTableKey(tablePathMainBranch);
    String table2 = joinedTableKey(tablePathTempBranch);
    String condition = joinConditionWithFullyQualifiedTableName(table1, table2);
    List<List<String>> joinResults = runSqlWithResults(
      joinTablesQueryWithAtBranchSyntax(table1, DEFAULT_BRANCH_NAME, table2, temporaryBranchName, condition));

    // Assert
    List<List<String>> expectedResults = new ArrayList<>();
    expectedResults.add(Arrays.asList("1", "first row", "1000.000", "1", "first row", "1000.000"));
    expectedResults.add(Arrays.asList("2", "second row", "2000.000", "2", "second row", "2000.000"));
    expectedResults.add(Arrays.asList("3", "third row", "3000.000", "3", "third row", "3000.000"));
    assertThat(joinResults).containsAll(expectedResults);
  }

  @Test
  public void testJoinReferenceWithATSyntaxAtRightSide() throws Exception {
    // Operations on the main branch
    final String tableNameInMainBranch = generateUniqueTableName();
    final List<String> tablePathMainBranch = tablePathWithFolders(tableNameInMainBranch);
    final String temporaryBranchName = generateUniqueBranchName();
    final String tableNameInTempBranch = generateUniqueTableName();
    final List<String> tablePathTempBranch = tablePathWithFolders(tableNameInTempBranch);

    createFolders(tablePathMainBranch, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePathMainBranch));
    runSQL(insertTableQuery(tablePathMainBranch));

    // Fork a branch from main
    runSQL(createBranchAtBranchQuery(temporaryBranchName, DEFAULT_BRANCH_NAME));

    // Operations on the temporary branch forked from main
    runSQL(useBranchQuery(temporaryBranchName));
    createFolders(tablePathTempBranch, VersionContext.ofBranch(temporaryBranchName));
    runSQL(createEmptyTableQuery(tablePathTempBranch));
    runSQL(insertTableQuery(tablePathTempBranch));

    // Act on Join now switching the context to main branch
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    String table1 = joinedTableKey(tablePathMainBranch);
    String table2 = joinedTableKey(tablePathTempBranch);
    String condition = joinConditionWithFullyQualifiedTableName(table1, table2);
    List<List<String>> joinResults = runSqlWithResults(
      joinTablesQueryWithAtBranchSyntaxRightSide(table1, table2, temporaryBranchName, condition));

    // Assert
    List<List<String>> expectedResults = new ArrayList<>();
    expectedResults.add(Arrays.asList("1", "first row", "1000.000", "1", "first row", "1000.000"));
    expectedResults.add(Arrays.asList("2", "second row", "2000.000", "2", "second row", "2000.000"));
    expectedResults.add(Arrays.asList("3", "third row", "3000.000", "3", "third row", "3000.000"));
    assertThat(joinResults).containsAll(expectedResults);
  }

  @Test
  public void testJoinReferenceWithATSyntaxAtLeftSide() throws Exception {
    // Operations on the main branch
    final String tableNameInMainBranch = generateUniqueTableName();
    final List<String> tablePathMainBranch = tablePathWithFolders(tableNameInMainBranch);
    final String temporaryBranchName = generateUniqueBranchName();
    final String tableNameInTempBranch = generateUniqueTableName();
    final List<String> tablePathTempBranch = tablePathWithFolders(tableNameInTempBranch);

    createFolders(tablePathMainBranch, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePathMainBranch));
    runSQL(insertTableQuery(tablePathMainBranch));

    // Fork a branch from main
    runSQL(createBranchAtBranchQuery(temporaryBranchName, DEFAULT_BRANCH_NAME));

    // Operations on the temporary branch forked from main
    runSQL(useBranchQuery(temporaryBranchName));
    createFolders(tablePathTempBranch, VersionContext.ofBranch(temporaryBranchName));
    runSQL(createEmptyTableQuery(tablePathTempBranch));
    runSQL(insertTableQuery(tablePathTempBranch));

    // Act on Join now switching the context to main branch
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    String table1 = joinedTableKey(tablePathMainBranch);
    String table2 = joinedTableKey(tablePathTempBranch);
    String condition = joinConditionWithFullyQualifiedTableName(table1, table2);
    List<List<String>> joinResults = runSqlWithResults(
      joinTablesQueryWithAtBranchSyntaxLeftSide(table2, temporaryBranchName, table1, condition));

    // Assert
    List<List<String>> expectedResults = new ArrayList<>();
    expectedResults.add(Arrays.asList("1", "first row", "1000.000", "1", "first row", "1000.000"));
    expectedResults.add(Arrays.asList("2", "second row", "2000.000", "2", "second row", "2000.000"));
    expectedResults.add(Arrays.asList("3", "third row", "3000.000", "3", "third row", "3000.000"));
    assertThat(joinResults).containsAll(expectedResults);
  }

  @Test
  public void testJoinReferencedWithATAndExpression() throws Exception {
    // Operations on the main branch
    final String tableNameInMainBranch = generateUniqueTableName();
    final List<String> tablePathMainBranch = tablePathWithFolders(tableNameInMainBranch);
    final String temporaryBranchName = generateUniqueBranchName();
    final String tableNameInTempBranch = generateUniqueTableName();
    final List<String> tablePathTempBranch = tablePathWithFolders(tableNameInTempBranch);

    createFolders(tablePathMainBranch, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePathMainBranch));
    runSQL(insertTableQuery(tablePathMainBranch));

    // Fork a branch from main
    runSQL(createBranchAtBranchQuery(temporaryBranchName, DEFAULT_BRANCH_NAME));

    // Operations on the temporary branch forked from main
    runSQL(useBranchQuery(temporaryBranchName));
    createFolders(tablePathTempBranch, VersionContext.ofBranch(temporaryBranchName));
    runSQL(createEmptyTableQuery(tablePathTempBranch));
    runSQL(insertTableQuery(tablePathTempBranch));

    // Act on Join now switching the context to main branch
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    final String exprTableName = generateUniqueTableName();
    String table1 = joinedTableKey(tablePathMainBranch);
    List<String> listTable2FQID = new ArrayList<>();
    listTable2FQID.add(DATAPLANE_PLUGIN_NAME);
    listTable2FQID.addAll(tablePathTempBranch);
    String table2FQID = joinedTableKey(listTable2FQID);
    String table2 = joinedTableKey(tablePathTempBranch);
    String condition = joinConditionWithTableName(exprTableName, table2FQID);
    List<List<String>> joinResults = runSqlWithResults(
      joinTablesQueryWithAtBranchSyntaxAndExpression(table2, temporaryBranchName, table1, exprTableName, condition));

    // Assert
    List<List<String>> expectedResults = new ArrayList<>();
    expectedResults.add(Arrays.asList("1", "first row", "1000.000", "1", "first row", "1000.000"));
    expectedResults.add(Arrays.asList("2", "second row", "2000.000", "2", "second row", "2000.000"));
    expectedResults.add(Arrays.asList("3", "third row", "3000.000", "3", "third row", "3000.000"));
    assertThat(joinResults).containsAll(expectedResults);
  }

  @Test
  public void testJoinWithTableNamesInOnCondition() throws Exception {
    // Operations on the main branch
    final String tableNameInMainBranch = generateUniqueTableName();
    final List<String> tablePathMainBranch = tablePathWithFolders(tableNameInMainBranch);
    final String temporaryBranchName = generateUniqueBranchName();
    final String tableNameInTempBranch = generateUniqueTableName();
    final List<String> tablePathTempBranch = tablePathWithFolders(tableNameInTempBranch);

    createFolders(tablePathMainBranch, VersionContext.ofBranch(DEFAULT_BRANCH_NAME));
    runSQL(createEmptyTableQuery(tablePathMainBranch));
    runSQL(insertTableQuery(tablePathMainBranch));

    // Fork a branch from main
    runSQL(createBranchAtBranchQuery(temporaryBranchName, DEFAULT_BRANCH_NAME));

    // Operations on the temporary branch forked from main
    runSQL(useBranchQuery(temporaryBranchName));
    createFolders(tablePathTempBranch, VersionContext.ofBranch(temporaryBranchName));
    runSQL(createEmptyTableQuery(tablePathTempBranch));
    runSQL(insertTableQuery(tablePathTempBranch));

    // Act on Join now switching the context to main branch
    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));
    String table1 = joinedTableKey(tablePathMainBranch);
    String table2 = joinedTableKey(tablePathTempBranch);
    String condition = joinConditionWithTableName(tableNameInMainBranch, tableNameInTempBranch);
    List<List<String>> joinResults = runSqlWithResults(
      joinTablesQueryWithAtBranchSyntax(table1, DEFAULT_BRANCH_NAME, table2, temporaryBranchName, condition));

    // Assert
    List<List<String>> expectedResults = new ArrayList<>();
    expectedResults.add(Arrays.asList("1", "first row", "1000.000", "1", "first row", "1000.000"));
    expectedResults.add(Arrays.asList("2", "second row", "2000.000", "2", "second row", "2000.000"));
    expectedResults.add(Arrays.asList("3", "third row", "3000.000", "3", "third row", "3000.000"));
    assertThat(joinResults).containsAll(expectedResults);
  }
}
