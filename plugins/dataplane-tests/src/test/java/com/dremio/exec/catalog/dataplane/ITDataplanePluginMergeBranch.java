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

import static com.dremio.exec.ExecConstants.ENABLE_MERGE_BRANCH_BEHAVIOR;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.DATAPLANE_PLUGIN_NAME;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.DEFAULT_BRANCH_NAME;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createBranchAtBranchQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createEmptyTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.createTableQueryWithAt;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.dropTableQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueBranchName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.generateUniqueTableName;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.getFakeEmployeeData;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.insertTableWithValuesQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.mergeBranchWithMergeOptions;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.selectStarQuery;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.tablePathWithFolders;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.tablePathWithSource;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.useBranchQuery;
import static com.dremio.exec.planner.sql.parser.ParserUtil.sqlToMergeBehavior;
import static org.assertj.core.api.Assertions.assertThat;

import com.dremio.exec.catalog.dataplane.test.ITDataplanePluginTestSetup;
import com.dremio.plugins.MergeBranchOptions;
import com.google.common.collect.ImmutableList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.jupiter.api.Test;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.MergeBehavior;

public class ITDataplanePluginMergeBranch extends ITDataplanePluginTestSetup {

  private static final String DISCARD = "DISCARD";
  private static final String OVERWRITE = "OVERWRITE";
  private static final String CANCEL = "CANCEL";
  private static final SqlLiteral DISCARD_SQL =
      SqlLiteral.createCharString(DISCARD, SqlParserPos.ZERO);
  private static final SqlLiteral OVERWRITE_SQL =
      SqlLiteral.createCharString(OVERWRITE, SqlParserPos.ZERO);
  private static final SqlLiteral CANCEL_SQL =
      SqlLiteral.createCharString(CANCEL, SqlParserPos.ZERO);

  private MergeBranchOptions buildMergeBranchOptions(
      boolean dryRun,
      String defaultMergeBehaviorSql,
      Map<ContentKey, MergeBehavior> mergeBehaviorMap) {
    return MergeBranchOptions.builder()
        .setDryRun(dryRun)
        .setDefaultMergeBehavior(
            sqlToMergeBehavior(
                SqlLiteral.createCharString(defaultMergeBehaviorSql, SqlParserPos.ZERO)))
        .setMergeBehaviorMap(mergeBehaviorMap)
        .build();
  }

  @Test
  public void mergeBranchNormalRun() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createEmptyTableQuery(tablePath));

    final String branchName = generateUniqueBranchName();
    runSQL(createBranchAtBranchQuery(branchName, DEFAULT_BRANCH_NAME));
    runSQL(useBranchQuery(branchName));

    // create table in dev
    final String tableName2 = generateUniqueTableName();
    final List<String> tablePath2 = tablePathWithFolders(tableName2);
    runSQL(createEmptyTableQuery(tablePath2));

    MergeBranchOptions mergeBranchOptions =
        MergeBranchOptions.builder()
            .setDryRun(false)
            .setDefaultMergeBehavior(MergeBehavior.NORMAL)
            .build();

    assertThat(
            runSqlWithResults(
                mergeBranchWithMergeOptions(branchName, DEFAULT_BRANCH_NAME, mergeBranchOptions)))
        .matches(result -> result.get(0).get(0).contains("has been merged"))
        .matches(result -> result.get(0).get(2).equals("SUCCESS"));

    // cleanup
    runSQL(dropTableQuery(tablePath));
    runSQL(dropTableQuery(tablePath2));
  }

  @Test
  public void mergeBranchFailsDueToNoCommit() throws Exception {
    final String branchName = generateUniqueBranchName();
    runSQL(createBranchAtBranchQuery(branchName, DEFAULT_BRANCH_NAME));
    runSQL(useBranchQuery(branchName));

    MergeBranchOptions mergeBranchOptions =
        MergeBranchOptions.builder()
            .setDryRun(false)
            .setDefaultMergeBehavior(MergeBehavior.NORMAL)
            .build();

    assertThat(
            runSqlWithResults(
                mergeBranchWithMergeOptions(branchName, DEFAULT_BRANCH_NAME, mergeBranchOptions)))
        .matches(result -> result.get(0).get(0).contains("Failed to merge"))
        .matches(result -> result.get(0).get(2).equals("FAILURE"));
  }

  @Test
  public void mergeBranchDryRun() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createEmptyTableQuery(tablePath));
    final String mainHash = getCommitHashForBranch(DEFAULT_BRANCH_NAME);

    final String branchName = generateUniqueBranchName();
    runSQL(createBranchAtBranchQuery(branchName, DEFAULT_BRANCH_NAME));
    runSQL(useBranchQuery(branchName));

    // create table in dev
    final String tableName2 = generateUniqueTableName();
    final List<String> tablePath2 = tablePathWithFolders(tableName2);
    runSQL(createEmptyTableQuery(tablePath2));
    final String devHash = getCommitHashForBranch(branchName);

    MergeBranchOptions mergeBranchOptions =
        MergeBranchOptions.builder()
            .setDryRun(true)
            .setDefaultMergeBehavior(MergeBehavior.NORMAL)
            .build();

    assertThat(
            runSqlWithResults(
                mergeBranchWithMergeOptions(branchName, DEFAULT_BRANCH_NAME, mergeBranchOptions)))
        .matches(result -> result.get(0).get(0).contains("can be merged"))
        .matches(result -> result.get(0).get(2).equals("SUCCESS"));

    assertThat(getCommitHashForBranch(DEFAULT_BRANCH_NAME)).isEqualTo(mainHash);
    assertThat(getCommitHashForBranch(branchName)).isEqualTo(devHash);

    // cleanup
    runSQL(dropTableQuery(tablePath));
    runSQL(dropTableQuery(tablePath2));
  }

  @Test
  public void mergeBranchOnConflictOverwriteSucceeds() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createTableQueryWithAt(tablePath, DEFAULT_BRANCH_NAME));

    final String branchName = generateUniqueBranchName();
    runSQL(createBranchAtBranchQuery(branchName, DEFAULT_BRANCH_NAME));

    List<String> mainValues = ImmutableList.of("(4, 5, 6)");
    runSQL(insertTableWithValuesQuery(tablePath, mainValues));

    runSQL(useBranchQuery(branchName));
    List<String> devValues = ImmutableList.of("(7, 8, 9)");
    runSQL(insertTableWithValuesQuery(tablePath, devValues));

    MergeBranchOptions mergeBranchOptions =
        MergeBranchOptions.builder()
            .setDryRun(false)
            .setDefaultMergeBehavior(MergeBehavior.FORCE)
            .build();

    assertThat(
            runSqlWithResults(
                mergeBranchWithMergeOptions(branchName, DEFAULT_BRANCH_NAME, mergeBranchOptions)))
        .matches(result -> result.get(0).get(0).contains("has been merged"))
        .matches(result -> result.get(0).get(2).equals("SUCCESS"));

    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));

    // This will succeed because we have ON CONFLICT OVERWRITE
    assertThat(runSqlWithResults(selectStarQuery(tablePath)))
        .hasSize(2)
        .contains(ImmutableList.of("1", "2", "3"))
        .contains(ImmutableList.of("7", "8", "9"));

    runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void mergeBranchOnConflictDiscardSucceeds() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createTableQueryWithAt(tablePath, DEFAULT_BRANCH_NAME));

    final String branchName = generateUniqueBranchName();
    runSQL(createBranchAtBranchQuery(branchName, DEFAULT_BRANCH_NAME));

    List<String> mainValues = ImmutableList.of("(4, 5, 6)");
    runSQL(insertTableWithValuesQuery(tablePath, mainValues));

    runSQL(useBranchQuery(branchName));
    List<String> devValues = ImmutableList.of("(7, 8, 9)");
    runSQL(insertTableWithValuesQuery(tablePath, devValues));

    MergeBranchOptions mergeBranchOptions =
        MergeBranchOptions.builder()
            .setDryRun(false)
            .setDefaultMergeBehavior(MergeBehavior.DROP)
            .build();

    assertThat(
            runSqlWithResults(
                mergeBranchWithMergeOptions(branchName, DEFAULT_BRANCH_NAME, mergeBranchOptions)))
        .matches(result -> result.get(0).get(0).contains("has been merged"))
        .matches(result -> result.get(0).get(2).equals("SUCCESS"));

    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));

    assertThat(runSqlWithResults(selectStarQuery(tablePath)))
        .hasSize(2)
        .contains(ImmutableList.of("1", "2", "3"))
        .contains(ImmutableList.of("4", "5", "6"));

    runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void mergeBranchOnConflictCancelSucceeds() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createTableQueryWithAt(tablePath, DEFAULT_BRANCH_NAME));

    final String branchName = generateUniqueBranchName();
    runSQL(createBranchAtBranchQuery(branchName, DEFAULT_BRANCH_NAME));

    List<String> mainValues = ImmutableList.of("(4, 5, 6)");
    runSQL(insertTableWithValuesQuery(tablePath, mainValues));

    runSQL(useBranchQuery(branchName));
    List<String> devValues = ImmutableList.of("(7, 8, 9)");
    runSQL(insertTableWithValuesQuery(tablePath, devValues));

    MergeBranchOptions mergeBranchOptions =
        MergeBranchOptions.builder()
            .setDryRun(false)
            .setDefaultMergeBehavior(MergeBehavior.NORMAL)
            .build();

    assertThat(
            runSqlWithResults(
                mergeBranchWithMergeOptions(branchName, DEFAULT_BRANCH_NAME, mergeBranchOptions)))
        .matches(result -> result.get(0).get(0).contains("Failed to merge"))
        .matches(result -> result.get(0).get(2).equals("FAILURE"));

    runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void mergeBranchOnConflictOverwriteExceptOverwriteThrows() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createTableQueryWithAt(tablePath, DEFAULT_BRANCH_NAME));

    final String branchName = generateUniqueBranchName();
    runSQL(createBranchAtBranchQuery(branchName, DEFAULT_BRANCH_NAME));

    List<String> mainValues = ImmutableList.of("(4, 5, 6)");
    runSQL(insertTableWithValuesQuery(tablePath, mainValues));

    runSQL(useBranchQuery(branchName));
    List<String> devValues = ImmutableList.of("(7, 8, 9)");
    runSQL(insertTableWithValuesQuery(tablePath, devValues));

    Map<ContentKey, MergeBehavior> contentMap = new HashMap<>();
    contentMap.put(ContentKey.of(tablePath), sqlToMergeBehavior(OVERWRITE_SQL));
    MergeBranchOptions mergeBranchOptions = buildMergeBranchOptions(false, OVERWRITE, contentMap);

    assertQueryThrowsExpectedError(
        mergeBranchWithMergeOptions(branchName, DEFAULT_BRANCH_NAME, mergeBranchOptions),
        "must be distinct");

    runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void mergeBranchOnConflictDiscardExceptDiscardThrows() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createTableQueryWithAt(tablePath, DEFAULT_BRANCH_NAME));

    final String branchName = generateUniqueBranchName();
    runSQL(createBranchAtBranchQuery(branchName, DEFAULT_BRANCH_NAME));

    List<String> mainValues = ImmutableList.of("(4, 5, 6)");
    runSQL(insertTableWithValuesQuery(tablePath, mainValues));

    runSQL(useBranchQuery(branchName));
    List<String> devValues = ImmutableList.of("(7, 8, 9)");
    runSQL(insertTableWithValuesQuery(tablePath, devValues));

    Map<ContentKey, MergeBehavior> contentMap = new HashMap<>();
    contentMap.put(ContentKey.of(tablePath), sqlToMergeBehavior(DISCARD_SQL));
    MergeBranchOptions mergeBranchOptions = buildMergeBranchOptions(false, DISCARD, contentMap);

    assertQueryThrowsExpectedError(
        mergeBranchWithMergeOptions(branchName, DEFAULT_BRANCH_NAME, mergeBranchOptions),
        "must be distinct");

    runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void mergeBranchOnConflictCancelExceptCancelThrows() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createTableQueryWithAt(tablePath, DEFAULT_BRANCH_NAME));

    final String branchName = generateUniqueBranchName();
    runSQL(createBranchAtBranchQuery(branchName, DEFAULT_BRANCH_NAME));

    List<String> mainValues = ImmutableList.of("(4, 5, 6)");
    runSQL(insertTableWithValuesQuery(tablePath, mainValues));

    runSQL(useBranchQuery(branchName));
    List<String> devValues = ImmutableList.of("(7, 8, 9)");
    runSQL(insertTableWithValuesQuery(tablePath, devValues));

    Map<ContentKey, MergeBehavior> contentMap = new HashMap<>();
    contentMap.put(ContentKey.of(tablePath), sqlToMergeBehavior(CANCEL_SQL));
    MergeBranchOptions mergeBranchOptions = buildMergeBranchOptions(false, CANCEL, contentMap);

    assertQueryThrowsExpectedError(
        mergeBranchWithMergeOptions(branchName, DEFAULT_BRANCH_NAME, mergeBranchOptions),
        "must be distinct");

    runSQL(dropTableQuery(tablePath));
  }

  @Test
  public void mergeBranchOnConflictOverwriteExceptDiscardSucceeds() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createTableQueryWithAt(tablePath, DEFAULT_BRANCH_NAME));

    final String tableName1 = generateUniqueTableName();
    final List<String> tablePath1 = tablePathWithFolders(tableName1);
    runSQL(createTableQueryWithAt(tablePath1, DEFAULT_BRANCH_NAME));

    final String branchName = generateUniqueBranchName();
    runSQL(createBranchAtBranchQuery(branchName, DEFAULT_BRANCH_NAME));

    List<String> mainValues = ImmutableList.of("(4, 5, 6)");
    runSQL(insertTableWithValuesQuery(tablePath, mainValues));
    runSQL(insertTableWithValuesQuery(tablePath1, mainValues));

    runSQL(useBranchQuery(branchName));
    List<String> devValues = ImmutableList.of("(7, 8, 9)");
    runSQL(insertTableWithValuesQuery(tablePath, devValues));
    runSQL(insertTableWithValuesQuery(tablePath1, devValues));

    Map<ContentKey, MergeBehavior> contentMap = new HashMap<>();
    contentMap.put(
        ContentKey.of((tablePathWithSource(DATAPLANE_PLUGIN_NAME, tablePath))),
        sqlToMergeBehavior(DISCARD_SQL));
    MergeBranchOptions mergeBranchOptions = buildMergeBranchOptions(false, OVERWRITE, contentMap);

    assertThat(
            runSqlWithResults(
                mergeBranchWithMergeOptions(branchName, DEFAULT_BRANCH_NAME, mergeBranchOptions)))
        .matches(result -> result.get(0).get(0).contains("has been merged"))
        .matches(result -> result.get(0).get(2).equals("SUCCESS"));

    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));

    // This should have [1, 2, 3] and [4, 5, 6], because we have EXCEPT DISCARD on tablePath.
    assertThat(runSqlWithResults(selectStarQuery(tablePath)))
        .hasSize(2)
        .contains(ImmutableList.of("1", "2", "3"))
        .contains(ImmutableList.of("4", "5", "6"));

    assertThat(runSqlWithResults(selectStarQuery(tablePath1)))
        .hasSize(2)
        .contains(ImmutableList.of("1", "2", "3"))
        .contains(ImmutableList.of("7", "8", "9"));

    runSQL(dropTableQuery(tablePath));
    runSQL(dropTableQuery(tablePath1));
  }

  @Test
  public void mergeBranchOnConflictCancelExceptDiscardExceptOverwriteSucceeds() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createTableQueryWithAt(tablePath, DEFAULT_BRANCH_NAME));

    final String tableName1 = generateUniqueTableName();
    final List<String> tablePath1 = tablePathWithFolders(tableName1);
    runSQL(createTableQueryWithAt(tablePath1, DEFAULT_BRANCH_NAME));

    final String tableName2 = generateUniqueTableName();
    final List<String> tablePath2 = tablePathWithFolders(tableName2);
    runSQL(createTableQueryWithAt(tablePath2, DEFAULT_BRANCH_NAME));

    final String branchName = generateUniqueBranchName();
    runSQL(createBranchAtBranchQuery(branchName, DEFAULT_BRANCH_NAME));

    List<String> mainValues = ImmutableList.of("(4, 5, 6)");
    runSQL(insertTableWithValuesQuery(tablePath1, mainValues));
    runSQL(insertTableWithValuesQuery(tablePath2, mainValues));

    runSQL(useBranchQuery(branchName));
    List<String> devValues = ImmutableList.of("(7, 8, 9)");
    runSQL(insertTableWithValuesQuery(tablePath1, devValues));
    runSQL(insertTableWithValuesQuery(tablePath2, devValues));

    Map<ContentKey, MergeBehavior> contentMap = new HashMap<>();
    contentMap.put(
        ContentKey.of(tablePathWithSource(DATAPLANE_PLUGIN_NAME, tablePath1)),
        sqlToMergeBehavior(DISCARD_SQL));
    contentMap.put(
        ContentKey.of(tablePathWithSource(DATAPLANE_PLUGIN_NAME, tablePath2)),
        sqlToMergeBehavior(OVERWRITE_SQL));
    MergeBranchOptions mergeBranchOptions = buildMergeBranchOptions(false, CANCEL, contentMap);

    assertThat(
            runSqlWithResults(
                mergeBranchWithMergeOptions(branchName, DEFAULT_BRANCH_NAME, mergeBranchOptions)))
        .matches(result -> result.get(0).get(0).contains("has been merged"))
        .matches(result -> result.get(0).get(2).equals("SUCCESS"));

    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));

    assertThat(runSqlWithResults(selectStarQuery(tablePath)))
        .hasSize(1)
        .contains(ImmutableList.of("1", "2", "3"));

    // This should have [1, 2, 3] and [4, 5, 6], because we have EXCEPT DISCARD on tablePath.
    assertThat(runSqlWithResults(selectStarQuery(tablePath1)))
        .hasSize(2)
        .contains(ImmutableList.of("1", "2", "3"))
        .contains(ImmutableList.of("4", "5", "6"));

    assertThat(runSqlWithResults(selectStarQuery(tablePath2)))
        .hasSize(2)
        .contains(ImmutableList.of("1", "2", "3"))
        .contains(ImmutableList.of("7", "8", "9"));

    runSQL(dropTableQuery(tablePath));
    runSQL(dropTableQuery(tablePath1));
    runSQL(dropTableQuery(tablePath2));
  }

  @Test
  public void mergeBranchOnConflictCancelExceptDiscardExceptOverwriteFails() throws Exception {
    final String tableName = generateUniqueTableName();
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createTableQueryWithAt(tablePath, DEFAULT_BRANCH_NAME));

    final String tableName1 = generateUniqueTableName();
    final List<String> tablePath1 = tablePathWithFolders(tableName1);
    runSQL(createTableQueryWithAt(tablePath1, DEFAULT_BRANCH_NAME));

    final String tableName2 = generateUniqueTableName();
    final List<String> tablePath2 = tablePathWithFolders(tableName2);
    runSQL(createTableQueryWithAt(tablePath2, DEFAULT_BRANCH_NAME));

    final String branchName = generateUniqueBranchName();
    runSQL(createBranchAtBranchQuery(branchName, DEFAULT_BRANCH_NAME));

    List<String> mainValues = ImmutableList.of("(4, 5, 6)");
    runSQL(insertTableWithValuesQuery(tablePath, mainValues));
    runSQL(insertTableWithValuesQuery(tablePath1, mainValues));
    runSQL(insertTableWithValuesQuery(tablePath2, mainValues));

    final String mainHash = getCommitHashForBranch(DEFAULT_BRANCH_NAME);

    runSQL(useBranchQuery(branchName));
    List<String> devValues = ImmutableList.of("(7, 8, 9)");
    runSQL(insertTableWithValuesQuery(tablePath, devValues));
    runSQL(insertTableWithValuesQuery(tablePath1, devValues));
    runSQL(insertTableWithValuesQuery(tablePath2, devValues));

    final String devHash = getCommitHashForBranch(branchName);

    Map<ContentKey, MergeBehavior> contentMap = new HashMap<>();
    contentMap.put(
        ContentKey.of(tablePathWithSource(DATAPLANE_PLUGIN_NAME, tablePath1)),
        sqlToMergeBehavior(DISCARD_SQL));
    contentMap.put(
        ContentKey.of(tablePathWithSource(DATAPLANE_PLUGIN_NAME, tablePath2)),
        sqlToMergeBehavior(OVERWRITE_SQL));
    MergeBranchOptions mergeBranchOptions = buildMergeBranchOptions(false, CANCEL, contentMap);

    // since we have conflict in tablePath, and the merge behavior is normal, the query fails and
    // does not perform any commit.
    assertThat(
            runSqlWithResults(
                mergeBranchWithMergeOptions(branchName, DEFAULT_BRANCH_NAME, mergeBranchOptions)))
        .matches(result -> result.get(0).get(0).contains("Failed to merge"))
        .matches(result -> result.get(0).get(2).equals("FAILURE"));

    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));

    assertThat(getCommitHashForBranch(DEFAULT_BRANCH_NAME)).isEqualTo(mainHash);
    assertThat(getCommitHashForBranch(branchName)).isEqualTo(devHash);

    runSQL(dropTableQuery(tablePath));
    runSQL(dropTableQuery(tablePath1));
    runSQL(dropTableQuery(tablePath2));
  }

  private void prepareTables(List<List<String>> data, String branchName) throws Exception {
    for (int i = 0; i < data.size(); i++) {
      runSQL(
          String.format(
              "CREATE TABLE %s.employee%s (\n"
                  + "    employee_id INT,\n"
                  + "    name VARCHAR(50),\n"
                  + "    department VARCHAR(50),\n"
                  + "    salary DECIMAL(10, 2));",
              DATAPLANE_PLUGIN_NAME, i));
      runSQL(
          String.format(
              "INSERT INTO %s.employee%s (employee_id, name, department, salary) VALUES (%s, '%s', '%s', %s)",
              DATAPLANE_PLUGIN_NAME,
              i,
              data.get(i).get(0),
              data.get(i).get(1),
              data.get(i).get(2),
              data.get(i).get(3)));
    }

    runSQL(createBranchAtBranchQuery(branchName, DEFAULT_BRANCH_NAME));
    runSQL(useBranchQuery(branchName));

    for (int i = 0; i < data.size(); i++) {
      runSQL(
          String.format("UPDATE %s.employee%s SET salary = salary * 2;", DATAPLANE_PLUGIN_NAME, i));
    }
  }

  @Test
  public void mergeBranchWithMergeOption() throws Exception {
    final String branchName = "next_year_proposal";
    final List<List<String>> data = getFakeEmployeeData();
    prepareTables(data, branchName);
    Map<ContentKey, MergeBehavior> contentMap = new HashMap<>();
    for (int i = 0; i < 3; i++) {
      contentMap.put(
          ContentKey.of(
              tablePathWithSource(DATAPLANE_PLUGIN_NAME, List.of(String.format("employee%s", i)))),
          sqlToMergeBehavior(DISCARD_SQL));
    }
    for (int i = 3; i < 6; i++) {
      contentMap.put(
          ContentKey.of(
              tablePathWithSource(DATAPLANE_PLUGIN_NAME, List.of(String.format("employee%s", i)))),
          sqlToMergeBehavior(OVERWRITE_SQL));
    }
    MergeBranchOptions mergeBranchOptions = buildMergeBranchOptions(false, CANCEL, contentMap);

    assertThat(
            runSqlWithResults(
                mergeBranchWithMergeOptions(branchName, DEFAULT_BRANCH_NAME, mergeBranchOptions)))
        .matches(result -> result.get(0).get(0).contains("has been merged"))
        .matches(result -> result.get(0).get(2).equals("SUCCESS"));

    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));

    for (int i = 0; i < 3; i++) {
      assertThat(runSqlWithResults(selectStarQuery(List.of(String.format("employee%s", i)))))
          .hasSize(1)
          .contains(data.get(i));
    }

    for (int i = 3; i < 6; i++) {
      assertThat(runSqlWithResults(selectStarQuery(List.of(String.format("employee%s", i)))))
          .hasSize(1)
          .contains(
              ImmutableList.of(
                  data.get(i).get(0),
                  data.get(i).get(1),
                  data.get(i).get(2),
                  String.format("%.2f", Double.parseDouble(data.get(i).get(3)) * 2)));
    }
  }

  @Test
  public void mergeBranchContentKeyWithDots() throws Exception {
    final String tableName = "m.y.t.a.b.l.e";
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createTableQueryWithAt(tablePath, DEFAULT_BRANCH_NAME));

    final String tableName1 = "m.y.t.a.b.l.e1";
    final List<String> tablePath1 = tablePathWithFolders(tableName1);
    runSQL(createTableQueryWithAt(tablePath1, DEFAULT_BRANCH_NAME));

    final String tableName2 = "m.y.t.a.b.l.e2";
    final List<String> tablePath2 = tablePathWithFolders(tableName2);
    runSQL(createTableQueryWithAt(tablePath2, DEFAULT_BRANCH_NAME));

    final String branchName = generateUniqueBranchName();
    runSQL(createBranchAtBranchQuery(branchName, DEFAULT_BRANCH_NAME));

    List<String> mainValues = ImmutableList.of("(4, 5, 6)");
    runSQL(insertTableWithValuesQuery(tablePath1, mainValues));
    runSQL(insertTableWithValuesQuery(tablePath2, mainValues));

    runSQL(useBranchQuery(branchName));
    List<String> devValues = ImmutableList.of("(7, 8, 9)");
    runSQL(insertTableWithValuesQuery(tablePath1, devValues));
    runSQL(insertTableWithValuesQuery(tablePath2, devValues));

    Map<ContentKey, MergeBehavior> contentMap = new HashMap<>();
    contentMap.put(
        ContentKey.of(tablePathWithSource(DATAPLANE_PLUGIN_NAME, tablePath1)),
        sqlToMergeBehavior(DISCARD_SQL));
    contentMap.put(
        ContentKey.of(tablePathWithSource(DATAPLANE_PLUGIN_NAME, tablePath2)),
        sqlToMergeBehavior(OVERWRITE_SQL));
    MergeBranchOptions mergeBranchOptions = buildMergeBranchOptions(false, CANCEL, contentMap);

    assertThat(
            runSqlWithResults(
                mergeBranchWithMergeOptions(branchName, DEFAULT_BRANCH_NAME, mergeBranchOptions)))
        .matches(result -> result.get(0).get(0).contains("has been merged"))
        .matches(result -> result.get(0).get(2).equals("SUCCESS"));

    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));

    assertThat(runSqlWithResults(selectStarQuery(tablePath)))
        .hasSize(1)
        .contains(ImmutableList.of("1", "2", "3"));

    // This should have [1, 2, 3] and [4, 5, 6], because we have EXCEPT DISCARD on tablePath.
    assertThat(runSqlWithResults(selectStarQuery(tablePath1)))
        .hasSize(2)
        .contains(ImmutableList.of("1", "2", "3"))
        .contains(ImmutableList.of("4", "5", "6"));

    assertThat(runSqlWithResults(selectStarQuery(tablePath2)))
        .hasSize(2)
        .contains(ImmutableList.of("1", "2", "3"))
        .contains(ImmutableList.of("7", "8", "9"));

    runSQL(dropTableQuery(tablePath));
    runSQL(dropTableQuery(tablePath1));
    runSQL(dropTableQuery(tablePath2));
  }

  @Test
  public void mergeBranchContentKeyWithUnderScore() throws Exception {
    final String tableName = "m_y_t_a_b_l_e";
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createTableQueryWithAt(tablePath, DEFAULT_BRANCH_NAME));

    final String tableName1 = "m_y_t_a_b_l_e1";
    final List<String> tablePath1 = tablePathWithFolders(tableName1);
    runSQL(createTableQueryWithAt(tablePath1, DEFAULT_BRANCH_NAME));

    final String tableName2 = "m_y_t_a_b_l_e2";
    final List<String> tablePath2 = tablePathWithFolders(tableName2);
    runSQL(createTableQueryWithAt(tablePath2, DEFAULT_BRANCH_NAME));

    final String branchName = generateUniqueBranchName();
    runSQL(createBranchAtBranchQuery(branchName, DEFAULT_BRANCH_NAME));

    List<String> mainValues = ImmutableList.of("(4, 5, 6)");
    runSQL(insertTableWithValuesQuery(tablePath1, mainValues));
    runSQL(insertTableWithValuesQuery(tablePath2, mainValues));

    runSQL(useBranchQuery(branchName));
    List<String> devValues = ImmutableList.of("(7, 8, 9)");
    runSQL(insertTableWithValuesQuery(tablePath1, devValues));
    runSQL(insertTableWithValuesQuery(tablePath2, devValues));

    Map<ContentKey, MergeBehavior> contentMap = new HashMap<>();
    contentMap.put(
        ContentKey.of(tablePathWithSource(DATAPLANE_PLUGIN_NAME, tablePath1)),
        sqlToMergeBehavior(DISCARD_SQL));
    contentMap.put(
        ContentKey.of(tablePathWithSource(DATAPLANE_PLUGIN_NAME, tablePath2)),
        sqlToMergeBehavior(OVERWRITE_SQL));
    MergeBranchOptions mergeBranchOptions = buildMergeBranchOptions(false, CANCEL, contentMap);

    assertThat(
            runSqlWithResults(
                mergeBranchWithMergeOptions(branchName, DEFAULT_BRANCH_NAME, mergeBranchOptions)))
        .matches(result -> result.get(0).get(0).contains("has been merged"))
        .matches(result -> result.get(0).get(2).equals("SUCCESS"));

    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));

    assertThat(runSqlWithResults(selectStarQuery(tablePath)))
        .hasSize(1)
        .contains(ImmutableList.of("1", "2", "3"));

    // This should have [1, 2, 3] and [4, 5, 6], because we have EXCEPT DISCARD on tablePath.
    assertThat(runSqlWithResults(selectStarQuery(tablePath1)))
        .hasSize(2)
        .contains(ImmutableList.of("1", "2", "3"))
        .contains(ImmutableList.of("4", "5", "6"));

    assertThat(runSqlWithResults(selectStarQuery(tablePath2)))
        .hasSize(2)
        .contains(ImmutableList.of("1", "2", "3"))
        .contains(ImmutableList.of("7", "8", "9"));

    runSQL(dropTableQuery(tablePath));
    runSQL(dropTableQuery(tablePath1));
    runSQL(dropTableQuery(tablePath2));
  }

  @Test
  public void mergeBranchContentKeyWithQuotes() throws Exception {
    final String tableName = "m\"y\"t\"a\"b\"l\"e";
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createTableQueryWithAt(tablePath, DEFAULT_BRANCH_NAME));

    final String tableName1 = "m\"y\"t\"a\"b\"l\"e1";
    final List<String> tablePath1 = tablePathWithFolders(tableName1);
    runSQL(createTableQueryWithAt(tablePath1, DEFAULT_BRANCH_NAME));

    final String tableName2 = "m\"y\"t\"a\"b\"l\"e2";
    final List<String> tablePath2 = tablePathWithFolders(tableName2);
    runSQL(createTableQueryWithAt(tablePath2, DEFAULT_BRANCH_NAME));

    final String branchName = generateUniqueBranchName();
    runSQL(createBranchAtBranchQuery(branchName, DEFAULT_BRANCH_NAME));

    List<String> mainValues = ImmutableList.of("(4, 5, 6)");
    runSQL(insertTableWithValuesQuery(tablePath1, mainValues));
    runSQL(insertTableWithValuesQuery(tablePath2, mainValues));

    runSQL(useBranchQuery(branchName));
    List<String> devValues = ImmutableList.of("(7, 8, 9)");
    runSQL(insertTableWithValuesQuery(tablePath1, devValues));
    runSQL(insertTableWithValuesQuery(tablePath2, devValues));

    Map<ContentKey, MergeBehavior> contentMap = new HashMap<>();
    contentMap.put(
        ContentKey.of(tablePathWithSource(DATAPLANE_PLUGIN_NAME, tablePath1)),
        sqlToMergeBehavior(DISCARD_SQL));
    contentMap.put(
        ContentKey.of(tablePathWithSource(DATAPLANE_PLUGIN_NAME, tablePath2)),
        sqlToMergeBehavior(OVERWRITE_SQL));
    MergeBranchOptions mergeBranchOptions = buildMergeBranchOptions(false, CANCEL, contentMap);

    assertThat(
            runSqlWithResults(
                mergeBranchWithMergeOptions(branchName, DEFAULT_BRANCH_NAME, mergeBranchOptions)))
        .matches(result -> result.get(0).get(0).contains("has been merged"))
        .matches(result -> result.get(0).get(2).equals("SUCCESS"));

    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));

    assertThat(runSqlWithResults(selectStarQuery(tablePath)))
        .hasSize(1)
        .contains(ImmutableList.of("1", "2", "3"));

    // This should have [1, 2, 3] and [4, 5, 6], because we have EXCEPT DISCARD on tablePath.
    assertThat(runSqlWithResults(selectStarQuery(tablePath1)))
        .hasSize(2)
        .contains(ImmutableList.of("1", "2", "3"))
        .contains(ImmutableList.of("4", "5", "6"));

    assertThat(runSqlWithResults(selectStarQuery(tablePath2)))
        .hasSize(2)
        .contains(ImmutableList.of("1", "2", "3"))
        .contains(ImmutableList.of("7", "8", "9"));

    runSQL(dropTableQuery(tablePath));
    runSQL(dropTableQuery(tablePath1));
    runSQL(dropTableQuery(tablePath2));
  }

  @Test
  public void mergeBranchContentKeyWithSlashes() throws Exception {
    final String tableName = "m\\y\\t\\a\\b\\l\\e";
    final List<String> tablePath = tablePathWithFolders(tableName);
    runSQL(createTableQueryWithAt(tablePath, DEFAULT_BRANCH_NAME));

    final String tableName1 = "m\\y\\t\\a\\b\\l\\e1";
    final List<String> tablePath1 = tablePathWithFolders(tableName1);
    runSQL(createTableQueryWithAt(tablePath1, DEFAULT_BRANCH_NAME));

    final String tableName2 = "m\\y\\t\\a\\b\\l\\e2";
    final List<String> tablePath2 = tablePathWithFolders(tableName2);
    runSQL(createTableQueryWithAt(tablePath2, DEFAULT_BRANCH_NAME));

    final String branchName = generateUniqueBranchName();
    runSQL(createBranchAtBranchQuery(branchName, DEFAULT_BRANCH_NAME));

    List<String> mainValues = ImmutableList.of("(4, 5, 6)");
    runSQL(insertTableWithValuesQuery(tablePath1, mainValues));
    runSQL(insertTableWithValuesQuery(tablePath2, mainValues));

    runSQL(useBranchQuery(branchName));
    List<String> devValues = ImmutableList.of("(7, 8, 9)");
    runSQL(insertTableWithValuesQuery(tablePath1, devValues));
    runSQL(insertTableWithValuesQuery(tablePath2, devValues));

    Map<ContentKey, MergeBehavior> contentMap = new HashMap<>();
    contentMap.put(
        ContentKey.of(tablePathWithSource(DATAPLANE_PLUGIN_NAME, tablePath1)),
        sqlToMergeBehavior(DISCARD_SQL));
    contentMap.put(
        ContentKey.of(tablePathWithSource(DATAPLANE_PLUGIN_NAME, tablePath2)),
        sqlToMergeBehavior(OVERWRITE_SQL));
    MergeBranchOptions mergeBranchOptions = buildMergeBranchOptions(false, CANCEL, contentMap);

    assertThat(
            runSqlWithResults(
                mergeBranchWithMergeOptions(branchName, DEFAULT_BRANCH_NAME, mergeBranchOptions)))
        .matches(result -> result.get(0).get(0).contains("has been merged"))
        .matches(result -> result.get(0).get(2).equals("SUCCESS"));

    runSQL(useBranchQuery(DEFAULT_BRANCH_NAME));

    assertThat(runSqlWithResults(selectStarQuery(tablePath)))
        .hasSize(1)
        .contains(ImmutableList.of("1", "2", "3"));

    // This should have [1, 2, 3] and [4, 5, 6], because we have EXCEPT DISCARD on tablePath.
    assertThat(runSqlWithResults(selectStarQuery(tablePath1)))
        .hasSize(2)
        .contains(ImmutableList.of("1", "2", "3"))
        .contains(ImmutableList.of("4", "5", "6"));

    assertThat(runSqlWithResults(selectStarQuery(tablePath2)))
        .hasSize(2)
        .contains(ImmutableList.of("1", "2", "3"))
        .contains(ImmutableList.of("7", "8", "9"));

    runSQL(dropTableQuery(tablePath));
    runSQL(dropTableQuery(tablePath1));
    runSQL(dropTableQuery(tablePath2));
  }

  @Test
  public void mergeBranchFeatureFlagTurnedOff() throws Exception {
    try {
      setSystemOption(ENABLE_MERGE_BRANCH_BEHAVIOR, "false");

      final String tableName = generateUniqueTableName();
      final List<String> tablePath = tablePathWithFolders(tableName);
      runSQL(createTableQueryWithAt(tablePath, DEFAULT_BRANCH_NAME));

      final String branchName = generateUniqueBranchName();
      runSQL(createBranchAtBranchQuery(branchName, DEFAULT_BRANCH_NAME));

      List<String> mainValues = ImmutableList.of("(4, 5, 6)");
      runSQL(insertTableWithValuesQuery(tablePath, mainValues));

      runSQL(useBranchQuery(branchName));
      List<String> devValues = ImmutableList.of("(7, 8, 9)");
      runSQL(insertTableWithValuesQuery(tablePath, devValues));

      MergeBranchOptions mergeBranchOptions =
          MergeBranchOptions.builder()
              .setDryRun(false)
              .setDefaultMergeBehavior(MergeBehavior.FORCE)
              .build();

      assertQueryThrowsExpectedError(
          mergeBranchWithMergeOptions(branchName, DEFAULT_BRANCH_NAME, mergeBranchOptions),
          "Specifying merge behavior is not supported");
    } finally {
      setSystemOption(ENABLE_MERGE_BRANCH_BEHAVIOR, "true");
    }
  }
}
