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

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import com.dremio.catalog.model.VersionContext;
import com.dremio.common.util.FileUtils;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.io.Files;

/**
 * All the constant declarations for OSS Dataplane Integration Tests
 */
public final class DataplaneTestDefines {

  private DataplaneTestDefines() {
  }
  // Constants
  public static final String S3_PREFIX = "s3://";
  public static final String BUCKET_NAME = "test.dataplane.bucket";
  public static final String ALTERNATIVE_BUCKET_NAME = "test.alternative.bucket";
  public static final String DATAPLANE_PLUGIN_NAME = "dataPlane_Test";
  public static final String DATAPLANE_PLUGIN_NAME_FOR_REFLECTION_TEST = "dataPlane_Test2";
  public static final String METADATA_FOLDER = "metadata";
  public static final String DEFAULT_BRANCH_NAME = "main";
  private static final String DEFAULT_TABLE_NAME_PREFIX = "table";
  private static final String DEFAULT_VIEW_NAME_PREFIX = "view";
  private static final String DEFAULT_FOLDER_NAME_PREFIX = "folder";
  private static final String DEFAULT_BRANCH_NAME_PREFIX = "branch";
  private static final String DEFAULT_TAG_NAME_PREFIX = "tag";
  private static final String DEFAULT_RAW_REF_NAME_PREFIX = "rawref";

  public static final String NO_ANCESTOR =
    "2e1cfa82b035c26cbbbdae632cea070514eb8b773f616aaeaf668e2f0be8f10d";


  // Query components
  public static final String DEFAULT_COLUMN_DEFINITION = "(id int, name varchar, distance Decimal(38, 3))";
  public static final String FIRST_DEFAULT_VALUE_CLAUSE =
    " (1, 'first row', 1000)";
  public static final String SECOND_DEFAULT_VALUE_CLAUSE =
    " (2, 'second row', 2000)";
  public static final String THIRD_DEFAULT_VALUE_CLAUSE =
    " (3, 'third row', 3000)";
  public static final String DEFAULT_VALUES_CLAUSE =
    " values" + String.join(",", Arrays.asList(FIRST_DEFAULT_VALUE_CLAUSE, SECOND_DEFAULT_VALUE_CLAUSE, THIRD_DEFAULT_VALUE_CLAUSE));
  public static final String DEFAULT_COUNT_COLUMN = "C";
  public static final String USER_NAME = "anonymous";
  public static final String DEFAULT_RECORD_DELIMITER = "(RECORD_DELIMITER '\n')";
  public static final String folderA = "folderA";
  public static final String folderB = "folderB";
  public static final String tableA = "tableA";

  public enum OptimizeMode {
    REWRITE_DATA,
    REWRITE_MANIFESTS,
    REWRITE_ALL
  }

  // Randomizers for preventing concurrency issues
  static int randomInt() {
    return ThreadLocalRandom.current().nextInt(1, 100000);
  }

  public static String generateUniqueTableName() {
    return DEFAULT_TABLE_NAME_PREFIX + randomInt();
  }

  public static String generateUniqueViewName() {
    return DEFAULT_VIEW_NAME_PREFIX + randomInt();
  }

  public static String generateUniqueFolderName() {
    return DEFAULT_FOLDER_NAME_PREFIX + randomInt();
  }

  public static String generateUniqueBranchName() {
    return DEFAULT_BRANCH_NAME_PREFIX + randomInt();
  }

  public static String generateUniqueTagName() {
    return DEFAULT_TAG_NAME_PREFIX + randomInt();
  }

  public static String generateUniqueRawRefName() { return DEFAULT_TABLE_NAME_PREFIX + randomInt(); }

  public static List<String> tablePathWithFolders(final String tableName) {
    Preconditions.checkNotNull(tableName);
    return Arrays.asList(
      generateUniqueFolderName(),
      generateUniqueFolderName(),
      tableName);
  }

  public static List<String> tablePathWithSource(
      final String sourceName, final List<String> tablePathList) {
    Preconditions.checkNotNull(tablePathList);
    Preconditions.checkArgument(!tablePathList.isEmpty());

    return new ArrayList<String>() {
      {
        add(sourceName);
        addAll(tablePathList);
      }
    };
  }

  public static List<String> generateSchemaPath() {
    return Arrays.asList(
      DATAPLANE_PLUGIN_NAME,
      generateUniqueFolderName(),
      generateUniqueFolderName());

  }

  public static List<String> generateFolderPath(final String folderName) {
    Preconditions.checkNotNull(folderName);
    return Arrays.asList(
      DATAPLANE_PLUGIN_NAME,
      folderName);
  }

  public static List<String> generateNestedFolderPath(final String parentFolderName, final String folderName) {
    Preconditions.checkNotNull(folderName);
    return Arrays.asList(
      DATAPLANE_PLUGIN_NAME,
      parentFolderName,
      folderName);
  }

  public static List<String> convertFolderNameToList(final String folderName) {
    Preconditions.checkNotNull(folderName);
    return Arrays.asList(
      folderName);
  }

  public static List<String> sqlFolderPathToNamespaceKey(final List<String> sqlFolderPath) {
    Preconditions.checkNotNull(sqlFolderPath);
    return sqlFolderPath.subList(1, sqlFolderPath.size());
  }

  public static String joinedTableKey(final List<String> tablePathList) {
    return String.join(".", tablePathList);
  }

  public static String fullyQualifiedTableName(String pluginName, List<String> tablePath) {
    return String.format("%s.%s", pluginName, joinedTableKey(tablePath));}

  // Query generators
  public static String createEmptyTableQuery(final List<String> tablePath) {
    Preconditions.checkNotNull(tablePath);
    return String.format(
      "CREATE TABLE %s.%s %s",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath),
      DEFAULT_COLUMN_DEFINITION);
  }

  public static String createEmptyTableQuery(final String sourceName, final List<String> tablePath) {
    Preconditions.checkNotNull(tablePath);
    return String.format(
      "CREATE TABLE %s.%s %s",
      sourceName,
      joinedTableKey(tablePath),
      DEFAULT_COLUMN_DEFINITION);
  }

  public static String createEmptyTableWithTablePropertiesQuery(final List<String> tablePath) {
    Preconditions.checkNotNull(tablePath);
    return String.format(
      "CREATE TABLE %s.%s %s TBLPROPERTIES ('property_name' = 'property_value')",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath),
      DEFAULT_COLUMN_DEFINITION);
  }

  public static String alterTableSetTablePropertiesQuery(final List<String> tablePath) {
    Preconditions.checkNotNull(tablePath);
    return String.format(
      "ALTER TABLE %s.%s SET TBLPROPERTIES ('property_name' = 'property_value')",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath));
  }

  public static String alterTableUnsetTablePropertiesQuery(final List<String> tablePath) {
    Preconditions.checkNotNull(tablePath);
    return String.format(
      "ALTER TABLE %s.%s UNSET TBLPROPERTIES ('property_name')",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath));
  }

  public static String showTablePropertiesQuery(final List<String> tablePath) {
    Preconditions.checkNotNull(tablePath);
    return String.format(
      "SHOW TBLPROPERTIES %s.%s",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath));
  }

  public static String createPartitionTableQuery(final List<String> tablePath) {
    Preconditions.checkNotNull(tablePath);
    return String.format(
      "CREATE TABLE %s.%s %s PARTITION BY (id)",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath),
      DEFAULT_COLUMN_DEFINITION);
  }

  public static String createEmptyTableQueryWithAt(final List<String> tablePath, String branchName) {
    Preconditions.checkNotNull(tablePath);
    return String.format(
      "CREATE TABLE %s.%s %s AT BRANCH %s",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath),
      DEFAULT_COLUMN_DEFINITION,
      branchName);
  }

  public static String createTableQueryWithAt(final List<String> tablePath, String branchName) {
    Preconditions.checkNotNull(tablePath);
    return String.format(
      "CREATE TABLE %s.%s %s AT BRANCH %s AS SELECT 1,2,3",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath),
      DEFAULT_COLUMN_DEFINITION,
      branchName);
  }

  public static String createViewQuery(final List<String> viewPath, final List<String> tablePath) {
    Preconditions.checkNotNull(viewPath);
    return String.format(
      "CREATE VIEW %s.%s AS SELECT * FROM %s.%s",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(viewPath),
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath)
    );
  }

  public static String createViewQueryWithAt(final List<String> viewPath, final List<String> tablePath, final String branchName) {
    Preconditions.checkNotNull(viewPath);
    return String.format(
      "CREATE VIEW %s.%s AT BRANCH %s AS SELECT * FROM %s.%s",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(viewPath),
      branchName,
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath)
    );
  }

  public static String createFolderQuery(final List<String> sqlFolderPath) {
    Preconditions.checkNotNull(sqlFolderPath);
    return String.format(
      "CREATE FOLDER %s",
      joinedTableKey(sqlFolderPath));
  }

  public static String createFolderAtQuery(final List<String> folderPath, VersionContext versionContext) {
    Preconditions.checkNotNull(folderPath);
    return String.format(
      "CREATE FOLDER %s AT %s %s",
      joinedTableKey(folderPath),
      versionContext.getType().name(),
      versionContext.getValue());
  }

  public static String createFolderAtQueryWithIfNotExists(final List<String> folderPath, VersionContext versionContext) {
    Preconditions.checkNotNull(folderPath);
    return String.format(
      "CREATE FOLDER IF NOT EXISTS %s AT %s %s",
      joinedTableKey(folderPath),
      versionContext.getType().name(),
      versionContext.getValue());
  }

  public static String dropFolderQuery(final List<String> sqlFolderPath) {
    Preconditions.checkNotNull(sqlFolderPath);
    return String.format(
      "DROP FOLDER %s",
      joinedTableKey(sqlFolderPath));
  }

  public static String dropFolderAtQuery(final List<String> folderPath, VersionContext versionContext) {
    Preconditions.checkNotNull(folderPath);
    return String.format(
      "DROP FOLDER %s AT %s %s",
      joinedTableKey(folderPath),
      versionContext.getType().name(),
      versionContext.getValue());
  }

  public static String dropFolderAtQueryWithIfNotExists(final List<String> folderPath, VersionContext versionContext) {
    Preconditions.checkNotNull(folderPath);
    return String.format(
      "DROP FOLDER IF NOT EXISTS %s AT %s %s",
      joinedTableKey(folderPath),
      versionContext.getType().name(),
      versionContext.getValue());
  }

  public static String createReplaceViewQuery(final List<String> viewPath, final List<String> tablePath) {
    Preconditions.checkNotNull(viewPath);
    return String.format(
      "CREATE OR REPLACE VIEW %s.%s AS SELECT * FROM %s.%s",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(viewPath),
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath)
    );
  }

  public static String dropViewQuery(final List<String> viewPath) {
    Preconditions.checkNotNull(viewPath);
    return String.format(
      "DROP VIEW %s.%s",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(viewPath)
    );
  }

  public static String dropViewQueryWithAt(final List<String> viewPath, final String devBranch) {
    Preconditions.checkNotNull(viewPath);
    return String.format(
      "DROP VIEW %s.%s AT BRANCH %s",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(viewPath),
      devBranch
    );
  }

  public static String createViewSelectQuery(final List<String> viewPath, final String sql) {
    Preconditions.checkNotNull(viewPath);
    return String.format(
      "CREATE VIEW %s.%s AS %s",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(viewPath),
      sql
    );
  }

  public static String createViewSelectQuery(String sourceName, final List<String> viewPath, final String sql) {
    Preconditions.checkNotNull(viewPath);
    return String.format(
      "CREATE VIEW %s.%s AS %s",
      sourceName,
      joinedTableKey(viewPath),
      sql
    );
  }

  public static String updateViewSelectQuery(final List<String> viewPath, final String sql) {
    Preconditions.checkNotNull(viewPath);
    return String.format(
      "CREATE OR REPLACE VIEW %s.%s AS %s",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(viewPath),
      sql
    );
  }

  public static String createViewQueryWithEmptySql(final List<String> viewPath, final List<String> tablePath) {
    Preconditions.checkNotNull(viewPath);
    return String.format(
      "CREATE VIEW %s.%s AS",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(viewPath)
    );
  }

  public static String createViewQueryWithIncompleteSql(final List<String> viewPath, final List<String> tablePath) {
    Preconditions.checkNotNull(viewPath);
    return String.format(
      "CREATE VIEW %s.%s AS SELECT * FROM",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(viewPath)
    );
  }

  public static String alterViewPropertyQuery(final List<String> viewPath, final String attribute, final String value) {
    Preconditions.checkNotNull(viewPath);
    return String.format(
      "ALTER VIEW %s.%s SET %s=%s",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(viewPath),
      attribute,
      value
    );
  }

  public static String alterViewPropertyQueryWithAt(final List<String> viewPath, final String attribute, final String value, final String devBranch) {
    Preconditions.checkNotNull(viewPath);
    return String.format(
      "ALTER VIEW %s.%s AT BRANCH %s SET %s=%s",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(viewPath),
      devBranch,
      attribute,
      value
    );
  }

  /**
   * @param colDefs
   *  Example format  "c1 int", "c2 int", "c3 varchar"
   */
  public static String createTableWithColDefsQuery(final List<String> tablePath, List<String> colDefs) {
    Preconditions.checkNotNull(tablePath);
    String columnDefsString = "(" + String.join(",", colDefs) + ")";
    return String.format(
      "CREATE TABLE %s.%s %s",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath),
      columnDefsString);
  }

  /**
   *
   * @param tablePath
   * @param colDefs Example format "c1 int", "c2 int", "c3 varchar"
   * @param sortColumns Example format "c1", "c2"
   * @return
   */
  public static String createSortedTableWithColDefsQuery(final List<String> tablePath, List<String> colDefs, List<String> sortColumns) {
    Preconditions.checkNotNull(tablePath);
    String columnDefsString = "(" + String.join(",", colDefs) + ")";
    String sortedColumnDefsString = "(" + String.join(",", sortColumns) + ")";
    return String.format(
      "CREATE TABLE %s.%s %s LOCALSORT BY %s",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath),
      columnDefsString,
      sortedColumnDefsString);
  }

  /**
   * @param colDefs
   * Example format  "c1 int", "c2 int", "c3 varchar"
   */
  public static String alterTableAddColumnsQuery(final List<String> tablePath, List<String> colDefs) {
    Preconditions.checkNotNull(tablePath);
    String columnDefsString =   "("+ String.join(",", colDefs) +")" ;
    return String.format("ALTER TABLE %s.%s add columns %s",
      DATAPLANE_PLUGIN_NAME,
      String.join(".", tablePath),
      columnDefsString);
  }

  public static String alterTableAddColumnsQueryWithAtSyntax(final List<String> tablePath, List<String> colDefs, String branchName) {
    Preconditions.checkNotNull(tablePath);
    String columnDefsString = "("+ String.join(",", colDefs) +")";
    return String.format("ALTER TABLE %s.%s AT BRANCH %s add columns %s",
      DATAPLANE_PLUGIN_NAME,
      String.join(".", tablePath),
      branchName,
      columnDefsString);
  }

  public static String alterTableDropColumnQuery(final List<String> tablePath, List<String> dropCols) {
    Preconditions.checkNotNull(tablePath);
    String dropColumnString = String.join(",", dropCols) ;
    return String.format("ALTER TABLE %s.%s drop column %s",
      DATAPLANE_PLUGIN_NAME,
      String.join(".",tablePath),
      dropColumnString);
  }

  public static String alterTableDropColumnQueryWithAtSyntax(final List<String> tablePath, List<String> dropCols, String branchName) {
    Preconditions.checkNotNull(tablePath);
    String dropColumnString = String.join(",", dropCols) ;
    return String.format("ALTER TABLE %s.%s AT BRANCH %s DROP COLUMN %s",
      DATAPLANE_PLUGIN_NAME,
      String.join(".",tablePath),
      branchName,
      dropColumnString);
  }

  public static String alterTableChangeColumnQuery(final List<String> tablePath, List<String> changeColumnList) {
    Preconditions.checkNotNull(tablePath);
    String changeColumns = String.join(",",changeColumnList);
    return String.format("ALTER TABLE %s.%s change column %s",
      DATAPLANE_PLUGIN_NAME,
      String.join(".",tablePath),
      changeColumns);
  }

  public static String alterTableChangeColumnQueryWithAtSyntax(final List<String> tablePath, List<String> changeColumnList, String branchName) {
    Preconditions.checkNotNull(tablePath);
    String changeColumns = String.join(",",changeColumnList);
    return String.format("ALTER TABLE %s.%s AT BRANCH %s MODIFY COLUMN %s",
      DATAPLANE_PLUGIN_NAME,
      String.join(".",tablePath),
      branchName,
      changeColumns);
  }

  public static String alterTableAddPrimaryKeyQuery(final List<String> tablePath, List<String> primaryKey) {
    Preconditions.checkNotNull(tablePath);
    String primaryKeyStr = String.join(",",primaryKey);
    return String.format("ALTER TABLE %s.%s ADD PRIMARY KEY (%s)",
      DATAPLANE_PLUGIN_NAME,
      String.join(".",tablePath),
      primaryKeyStr);
  }

  public static String alterTableAddPartitionQueryAt(final List<String> tablePath, String partitionField, String  branchname) {
    Preconditions.checkNotNull(tablePath);
    return String.format("ALTER TABLE %s.%s AT BRANCH %s ADD PARTITION FIELD %s",
      DATAPLANE_PLUGIN_NAME,
      String.join(".",tablePath),
      branchname,
      partitionField);
  }

  public static String alterTableReplaceSortOrder(final List<String> tablePath, List<String> sortOrder) {
    Preconditions.checkNotNull(tablePath);
    String sortStatement = "(" + String.join(",", sortOrder) + ")";
    return String.format("ALTER TABLE %s.%s LOCALSORT BY %s",
      DATAPLANE_PLUGIN_NAME, String.join(".", tablePath), sortStatement);
  }

  public static String alterTableAddPartitionQuery(final List<String> tablePath,String partitionField ) {
    Preconditions.checkNotNull(tablePath);
    return String.format("ALTER TABLE %s.%s  ADD PARTITION FIELD %s",
      DATAPLANE_PLUGIN_NAME,
      String.join(".",tablePath),
      partitionField);
  }
  public static String alterTableAddPrimaryKeyQueryWithAtSyntax(final List<String> tablePath, List<String> primaryKey, String branchName) {
    Preconditions.checkNotNull(tablePath);
    String primaryKeyStr = String.join(",",primaryKey);
    return String.format("ALTER TABLE %s.%s AT BRANCH %s ADD PRIMARY KEY (%s)",
      DATAPLANE_PLUGIN_NAME,
      String.join(".",tablePath),
      branchName,
      primaryKeyStr);
  }
  public static String alterTableDropPrimaryKeyQuery(final List<String> tablePath) {
    Preconditions.checkNotNull(tablePath);
    return String.format("ALTER TABLE %s.%s DROP PRIMARY KEY",
      DATAPLANE_PLUGIN_NAME,
      String.join(".",tablePath));
  }

  public static String alterTableDropPrimaryKeyQueryWithAtSyntax(final List<String> tablePath, String branchName) {
    Preconditions.checkNotNull(tablePath);
    return String.format("ALTER TABLE %s.%s AT BRANCH %s DROP PRIMARY KEY",
      DATAPLANE_PLUGIN_NAME,
      String.join(".",tablePath),
      branchName);
  }

  public static String alterTableModifyColumnQuery(final List<String> tablePath, final String columnName,
                                                   final List<String> newColDef) {
    Preconditions.checkNotNull(tablePath);
    return String.format("ALTER TABLE %s.%s MODIFY COLUMN %s %s",
      DATAPLANE_PLUGIN_NAME,
      String.join(".",tablePath),
      columnName,
      String.join(" ", newColDef));
  }

  public static String alterBranchAssignBranchQuery(final String branchName, final String sourceBranchName) {
    Preconditions.checkNotNull(branchName);
    Preconditions.checkNotNull(sourceBranchName);
    return String.format("ALTER BRANCH %s ASSIGN BRANCH %s in %s",
      branchName,
      sourceBranchName,
      DATAPLANE_PLUGIN_NAME
      );
  }

  public static String alterBranchAssignTagQuery(final String branchName, final String tagName) {
    Preconditions.checkNotNull(branchName);
    Preconditions.checkNotNull(tagName);
    return String.format("ALTER BRANCH %s ASSIGN TAG %s in %s",
      branchName,
      tagName,
      DATAPLANE_PLUGIN_NAME
    );
  }

  public static String alterBranchAssignCommitQuery(final String branchName, final String commitHash) {
    Preconditions.checkNotNull(branchName);
    Preconditions.checkNotNull(commitHash);
    return String.format("ALTER BRANCH %s ASSIGN COMMIT %s in %s",
      branchName,
      commitHash,
      DATAPLANE_PLUGIN_NAME
    );
  }

  public static String alterBranchAssignSpecifierQuery(final String branchName, final String specifer) {
    Preconditions.checkNotNull(branchName);
    Preconditions.checkNotNull(specifer);
    return String.format("ALTER BRANCH %s ASSIGN %s in %s",
      branchName,
      specifer,
      DATAPLANE_PLUGIN_NAME
    );
  }

  public static String alterTagAssignTagQuery(final String tagName, final String sourceTagName) {
    Preconditions.checkNotNull(tagName);
    Preconditions.checkNotNull(sourceTagName);
    return String.format("ALTER TAG %s ASSIGN TAG %s in %s",
      tagName,
      sourceTagName,
      DATAPLANE_PLUGIN_NAME
    );
  }

  public static String alterTagAssignBranchQuery(final String tagName, final String branchName) {
    Preconditions.checkNotNull(tagName);
    Preconditions.checkNotNull(branchName);
    return String.format("ALTER TAG %s ASSIGN BRANCH %s in %s",
      tagName,
      branchName,
      DATAPLANE_PLUGIN_NAME
    );
  }

  public static String alterTagAssignCommitQuery(final String tagName, final String commitHash) {
    Preconditions.checkNotNull(tagName);
    Preconditions.checkNotNull(commitHash);
    return String.format("ALTER TAG %s ASSIGN COMMIT %s in %s",
      tagName,
      commitHash,
      DATAPLANE_PLUGIN_NAME
    );
  }

  public static String alterTagAssignSpecifierQuery(final String tagName, final String specifer) {
    Preconditions.checkNotNull(tagName);
    Preconditions.checkNotNull(specifer);
    return String.format("ALTER TAG %s ASSIGN %s in %s",
      tagName,
      specifer,
      DATAPLANE_PLUGIN_NAME
    );
  }

  public static String alterViewPropertyQuery(
      final String viewName, final String propertyName, final String propertyValue) {
    Preconditions.checkNotNull(viewName);
    Preconditions.checkNotNull(propertyName);
    Preconditions.checkNotNull(propertyValue);

    return String.format("ALTER VIEW %s SET %s = %s", viewName, propertyName, propertyValue);
  }

  public static String selectCountQuery(final List<String> tablePath, String countColumn) {
    Preconditions.checkNotNull(tablePath);
    return String.format("SELECT count(*) %s from %s.%s",
        countColumn,
        DATAPLANE_PLUGIN_NAME,
        String.join(".", tablePath));
  }

  public static String selectCountAtBranchQuery(final List<String> tablePath, String atBranch, String countColumn) {
    Preconditions.checkNotNull(tablePath);
    return String.format("SELECT count(*) %s from %s.%s at BRANCH %s",
      countColumn,
      DATAPLANE_PLUGIN_NAME,
      String.join(".", tablePath),
      atBranch);
  }

  public static String selectCountSnapshotQuery(final List<String> tablePath, String countColumn) {
    Preconditions.checkNotNull(tablePath);
    return String.format("SELECT count(*) as %s FROM table(table_snapshot('%s.%s'))",
      countColumn,
      DATAPLANE_PLUGIN_NAME,
      String.join(".", tablePath));
  }

  public static String selectCountDataFilesQuery(final List<String> tablePath, String countColumn) {
    Preconditions.checkNotNull(tablePath);
    return String.format("SELECT count(*) as %s FROM table(table_files('%s.%s'))",
      countColumn,
      DATAPLANE_PLUGIN_NAME,
      String.join(".", tablePath));
  }

  public static String selectCountTablePartitionQuery(final List<String> tablePath, String countColumn) {
    Preconditions.checkNotNull(tablePath);
    return String.format("SELECT count(*) as %s FROM table(table_partitions('%s.%s'))",
      countColumn,
      DATAPLANE_PLUGIN_NAME,
      String.join(".", tablePath));
  }

  public static String selectCountManifestsQuery(final List<String> tablePath, String countColumn) {
    Preconditions.checkNotNull(tablePath);
    return String.format("SELECT count(*) as %s FROM table(table_manifests('%s.%s'))",
      countColumn,
      DATAPLANE_PLUGIN_NAME,
      String.join(".", tablePath));
  }

  public static String selectCountQueryWithSpecifier(List<String> tablePath, String countColumn, String specifier) {
    Preconditions.checkNotNull(tablePath);
    return String.format("SELECT count(*) %s from %s.%s AT %s",
        countColumn,
        DATAPLANE_PLUGIN_NAME,
        String.join(".", tablePath),
        specifier);
  }

  public static String selectStarQueryWithSpecifier(List<String> tablePath, String specifier) {
    Preconditions.checkNotNull(tablePath);
    return String.format("SELECT * from %s.%s AT %s",
      DATAPLANE_PLUGIN_NAME,
      String.join(".", tablePath),
      specifier);
  }

  public static String selectStarQueryWithSnapshotAndSpecifier(List<String> tablePath, long snapshot, String specifier) {
    Preconditions.checkNotNull(tablePath);
    return String.format("SELECT * from %s.%s AT %s",
      DATAPLANE_PLUGIN_NAME,
      String.join(".", tablePath),
      specifier);
  }

  public static String truncateTableQuery(final List<String> tablePath) {
    Preconditions.checkNotNull(tablePath);
    return String.format("TRUNCATE TABLE %s.%s ",
        DATAPLANE_PLUGIN_NAME,
        String.join(".", tablePath));
  }

  public static String selectStarQuery(final List<String> tablePath) {
    Preconditions.checkNotNull(tablePath);
    return String.format("SELECT * from %s.%s",
      DATAPLANE_PLUGIN_NAME,
      String.join(".", tablePath));
  }

  public static String selectStarOnSnapshotQuery(final List<String> tablePath, String snapshot) {
    Preconditions.checkNotNull(tablePath);
    return String.format("SELECT * from %s.%s AT SNAPSHOT '%s'",
      DATAPLANE_PLUGIN_NAME,
      String.join(".", tablePath),
      snapshot);
  }

  public static String selectStarQueryWithoutSpecifyingSource(final List<String> tablePath) {
    Preconditions.checkNotNull(tablePath);
    return String.format("SELECT * from %s", String.join(".", tablePath));
  }

  public static String dropTableQuery(final List<String> tablePath) {
    Preconditions.checkNotNull(tablePath);
    return String.format("DROP TABLE %s.%s ",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath));
  }

  public static String dropTableQueryWithAt(final List<String> tablePath, String branchName) {
    Preconditions.checkNotNull(tablePath);
    return String.format("DROP TABLE %s.%s AT BRANCH %s",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath),
      branchName);
  }


  public static String dropTableIfExistsQuery(final List<String> tablePath) {
    Preconditions.checkNotNull(tablePath);
    return String.format("DROP TABLE IF EXISTS %s.%s ",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath));
  }

  public static String insertTableQuery(final List<String> tablePath) {
    Preconditions.checkNotNull(tablePath);
    return String.format("INSERT INTO %s.%s  %s",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath),
      DEFAULT_VALUES_CLAUSE);
  }

  public static String insertTableAtQuery(final List<String> tablePath, final String atBranch) {
    Preconditions.checkNotNull(tablePath);
    return String.format("INSERT INTO %s.%s AT BRANCH %s %s",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath),
      atBranch,
      DEFAULT_VALUES_CLAUSE);
  }

  public static String insertTableAtQueryWithRef(final List<String> tablePath, final String atBranch) {
    Preconditions.checkNotNull(tablePath);
    return String.format("INSERT INTO %s.%s AT REF %s %s",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath),
      atBranch,
      DEFAULT_VALUES_CLAUSE);
  }

  public static String insertTableAtQueryWithSelect(final List<String> tablePath, final String atBranch, final List<String> selectTablePath, final String specifier) {
    Preconditions.checkNotNull(tablePath);
    return String.format("INSERT INTO %s.%s AT BRANCH %s %s",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath),
      atBranch,
      selectStarQueryWithSpecifier(selectTablePath, specifier));
  }

  public static String copyIntoTableQuery(final List<String> tablePath, String filePath, String fileName) {
    Preconditions.checkNotNull(tablePath);
    Preconditions.checkNotNull(filePath);
    return String.format("COPY INTO %s.%s FROM %s FILES(\'%s\') %s",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath),
      filePath,
      fileName,
      DEFAULT_RECORD_DELIMITER);
  }

  public static String copyIntoTableQueryWithAt(final List<String> tablePath, String filePath, String fileName, String branchName) {
    Preconditions.checkNotNull(tablePath);
    Preconditions.checkNotNull(filePath);
    return String.format("COPY INTO %s.%s AT BRANCH %s FROM %s FILES(\'%s\') %s",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath),
      branchName,
      filePath,
      fileName,
      DEFAULT_RECORD_DELIMITER);
  }

  public static String optimizeTableQuery(final List<String> tablePath, OptimizeMode mode) {
    String modeContext = "";
    switch (mode) {
      case REWRITE_DATA:
        modeContext = "REWRITE DATA (MIN_INPUT_FILES=2)";
        break;
      case REWRITE_MANIFESTS:
        modeContext = "REWRITE MANIFESTS";
        break;
      case REWRITE_ALL:
      default:
        modeContext = "(MIN_INPUT_FILES=2)"; // default mode
        break;
    }
    Preconditions.checkNotNull(tablePath);
    return String.format("OPTIMIZE TABLE %s.%s %s",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath),
      modeContext);
  }

  public static String rollbackTableQuery(final List<String> tablePath, long timestampInMillis) {
    Preconditions.checkNotNull(tablePath);
    String timestamp = getTimestampFromMillis(timestampInMillis);
    return String.format("ROLLBACK TABLE %s.%s TO TIMESTAMP '%s'",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath),
      timestamp);
  }

  public static String vacuumTableQuery(final List<String> tablePath, long timestampInMillis) {
    Preconditions.checkNotNull(tablePath);
    String timestamp = getTimestampFromMillis(timestampInMillis);
    return String.format("VACUUM TABLE %s.%s EXPIRE SNAPSHOTS OLDER_THAN = '%s'",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath),
      timestamp);
  }

  public static String selectFileLocationsQuery(final List<String> tablePath) {
    Preconditions.checkNotNull(tablePath);
    return String.format("SELECT file_path FROM TABLE(TABLE_FILES('%s.%s'))", DATAPLANE_PLUGIN_NAME, joinedTableKey(tablePath));
  }

  public static String joinTpcdsTablesQuery() {
    return String.format("SELECT * FROM cp.tpch.\"customer.parquet\"" +
      "JOIN cp.tpch.\"orders.parquet\" ON TRUE ");
  }

  public static String joinTablesQuery(String table1, String table2, String condition) {
    return String.format("Select * from %s.%s JOIN %s.%s  ON %s" ,
      DATAPLANE_PLUGIN_NAME, table1,
      DATAPLANE_PLUGIN_NAME, table2,
      condition);
  }

  public static String joinTablesQueryWithAtBranchSyntax(String table1, String branch1, String table2, String branch2, String condition) {
    return String.format("Select * from %s.%s AT BRANCH %s INNER JOIN %s.%s AT BRANCH %s ON %s",
      DATAPLANE_PLUGIN_NAME, table1,
      branch1,
      DATAPLANE_PLUGIN_NAME, table2,
      branch2,
      condition);
  }

  public static String joinTablesQueryWithAtBranchSyntaxRightSide(String table1, String table2, String branch, String condition) {
    return String.format("Select * from %s.%s INNER JOIN %s.%s AT BRANCH %s ON %s",
      DATAPLANE_PLUGIN_NAME, table1,
      DATAPLANE_PLUGIN_NAME, table2,
      branch,
      condition);
  }

  public static String joinTablesQueryWithAtBranchSyntaxLeftSide(String table1, String branch, String table2, String condition) {
    return String.format("Select * from %s.%s AT BRANCH %s INNER JOIN %s.%s ON %s",
      DATAPLANE_PLUGIN_NAME, table1,
      branch,
      DATAPLANE_PLUGIN_NAME, table2,
      condition);
  }

  public static String joinTablesQueryWithAtBranchSyntaxAndExpression(String table1, String branch, String table2, String exprTable, String condition) {
    return String.format("Select * from %s.%s AT BRANCH %s INNER JOIN (SELECT * FROM %s.%s) AS %s ON %s",
      DATAPLANE_PLUGIN_NAME, table1,
      branch,
      DATAPLANE_PLUGIN_NAME, table2,
      exprTable,
      condition);
  }

  public static String joinConditionWithFullyQualifiedTableName(String table1, String table2) {
    return String.format("%s.%s.id = %s.%s.id", DATAPLANE_PLUGIN_NAME, table1, DATAPLANE_PLUGIN_NAME, table2);
  }

  public static String joinConditionWithTableName(String table1, String table2) {
    return String.format("%s.id = %s.id", table1, table2);
  }

  /**
   * @param valuesList
   *  Example format : "(1,1)", "(2,2)", "(3,3)"
   */
  public static String insertTableWithValuesQuery(final List<String> tablePath, List<String> valuesList) {
    Preconditions.checkNotNull(tablePath);
    String valuesString = "values" + String.join(",", valuesList);
    return String.format("INSERT INTO %s.%s  %s",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath),
      valuesString);
  }

  public static String createTableAsQuery(final List<String> tablePath, final int limit) {
    Preconditions.checkNotNull(tablePath);
    return String.format(
      "CREATE TABLE %s.%s  "
        + " AS SELECT n_nationkey, n_regionkey from cp.\"tpch/nation.parquet\" limit %d",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath),
      limit);
  }

  public static String createTableAsQueryWithAt(final List<String> tablePath, final int limit, final String branchName) {
    Preconditions.checkNotNull(tablePath);
    return String.format(
      "CREATE TABLE %s.%s AT BRANCH %s"
        + " AS SELECT n_nationkey, n_regionkey from cp.\"tpch/nation.parquet\" limit %d",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath),
      branchName,
      limit);
  }

  public static String insertSelectQuery(final List<String> tablePath, final int limit) {
    Preconditions.checkNotNull(tablePath);
    return String.format(
      "INSERT INTO %s.%s "
        + "SELECT n_nationkey, n_regionkey from cp.\"tpch/nation.parquet\" limit %d",
      DATAPLANE_PLUGIN_NAME, joinedTableKey(tablePath), limit);
  }

  public static String deleteAllQuery(final String source, final List<String> tablePath) {
    return String.format("DELETE FROM %s", Strings.isNullOrEmpty(source)
      ? joinedTableKey(tablePath)
      : String.format("%s.%s", source, joinedTableKey(tablePath)));
  }

  public static String deleteAllQuery(final List<String> tablePath) {
    return deleteAllQuery(DATAPLANE_PLUGIN_NAME, tablePath);
  }

  public static String deleteAllQueryWithoutContext(final List<String> tablePath) {
    return deleteAllQuery(null, tablePath);
  }


  public static String deleteQueryWithSpecifier(final List<String> tablePath, String specifier) {
    return String.format("DELETE FROM %s.%s %s",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath),
      specifier);
  }

  public static String updateByIdQuery(final List<String> tablePath) {
    return String.format(
      "UPDATE %s.%s"
        + " SET distance = CAST(30000 AS DECIMAL(38,3)) WHERE id = 3",
      DATAPLANE_PLUGIN_NAME, joinedTableKey(tablePath));
  }

  public static String updateByIdFromAnotherBranchQuery(final List<String> tablePath, String selectBranchName) {
    return String.format(
      "UPDATE %s.%s"
        + " SET distance = (SELECT distance FROM %s.%s AT BRANCH %s WHERE id = 4 LIMIT 1) WHERE id = 3",
      DATAPLANE_PLUGIN_NAME, joinedTableKey(tablePath),
      DATAPLANE_PLUGIN_NAME, joinedTableKey(tablePath), selectBranchName);
  }

  public static String mergeByIdQuery(final List<String> targetTablePath, final List<String> sourceTablePath) {
    String target = String.format("%s.%s", DATAPLANE_PLUGIN_NAME, joinedTableKey(targetTablePath));
    String source = String.format("%s.%s", DATAPLANE_PLUGIN_NAME, joinedTableKey(sourceTablePath));
    return String.format(
      "MERGE INTO %s USING %s ON (%s.id = %s.id)"
        + " WHEN MATCHED THEN UPDATE SET distance = CAST(1 AS DECIMAL(38,3))"
        + " WHEN NOT MATCHED THEN INSERT VALUES (4, CAST('fourth row' AS VARCHAR(65536)), CAST(0 AS DECIMAL(38,3)))",
      target, source, target, source);
  }

  public static String createBranchAtBranchQuery(final String branchName, final String parentBranchName) {
    return createBranchAtSpecifierQuery(branchName, "BRANCH " + parentBranchName);
  }

  public static String createBranchFromBranchQuery(final String branchName, final String parentBranchName) {
    return createBranchFromSpecifierQuery(branchName, "BRANCH " + parentBranchName);
  }

  public static String showBranchQuery(final String sourceName) {
    return String.format("SHOW BRANCHES IN %s", sourceName);
  }

  public static String showBranchQuery() {
    return String.format("SHOW BRANCHES ");
  }

  public static String createBranchAtSpecifierQuery(final String branchName, final String specifier) {
    Preconditions.checkNotNull(branchName);
    Preconditions.checkNotNull(specifier);
    return String.format("CREATE BRANCH %s AT %s in %s",
      branchName,
      specifier,
      DATAPLANE_PLUGIN_NAME);
  }

  public static String createBranchFromSpecifierQuery(final String branchName, final String specifier) {
    Preconditions.checkNotNull(branchName);
    Preconditions.checkNotNull(specifier);
    return String.format("CREATE BRANCH %s FROM %s in %s",
      branchName,
      specifier,
      DATAPLANE_PLUGIN_NAME);
  }

  public static String mergeBranchQuery(final String branchName, final String targetBranchName) {
    Preconditions.checkNotNull(branchName);
    Preconditions.checkNotNull(targetBranchName);

    return String.format("MERGE BRANCH %s INTO %s in %s",
      branchName,
      targetBranchName,
      DATAPLANE_PLUGIN_NAME);
  }

  public static String createTagQuery(final String tagName, final String branchName) {
    Preconditions.checkNotNull(tagName);
    Preconditions.checkNotNull(branchName);
    return String.format("CREATE TAG %s AT BRANCH %s in %s",
      tagName,
      branchName,
      DATAPLANE_PLUGIN_NAME
    );
  }

  public static String createTagQueryWithFrom(final String tagName, final String branchName) {
    Preconditions.checkNotNull(tagName);
    Preconditions.checkNotNull(branchName);
    return String.format("CREATE TAG %s FROM BRANCH %s in %s",
      tagName,
      branchName,
      DATAPLANE_PLUGIN_NAME
    );
  }

  public static String createTagAtSpecifierQuery(final String tagName, final String specifier) {
    Preconditions.checkNotNull(tagName);
    Preconditions.checkNotNull(specifier);
    return String.format("CREATE TAG %s AT %s in %s",
      tagName,
      specifier,
      DATAPLANE_PLUGIN_NAME);
  }

  public static String showTagQuery(final String sourceName) {
    return String.format("SHOW TAGS IN %s", sourceName);
  }

  public static String showTagQuery() {
    return String.format("SHOW TAGS ");
  }

  public static String useContextQuery() {
    return String.format("USE %s", DATAPLANE_PLUGIN_NAME);
  }

  public static String useContextQuery(List<String> workspaceSchema) {
    String workspaceSchemaPath = joinedTableKey(workspaceSchema);
    return String.format("USE %s", workspaceSchemaPath);
  }

  public static String useBranchQuery(final String branchName) {
    Preconditions.checkNotNull(branchName);
    return String.format("USE BRANCH %s IN %s", branchName, DATAPLANE_PLUGIN_NAME);
  }

  public static String useTagQuery(final String tagName) {
    Preconditions.checkNotNull(tagName);
    return String.format("USE TAG %s IN %s", tagName, DATAPLANE_PLUGIN_NAME);
  }

  public static String useCommitQuery(final String commitHash) {
    Preconditions.checkNotNull(commitHash);
    return String.format("USE COMMIT %s IN %s", quoted(commitHash), DATAPLANE_PLUGIN_NAME);
  }

  // Nearly identical to useReferenceQuery, but we support both syntaxes
  public static String useRefQuery(final String refName) {
    Preconditions.checkNotNull(refName);
    return String.format("USE REF %s IN %s", quoted(refName), DATAPLANE_PLUGIN_NAME);
  }

  // Nearly identical to useRefQuery, but we support both syntaxes
  public static String useReferenceQuery(final String referenceName) {
    Preconditions.checkNotNull(referenceName);
    return String.format("USE REFERENCE %s IN %s", quoted(referenceName), DATAPLANE_PLUGIN_NAME);
  }

  public static String useSpecifierQuery(final String specifier) {
    Preconditions.checkNotNull(specifier);
    return String.format("USE %s IN %s", specifier, DATAPLANE_PLUGIN_NAME);
  }

  public static String dropBranchForceQuery(final String branchName) {
    Preconditions.checkNotNull(branchName);
    return String.format("DROP BRANCH %s FORCE IN %s", branchName, DATAPLANE_PLUGIN_NAME);
  }

  public static String dropBranchQuery(final String branchName) {
    Preconditions.checkNotNull(branchName);
    return String.format("DROP BRANCH %s IN %s", branchName, DATAPLANE_PLUGIN_NAME);
  }

  public static String dropBranchAtCommitQuery(final String branchName, final String commitHash) {
    Preconditions.checkNotNull(branchName);
    return String.format("DROP BRANCH %s AT COMMIT \"%s\" IN %s", branchName, commitHash, DATAPLANE_PLUGIN_NAME);
  }

  public static String dropTagForceQuery(final String tagName) {
    Preconditions.checkNotNull(tagName);
    return String.format("DROP TAG %s FORCE IN %s", tagName, DATAPLANE_PLUGIN_NAME);
  }

  public static String dropTagQuery(final String tagName) {
    Preconditions.checkNotNull(tagName);
    return String.format("DROP TAG %s IN %s", tagName, DATAPLANE_PLUGIN_NAME);
  }

  public static String dropTagBranchAtCommitQuery(final String tagName, final String commitHash) {
    Preconditions.checkNotNull(tagName);
    return String.format("DROP Tag %s AT COMMIT \"%s\" IN %s", tagName, commitHash, DATAPLANE_PLUGIN_NAME);
  }

  public static String showBranchesQuery() {
    return String.format("SHOW BRANCHES IN %s", DATAPLANE_PLUGIN_NAME);
  }

  public static String quoted(String string) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(string));
    return "\"" + string + "\"";
  }

  public static File generateSourceFiles(String fileName, File location) throws Exception {
    String relativePath = String.format("/copyinto/%s", fileName);
    File newSourceFile = new File(location.toString(), fileName);
    File oldSourceFile = FileUtils.getResourceAsFile(relativePath);
    Files.copy(oldSourceFile, newSourceFile);

    return newSourceFile;
  }

  public static String createRawReflection(final List<String> tablePath, final String rawRefName, List<String> colNames){
    Preconditions.checkNotNull(tablePath);
    String commaSeparatedDisplayNames = String.join(",", colNames);
    return String.format("ALTER TABLE %s.%s CREATE RAW REFLECTION %s using display (%s)",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath),
      rawRefName,
      commaSeparatedDisplayNames

    );
  }

  public static String createRawReflectionAtSpecifierQuery(final List<String> tablePath, final String specifier,
                                                           final String reflectionName, final List<String> colNames) {
    Preconditions.checkNotNull(tablePath);
    String commaSeparatedDisplayNames = String.join(",", colNames);
    return String.format(
      "ALTER DATASET %s.%s AT %s CREATE RAW REFLECTION %s USING DISPLAY (%s)",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath),
      specifier,
      reflectionName,
      commaSeparatedDisplayNames
    );
  }

  public static String createAggReflectionAtSpecifierQuery(final List<String> tablePath, final String specifier, final String reflectionName) {
    Preconditions.checkNotNull(tablePath);
    return String.format(
      "ALTER DATASET %s.%s AT %s CREATE AGGREGATE REFLECTION %s USING DIMENSIONS (n_nationkey) MEASURES (n_regionkey (COUNT, MAX, MIN))",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath),
      specifier,
      reflectionName
    );
  }

  public static String selectCountMinMaxByGroupAtSpecifierQuery(final List<String> SourcePath, final String specifier) {
    Preconditions.checkNotNull(SourcePath);
    return String.format("SELECT COUNT(n_regionkey), MIN(n_regionkey), MAX(n_regionkey) from %s.%s AT %s GROUP BY n_nationkey",
      DATAPLANE_PLUGIN_NAME,
      String.join(".", SourcePath),
      specifier
    );
  }

  public static String selectColsQuery(final List<String> SourcePath, final List<String> colNames) {
    Preconditions.checkNotNull(SourcePath);
    String commaSeparatedDisplayNames = String.join(",", colNames);
    return String.format("SELECT %s from %s.%s",
      commaSeparatedDisplayNames,
      DATAPLANE_PLUGIN_NAME,
      String.join(".", SourcePath)
    );
  }

  public static String selectColsAtSpecifierQuery(final List<String> SourcePath, final String specifier, final List<String> colNames) {
    Preconditions.checkNotNull(SourcePath);
    String commaSeparatedDisplayNames = String.join(",", colNames);
    return String.format("SELECT %s from %s.%s AT %s",
      commaSeparatedDisplayNames,
      DATAPLANE_PLUGIN_NAME,
      String.join(".", SourcePath),
      specifier
    );
  }

  public static String dropReflectionAtSpecifierQuery(final List<String> tablePath, final String specifier, final String reflectionName) {
    Preconditions.checkNotNull(tablePath);
    return String.format(
      "ALTER DATASET %s.%s AT %s DROP REFLECTION %s",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath),
      specifier,
      reflectionName
    );
  }

  public static String disableRawReflectionAtSpecifierQuery(final List<String> tablePath, final String specifier) {
    Preconditions.checkNotNull(tablePath);
    return String.format(
      "ALTER DATASET %s.%s AT %s DISABLE RAW ACCELERATION",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath),
      specifier
    );
  }

  public static String disableAggReflectionAtSpecifierQuery(final List<String> tablePath, final String specifier) {
    Preconditions.checkNotNull(tablePath);
    return String.format(
      "ALTER DATASET %s.%s AT %s DISABLE AGGREGATE ACCELERATION",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath),
      specifier
    );
  }

  public static String enableRawReflectionAtSpecifierQuery(final List<String> tablePath, final String specifier) {
    Preconditions.checkNotNull(tablePath);
    return String.format(
      "ALTER DATASET %s.%s AT %s ENABLE RAW ACCELERATION",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath),
      specifier
    );
  }

  public static String enableAggReflectionAtSpecifierQuery(final List<String> tablePath, final String specifier) {
    Preconditions.checkNotNull(tablePath);
    return String.format(
      "ALTER DATASET %s.%s AT %s ENABLE AGGREGATE ACCELERATION",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath),
      specifier
    );
  }

  public static String createRawReflectionQuery(final List<String> tablePath, final String reflectionName, final List<String> colNames) {
    Preconditions.checkNotNull(tablePath);
    String commaSeparatedDisplayNames = String.join(",", colNames);
    return String.format(
      "ALTER DATASET %s.%s CREATE RAW REFLECTION %s USING DISPLAY (%s)",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath),
      reflectionName,
      commaSeparatedDisplayNames
    );
  }

  public static String createAggReflectionQuery(final List<String> tablePath, final String reflectionName) {
    Preconditions.checkNotNull(tablePath);
    return String.format(
      "ALTER DATASET %s.%s CREATE AGGREGATE REFLECTION %s USING DIMENSIONS (n_nationkey) MEASURES (n_regionkey (COUNT, MAX, MIN))",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath),
      reflectionName
    );
  }

  public static String dropReflectionQuery(final List<String> tablePath, final String reflectionName) {
    Preconditions.checkNotNull(tablePath);
    return String.format(
      "ALTER DATASET %s.%s DROP REFLECTION %s",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath),
      reflectionName
    );
  }

  public static String disableRawReflectionQuery(final List<String> tablePath) {
    Preconditions.checkNotNull(tablePath);
    return String.format(
      "ALTER DATASET %s.%s DISABLE RAW ACCELERATION",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath)
    );
  }

  public static String disableAggReflectionQuery(final List<String> tablePath) {
    Preconditions.checkNotNull(tablePath);
    return String.format(
      "ALTER DATASET %s.%s DISABLE AGGREGATE ACCELERATION",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath)
    );
  }

  public static String enableRawReflectionQuery(final List<String> tablePath) {
    Preconditions.checkNotNull(tablePath);
    return String.format(
      "ALTER DATASET %s.%s ENABLE RAW ACCELERATION",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath)
    );
  }

  public static String enableAggReflectionQuery(final List<String> tablePath) {
    Preconditions.checkNotNull(tablePath);
    return String.format(
      "ALTER DATASET %s.%s ENABLE AGGREGATE ACCELERATION",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath)
    );
  }

  public static String getSysReflectionsQuery(final List<String> tablePath) {
    Preconditions.checkNotNull(tablePath);
    return String.format(
      "SELECT * FROM sys.reflections WHERE dataset_name = '%s.%s'",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath)
    );
  }

  public static String getSnapshotTableQuery(final List<String> tablePath) {
    Preconditions.checkNotNull(tablePath);
    return String.format("SELECT * FROM table(table_snapshot('%s.%s'))",
      DATAPLANE_PLUGIN_NAME,
      String.join(".", tablePath));
  }

  public static String getSnapshotIdQueryWithSpecifier(final List<String> tablePath, String specifier) {
    Preconditions.checkNotNull(tablePath);
    return String.format("SELECT snapshot_id FROM table(table_snapshot('%s.%s')) AT %s",
      DATAPLANE_PLUGIN_NAME,
      String.join(".", tablePath),
      specifier);
  }

  public static String getLastSnapshotQuery(final List<String> tablePath) {
    Preconditions.checkNotNull(tablePath);
    return String.format("SELECT snapshot_id FROM table(table_snapshot('%s.%s')) order by committed_at DESC LIMIT 1",
        DATAPLANE_PLUGIN_NAME,
        String.join(".", tablePath));
  }

  public static String createViewAtSpecifierQuery(final List<String> viewPath, final List<String> tablePath, final String specifier) {
    Preconditions.checkNotNull(viewPath);
    return String.format(
      "CREATE VIEW %s.%s AS SELECT * FROM %s.%s AT %s",
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(viewPath),
      DATAPLANE_PLUGIN_NAME,
      joinedTableKey(tablePath),
      specifier
    );
  }

  public static String showObjectWithSpecifierQuery(final String object, final String specifer) {
    Preconditions.checkNotNull(specifer);
    return String.format("SHOW %s %s IN %s",
      object,
      specifer,
      DATAPLANE_PLUGIN_NAME
    );
  }

  public static String updateAtQuery(String sourceName, String tableName, String branchName) {
    return String.format(
      "update %s.%s at branch %s set EXPR$0 = 2",
      sourceName,
      tableName,
      branchName);
  }

  public static String updateAtQueryWithAtRef(String sourceName, String tableName, String branchName) {
    return String.format(
      "UPDATE %s.%s at REF %s set EXPR$0 = 2",
      sourceName,
      tableName,
      branchName);
  }

  private static String getTimestampFromMillis(long timestampInMillis) {
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS000000");
    return simpleDateFormat.format(new Date(timestampInMillis));
  }
}
