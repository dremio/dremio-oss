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
package com.dremio.service.reflection;

import static com.dremio.PlanTestBase.testPlanSubstrPatternsInOrderPlanInput;
import static com.dremio.PlanTestBase.testPlanWithAttributesMatchingPatternsPlanInput;
import static com.dremio.service.reflection.ReflectionServiceImpl.ACCELERATOR_STORAGEPLUGIN_NAME;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.dremio.service.jobs.JobNotFoundException;
import com.dremio.service.reflection.proto.Materialization;
import java.util.stream.Stream;
import org.apache.calcite.util.Pair;

public class SnapshotBasedIncrementalRefreshTestUtils {
  /**
   * Validate that we did NOT perform an Incremental Refresh by Snapshot for materialization
   *
   * @param planReflection reflection refresh plan to check for
   * @param tablePath iceberg table being scanned in plan
   * @param snapshotIdPairs snapshot pair candidates
   * @throws JobNotFoundException
   */
  public static void validateFullRefresh(
      final String planReflection,
      final String tablePath,
      final Pair<String, String>... snapshotIdPairs) {
    // we expect only the latest snapshot to be used
    for (Pair<String, String> snapshotIdPair : snapshotIdPairs) {
      testPlanSubstrPatternsInOrderPlanInput(
          planReflection,
          new String[] {
            String.format(
                "IcebergManifestList(table=[%s], snapshot=[%s]", tablePath, snapshotIdPair.right)
          },
          snapshotIdPair.right.equals(snapshotIdPair.left)
              ? null
              : new String[] {
                String.format(
                    "IcebergManifestList(table=[%s], snapshot=[%s]", tablePath, snapshotIdPair.left)
              });
    }
  }

  /**
   * Validate that we did NOT perform any incremental refresh The plan contains an Empty operator.
   * TablePath, beginning and ending snapshots are not part of the plan
   *
   * @param planReflection reflection refresh plan to check for
   * @param tablePath iceberg table being scanned in plan
   * @param snapshotIdPairs snapshot pair candidates
   * @throws JobNotFoundException
   */
  public static void validateNoRefresh(
      final String planReflection,
      final String tablePath,
      final Pair<String, String>... snapshotIdPairs) {
    // we expect an empty operator
    // tablePath, snapshotIdPair.left and snapshotIdPair.right should not be part of the plan at all
    for (Pair<String, String> snapshotIdPair : snapshotIdPairs) {
      testPlanSubstrPatternsInOrderPlanInput(
          planReflection,
          new String[] {"Empty("},
          new String[] {snapshotIdPair.left, snapshotIdPair.right, tablePath});
    }
  }

  /**
   * Validate that we did perform a Snapshot Based Incremental Refresh Also validate that the plan
   * for that Snapshot Based Incremental Refresh is correct
   *
   * @param planReflection reflection refresh plan to check for
   * @param tablePath iceberg table being scanned in plan
   * @param snapshotIdPairs a set of snapshot pairs to check that the plan is correct for
   * @throws Exception possible to throw exception
   */
  public static void validateSnapshotBasedIncrementalRefresh(
      final String planReflection,
      final String tablePath,
      final Pair<String, String>... snapshotIdPairs)
      throws Exception {
    for (final Pair<String, String> snapshotIdPair : snapshotIdPairs) {
      // Validate the order of the operators
      // We want to make sure the following operators are present in the plan,
      // and they are in this particular order
      testPlanSubstrPatternsInOrderPlanInput(
          planReflection,
          new String[] {
            "TableFunction",
            "IcebergSplitGen",
            "SelectionVectorRemover",
            "Filter(condition=[IS NULL(",
            "HashJoin",
            "IcebergManifestScan",
            String.format(
                "IcebergManifestList(table=[%s], snapshot=[%s]", tablePath, snapshotIdPair.right),
            "Project",
            "IcebergManifestScan",
            String.format(
                "IcebergManifestList(table=[%s], snapshot=[%s]", tablePath, snapshotIdPair.left)
          },
          null);
      // Substr check above has the limitation that it requires an exact match,
      // and some parts of the operators are different for every query.
      // We want to compare a portion after the difference.
      // Here we use regular expressions to skip any characters other than new line
      // that connect the relevant contents of the same operator
      testPlanWithAttributesMatchingPatternsPlanInput(
          planReflection,
          new String[] {
            ".*WriterCommitter(?s:(?!\\R).)*iceberg_operation=\\[INSERT\\].*",
            String.format(
                ".*TableFunction(?s:(?!\\R).)*Table Function Type=\\[DATA_FILE_SCAN\\].*table=\\[%s\\]*",
                tablePath),
            ".*IcebergSplitGen(?s:(?!\\R).)*Table Function Type=\\[ICEBERG_SPLIT_GEN\\].*",
            "SelectionVectorRemover",
            "Filter\\(condition=\\[IS NULL\\(\\$\\d+\\)\\]\\)",
            "HashJoin\\(condition=\\[=\\(\\$\\d+, \\$\\d+\\)\\], joinType=\\[left\\]\\)",
            ".*IcebergManifestScan(?s:(?!\\R).)*manifestContent=\\[DATA\\].*",
            String.format(
                "IcebergManifestList\\(table=\\[%s\\], snapshot=\\[%s\\](?s:(?!\\R).)*manifestContent=\\[DATA\\].*",
                tablePath, snapshotIdPair.right),
            String.format(
                "IcebergManifestList\\(table=\\[%s\\], snapshot=\\[%s\\](?s:(?!\\R).)*manifestContent=\\[DATA\\].*",
                tablePath, snapshotIdPair.left)
          },
          null);
    }
    // make sure that there is no UnionAll if there is just one pair
    // and that there is UnionAll otherwise
    if (snapshotIdPairs.length == 1) {
      assertFalse(planReflection.contains("UnionAll"));
    } else {
      assertTrue(planReflection.contains("UnionAll"));
    }
  }

  /**
   * Validate that we did perform a Snapshot Based Incremental Refresh Also validate that the plan
   * is optimized for the case where the snapshotID has not changed since the last refresh
   *
   * @param planReflection reflection refresh plan to check for
   * @param tablePath iceberg table being scanned in plan
   * @param snapshotIdPairs a set of snapshot pairs to check that the plan is correct for
   * @throws Exception possible to throw exception
   */
  public static void validateEmptySnapshotBasedIncrementalRefresh(
      final String planReflection,
      final String tablePath,
      final Pair<String, String>... snapshotIdPairs)
      throws Exception {
    for (final Pair<String, String> snapshotIdPair : snapshotIdPairs) {
      // Validate the order of the operators
      // We want to make sure the following operators are present in the plan,
      // and they are in this particular order
      testPlanSubstrPatternsInOrderPlanInput(
          planReflection,
          new String[] {"Empty("},
          new String[] {
            String.format(
                "IcebergManifestList\\(table=\\[%s\\], snapshot=\\[%s\\](?s:(?!\\R).)*manifestContent=\\[DATA\\].*",
                tablePath, snapshotIdPair.right),
            String.format(
                "IcebergManifestList\\(table=\\[%s\\], snapshot=\\[%s\\](?s:(?!\\R).)*manifestContent=\\[DATA\\].*",
                tablePath, snapshotIdPair.left)
          });
    }
  }

  /**
   * Validate that we did perform a Snapshot Based Incremental Refresh by Partition Also validate
   * that the plan for that Snapshot Based Incremental Refresh is correct
   */
  public static void validatePartitionBasedIncrementalRefresh(
      final String planReflection,
      final String baseTableName,
      final String beginSnapshotID,
      final String endSnapshotID,
      final String joinKeyTransform,
      final Materialization m) {
    try {
      // Validate the order of the operators
      // We want to make sure the following operators are present in the plan,
      // and they are in this particular order

      // verify common part for both delete and insert
      final String[] commonPartPlan =
          new String[] {
            "WriterCommitter",
            "iceberg_operation=[UPDATE]",
            "\n", // commits the changes, adding some files, deleting others
            "UnionAll(all=[true])",
            "\n"
          }; // union the files to add and delete for the reflection

      // verify the plan to select and insert new data into the reflection from the base dataset
      final String[] selectDataFromMainDatasetPlan =
          new String[] {
            // writes the manifests for the files to add
            "IcebergManifestWriter",
            "\n",
            // some operators are skipped here, as they are dependent on the particular query
            // ...
            // scans the base dataset
            "TableFunction",
            String.format("Table Function Type=[DATA_FILE_SCAN], table=[%s]", baseTableName),
            "\n",
            // split gen for the base dataset
            "IcebergSplitGen",
            "Table Function Type=[ICEBERG_SPLIT_GEN]",
            "\n",
            // main join on ICEBERG_INCREMENTAL_REFRESH_JOIN_KEY to filter the data to only include
            // partitions
            // that have been modified since last incremental update
            "HashJoin(condition=[=",
            "joinType=[inner]",
            "\n",
            // verify the main scan from the base table
            // calculate the affected partitions for the base dataset
            "IcebergIncrementalRefreshJoinKey",
            String.format("join key=[%s])", joinKeyTransform),
            "\n",
            // scan data files from the base dataset
            "IcebergManifestScan",
            "manifestContent=[DATA]",
            "\n",
            // scan manifest files from the end snapshot
            String.format(
                "IcebergManifestList(table=[%s], snapshot=[%s]", baseTableName, endSnapshotID),
            "\n"
          };

      // verify the plan to generate the list of partitions with any files added or deleted
      final String[] selectModifiedPartitionsPlan =
          new String[] {
            "Project(incrementalRefreshJoinKey0=[$0])",
            "\n",
            // remove duplicates, we could have duplicates if multiple files in the same partition
            // are added or deleted
            "HashAgg(group=[{0}])",
            "\n",
            // calculate the affected partitions for the reflection
            "IcebergIncrementalRefreshJoinKey",
            String.format("join key=[%s])", joinKeyTransform),
            "\n",
            // project, so that the columns from old and new snapshot align
            "Project(partitionSpecId=[CASE(IS NULL(",
            "partitionKey=[CASE(IS NULL(",
            "partitionInfo=[CASE(IS NULL(",
            "\n",
            "SelectionVectorRemover",
            "\n",
            // only keep files that are in the old snapshot or the new snapshot, but not both
            "Filter(condition=[OR(IS NULL",
            "IS NULL(",
            "\n",
            // join of file path
            "HashJoin(condition=[=",
            "joinType=[full]",
            "\n",
            // get all files (DATA or DELETE) from the end snapshot
            "IcebergManifestScan",
            "manifestContent=[ALL]",
            "\n",
            // get manifests for end snapshot
            String.format(
                "IcebergManifestList(table=[%s], snapshot=[%s]", baseTableName, endSnapshotID),
            "\n",
            "Project(",
            "\n",
            // get all files (DATA or DELETE) from the beginning snapshot
            "IcebergManifestScan",
            "manifestContent=[ALL]",
            "\n",
            // get manifests for old snapshot
            String.format(
                "IcebergManifestList(table=[%s], snapshot=[%s]", baseTableName, beginSnapshotID),
            "\n"
          };

      // verify the plan to delete from the reflection
      final String[] deleteFromReflectionPlan =
          new String[] {
            // build metadata for deleting files from the reflection
            "TableFunction",
            "Table Function Type=[DELETED_FILES_METADATA]",
            "\n",
            "Project(",
            "\n",
            // filter the files on ICEBERG_INCREMENTAL_REFRESH_JOIN_KEY to only files belonging to
            // affected partitions
            "HashJoin(condition=[=",
            "joinType=[inner]",
            "\n",
            // verify selecting the partitions to delete from the reflection
            // calculate join key to be used for filtering the reflection files
            "IcebergIncrementalRefreshJoinKey",
            String.format("join key=[%s])", joinKeyTransform),
            "\n",
            // get a list of files for the old version of the reflection
            "IcebergManifestScan",
            "manifestContent=[DATA]",
            "\n",
            // scan the reflection, get manifests
            String.format(
                "IcebergManifestList(table=[\"%s\".\"%s\".",
                ACCELERATOR_STORAGEPLUGIN_NAME, m.getReflectionId().getId()),
            "\n"
          };

      // build a unified plan from all 4 plans above together
      // notice that selectModifiedPartitionsPlan is used twice,
      // once to filter the files from the reflection to be deleted
      // and once to filter the files from the base dataset to be selected
      final String[] plan =
          Stream.of(
                  commonPartPlan,
                  selectDataFromMainDatasetPlan,
                  selectModifiedPartitionsPlan,
                  deleteFromReflectionPlan,
                  selectModifiedPartitionsPlan)
              .flatMap(Stream::of)
              .toArray(String[]::new);

      // perform actual validation on the plan
      testPlanSubstrPatternsInOrderPlanInput(planReflection, plan, null);
    } catch (final Exception e) {
      throw new RuntimeException(e.getMessage());
    }
  }
}
