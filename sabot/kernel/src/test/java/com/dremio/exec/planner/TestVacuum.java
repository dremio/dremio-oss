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
package com.dremio.exec.planner;

import static com.dremio.exec.planner.common.TestPlanHelper.findSingleNode;
import static org.assertj.core.api.Assertions.fail;

import java.util.List;
import java.util.Map;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.CatalogUtil;
import com.dremio.exec.planner.physical.HashJoinPrel;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.query.SqlToPlanHandler;
import com.dremio.exec.planner.sql.handlers.query.VacuumTableHandler;
import com.dremio.exec.planner.sql.parser.SqlVacuumTable;
import com.google.common.collect.ImmutableMap;

/**
 * Test VACUUM plans
 */
public class TestVacuum extends TestTableManagementBase {

  @BeforeClass
  public static void beforeClass() {
    setSystemOption(ExecConstants.ENABLE_ICEBERG_VACUUM, "true");
    setSystemOption(ExecConstants.ENABLE_ICEBERG_VACUUM_REMOVE_ORPHAN_FILES, "true");
  }

  @AfterClass
  public static void afterClass() {
    setSystemOption(ExecConstants.ENABLE_ICEBERG_VACUUM,
      ExecConstants.ENABLE_ICEBERG_VACUUM.getDefault().getBoolVal().toString());
    setSystemOption(ExecConstants.ENABLE_ICEBERG_VACUUM_REMOVE_ORPHAN_FILES,
      ExecConstants.ENABLE_ICEBERG_VACUUM_REMOVE_ORPHAN_FILES.getDefault().getBoolVal().toString());
  }


  @Test
  public void testRemoveOrphanFilesPlanTrimScheme() throws Exception {
    final String vacuumQuery = "VACUUM TABLE " + table.getTableName() + " REMOVE ORPHAN FILES OLDER_THAN '2023-09-08 00:00:00.782'";
    Prel plan = getVacuumTablePlan(vacuumQuery);
    HashJoinPrel hashJoinPrel = getJoinPlan(plan);
    Assert.assertNotNull(hashJoinPrel.getLeft());
    Assert.assertNotNull(hashJoinPrel.getRight());

    validateTrimSchemeOnLeftSideOfJoin(hashJoinPrel.getLeft());
    validateTrimSchemeOnRightSideOfJoin(hashJoinPrel.getRight());
  }

  private Prel getVacuumTablePlan(String sql) throws Exception {
    return getVacuumTablePlan(config, converter.parse(sql));
  }
  public static Prel getVacuumTablePlan(SqlHandlerConfig config, SqlNode sqlNode) throws Exception {
    VacuumTableHandler vacuumTableHandler = (VacuumTableHandler) getVacuumHandler(sqlNode);
    vacuumTableHandler.getPlan(
      config,
      null,
      sqlNode,
      CatalogUtil.getResolvePathForTableManagement(config.getContext().getCatalog(), vacuumTableHandler.getTargetTablePath(sqlNode)));
    return vacuumTableHandler.getPrel();
  }

  private static SqlToPlanHandler getVacuumHandler(SqlNode node) {
    if (node instanceof SqlVacuumTable) {
      return new VacuumTableHandler();
    }
    fail("Add VACUUM CATALOG handler, if needed");

    return null;
  }

  private static HashJoinPrel getJoinPlan(RelNode plan) {
    Map<String, String> attributes = ImmutableMap.of(
      "joinType", JoinRelType.LEFT.toString(),
      "condition", getJoinCondition()
    );

    HashJoinPrel hashJoinPrel = findSingleNode(plan, HashJoinPrel.class, attributes);
    Assert.assertNotNull(hashJoinPrel);
    return hashJoinPrel;
  }

  // get HashJoin condition, left.file_path = right.file_path
  private static String getJoinCondition() {
    return String.format("=($%d, $%d)", 0, 2); // file_path column
  }

  private static void validateTrimSchemeOnLeftSideOfJoin(RelNode plan) {
    Assert.assertTrue(plan instanceof ProjectPrel);
    ProjectPrel filePathTypeProjectPrel = (ProjectPrel) plan;
    List<RexNode> projectExpressions = filePathTypeProjectPrel.getChildExps();
    Assert.assertEquals(2, projectExpressions.size());
    Assert.assertTrue(projectExpressions.get(0) instanceof RexCall);
    RexCall trimExpr = (RexCall) projectExpressions.get(0);
    Assert.assertEquals("CASE(>=(POSITION('://':VARCHAR(3), $0), 1), SUBSTRING($0, +(2, POSITION('://':VARCHAR(3), $0))), CASE(>(POSITION(':/':VARCHAR(2), $0), 0), SUBSTRING($0, +(1, POSITION(':/':VARCHAR(2), $0))), $0))",
      trimExpr.toString());
  }

  private static void validateTrimSchemeOnRightSideOfJoin(RelNode plan) {
    Assert.assertTrue(plan instanceof ProjectPrel);
    ProjectPrel filePathTypeProjectPrel = (ProjectPrel) plan;
    Assert.assertTrue(filePathTypeProjectPrel.getInput() instanceof ProjectPrel);
    ProjectPrel schemeTrimProjectPrel = (ProjectPrel) filePathTypeProjectPrel.getInput();
    List<RexNode> projectExpressions = schemeTrimProjectPrel.getChildExps();
    Assert.assertEquals(2, projectExpressions.size());
    Assert.assertTrue(projectExpressions.get(0) instanceof RexCall);
    RexCall trimExpr = (RexCall) projectExpressions.get(0);
    Assert.assertEquals("CASE(>=(POSITION('://':VARCHAR(3), $0), 1), SUBSTRING($0, +(CASE(>=(POSITION('file://':VARCHAR(7), $0), 1), 3, 2), POSITION('://':VARCHAR(3), $0))), $0)",
      trimExpr.toString());
  }
}
