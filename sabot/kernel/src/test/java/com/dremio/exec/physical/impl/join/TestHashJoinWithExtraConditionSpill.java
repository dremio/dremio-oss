package com.dremio.exec.physical.impl.join;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.dremio.sabot.op.join.hash.HashJoinOperator;

/* All test cases from TestHashJoinWithExtraCondition will be executed in addition to separate test cases added below. */
public class TestHashJoinWithExtraConditionSpill extends TestHashJoinWithExtraCondition {

  @BeforeClass
  public static void beforeClass() throws Exception{
    setSystemOption(HashJoinOperator.ENABLE_SPILL, "true");
  }

  @AfterClass
  public static void afterClass() throws Exception {
    setSystemOption(HashJoinOperator.ENABLE_SPILL, HashJoinOperator.ENABLE_SPILL.getDefault().getBoolVal().toString());
  }

}
