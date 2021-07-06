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
package com.dremio.exec.store.json;

import static com.dremio.ArrowDsUtil.doubleList;
import static com.dremio.ArrowDsUtil.doubleStruct;
import static com.dremio.ArrowDsUtil.wrapDoubleListInList;
import static com.dremio.ArrowDsUtil.wrapListInStruct;
import static com.dremio.ArrowDsUtil.wrapStructInList;
import static com.dremio.ArrowDsUtil.wrapStructInStruct;
import static com.dremio.exec.ExecConstants.MIXED_TYPES_DISABLED;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.PlanTestBase;
import com.dremio.TestBuilder;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.exec.proto.UserBitShared;

public class TestComplexJsonSchemaUpPromotionAndTypeCoercion extends PlanTestBase {

  @BeforeClass
  public static void disableMixedTypesSupport() {
    setSystemOption(MIXED_TYPES_DISABLED.getOptionName(), "true");
  }

  @AfterClass
  public static void resetMixedTypesSupport() {
    resetSystemOption(MIXED_TYPES_DISABLED.getOptionName());
  }

  @Test
  public void testBigintToDoubleCoercionForSimpleListType() throws Exception {
    Path jsonDir = copyFiles("array_double_bigint");
    verifyRecords(jsonDir)
      .baselineValues(doubleList(1.0, 2.0), doubleList(1.1, 2.2))
      .baselineValues(doubleList(1.1, 2.2), doubleList(1.0, 2.0))
      .go();
    verifyCountStar(jsonDir, 2);
  }

  @Test
  public void testBigintToDoubleCoercionForNestedListType() throws Exception {
    Path jsonDir = copyFiles("array_array_double_bigint");
    verifyRecords(jsonDir)
      .baselineValues(wrapDoubleListInList(doubleList(1.0, 2.0)), wrapDoubleListInList(doubleList(1.1, 2.2)))
      .baselineValues(wrapDoubleListInList(doubleList(1.1, 2.2)), wrapDoubleListInList(doubleList(1.0, 2.0)))
      .go();
    verifyCountStar(jsonDir, 2);
  }

  @Test
  public void testBigintToDoubleCoercionForSimpleStructType() throws Exception {
    Path jsonDir = copyFiles("struct_double_bigint");
    verifyRecords(jsonDir)
      .baselineValues(doubleStruct("f1", 2.3), doubleStruct("f2", 2.0))
      .baselineValues(doubleStruct("f1", 2.0), doubleStruct("f2", 2.3))
      .go();
    verifyCountStar(jsonDir, 2);
  }

  @Test
  public void testBigintToDoubleCoercionForSimpleStructTypeSingleFile() throws Exception {
    String dirName = "struct_mixed_file";
    Path jsonDir = createTempDirWithName(dirName).toPath();
    writeDir(jsonDir, dirName);
    verifyRecords(jsonDir)
      .baselineValues(doubleStruct("f1", 12.3), doubleStruct("f2", 12.0))
      .baselineValues(doubleStruct("f1", 12.0), doubleStruct("f2", 12.3))
      .go();
    verifyCountStar(jsonDir, 2);
  }

  @Test
  public void testBigintToDoubleCoercionForNestedStructType() throws Exception {
    Path jsonDir = copyFiles("struct_struct_double_bigint");
    verifyRecords(jsonDir)
      .baselineValues(
        wrapStructInStruct("f1", doubleStruct("f1", 2.0)),
        wrapStructInStruct("f2", doubleStruct("f2", 2.3)))
      .baselineValues(
        wrapStructInStruct("f1", doubleStruct("f1", 2.3)),
        wrapStructInStruct("f2", doubleStruct("f2", 2.0)))
      .go();
    verifyCountStar(jsonDir, 2);
  }

  @Test
  public void testBigintToDoubleCoercionForListOfStruct() throws Exception {
    Path jsonDir = copyFiles("array_struct_double_bigint");
    verifyRecords(jsonDir)
      .baselineValues(wrapStructInList(doubleStruct("f1", 2.3)), wrapStructInList(doubleStruct("f2", 2.0)))
      .baselineValues(wrapStructInList(doubleStruct("f1", 2.0)), wrapStructInList(doubleStruct("f2", 2.3)))
      .go();
    verifyCountStar(jsonDir, 2);
  }

  @Test
  public void testBigintToDoubleCoercionForStructOfList() throws Exception {
    Path jsonDir = copyFiles("struct_array_bigint_double");
    verifyRecords(jsonDir)
      .baselineValues(wrapListInStruct("f1", doubleList(2.3)), wrapListInStruct("f2", doubleList(2.0)))
      .baselineValues(wrapListInStruct("f1", doubleList(2.0)), wrapListInStruct("f2", doubleList(2.3)))
      .go();
    verifyCountStar(jsonDir, 2);
  }

  @Test
  public void testBigintToDoubleCoercionForIntAndStructTypes() throws Exception {
    Path jsonDir = copyFiles("prim_struct_double_bigint");
    verifyRecords(jsonDir)
      .baselineValues(2.3, doubleStruct("f1", 2.0))
      .baselineValues(2.0, doubleStruct("f1", 2.3))
      .go();
    verifyCountStar(jsonDir, 2);
  }

  @Test
  public void testBigintToDoubleCoercionForIntAndListTypes() throws Exception {
    Path jsonDir = copyFiles("prim_list_double_bigint");
    verifyRecords(jsonDir)
      .baselineValues(2.0, doubleList(2.1, 2.2, 2.3))
      .baselineValues(2.3, doubleList(2.0, 2.0, 2.0))
      .go();
    verifyCountStar(jsonDir, 2);
  }

  @Test
  public void testInvalidUpPromotionAndTypeCoercionInUnions() throws Exception {
    Path jsonDir = createTempDirWithName("invalid_array").toPath();
    writeDir(jsonDir, "invalid_array");
    String query = String.format("SELECT * FROM dfs.\"%s\"", jsonDir);
    try {
      testRunAndReturn(UserBitShared.QueryType.SQL, query);
      fail("Expected Exception");
    } catch (Exception e) {
      assertThat(e.getCause(), instanceOf(UserException.class));
      assertThat(e.getCause().getMessage(), containsString("Unable to coerce from the file's data type \"varchar\" to the column's data type \"list<varchar>\" in table"));
      assertThat(e.getCause().getMessage(), containsString("invalid_array"));
      assertThat(e.getCause().getMessage(), containsString(", column \"x\" and file"));
      assertThat(e.getCause().getMessage(), containsString("invalid_array.json"));
    }
  }

  private void writeDir(Path dest, String srcDirName) {
    writeDir(Paths.get("json/schema_changes/no_mixed_types//complex/"), dest, srcDirName);
  }

  private Path copyFiles(String dirName) {
    Path jsonDir = createTempDirWithName(dirName).toPath();
    writeDir(jsonDir, dirName);

    // Run a query to trigger a schema change
    triggerSchemaLearning(jsonDir);
    return jsonDir;
  }

  private TestBuilder verifyRecords(Path jsonDir) {
    return testBuilder()
      .sqlQuery("SELECT * from dfs.\"" + jsonDir + "\"")
      .unOrdered()
      .baselineColumns("col1", "col2");
  }

  private void triggerSchemaLearning(Path jsonDir) {
    String query = "SELECT * from dfs.\"" + jsonDir + "\"";
    try {
      testRunAndReturn(UserBitShared.QueryType.SQL, query);
      fail("Expected UserRemoteException");
    } catch (Exception e) {
      assertThat(e.getCause(), instanceOf(UserRemoteException.class));
      assertThat(e.getCause().getMessage(), containsString("New schema found"));
    }
  }

  private void verifyCountStar(Path jsonDir, long result) throws Exception {
    String query = String.format("SELECT count(*) FROM dfs.\"%s\"", jsonDir);
    TestBuilder testBuilder = testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("EXPR$0")
      .baselineValues(result);
    testBuilder.go();
  }
}
