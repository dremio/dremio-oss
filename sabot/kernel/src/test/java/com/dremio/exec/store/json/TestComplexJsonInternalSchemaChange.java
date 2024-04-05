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

import static com.dremio.ArrowDsUtil.decimalStruct;
import static com.dremio.ArrowDsUtil.doubleList;
import static com.dremio.ArrowDsUtil.doubleStruct;
import static com.dremio.ArrowDsUtil.longStruct;
import static com.dremio.ArrowDsUtil.textList;
import static com.dremio.ArrowDsUtil.wrapDoubleListInList;
import static com.dremio.ArrowDsUtil.wrapListInStruct;
import static com.dremio.ArrowDsUtil.wrapStructInList;
import static com.dremio.ArrowDsUtil.wrapStructInStruct;
import static com.dremio.ArrowDsUtil.wrapTextListInList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import com.dremio.common.exceptions.UserRemoteException;
import org.apache.arrow.vector.util.JsonStringHashMap;
import org.junit.Test;

public class TestComplexJsonInternalSchemaChange extends InternalSchemaTestBase {

  @Test
  public void testInternalSchemaChangeForList() throws Exception {
    String dirName = "array_double";
    copyFilesFromInternalSchemaComplex(dirName);
    verifyRecords(dirName, "col1", doubleList(1.1, 2.2));
    verifyCountStar(dirName, 1);

    alterTableChangeColumn(dirName, "col1", "ARRAY(VARCHAR)");
    verifyRecords(dirName, "col1", textList("1.1", "2.2"));
    verifyCountStar(dirName, 1);

    runMetadataRefresh(dirName);
    verifyRecords(dirName, "col1", textList("1.1", "2.2"));
  }

  @Test
  public void testInternalSchemaChangeForListOfList() throws Exception {
    String dirName = "array_array_double";
    copyFilesFromInternalSchemaComplex(dirName);
    verifyRecords(dirName, "col1", wrapDoubleListInList(doubleList(1.1, 2.2)));
    verifyCountStar(dirName, 1);

    alterTableChangeColumn(dirName, "col1", "ARRAY(ARRAY(VARCHAR))");
    verifyRecords(dirName, "col1", wrapTextListInList(textList("1.1", "2.2")));
    verifyCountStar(dirName, 1);

    runMetadataRefresh(dirName);
    verifyRecords(dirName, "col1", wrapTextListInList(textList("1.1", "2.2")));
  }

  @Test
  public void testInternalSchemaChangeForListOfStruct() throws Exception {
    String dirName = "array_struct_double";
    copyFilesFromInternalSchemaComplex(dirName);
    verifyRecords(dirName, "col1", wrapStructInList(doubleStruct("f1", 2.3)));
    verifyCountStar(dirName, 1);

    alterTableChangeColumn(dirName, "col1", "ARRAY(ROW(f1 BIGINT))");
    verifyRecords(dirName, "col1", wrapStructInList(longStruct("f1", 2L)));
    verifyCountStar(dirName, 1);

    runMetadataRefresh(dirName);
    verifyRecords(dirName, "col1", wrapStructInList(longStruct("f1", 2L)));
  }

  @Test
  public void testInternalSchemaChangeForStruct() throws Exception {
    String dirName = "struct_double";
    copyFilesFromInternalSchemaComplex(dirName);
    verifyRecords(dirName, "col1", doubleStruct("f1", 2.3));
    verifyCountStar(dirName, 1);

    alterTableChangeColumn(dirName, "col1", "ROW(f1 DECIMAL(3,1))");
    verifyRecords(dirName, "col1", decimalStruct("f1", "2.3"));
    verifyCountStar(dirName, 1);

    runMetadataRefresh(dirName);
    verifyRecords(dirName, "col1", decimalStruct("f1", "2.3"));
  }

  @Test
  public void testInternalSchemaChangeForStructOfList() throws Exception {
    String dirName = "struct_array_double";
    copyFilesFromInternalSchemaComplex(dirName);
    verifyRecords(dirName, "col1", wrapListInStruct("f1", doubleList(2.3)));
    verifyCountStar(dirName, 1);

    alterTableChangeColumn(dirName, "col1", "ROW(f1 ARRAY(VARCHAR))");
    verifyRecords(dirName, "col1", wrapListInStruct("f1", textList("2.3")));
    verifyCountStar(dirName, 1);

    runMetadataRefresh(dirName);
    verifyRecords(dirName, "col1", wrapListInStruct("f1", textList("2.3")));
  }

  @Test
  public void testInternalSchemaChangeForStructOfStruct() throws Exception {
    String dirName = "struct_struct_double";
    copyFilesFromInternalSchemaComplex(dirName);
    verifyRecords(dirName, "col1", wrapStructInStruct("f1", doubleStruct("f1", 2.3)));
    verifyCountStar(dirName, 1);

    alterTableChangeColumn(dirName, "col1", "ROW(f1 ROW(f1 BIGINT))");
    verifyRecords(dirName, "col1", wrapStructInStruct("f1", longStruct("f1", 2L)));
    verifyCountStar(dirName, 1);

    runMetadataRefresh(dirName);
    verifyRecords(dirName, "col1", wrapStructInStruct("f1", longStruct("f1", 2L)));
    alterTableForgetMetadata(dirName);
  }

  @Test
  public void testInternalSchemaChangeInvalidComplexToPrim() throws Exception {
    String dirName = "struct_struct_double";
    copyFilesFromInternalSchemaComplex(dirName);
    try {
      alterTableChangeColumn(dirName, "col1", "ROW(f1 BIGINT)");
      fail("Complex type cannot be changed to prim");
    } catch (UserRemoteException e) {
      assertThat(e.getMessage())
          .contains(
              "INVALID_DATASET_METADATA ERROR: Field f1: Struct<f1: FloatingPoint(DOUBLE)> and Int(64, true) are incompatible types, for type changes please ensure both columns are either of primitive types or complex but not mixed.");
    }
    alterTableForgetMetadata(dirName);
  }

  @Test
  public void testInternalSchemaChangeInvalidComplexToComplex() throws Exception {
    String dirName = "struct_struct_double";
    copyFilesFromInternalSchemaComplex(dirName);

    alterTableChangeColumn(dirName, "col1", "ROW(f1 ARRAY(BIGINT))");
    assertCoercionFailure(dirName, "struct<f1::double>", "list<int64>");
    alterTableForgetMetadata(dirName);
  }

  @Test
  public void testInternalSchemaChangeForStructOfStructAfterSchemaLearning() throws Exception {
    String dirName = "struct_struct_double_bigint";
    copyFilesFromNoMixedTypesComplex(dirName);
    triggerSchemaLearning(dirName);
    verifyRecords(
        dirName,
        "col1",
        wrapStructInStruct("f1", doubleStruct("f1", 2.3)),
        wrapStructInStruct("f1", doubleStruct("f1", 2.0)));
    verifyCountStar(dirName, 2);

    alterTableChangeColumn(dirName, "col1", "ROW(f1 ROW(f1 BIGINT))");
    verifyRecords(
        dirName,
        "col1",
        wrapStructInStruct("f1", longStruct("f1", 2L)),
        wrapStructInStruct("f1", longStruct("f1", 2L)));
    verifyCountStar(dirName, 2);
    alterTableChangeColumn(dirName, "col1", "ROW(f1 ROW(f1 DOUBLE))");
  }

  @Test
  public void testInternalSchemaChangeForStructOfStructWithMultipleFields() throws Exception {
    String dirName = "struct_struct_multiple_fields";
    copyFilesFromInternalSchemaComplex(dirName);

    JsonStringHashMap<String, Object> innerStruct = longStruct("id", 2L);
    innerStruct.putAll(doubleStruct("data", 2.3));

    JsonStringHashMap<String, JsonStringHashMap<String, Object>> outerStruct =
        wrapStructInStruct("f1", innerStruct);

    verifyRecords(dirName, "col1", outerStruct);
    alterTableChangeColumn(dirName, "col1", "ROW(f1 ROW(id DOUBLE))");

    verifyRecords(dirName, "col1", wrapStructInStruct("f1", doubleStruct("id", 2.0)));
  }

  @Test
  public void testInternalSchemaChangeForStructOfStructBeforeSchemaLearning() throws Exception {
    String dirName = "struct_struct_double_bigint";
    copyFilesFromNoMixedTypesComplex(dirName);
    alterTableChangeColumn(dirName, "col1", "ROW(f1 ROW(f1 BIGINT))");
    triggerOptionalSchemaLearning(dirName);
    verifyRecords(
        dirName,
        "col1",
        wrapStructInStruct("f1", longStruct("f1", 2L)),
        wrapStructInStruct("f1", longStruct("f1", 2L)));
    verifyCountStar(dirName, 2);
    alterTableChangeColumn(dirName, "col1", "ROW(f1 ROW(f1 DOUBLE))");
  }

  @Test
  public void testInternalSchemaChangeForStructOfListDropAndAddField() throws Exception {
    String dirName = "struct_array_bigint_double";
    copyFilesFromNoMixedTypesComplex(dirName);
    triggerSchemaLearning(dirName);
    verifyRecords(
        dirName,
        "col1",
        wrapListInStruct("f1", doubleList(2.0)),
        wrapListInStruct("f1", doubleList(2.3)));
    verifyCountStar(dirName, 2L);
    alterTableChangeColumn(dirName, "col1", "ROW(f2 ROW(f1 DOUBLE))");
    try {
      verifyRecords(
          dirName,
          "col1",
          wrapListInStruct("f1", doubleList(2.0)),
          wrapListInStruct("f1", doubleList(2.3)));
      fail("dropped field should not be read.");
    } catch (Exception e) {
      assertThat(e.getMessage()).contains("did not find expected record in result set");
    }
  }
}
