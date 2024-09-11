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

import com.dremio.PlanTestBase;
import org.junit.Test;

public class TestComplexFieldValidation extends PlanTestBase {

  @Test
  public void testAnyTypeWithArray() throws Exception {
    test(
        "select complex.a[0].b.c from (select CONVERT_FROM('{\"a\":[{\"b\":{\"c\":2}}]}','JSON') complex)");
  }

  @Test
  public void testAnyTypeWithArrayMissingColumn1() {
    errorMsgTestHelper(
        "select complex.a[0].b.this_column_does_not_exist from (select CONVERT_FROM('{\"a\":[{\"b\":{\"c\":2}}]}','JSON') complex)",
        "VALIDATION ERROR: Unable to find the referenced field: [replacement0.a[0].b.this_column_does_not_exist]");
  }

  @Test
  public void testAnyTypeWithArrayMissingColumn2() {
    errorMsgTestHelper(
        "select complex.a[0].this_column_does_not_exist from (select CONVERT_FROM('{\"a\":[{\"b\":{\"c\":2}}]}','JSON') complex)",
        "VALIDATION ERROR: Unable to find the referenced field: [replacement0.a[0].this_column_does_not_exist]");
  }

  @Test
  public void testAnyType() throws Exception {
    test(
        "select complex.a.b.c from (select CONVERT_FROM('{\"a\":{\"b\":{\"c\":2}}}','JSON') complex)");
  }

  @Test
  public void testAnyTypeMissingColumn1() {
    errorMsgTestHelper(
        "select complex.a.b.this_column_does_not_exist from (select CONVERT_FROM('{\"a\":{\"b\":{\"c\":2}}}','JSON') complex)",
        "VALIDATION ERROR: Unable to find the referenced field: [replacement0.a.b.this_column_does_not_exist]");
  }

  @Test
  public void testAnyTypeMissingColumn2() {
    errorMsgTestHelper(
        "select complex.a.this_column_does_not_exist from (select CONVERT_FROM('{\"a\":{\"b\":{\"c\":2}}}','JSON') complex)",
        "VALIDATION ERROR: Unable to find the referenced field: [replacement0.a.this_column_does_not_exist]");
  }

  @Test
  public void testComplexType1() throws Exception {
    final String query = "select complex.a.b.c from cp.\"complex_field.json\" complex";
    test(query);
  }

  @Test
  public void testComplexType2() throws Exception {
    final String query = "select complex.a.b from cp.\"complex_field.json\" complex";
    test(query);
  }

  @Test
  public void testComplexType3() throws Exception {
    final String query = "select complex.a from cp.\"complex_field.json\" complex";
    test(query);
  }

  @Test
  public void testComplexTypeMissingColumn1() throws Exception {
    final String query =
        "select complex.a.b.this_column_does_not_exist from cp.\"complex_field.json\" complex";
    errorMsgTestHelper(
        query,
        "VALIDATION ERROR: Column 'a.b.this_column_does_not_exist' not found in table 'complex'");
  }

  @Test
  public void testComplexTypeMissingColumn2() throws Exception {
    final String query =
        "select complex.a.this_column_does_not_exist from cp.\"complex_field.json\" complex";
    errorMsgTestHelper(
        query,
        "VALIDATION ERROR: Column 'a.this_column_does_not_exist' not found in table 'complex'");
  }

  @Test
  public void testComplexTypeMissingColumn3() throws Exception {
    final String query =
        "select complex.this_column_does_not_exist from cp.\"complex_field.json\" complex";
    errorMsgTestHelper(
        query,
        "VALIDATION ERROR: Column 'this_column_does_not_exist' not found in table 'complex'");
  }

  @Test
  public void testComplexTypeWithArray1() throws Exception {
    final String query = "select complex.arr[0].a from cp.\"complex_field.json\" complex";
    test(query);
  }

  @Test
  public void testComplexTypeWithArray2() throws Exception {
    final String query = "select complex.arr[0].a.b from cp.\"complex_field.json\" complex";
    test(query);
  }

  @Test
  public void testComplexTypeWithArray3() throws Exception {
    final String query = "select complex.arr[0].a.b.c from cp.\"complex_field.json\" complex";
    test(query);
  }

  @Test
  public void testComplexTypeWithArrayMissingColumn1() throws Exception {
    final String query =
        "select complex.arr[0].this_column_does_not_exist from cp.\"complex_field.json\" complex";
    errorMsgTestHelper(query, "VALIDATION ERROR: Unknown field 'this_column_does_not_exist'");
  }

  @Test
  public void testComplexTypeWithArrayMissingColumn2() throws Exception {
    final String query =
        "select complex.arr[0].a.this_column_does_not_exist from cp.\"complex_field.json\" complex";
    errorMsgTestHelper(query, "VALIDATION ERROR: Unknown field 'this_column_does_not_exist'");
  }

  @Test
  public void testComplexTypeWithArrayMissingColumn3() throws Exception {
    final String query =
        "select complex.arr[0].a.b.this_column_does_not_exist from cp.\"complex_field.json\" complex";
    errorMsgTestHelper(query, "VALIDATION ERROR: Unknown field 'this_column_does_not_exist'");
  }
}
