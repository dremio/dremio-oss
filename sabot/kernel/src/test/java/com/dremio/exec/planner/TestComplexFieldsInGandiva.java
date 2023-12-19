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

import static com.dremio.TestBuilder.listOf;
import static com.dremio.TestBuilder.mapOf;

import org.junit.Test;

import com.dremio.PlanTestBase;

public class TestComplexFieldsInGandiva extends PlanTestBase {

  @Test
  public void testComplexTypeWithDuplicateField() throws Exception {
    final String query = "select complex.a.b.d + complex.a.d from cp.\"complex_field.json\" complex";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("EXPR$0")
      .baselineValues(1100L)
      .baselineValues(null)
      .baselineValues(null)
      .baselineValues(1100L)
      .baselineValues(1100L)
      .go();
  }

  @Test
  public void testComplexFieldsWithNullValues() throws Exception {
    final String query = "select complex.a.b.c + complex.a.b.d from cp.\"complex_field.json\" complex";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("EXPR$0")
      .baselineValues(1010L)
      .baselineValues(null)
      .baselineValues(null)
      .baselineValues(1020L)
      .baselineValues(null)
      .go();
  }

  @Test
  public void testComplexFieldsWithFilterOperator() throws Exception {
    final String query = "select complex.a.b.c + 1 from cp.\"complex_field.json\" complex where complex.a.d + complex.a.b.d = 1100";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("EXPR$0")
      .baselineValues(11L)
      .baselineValues(21L)
      .baselineValues(null)
      .go();
  }

  @Test
  public void testComplexField() throws Exception {
    final String query = "select  complex.a from cp.\"complex_field.json\" complex";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("a")
      .baselineValues(mapOf("b",mapOf("c",10L, "d",1000L),"d",100L))
      .baselineValues(mapOf("d", 100L))
      .baselineValues(null)
      .baselineValues(mapOf("b",mapOf("c",20L, "d",1000L),"d",100L))
      .baselineValues(mapOf("b",mapOf("d",1000L), "d",100L))
      .go();
  }

  @Test
  public void testComplexFieldsContaningNullWithProjectAndFilter() throws Exception {
    final String query = "select complex.a.b.c + 1, complex.a.b.d + 1 from cp.\"complex_field.json\" complex where complex.a.d + complex.a.b.d = 1100";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("EXPR$0", "EXPR$1")
      .baselineValues(11L, 1001L)
      .baselineValues(21L, 1001L)
      .baselineValues(null, 1001L)
      .go();
  }

  @Test
  public void testComplexFieldsWithIn() throws Exception {
    final String query = "select  complex.a.b.d, complex.a.d, complex.a.b.c from cp.\"complex_field.json\" complex where complex.a.b.c IN (complex.a.b.d, complex.a.d, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 21, 22, 23, 24, 25, 27) ";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("d", "d0", "c")
      .baselineValues(1000L, 100L, 10L)
      .go();
  }

  @Test
  public void testSimpleFieldsWithIn() throws Exception {
    final String query = "select rownum, age, studentnum from cp.\"complex_student.json\" complex where rownum IN (age, studentnum, 1, 2, 11, 12, 13, 14, 15, 16, 17, 18, 19, 21, 22, 23, 24, 25, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45)";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("rownum", "age", "studentnum")
      .baselineValues(1L, 76L, 692315658449L)
      .baselineValues(2L, 63L, 650706039334L)
      .go();
  }

  @Test
  public void testComplexFieldsWithIn1() throws Exception {
    final String query = "select  complex.a.b.d, complex.a.d, complex.a.b.c from cp.\"complex_field.json\" complex where complex.a.b.c IN (100, 1000, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 21, 22, 23, 24, 25, 27, 28, 29, 30)";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("d", "d0", "c")
      .baselineValues(1000L, 100L, 10L)
      .go();
  }

  @Test
  public void testSimpleFieldsWithLists() throws Exception {
    final String query = "select interests from cp.\"complex_student.json\" where rownum = 1";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("interests")
      .baselineValues(listOf("Reading", "Mountain Biking", "Hacking"))
      .go();
  }
  @Test
  public void testComplexFieldsWithLists() throws Exception {
    final String query = "select complex.arr[0].a.b.d + complex.arr[0].a.d from cp.\"complex_field.json\" complex";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("EXPR$0")
      .baselineValues(1100L)
      .baselineValues(1100L)
      .baselineValues(null)
      .baselineValues(1100L)
      .baselineValues(1100L)
      .go();
  }
  @Test
  public void testComplexFieldsWithListsInStruct() throws Exception {
    final String query = "select complex.a.b[0].d + complex.e from cp.\"complex_list.json\" complex";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .baselineColumns("EXPR$0")
      .baselineValues(110L)
      .go();
  }
}
