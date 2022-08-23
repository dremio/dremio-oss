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
package com.dremio.exec.planner.sql;

import org.junit.Test;

/**
 * Runs test cases on the local filesystem-based Hadoop source.
 *
 * Note: Contains all tests in DeleteTests.
 */
public class ITDelete extends ITDmlQueryBase {

  // Defining SOURCE such that you can easily copy and paste the same test across other test variations
  private static final String SOURCE = TEMP_SCHEMA_HADOOP;

  @Test
  public void testMalformedDeleteQueries() throws Exception {
    DeleteTests.testMalformedDeleteQueries(SOURCE);
  }

  @Test
  public void testDeleteOnView() throws Exception {
    DeleteTests.testDeleteOnView(allocator, SOURCE);
  }

  @Test
  public void testDeleteAll() throws Exception {
    DeleteTests.testDeleteAll(allocator, SOURCE);
  }

  @Test
  public void testDeleteById() throws Exception {
    DeleteTests.testDeleteById(allocator, SOURCE);
  }

  @Test
  public void testDeleteTargetTableWithAndWithoutAlias() throws Exception {
    DeleteTests.testDeleteTargetTableWithAndWithoutAlias(allocator, SOURCE);
  }

  @Test
  public void testDeleteByIdInEquality() throws Exception {
    DeleteTests.testDeleteByIdInEquality(allocator, SOURCE);
  }

  @Test
  public void testDeleteByEvenIds() throws Exception {
    DeleteTests.testDeleteByEvenIds(allocator, SOURCE);
  }

  @Test
  public void testDeleteByIdAndColumn0() throws Exception {
    DeleteTests.testDeleteByIdAndColumn0(allocator, SOURCE);
  }

  @Test
  public void testDeleteByIdOrColumn0() throws Exception {
    DeleteTests.testDeleteByIdOrColumn0(allocator, SOURCE);
  }

  @Test
  public void testDeleteWithWrongContextWithFqn() throws Exception {
    DeleteTests.testDeleteWithWrongContextWithFqn(allocator, SOURCE);
  }

  @Test
  public void testDeleteWithWrongContextWithPathTable() throws Exception {
    DeleteTests.testDeleteWithWrongContextWithPathTable(allocator, SOURCE);
  }

  @Test
  public void testDeleteWithWrongContextWithTable() throws Exception {
    DeleteTests.testDeleteWithWrongContextWithTable(allocator, SOURCE);
  }

  @Test
  public void testDeleteWithContextWithFqn() throws Exception {
    DeleteTests.testDeleteWithContextWithFqn(allocator, SOURCE);
  }

  @Test
  public void testDeleteWithContextWithPathTable() throws Exception {
    DeleteTests.testDeleteWithContextWithPathTable(allocator, SOURCE);
  }

  @Test
  public void testDeleteWithContextWithTable() throws Exception {
    DeleteTests.testDeleteWithContextWithTable(allocator, SOURCE);
  }

  @Test
  public void testDeleteWithContextPathWithFqn() throws Exception {
    DeleteTests.testDeleteWithContextPathWithFqn(allocator, SOURCE);
  }

  @Test
  public void testDeleteWithContextPathWithPathTable() throws Exception {
    DeleteTests.testDeleteWithContextPathWithPathTable(allocator, SOURCE);
  }

  @Test
  public void testDeleteWithContextPathWithTable() throws Exception {
    DeleteTests.testDeleteWithContextPathWithTable(allocator, SOURCE);
  }

  @Test
  public void testDeleteWithSourceAsPathTableWithWrongContextWithPathTable() throws Exception {
    DeleteTests.testDeleteWithSourceAsPathTableWithWrongContextWithPathTable(allocator, SOURCE);
  }

  @Test
  public void testDeleteWithSourceAsPathTableWithContextWithPathTable() throws Exception {
    DeleteTests.testDeleteWithSourceAsPathTableWithContextWithPathTable(allocator, SOURCE);
  }
}
