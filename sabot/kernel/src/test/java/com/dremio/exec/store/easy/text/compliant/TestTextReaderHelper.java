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
package com.dremio.exec.store.easy.text.compliant;

import com.dremio.BaseTestQuery;
import com.dremio.service.namespace.file.proto.TextFileConfig;

public class TestTextReaderHelper extends BaseTestQuery {

  private static final String generalQuery = "select %s from table(cp.\"store/text/%s\"" + " (%s))";

  private final String[][] expected;

  private final TextFileConfig fileFormat;

  private final String testFileName;

  public TestTextReaderHelper(TextFileConfig fileFormat, String[][] expected, String testFileName) {
    this.expected = expected;
    this.fileFormat = fileFormat;
    this.testFileName = testFileName;
  }

  /** Helper function to perform select * query */
  public void testTableOptionsSelectAll() throws Exception {
    fileFormat.setExtractHeader(true);
    String query = String.format(generalQuery, "*", testFileName, fileFormat.toTableOptions());
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns(expected[0][0], expected[0][1], expected[0][2])
        .baselineValues(expected[1][0], expected[1][1], expected[1][2])
        .baselineValues(expected[2][0], expected[2][1], expected[2][2])
        .go();
  }

  /** Helper function to perform select col query for each column */
  public void testTableOptionsSelectCol() throws Exception {
    fileFormat.setExtractHeader(true);
    String query;
    for (int i = 0; i < expected[0].length; i++) {
      query =
          String.format(generalQuery, expected[0][i], testFileName, fileFormat.toTableOptions());
      testBuilder()
          .sqlQuery(query)
          .unOrdered()
          .baselineColumns(expected[0][i])
          .baselineValues(expected[1][i])
          .baselineValues(expected[2][i])
          .go();
    }
  }
}
