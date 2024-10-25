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
package com.dremio.exec.store.text;

import static org.junit.Assert.assertEquals;

import com.dremio.BaseTestQuery;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.record.RecordBatchLoader;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.vector.ValueVector;
import org.junit.Test;

public class TestTextColumn extends BaseTestQuery {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestTextColumn.class);

  @Test
  public void testCsvColumnSelection() throws Exception {
    test(
        "select columns[0] as region_id, columns[1] as country from dfs.\"[WORKING_PATH]/src/test/resources/store/text/data/regions.csv\"");
  }

  @Test
  public void testDefaultDelimiterColumnSelection() throws Exception {
    List<QueryDataBatch> batches =
        testSqlWithResults(
            "SELECT columns[0] as entire_row "
                + "from dfs_root.\"[WORKING_PATH]/src/test/resources/store/text/data/letters.txt\"");

    List<List<String>> expectedOutput =
        Arrays.asList(
            Arrays.asList("a, b,\",\"c\",\"d,, \\n e"),
            Arrays.asList("d, e,\",\"f\",\"g,, \\n h"),
            Arrays.asList("g, h,\",\"i\",\"j,, \\n k"));

    List<List<String>> actualOutput = getOutput(batches);
    System.out.println(actualOutput);
    validateOutput(expectedOutput, actualOutput);
  }

  @Test
  public void testCsvColumnSelectionCommasInsideQuotes() throws Exception {
    List<QueryDataBatch> batches =
        testSqlWithResults(
            "SELECT columns[0] as col1, columns[1] as col2, columns[2] as col3,"
                + "columns[3] as col4 from dfs.\"[WORKING_PATH]/src/test/resources/store/text/data/letters.csv\"");

    List<List<String>> expectedOutput =
        Arrays.asList(
            Arrays.asList("a, b,", "c", "d,, \\n e", "f\\\"g"),
            Arrays.asList("d, e,", "f", "g,, \\n h", "i\\\"j"),
            Arrays.asList("g, h,", "i", "j,, \\n k", "l\\\"m"));

    List<List<String>> actualOutput = getOutput(batches);
    validateOutput(expectedOutput, actualOutput);
  }

  private List<List<String>> getOutput(List<QueryDataBatch> batches) throws SchemaChangeException {
    List<List<String>> output = new ArrayList<>();
    RecordBatchLoader loader = new RecordBatchLoader(getTestAllocator());
    int last = 0;
    for (QueryDataBatch batch : batches) {
      int rows = batch.getHeader().getRowCount();
      if (batch.getData() != null) {
        loader.load(batch.getHeader().getDef(), batch.getData());
        // TODO:  Clean:  DRILL-2933:  That load(...) no longer throws
        // SchemaChangeException, so check/clean throws clause above.
        for (int i = 0; i < rows; ++i) {
          output.add(new ArrayList<String>());
          for (VectorWrapper<?> vw : loader) {
            ValueVector vv = vw.getValueVector();
            Object o = vv.getObject(i);
            output.get(last).add(o == null ? null : o.toString());
          }
          ++last;
        }
      }
      loader.clear();
      batch.release();
    }
    return output;
  }

  private void validateOutput(List<List<String>> expected, List<List<String>> actual) {
    assertEquals(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); ++i) {
      assertEquals(expected.get(i).size(), actual.get(i).size());
      for (int j = 0; j < expected.get(i).size(); ++j) {
        assertEquals(expected.get(i).get(j), actual.get(i).get(j));
      }
    }
  }
}
