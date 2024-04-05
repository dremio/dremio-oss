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
package com.dremio.exec.expr.fn.impl;

import com.dremio.BaseTestQuery;
import org.junit.Test;

public class TestIsListFunction extends BaseTestQuery {

  @Test
  public void testIsListOnArrayColumn() throws Exception {
    testBuilder()
        .unOrdered()
        .sqlQuery("SELECT " + "IS_LIST(intList) l1 " + "FROM cp.\"array.json\"")
        .baselineColumns("l1")
        .baselineValues(true)
        .go();
  }

  @Test
  public void testIsListOnIntColumn() throws Exception {
    testBuilder()
        .unOrdered()
        .sqlQuery("SELECT " + "IS_LIST(intValue) l1 " + "FROM cp.\"array.json\"")
        .baselineColumns("l1")
        .baselineValues(false)
        .go();
  }

  @Test
  public void testIsListForConvertFrom() throws Exception {
    testBuilder()
        .unOrdered()
        .sqlQuery("SELECT " + "IS_LIST(CONVERT_FROM('[\"foo\",\"bar\",\"qux\"]', 'JSON')) l1 ")
        .baselineColumns("l1")
        .baselineValues(true)
        .go();
  }

  @Test
  public void testIsListEmptyList() throws Exception {
    testBuilder()
        .unOrdered()
        .sqlQuery("SELECT " + "IS_LIST(col3) l1 " + "FROM cp.\"nulllistsForArray.parquet\"")
        .baselineColumns("l1")
        .baselineValues(true)
        .go();
  }

  // null is in the list and value searched is not in the list
  @Test
  public void testIsListForNullInList() throws Exception {
    testBuilder()
        .unOrdered()
        .sqlQuery("SELECT " + "IS_LIST(col1) l1 " + "FROM cp.\"nulllistsForArray.parquet\"")
        .baselineColumns("l1")
        .baselineValues(true)
        .go();
  }

  @Test
  public void testIsListForListOfNulls() throws Exception {
    testBuilder()
        .unOrdered()
        .sqlQuery("SELECT " + "IS_LIST(col2) l1 " + "FROM cp.\"nulllistsForArray.parquet\"")
        .baselineColumns("l1")
        .baselineValues(true)
        .go();
  }
}
