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
package com.dremio.exec.store.excel;

import static org.junit.Assert.assertEquals;

import com.dremio.exec.store.easy.excel.ColumnNameHandler;
import org.junit.Before;
import org.junit.Test;

public class TestColumnNameHandler {

  private ColumnNameHandler columnNameHandler;

  @Before
  public void setup() {
    columnNameHandler = new ColumnNameHandler();
  }

  // colA, colB, colC => colA, colB, colC, D, E, ...
  @Test
  public void testSimpleHeader() {
    columnNameHandler.setColumnName(0, "colA");
    columnNameHandler.setColumnName(1, "colB");
    columnNameHandler.setColumnName(2, "colC");

    assertEquals("colA", columnNameHandler.getColumnName(0));
    assertEquals("colB", columnNameHandler.getColumnName(1));
    assertEquals("colC", columnNameHandler.getColumnName(2));
    // try accessing columns not defined in the header
    assertEquals("D", columnNameHandler.getColumnName(3));
    assertEquals("E", columnNameHandler.getColumnName(4));
  }

  // colA, colB, colA, colA => colA, colB, colA0, colA1, E, ...
  @Test
  public void testHeaderWithDuplicates() {
    columnNameHandler.setColumnName(0, "colA");
    columnNameHandler.setColumnName(1, "colB");
    columnNameHandler.setColumnName(2, "colA");
    columnNameHandler.setColumnName(3, "colA");

    assertEquals("colA", columnNameHandler.getColumnName(0));
    assertEquals("colB", columnNameHandler.getColumnName(1));
    assertEquals("colA0", columnNameHandler.getColumnName(2));
    assertEquals("colA1", columnNameHandler.getColumnName(3));
    // try accessing columns not defined in the header
    assertEquals("E", columnNameHandler.getColumnName(4));
  }

  // colA, colA0, colA => colA, colA0, colA00, D, E
  @Test
  public void testHeaderWithDuplicates2() {
    columnNameHandler.setColumnName(0, "colA");
    columnNameHandler.setColumnName(1, "colA0");
    columnNameHandler.setColumnName(2, "colA");

    assertEquals("colA", columnNameHandler.getColumnName(0));
    assertEquals("colA0", columnNameHandler.getColumnName(1));
    assertEquals("colA00", columnNameHandler.getColumnName(2));
    // try accessing columns not defined in the header
    assertEquals("D", columnNameHandler.getColumnName(3));
    assertEquals("E", columnNameHandler.getColumnName(4));
  }

  // colA, colA0, colA00, colA => colA, colA0, colA00, colA000, E
  @Test
  public void testHeaderWithDuplicates3() {
    columnNameHandler.setColumnName(0, "colA");
    columnNameHandler.setColumnName(1, "colA0");
    columnNameHandler.setColumnName(2, "colA00");
    columnNameHandler.setColumnName(3, "colA000");

    assertEquals("colA", columnNameHandler.getColumnName(0));
    assertEquals("colA0", columnNameHandler.getColumnName(1));
    assertEquals("colA00", columnNameHandler.getColumnName(2));
    assertEquals("colA000", columnNameHandler.getColumnName(3));
    // try accessing columns not defined in the header
    assertEquals("E", columnNameHandler.getColumnName(4));
  }

  // colA, , colC => colA, B, colC, D, E, ...
  @Test
  public void testHeaderWithEmpty() {
    columnNameHandler.setColumnName(0, "colA");
    columnNameHandler.setColumnName(2, "colC");

    assertEquals("colA", columnNameHandler.getColumnName(0));
    assertEquals("B", columnNameHandler.getColumnName(1));
    assertEquals("colC", columnNameHandler.getColumnName(2));
    // try accessing columns not defined in the header
    assertEquals("D", columnNameHandler.getColumnName(3));
    assertEquals("E", columnNameHandler.getColumnName(4));
  }

  // colA, , B => colA, B0, B, D, E, ...
  @Test
  public void testHeaderWithEmptyAndDuplication() {
    columnNameHandler.setColumnName(0, "colA");
    columnNameHandler.setColumnName(2, "B");

    assertEquals("colA", columnNameHandler.getColumnName(0));
    assertEquals("B0", columnNameHandler.getColumnName(1));
    assertEquals("B", columnNameHandler.getColumnName(2));
    // try accessing columns not defined in the header
    assertEquals("D", columnNameHandler.getColumnName(3));
    assertEquals("E", columnNameHandler.getColumnName(4));
  }
}
