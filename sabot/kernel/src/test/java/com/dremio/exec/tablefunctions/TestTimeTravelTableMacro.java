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
package com.dremio.exec.tablefunctions;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class TestTimeTravelTableMacro {

  @Test
  public void testSplitTableIdentifier() {
    List<String> actual, expected;

    actual = TimeTravelTableMacro.splitTableIdentifier("table1");
    expected = ImmutableList.of("table1");
    Assert.assertEquals(expected, actual);

    actual = TimeTravelTableMacro.splitTableIdentifier("schema1.schema2.table1");
    expected = ImmutableList.of("schema1", "schema2", "table1");
    Assert.assertEquals(expected, actual);

    actual = TimeTravelTableMacro.splitTableIdentifier("\"tab.le1\"");
    expected = ImmutableList.of("tab.le1");
    Assert.assertEquals(expected, actual);

    actual = TimeTravelTableMacro.splitTableIdentifier("\"sch.ema1\".schema2.\"ta.ble1\"");
    expected = ImmutableList.of("sch.ema1", "schema2", "ta.ble1");
    Assert.assertEquals(expected, actual);

    actual = TimeTravelTableMacro.splitTableIdentifier("\"sch.e\"\"ma1\".schema2.\"sche\"\" ma3\"");
    expected = ImmutableList.of("sch.e\"ma1", "schema2", "sche\" ma3");
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testSplitTableIdentifierWithInvalidIdentifier() {
    assertThatThrownBy(() -> TimeTravelTableMacro.splitTableIdentifier("table1..table2"))
      .isInstanceOf(IllegalArgumentException.class);

    assertThatThrownBy(() -> TimeTravelTableMacro.splitTableIdentifier("\"tab\"le1..table2"))
      .isInstanceOf(IllegalArgumentException.class);
  }
}
