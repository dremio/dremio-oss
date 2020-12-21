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
package com.dremio.service.flight;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.util.Text;

/**
 * Builder for Flight correctness tests. Based on com.dremio.TestBuilder,
 * but simplified to only do in-memory tests.
 */
public class FlightTestBuilder {
  private final FlightClientUtils.FlightClientWrapper flightClientWrapper;
  private List<Map<String, Object>> baselineRecords;
  private String[] baselineColumns;
  private String query;

  public FlightTestBuilder(FlightClientUtils.FlightClientWrapper flightClientWrapper) {
    this.flightClientWrapper = flightClientWrapper;
  }

  public FlightTestBuilder expectsEmptyResultSet() {
    baselineRecords = new ArrayList<>();
    return this;
  }

  public FlightTestBuilder sqlQuery(String query) {
    this.query = query;
    return this;
  }

  FlightTestBuilder baselineColumns(String... columns) {
    baselineColumns = columns;
    return this;
  }

  public FlightQueryTestWrapper build() {
    return new FlightQueryTestWrapper(flightClientWrapper, baselineRecords, query);
  }

  public void go() throws Exception {
    build().run();
  }

  public FlightTestBuilder baselineValues(Object ... baselineValues) {
    if (baselineRecords == null) {
      baselineRecords = new ArrayList<>();
    }
    Map<String, Object> ret = new LinkedHashMap<>();
    int i = 0;
    assertTrue("Must set expected columns before baseline values/records.", baselineColumns != null);
    if (baselineValues == null) {
      baselineValues = new Object[] {null};
    }
    assertEquals("Must supply the same number of baseline values as columns.", baselineValues.length, baselineColumns.length);
    for (String s : baselineColumns) {
      ret.put(s, toArrowType(baselineValues[i]));
      i++;
    }
    this.baselineRecords.add(ret);
    return this;
  }

  /**
   * Coerce standard value types used an inputs to testBuilder to Arrow types.
   */
  static Object toArrowType(Object originalValue) {
    if (originalValue instanceof String) {
      return new Text((String) originalValue);
    }

    return originalValue;
  }
}
