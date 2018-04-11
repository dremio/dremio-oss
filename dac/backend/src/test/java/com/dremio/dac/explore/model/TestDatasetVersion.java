/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.dac.explore.model;

import static com.dremio.service.namespace.dataset.DatasetVersion.MAX_VERSION;
import static com.dremio.service.namespace.dataset.DatasetVersion.MIN_VERSION;
import static java.lang.Long.MAX_VALUE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.text.SimpleDateFormat;

import org.junit.Assert;
import org.junit.Test;

import com.dremio.service.namespace.dataset.DatasetVersion;

/**
 * DatasetVersion must work in a given range
 */
public class TestDatasetVersion {

  @Test
  public void test() throws Exception {
    DatasetVersion p = DatasetVersion.newVersion();
    int n = 10000;
    for (int i = 0; i < n; i++) {
      DatasetVersion v = DatasetVersion.newVersion();
      Assert.assertTrue(p.getLowerBound().compareTo(v) <= 0);
      Assert.assertTrue(p.compareTo(v.getUpperBound()) <= 0);
      p = v;
    }
  }

  @Test
  public void testRange() throws Exception {
    SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
    long o = f.parse("2015-08-17").getTime();
    long e = f.parse("2154-12-29").getTime();
    DatasetVersion v1 = new DatasetVersion(o, 0);
    DatasetVersion v2 = new DatasetVersion(e, 0);
    Assert.assertEquals(o, v1.getTimestamp());
    Assert.assertEquals(e, v2.getTimestamp());
    Assert.assertTrue(v1.compareTo(v2) < 0);
  }

  @Test
  public void testRandom() {
    DatasetVersion v1 = DatasetVersion.newVersion();
    assertEquals(v1, new DatasetVersion(v1.toString()));
    assertTrue(v1.compareTo(MIN_VERSION) >= 0);
    assertTrue(v1.compareTo(MAX_VERSION) <= 0);
    assertTrue(MIN_VERSION.compareTo(MAX_VERSION) < 0);
  }

  @Test
  public void testMin() {
    String string = MIN_VERSION.toString();
    DatasetVersion v2 = new DatasetVersion(string);
    Assert.assertEquals(0, v2.getValue());
    Assert.assertEquals(MIN_VERSION, v2);
    Assert.assertTrue(MIN_VERSION.getLowerBound().compareTo(MIN_VERSION) == 0);
  }

  @Test
  public void testMax() {
    String string = MAX_VERSION.toString();
    DatasetVersion v2 = new DatasetVersion(string);
    Assert.assertEquals(MAX_VALUE, v2.getValue());
    Assert.assertEquals(MAX_VERSION, v2);
    Assert.assertTrue(MAX_VERSION.compareTo(MAX_VERSION.getUpperBound()) == 0);
  }
}
