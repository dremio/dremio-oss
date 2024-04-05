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
package com.dremio.exec.physical.impl;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dremio.BaseTestQuery;
import com.dremio.common.exceptions.UserRemoteException;
import java.util.ArrayList;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;

/** Test for GeoHash Decode and Encode. */
public class TestGeoHash extends BaseTestQuery {
  class TestInput {
    String hash;
    double lat;
    double lon;
    Optional<Long> precision;

    TestInput(String newHash, double newLat, double newLon) {
      hash = newHash;
      lon = newLon;
      lat = newLat;
      precision = Optional.empty();
    }

    TestInput(String newHash, double newLat, double newLon, long p) {
      hash = newHash;
      lon = newLon;
      lat = newLat;
      precision = Optional.of(p);
    }
  }

  final ArrayList<TestInput> testData = new ArrayList<>();
  final ArrayList<TestInput> testErrorData = new ArrayList<>();
  final ArrayList<TestInput> testDataDecodeOnly = new ArrayList<>();
  final ArrayList<TestInput> testLonRangeErrorData = new ArrayList<>();
  final ArrayList<TestInput> testLatRangeErrorData = new ArrayList<>();

  @Before
  public void setupTestData() {
    testData.add(new TestInput("ezs427zzzzzz7zzzzzzz", 42.60498038493089, -5.603027511388223));
    testData.add(new TestInput("ezs42", 42.60498046875, -5.60302734375, 5));
    testData.add(new TestInput("e", 22.5, -22.5, 1));
    testData.add(new TestInput("ezs427zzzzzz", 42.60498038493097, -5.603027511388063, 12));
    testData.add(new TestInput("ezs42ebpbpbj1n92syp0", 42.60498, -5.6030273));
    testData.add(new TestInput("u4pruydqqvj8pr9yc27r", 57.64911, 10.40744));
    testData.add(new TestInput("zzzzzzzzzzzzzzzzzzzz", 90.0, 180.0));
    testData.add(new TestInput("00000000000000000000", -90.0, -180.0));

    testDataDecodeOnly.add(new TestInput("", 0.0, 0.0));
    testDataDecodeOnly.add(new TestInput("aa", 0.0, 0.0));
    testDataDecodeOnly.add(new TestInput("eae", 0.0, 0.0));

    testErrorData.add(new TestInput("???", 57.64911, 10.40744, -12));
    testErrorData.add(new TestInput("???", 57.64911, 10.40744, 0));
    testErrorData.add(new TestInput("???", 57.64911, 10.40744, 21));

    testLonRangeErrorData.add(new TestInput("???", 57.64911, 910.40744, 12));
    testLonRangeErrorData.add(new TestInput("???", 57.64911, -710.40744));
    testLonRangeErrorData.add(new TestInput("???", 57.64911, 180.40744));

    testLatRangeErrorData.add(new TestInput("???", 97.64911, 10.40744, 12));
    testLatRangeErrorData.add(new TestInput("???", -157.64911, -10.40744));
    testLatRangeErrorData.add(new TestInput("???", 90.01, 80.40744));
  }

  private void testDecode(TestInput test) throws Exception {
    {
      final String queryKey = "select st_fromgeohash('" + test.hash + "')['Latitude'] as Latitude";

      testBuilder()
          .sqlQuery(queryKey)
          .unOrdered()
          .baselineColumns("Latitude")
          .baselineValues(test.lat)
          .go();
    }
    {
      final String queryKey =
          "select st_fromgeohash('" + test.hash + "')['Longitude'] as Longitude";

      testBuilder()
          .sqlQuery(queryKey)
          .unOrdered()
          .baselineColumns("Longitude")
          .baselineValues(test.lon)
          .go();
    }
  }

  @Test
  public void hashDecode() throws Exception {
    for (TestInput test : testData) {
      testDecode(test);
    }
  }

  @Test
  public void hashEncode() throws Exception {
    for (TestInput test : testData) {
      String precisionOption = "";
      if (test.precision.isPresent()) {
        precisionOption = ", " + test.precision.get();
      }
      final String queryKey =
          "select st_geohash(" + test.lat + "," + test.lon + precisionOption + ") as hash";

      testBuilder()
          .sqlQuery(queryKey)
          .unOrdered()
          .baselineColumns("hash")
          .baselineValues(test.hash)
          .go();
    }
  }

  @Test
  public void hashEncodeErrors() throws Exception {
    for (TestInput test : testErrorData) {
      String precisionOption = "";
      if (test.precision.isPresent()) {
        precisionOption = ", " + test.precision.get();
      }
      final String queryKey =
          "select st_geohash(" + test.lat + "," + test.lon + precisionOption + ") as hash";

      assertThatThrownBy(() -> test(queryKey))
          .isInstanceOf(UserRemoteException.class)
          .hasMessageContaining("precision must be between 1 and 20");
    }
  }

  @Test
  public void hashEncodeLonRange() throws Exception {
    for (TestInput test : testLonRangeErrorData) {
      String precisionOption = "";
      if (test.precision.isPresent()) {
        precisionOption = ", " + test.precision.get();
      }
      final String queryKey =
          "select st_geohash(" + test.lat + "," + test.lon + precisionOption + ") as hash";

      assertThatThrownBy(() -> test(queryKey))
          .isInstanceOf(UserRemoteException.class)
          .hasMessageContaining("longitude must be between –180° and +180°");
    }
  }

  @Test
  public void hashEncodeLatRange() throws Exception {
    for (TestInput test : testLatRangeErrorData) {
      String precisionOption = "";
      if (test.precision.isPresent()) {
        precisionOption = ", " + test.precision.get();
      }
      final String queryKey =
          "select st_geohash(" + test.lat + "," + test.lon + precisionOption + ") as hash";

      assertThatThrownBy(() -> test(queryKey))
          .isInstanceOf(UserRemoteException.class)
          .hasMessageContaining("latitude must be between –90° and +90°");
    }
  }

  @Test
  public void hashBadHash() throws Exception {
    for (TestInput test : testDataDecodeOnly) {
      assertThatThrownBy(() -> testDecode(test))
          .hasMessageContaining("geohash must be a valid, base32-encoded geohash");
    }
  }
}
