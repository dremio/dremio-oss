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
package com.dremio.datastore;

import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.FindByRange;
import com.dremio.datastore.api.ImmutableFindByRange;
import com.dremio.datastore.api.options.ImmutableMaxResultsOption;
import com.dremio.datastore.api.options.MaxResultsOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestMapStore {
  private static final int RECORDS_TO_GENERATE = 100;

  private final MapStore store = new MapStore("test");

  @BeforeEach
  public void setUp() {
    Random random = new Random(42);
    for (int i = 0; i < RECORDS_TO_GENERATE; i++) {
      store.put(newRandomValue(random), newRandomValue(random));
    }
  }

  @Test
  public void testFindByRange_maxResults() {
    // Get all data.
    List<Document<byte[], byte[]>> allData = new ArrayList<>();
    store.find().forEach(allData::add);

    // Get subset of the data.
    MaxResultsOption maxResultsOption =
        new ImmutableMaxResultsOption.Builder().setMaxResults(10).build();
    int startIndex = allData.size() / 4;
    FindByRange<byte[]> findByRange =
        new ImmutableFindByRange.Builder<byte[]>()
            .setStart(allData.get(startIndex).getKey())
            .setIsStartInclusive(true)
            .build();
    Assertions.assertTrue(allData.size() > maxResultsOption.maxResults());
    List<Document<byte[], byte[]>> dataSubset = new ArrayList<>();
    store.find(findByRange, maxResultsOption).forEach(dataSubset::add);

    // Compare.
    List<Document<byte[], byte[]>> expectedSubset =
        allData.subList(startIndex, startIndex + dataSubset.size());
    Assertions.assertEquals(maxResultsOption.maxResults(), dataSubset.size());
    Assertions.assertEquals(maxResultsOption.maxResults(), expectedSubset.size());
    for (int i = 0; i < dataSubset.size(); i++) {
      Assertions.assertArrayEquals(dataSubset.get(i).getKey(), expectedSubset.get(i).getKey());
      Assertions.assertArrayEquals(dataSubset.get(i).getValue(), expectedSubset.get(i).getValue());
    }
  }

  private static byte[] newRandomValue(Random r) {
    int size = r.nextInt(Byte.MAX_VALUE);
    byte[] res = new byte[size];
    r.nextBytes(res);
    return res;
  }
}
