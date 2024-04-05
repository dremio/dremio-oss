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
package com.dremio.exec.store.cache;

import static com.dremio.sabot.op.scan.ScanOperator.Metric.BLOCK_AFFINITY_CACHE_HITS;
import static com.dremio.sabot.op.scan.ScanOperator.Metric.BLOCK_AFFINITY_CACHE_MISSES;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OpProfileDef;
import com.dremio.sabot.exec.context.OperatorStats;
import java.util.Collections;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Tests for {@link RecordingCacheReaderWriter} */
public class RecordingCacheReaderWriterTest {
  private OperatorStats operatorStats;
  private RecordingCacheReaderWriter cacheReaderWriter;

  @ClassRule public static final TemporaryFolder tempDir = new TemporaryFolder();

  @Before
  public void setup() throws Exception {
    FileSystem fs = mock(FileSystem.class);
    when(fs.getFileBlockLocations((com.dremio.io.file.Path) any(), anyLong(), anyLong()))
        .then(mock -> Collections.emptyList());

    operatorStats = new OperatorStats(new OpProfileDef(1, 1, 1, 1), null);
    RocksDbBroker broker = new RocksDbBroker(tempDir.newFolder().toString());
    cacheReaderWriter = new RecordingCacheReaderWriter(fs, broker, operatorStats);
  }

  @After
  public void tearDown() throws Exception {
    cacheReaderWriter.close();
  }

  @Test
  public void testGetPut() {
    byte[] key = "hello".getBytes();
    byte[] value = cacheReaderWriter.get(key, true);
    assertNull(value);

    byte[] putValue = cacheReaderWriter.put(key, "file-1.parquet", 256);
    assertArrayEquals(putValue, cacheReaderWriter.get(key, true));
  }

  @Test
  public void testNoMetricsWithoutClose() {
    byte[] key = "hello".getBytes();
    byte[] value = cacheReaderWriter.get(key, true);
    assertNull(value);

    cacheReaderWriter.put(key, "file-1.parquet", 256);
    value = cacheReaderWriter.get(key, true);
    assertNotNull(value);

    assertEquals(0, operatorStats.getLongStat(BLOCK_AFFINITY_CACHE_MISSES));
    assertEquals(0, operatorStats.getLongStat(BLOCK_AFFINITY_CACHE_HITS));
  }

  @Test
  public void testMetricsUponClose() throws Exception {
    byte[] key = "hello".getBytes();
    cacheReaderWriter.get(key, true);
    cacheReaderWriter.put(key, "file-1.parquet", 256);

    int totalGets = 10;
    for (int i = 0; i < totalGets; i++) {
      cacheReaderWriter.get(key, true);
    }

    cacheReaderWriter.close();
    assertEquals(1, operatorStats.getLongStat(BLOCK_AFFINITY_CACHE_MISSES)); // for the first GET
    assertEquals(
        totalGets, operatorStats.getLongStat(BLOCK_AFFINITY_CACHE_HITS)); // for rest of the GETs
  }

  @Test
  public void testNoMetricsIfMetricsRecordingIsOff() throws Exception {
    byte[] key = "hello".getBytes();
    byte[] value = cacheReaderWriter.get(key, false);
    assertNull(value);

    cacheReaderWriter.close();
    assertEquals(0, operatorStats.getLongStat(BLOCK_AFFINITY_CACHE_MISSES));
    assertEquals(0, operatorStats.getLongStat(BLOCK_AFFINITY_CACHE_HITS));
  }

  @Test
  public void testIdempotenceOfClose() throws Exception {
    cacheReaderWriter.get("hello".getBytes(), true);
    cacheReaderWriter.close();
    assertEquals(1, operatorStats.getLongStat(BLOCK_AFFINITY_CACHE_MISSES));

    // Another close() shouldn't change the stats
    cacheReaderWriter.close();
    assertEquals(1, operatorStats.getLongStat(BLOCK_AFFINITY_CACHE_MISSES));
  }
}
