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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import com.dremio.common.concurrent.NamedThreadFactory;
import com.dremio.config.DremioConfig;
import com.dremio.exec.ExecConstants;
import com.dremio.io.file.FileBlockLocation;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.options.OptionManager;
import com.dremio.sabot.exec.context.OpProfileDef;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.BlockLocationsList;

/**
 * Test for {@link BlockLocationsCacheManager}
 */
@RunWith(value = Enclosed.class)
public class BlockLocationsCacheManagerTest {
  public static class SchemeTest {
    @ClassRule
    public static final TemporaryFolder tempDir = new TemporaryFolder();
    private static final int FILE_SIZE = 100;

    private BlockLocationsCacheManager cacheManager;
    private OperatorStats operatorStats;

    private void setUp(String filePath) throws Exception {
      FileSystem fileSystem = mock(FileSystem.class);
      when(fileSystem.supportsBlockAffinity()).thenReturn(true);
      when(fileSystem.getFileBlockLocations(Path.of(filePath), 0, FILE_SIZE)).thenReturn(Collections.singletonList(new FileBlockLocation() {
        @Override
        public long getOffset() {
          return 0;
        }

        @Override
        public long getSize() {
          return FILE_SIZE;
        }

        @Override
        public List<String> getHosts() {
          return Collections.singletonList("host");
        }
      }));

      OptionManager optionManager = mock(OptionManager.class);
      when(optionManager.getOption(ExecConstants.HADOOP_BLOCK_CACHE_ENABLED)).thenReturn(true);
      OperatorContext operatorContext = mock(OperatorContext.class);
      when(operatorContext.getOptions()).thenReturn(optionManager);
      DremioConfig dremioConfig = mock(DremioConfig.class);
      when(dremioConfig.getString(DremioConfig.CACHE_DB_PATH)).thenReturn(tempDir.newFolder().toString());

      operatorStats = new OperatorStats(new OpProfileDef(1, 1, 1, 1), null);
      when(operatorContext.getStats()).thenReturn(operatorStats);
      Function<RocksDbBroker, RecordingCacheReaderWriter> brokerFunction =
        broker -> spy(new RecordingCacheReaderWriter(fileSystem, broker, operatorStats));

      cacheManager = BlockLocationsCacheManager.newInstance(fileSystem, "plugin-id", operatorContext, dremioConfig, brokerFunction);
    }

    @After
    public void tearDown() throws Exception {
      cacheManager.close();
    }

    @Test
    public void testCreateCacheWithUnsupportedScheme() throws Exception {
      String filePath = "s3://path";
      setUp(filePath);
      assertNull(cacheManager.createIfAbsent(filePath, FILE_SIZE));
    }

    @Test
    public void testCreateCacheWithUnsupportedSchemeAndWhitespaceInPath() throws Exception {
      String filePath = "s3://new path";
      setUp(filePath);
      assertNull(cacheManager.createIfAbsent(filePath, FILE_SIZE));
    }

    @Test
    public void testCreateCacheWithUnsupportedSchemeContainingAuthorityAndWhitespaceInPath() throws Exception {
      String filePath = "s3://some-host:12345/new path";
      setUp(filePath);
      assertNull(cacheManager.createIfAbsent(filePath, FILE_SIZE));
    }

    @Test
    public void testCreateCacheWithSupportedScheme() throws Exception {
      String filePath = "hdfs://path";
      setUp(filePath);
      assertNotNull(cacheManager.createIfAbsent(filePath, FILE_SIZE));
    }

    @Test
    public void testCreateCacheWithSupportedSchemeAndWhitespaceInPath() throws Exception {
      String filePath = "hdfs://new path";
      setUp(filePath);
      assertNotNull(cacheManager.createIfAbsent(filePath, FILE_SIZE));
    }

    @Test
    public void testCreateCacheWithSupportedSchemeContainingAuthorityAndWhitespaceInPath() throws Exception {
      String filePath = "hdfs://some-host:12345/new path";
      setUp(filePath);
      assertNotNull(cacheManager.createIfAbsent(filePath, FILE_SIZE));
    }

    @Test
    public void testCreateCacheWithNoScheme() throws Exception {
      String filePath = "/path";
      setUp(filePath);
      assertNotNull(cacheManager.createIfAbsent(filePath, FILE_SIZE));
    }

    @Test
    public void testCreateCacheWithNoSchemeAndWhitespaceInPath() throws Exception {
      String filePath = "/new path";
      setUp(filePath);
      assertNotNull(cacheManager.createIfAbsent(filePath, FILE_SIZE));
    }

    @Test
    public void testCreateCacheWithNoSchemeAndRelativePath() throws Exception {
      String filePath = "path";
      setUp(filePath);
      assertNotNull(cacheManager.createIfAbsent(filePath, FILE_SIZE));
    }

    @Test
    public void testCreateCacheWithNoSchemeAndWhitespaceInRelativePath() throws Exception {
      String filePath = "new path";
      setUp(filePath);
      assertNotNull(cacheManager.createIfAbsent(filePath, FILE_SIZE));
    }
  }

  public static class ComplexTests {
    @ClassRule
    public static final TemporaryFolder tempDir = new TemporaryFolder();

    private final int fileSizeBase = 256;
    private final int uniqueFileCount = 30;
    private final String filePathBase = "filePath";
    private final List<byte[]> uniqueKeys = new ArrayList<>();

    private BlockLocationsCacheManager cacheManager;
    private RecordingCacheReaderWriter readerWriter;
    private OperatorStats operatorStats;

    @Before
    public void setUp() throws Exception {
      FileSystem fileSystem = mock(FileSystem.class);
      when(fileSystem.supportsBlockAffinity()).thenReturn(true);
      for (int i = 0; i < uniqueFileCount; i++) {
        uniqueKeys.add(("hello" + i).getBytes());
        int finalI = i;
        when(fileSystem.getFileBlockLocations(Path.of(filePathBase + i), 0, fileSizeBase + finalI)).thenReturn(Collections.singletonList(new FileBlockLocation() {
          @Override
          public long getOffset() {
            return finalI;
          }

          @Override
          public long getSize() {
            return finalI;
          }

          @Override
          public List<String> getHosts() {
            return Collections.singletonList("host" + finalI);
          }
        }));
      }

      OptionManager optionManager = mock(OptionManager.class);
      when(optionManager.getOption(ExecConstants.HADOOP_BLOCK_CACHE_ENABLED)).thenReturn(true);
      OperatorContext operatorContext = mock(OperatorContext.class);
      when(operatorContext.getOptions()).thenReturn(optionManager);
      DremioConfig dremioConfig = mock(DremioConfig.class);
      when(dremioConfig.getString(DremioConfig.CACHE_DB_PATH)).thenReturn(tempDir.newFolder().toString());

      operatorStats = new OperatorStats(new OpProfileDef(1, 1, 1, 1), null);
      when(operatorContext.getStats()).thenReturn(operatorStats);
      Function<RocksDbBroker, RecordingCacheReaderWriter> brokerFunction =
        broker -> spy(new RecordingCacheReaderWriter(fileSystem, broker, operatorStats));

      cacheManager = BlockLocationsCacheManager.newInstance(fileSystem, "plugin-id", operatorContext, dremioConfig, brokerFunction);
      readerWriter = cacheManager.getReaderWriter();
    }

    @After
    public void tearDown() throws Exception {
      cacheManager.close();
      BlockLocationsCacheManager.resetRocksDbBroker();
    }

    @Test
    public void testCorrectMetricsPopulation() throws Exception {
      for (int i = 0; i < uniqueFileCount; i++) {
        cacheManager.createIfAbsent(filePathBase + i, fileSizeBase + i);
        cacheManager.createIfAbsent(filePathBase + i, fileSizeBase + i);
      }
      cacheManager.close();

      assertEquals(uniqueFileCount, operatorStats.getLongStat(BLOCK_AFFINITY_CACHE_MISSES)); // from first call
      assertEquals(uniqueFileCount, operatorStats.getLongStat(BLOCK_AFFINITY_CACHE_HITS)); // from second call
    }

    @Test
    public void testCreateReturnsTheSameOutput() {
      BlockLocationsList blockLocations = cacheManager.createIfAbsent(filePathBase + '0', fileSizeBase);
      BlockLocationsList blockLocations2 = cacheManager.createIfAbsent(filePathBase + '0', fileSizeBase);
      assertEquals(blockLocations, blockLocations2);
    }

    @Test
    public void testPutHappensOnceOnConcurrentCreate() {
      final int threadCount = 10;
      ExecutorService executorService = Executors.newFixedThreadPool(threadCount, new NamedThreadFactory("thread-"));
      List<Future<BlockLocationsList>> futures = new ArrayList<>();
      for (int j = 0; j < uniqueFileCount; j++) {
        for (int i = 0; i < threadCount; i++) {
          int finalJ = j;
          Future<BlockLocationsList> future = executorService.submit(() -> cacheManager.putOnce(uniqueKeys.get(finalJ), filePathBase + finalJ, fileSizeBase + finalJ));
          futures.add(future);
        }
      }

      assertEquals(uniqueFileCount, getBlockLocations(futures).size());
      for (int i = 0; i < uniqueFileCount; i++) {
        verify(readerWriter, times(1)).put(uniqueKeys.get(i), filePathBase + i, fileSizeBase + i);
      }
    }

    private Set<BlockLocationsList> getBlockLocations(List<Future<BlockLocationsList>> futures) {
      Set<BlockLocationsList> blockLocations = new HashSet<>();
      for (Future<BlockLocationsList> future : futures) {
        try {
          blockLocations.add(future.get());
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
      return blockLocations;
    }
  }
}