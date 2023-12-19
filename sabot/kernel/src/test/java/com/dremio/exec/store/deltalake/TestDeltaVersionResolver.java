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

package com.dremio.exec.store.deltalake;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.connector.metadata.options.TimeTravelOption;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.proto.UserBitShared.DremioPBError.ErrorType;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.dfs.FileSelection;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.test.UserExceptionAssert;
import com.google.common.collect.ImmutableMap;

/**
 * Tests for {@link DeltaVersionResolver}
 */
public class TestDeltaVersionResolver extends BaseTestQuery {

  private static final Map<Long, Long> VERSION_TO_TIMESTAMP;
  private static final long OLDEST_VERSION, LASTEST_VERSION;
  private static final long OLDEST_TIMESTAMP, LASTEST_TIMESTAMP;

  static {
    VERSION_TO_TIMESTAMP = ImmutableMap.<Long, Long>builder()
      .put(0L, 1610656189269L)
      .put(1L, 1610656241989L)
      .put(2L, 1610656265268L)
      .put(3L, 1610656300374L)
      .put(4L, 1610656307820L)
      .put(5L, 1610656317464L)
      .put(6L, 1610656326407L)
      .put(7L, 1610656335387L)
      .put(8L, 1610656345288L)
      .put(9L, 1610656353823L)
      .put(10L, 1610656362281L)
      .put(11L, 1610656374302L)
      .put(12L, 1610656404221L)
      .put(13L, 1610656412485L)
      .put(14L, 1610656421489L)
      .put(15L, 1610656561811L)
      .put(16L, 1610656585855L)
      .put(17L, 1610656595704L)
      .put(18L, 1610656604782L)
      .put(19L, 1610656614408L)
      .put(20L, 1610656622916L)
      .put(21L, 1610656634347L)
      .put(22L, 1610656648256L)
      .put(23L, 1610656656293L)
      .put(24L, 1610656668686L)
      .put(25L, 1610656676559L)
      .build();
    OLDEST_VERSION = 0L;
    LASTEST_VERSION = 25L;
    OLDEST_TIMESTAMP = 1610656189269L;
    LASTEST_TIMESTAMP = 1610656676559L;
  }

  private FileSystem fs;
  private SabotContext sabotContext;
  private Path metadataDir;

  @Before
  public void setup() throws Exception {
    fs = HadoopFileSystem.getLocal(new Configuration());
    sabotContext = getSabotContext();
    metadataDir = resolveMetadataDir("src/test/resources/deltalake/covid_cases");
  }

  private Path resolveMetadataDir(String path) throws IOException {
    File f = new File(path);
    FileSelection selection = FileSelection.createNotExpanded(fs, Path.of(f.getAbsolutePath()));
    return Path.of(selection.getSelectionRoot()).resolve(DeltaConstants.DELTA_LOG_DIR);
  }

  @Test
  public void testLastCheckpointRead() throws IOException {
    Path metaDir = resolveMetadataDir("src/test/resources/deltalake/checkpoint_test");
    DeltaVersionResolver resolver = new DeltaVersionResolver(sabotContext, fs, metaDir);
    DeltaVersion lastCheckPoint = resolver.getLastCheckpoint();
    assertEquals(true, lastCheckPoint.isCheckpoint());
    assertEquals(10L, lastCheckPoint.getVersion());
    assertEquals(1, lastCheckPoint.getSubparts());
  }

  @Test
  public void testEmptyLastCheckpointRead() throws IOException {
    Path metaDir = resolveMetadataDir("src/test/resources/deltalake/checkpoint_empty");
    DeltaVersionResolver resolver = new DeltaVersionResolver(sabotContext, fs, metaDir);
    UserExceptionAssert.assertThatThrownBy(resolver::getLastCheckpoint)
      .hasMessageContaining("Failed to read _last_checkpoint file");
  }

  @Test
  public void testSnapshotIdRequest() throws Exception {
    DeltaVersionResolver resolver = new DeltaVersionResolver(sabotContext, fs, metadataDir);

    try (AutoCloseable ac = withSystemOption(ExecConstants.ENABLE_DELTALAKE_TIME_TRAVEL, true)) {
      for (long version : VERSION_TO_TIMESTAMP.keySet()) {
        assertEquals(DeltaVersion.of(version), resolver.resolve(TimeTravelOption.newSnapshotIdRequest(String.valueOf(version))));
      }
    }
  }

  @Test
  public void testSnapshotIdRequestInvalidIds() throws Exception {
    DeltaVersionResolver resolver = new DeltaVersionResolver(sabotContext, fs, metadataDir);

    try (AutoCloseable ac = withSystemOption(ExecConstants.ENABLE_DELTALAKE_TIME_TRAVEL, true)) {
      UserExceptionAssert.assertThatThrownBy(() -> resolver.resolve(TimeTravelOption.newSnapshotIdRequest("abc")))
        .hasMessageContaining("The provided snapshot ID 'abc' is invalid")
        .hasErrorType(ErrorType.VALIDATION)
        .cause().isInstanceOf(NumberFormatException.class);

      UserExceptionAssert.assertThatThrownBy(() -> resolver.resolve(TimeTravelOption.newSnapshotIdRequest("")))
        .hasMessageContaining("The provided snapshot ID '' is invalid")
        .hasErrorType(ErrorType.VALIDATION)
        .cause().isInstanceOf(NumberFormatException.class);

      UserExceptionAssert.assertThatThrownBy(() -> resolver.resolve(TimeTravelOption.newSnapshotIdRequest("-5")))
        .hasMessageContaining("The provided snapshot ID '-5' is invalid")
        .hasErrorType(ErrorType.VALIDATION)
        .cause().isInstanceOf(FileNotFoundException.class);

      UserExceptionAssert.assertThatThrownBy(() -> resolver.resolve(TimeTravelOption.newSnapshotIdRequest("26")))
        .hasMessageContaining("The provided snapshot ID '26' is invalid")
        .hasErrorType(ErrorType.VALIDATION)
        .cause().isInstanceOf(FileNotFoundException.class);
    }
  }

  @Test
  public void testTimestampRequestExactTImestamps() throws Exception {
    DeltaVersionResolver resolver = new DeltaVersionResolver(sabotContext, fs, metadataDir);

    try (AutoCloseable ac = withSystemOption(ExecConstants.ENABLE_DELTALAKE_TIME_TRAVEL, true)) {
      for (Long version : VERSION_TO_TIMESTAMP.keySet()) {
        assertEquals(DeltaVersion.of(version), resolver.resolve(TimeTravelOption.newTimestampRequest(VERSION_TO_TIMESTAMP.get(version))));
      }
    }
  }

  @Test
  public void testTimestampRequestInaccurateTimestamps() throws Exception {
    DeltaVersionResolver resolver = new DeltaVersionResolver(sabotContext, fs, metadataDir);

    try (AutoCloseable ac = withSystemOption(ExecConstants.ENABLE_DELTALAKE_TIME_TRAVEL, true)) {
      // DeltaVersionResolver resolves version before or at timestamp.

      for (long version = OLDEST_VERSION + 1; version <= LASTEST_VERSION; ++version) {
        long timestamp = VERSION_TO_TIMESTAMP.get(version);
        assertEquals(DeltaVersion.of(version), resolver.resolve(TimeTravelOption.newTimestampRequest(timestamp)));
        assertEquals(DeltaVersion.of(version), resolver.resolve(TimeTravelOption.newTimestampRequest(timestamp + 10L)));
        assertEquals(DeltaVersion.of(version - 1), resolver.resolve(TimeTravelOption.newTimestampRequest(timestamp - 10L)));
      }
    }
  }

  @Test
  public void testTimestampRequestBoundaryTimestamps() throws Exception {
    DeltaVersionResolver resolver = new DeltaVersionResolver(sabotContext, fs, metadataDir);

    try (AutoCloseable ac = withSystemOption(ExecConstants.ENABLE_DELTALAKE_TIME_TRAVEL, true)) {
      assertEquals(DeltaVersion.of(OLDEST_VERSION), resolver.resolve(TimeTravelOption.newTimestampRequest(OLDEST_TIMESTAMP)));
      assertEquals(DeltaVersion.of(OLDEST_VERSION), resolver.resolve(TimeTravelOption.newTimestampRequest(OLDEST_TIMESTAMP + 10L)));
      UserExceptionAssert.assertThatThrownBy(() -> resolver.resolve(TimeTravelOption.newTimestampRequest(OLDEST_TIMESTAMP - 10L)))
        .hasMessageContaining("The provided Time Travel timestamp value '%s' is out of range", String.valueOf(OLDEST_TIMESTAMP - 10L))
        .hasErrorType(ErrorType.VALIDATION);

      // resolves timestamps after the latest timestamp as the last version
      assertEquals(DeltaVersion.of(LASTEST_VERSION), resolver.resolve(TimeTravelOption.newTimestampRequest(LASTEST_TIMESTAMP + 10)));
      assertEquals(DeltaVersion.of(LASTEST_VERSION), resolver.resolve(TimeTravelOption.newTimestampRequest(System.currentTimeMillis())));
    }
  }

  @Test
  public void testTimestampRequestBoundaryTimestampsWithCustomResolving() throws Exception {
    DeltaVersionResolver resolver = new DeltaVersionResolver(sabotContext, fs, metadataDir);

    try (AutoCloseable ac = withSystemOption(ExecConstants.ENABLE_DELTALAKE_TIME_TRAVEL, true)) {
      // alter the default settings to enable reading before the oldest available version
      resolver.setResolveOldest(true);
      assertEquals(DeltaVersion.of(OLDEST_VERSION), resolver.resolve(TimeTravelOption.newTimestampRequest(OLDEST_TIMESTAMP - 10L)));

      // alter the default settings to disable reading after the latest available version
      resolver.setResolveLatest(false);
      UserExceptionAssert.assertThatThrownBy(() -> resolver.resolve(TimeTravelOption.newTimestampRequest(LASTEST_TIMESTAMP + 10L)))
        .hasMessageContaining("The provided Time Travel timestamp value '%s' is out of range", String.valueOf(LASTEST_TIMESTAMP + 10L))
        .hasErrorType(ErrorType.VALIDATION);
    }
  }

  @Test
  public void testEmptyTimeTravelRequestLastCheckpoint() {
    // DeltaVersionResolver resolves empty request to the last checkpoint version

    DeltaVersionResolver resolver = new DeltaVersionResolver(sabotContext, fs, metadataDir);
    assertEquals(DeltaVersion.ofCheckpoint(20L, 1), resolver.resolve(null));
  }

  @Test
  public void testEmptyTimeTravelRequestJsonDataset() throws Exception {
    // DeltaVersionResolver resolves empty request to '0' version, if no checkpoint is present

    Path metaDir = resolveMetadataDir("src/test/resources/deltalake/JsonDataset");
    DeltaVersionResolver resolver = new DeltaVersionResolver(sabotContext, fs, metaDir);
    assertEquals(DeltaVersion.ofCheckpoint(0L, 1), resolver.resolve(null));
  }
}
