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
package com.dremio.services.nodemetrics.persistence;

import com.dremio.io.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestNodeMetricsCompactedFile {
  private static final String SINGLE_COMPACTED = "s_20240513T143026Z.csv";
  private static final String DOUBLE_COMPACTED = "d_20240512T081459Z.csv";

  @Test
  public void testFrom_SingleCompacted() {
    NodeMetricsFile nodeMetricsFile = NodeMetricsCompactedFile.from(SINGLE_COMPACTED);
    Assertions.assertEquals(CompactionType.Single, nodeMetricsFile.getCompactionType());
    Assertions.assertEquals(SINGLE_COMPACTED, nodeMetricsFile.getName());
    ZonedDateTime expectedWriteTimestamp =
        ZonedDateTime.of(2024, 5, 13, 14, 30, 26, 0, ZoneId.of("UTC"));
    Assertions.assertEquals(expectedWriteTimestamp, nodeMetricsFile.getWriteTimestamp());
  }

  @Test
  public void testFrom_DoubleCompacted() {
    NodeMetricsFile nodeMetricsFile = NodeMetricsCompactedFile.from(DOUBLE_COMPACTED);
    Assertions.assertEquals(CompactionType.Double, nodeMetricsFile.getCompactionType());
    Assertions.assertEquals(DOUBLE_COMPACTED, nodeMetricsFile.getName());
    ZonedDateTime expectedWriteTimestamp =
        ZonedDateTime.of(2024, 5, 12, 8, 14, 59, 0, ZoneId.of("UTC"));
    Assertions.assertEquals(expectedWriteTimestamp, nodeMetricsFile.getWriteTimestamp());
  }

  @Test
  public void testNewInstance_SingleCompacted() {
    Instant before = TimeUtils.getNowSeconds();
    NodeMetricsFile nodeMetricsFile = NodeMetricsCompactedFile.newInstance(CompactionType.Single);
    Assertions.assertEquals(CompactionType.Single, nodeMetricsFile.getCompactionType());
    Instant writeTimestamp = nodeMetricsFile.getWriteTimestamp().toInstant();
    Assertions.assertTrue(
        TimeUtils.firstIsBeforeOrEqualsSecond(before, writeTimestamp)
            && TimeUtils.firstIsAfterOrEqualsSecond(Instant.now(), writeTimestamp));
  }

  @Test
  public void testNewInstance_DoubleCompacted() {
    Instant before = TimeUtils.getNowSeconds();
    NodeMetricsFile nodeMetricsFile = NodeMetricsCompactedFile.newInstance(CompactionType.Double);
    Assertions.assertEquals(CompactionType.Double, nodeMetricsFile.getCompactionType());
    Instant writeTimestamp = nodeMetricsFile.getWriteTimestamp().toInstant();
    Assertions.assertTrue(
        TimeUtils.firstIsBeforeOrEqualsSecond(before, writeTimestamp)
            && TimeUtils.firstIsAfterOrEqualsSecond(Instant.now(), writeTimestamp));
  }

  @Test
  public void testGetGlob_SingleCompacted() {
    Path baseDirectory = Path.of("/a/b");
    Path glob = NodeMetricsCompactedFile.getGlob(baseDirectory, CompactionType.Single);
    Path expected =
        Path.of(
            "/a/b/s_[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]T[0-9][0-9][0-9][0-9][0-9][0-9]Z.csv");
    Assertions.assertEquals(expected, glob);
  }

  @Test
  public void testGetGlob_DoubleCompacted() {
    Path baseDirectory = Path.of("/a/b");
    Path glob = NodeMetricsCompactedFile.getGlob(baseDirectory, CompactionType.Double);
    Path expected =
        Path.of(
            "/a/b/d_[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]T[0-9][0-9][0-9][0-9][0-9][0-9]Z.csv");
    Assertions.assertEquals(expected, glob);
  }

  @Test
  public void testIsValid_InvalidPath() {
    String invalidName = ".DS_Store";
    Assertions.assertFalse(NodeMetricsCompactedFile.isValid(invalidName));
  }

  @Test
  public void testIsValid_Uncompacted() {
    String uncompactedName = "20240511T102011Z.csv";
    Assertions.assertFalse(NodeMetricsCompactedFile.isValid(uncompactedName));
  }
}
