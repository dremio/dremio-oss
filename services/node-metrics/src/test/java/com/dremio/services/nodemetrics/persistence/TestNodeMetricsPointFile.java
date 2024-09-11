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

public class TestNodeMetricsPointFile {
  @Test
  public void testFrom() {
    String name = "20240513T143026Z.csv";
    NodeMetricsFile nodeMetricsFile = NodeMetricsPointFile.from(name);
    Assertions.assertEquals(CompactionType.Uncompacted, nodeMetricsFile.getCompactionType());
    Assertions.assertEquals(name, nodeMetricsFile.getName());
    ZonedDateTime expectedWriteTimestamp =
        ZonedDateTime.of(2024, 5, 13, 14, 30, 26, 0, ZoneId.of("UTC"));
    Assertions.assertEquals(expectedWriteTimestamp, nodeMetricsFile.getWriteTimestamp());
  }

  @Test
  public void testNewInstance() {
    Instant before = TimeUtils.getNowSeconds();
    NodeMetricsFile nodeMetricsFile = NodeMetricsPointFile.newInstance();
    Instant writeTimestamp = nodeMetricsFile.getWriteTimestamp().toInstant();
    Assertions.assertTrue(
        TimeUtils.firstIsBeforeOrEqualsSecond(before, writeTimestamp)
            && TimeUtils.firstIsAfterOrEqualsSecond(Instant.now(), writeTimestamp));
  }

  @Test
  public void testGetGlob_SingleCompacted() {
    Path baseDirectory = Path.of("/a/b");
    Path glob = NodeMetricsPointFile.getGlob(baseDirectory);
    Path expected =
        Path.of(
            "/a/b/[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]T[0-9][0-9][0-9][0-9][0-9][0-9]Z.csv");
    Assertions.assertEquals(expected, glob);
  }

  @Test
  public void testIsValid_InvalidPath() {
    String invalidName = ".DS_Store";
    Assertions.assertFalse(NodeMetricsPointFile.isValid(invalidName));
  }

  @Test
  public void testIsValid_Compacted() {
    String compactedName = "s_20240511T102011Z.csv";
    Assertions.assertFalse(NodeMetricsPointFile.isValid(compactedName));
  }
}
