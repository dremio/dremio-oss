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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.Objects;
import org.jetbrains.annotations.NotNull;

/**
 * An abstract representation of a compacted node metrics file, i.e. that represents aggregate node
 * metrics at multiple points in time
 */
final class NodeMetricsCompactedFile implements NodeMetricsFile {
  private static final NodeMetricsDateTimeFormatter DATETIME_FORMATTER =
      new SecondPrecisionDateTimeFormatter();
  private static final Map<CompactionType, Character> COMPACTION_FILE_PREFIX =
      ImmutableMap.of(CompactionType.Single, 's', CompactionType.Double, 'd');

  private final String name;
  private final CompactionType compactionType;

  private NodeMetricsCompactedFile(String name, CompactionType compactionType) {
    this.name = name;
    this.compactionType = compactionType;
  }

  /** Instantiate from a specified file path */
  static NodeMetricsFile from(String fileName) {
    Preconditions.checkArgument(isValid(fileName));
    CompactionType compactionType = getCompactionType(fileName);
    return new NodeMetricsCompactedFile(fileName, compactionType);
  }

  /** Instantiate with a generated file name given a specified directory and compaction type */
  static NodeMetricsFile newInstance(CompactionType compactionType) {
    Preconditions.checkArgument(compactionType != CompactionType.Uncompacted);
    String fileName = createNewFilename(compactionType);
    return new NodeMetricsCompactedFile(fileName, compactionType);
  }

  /**
   * Returns a FileSystem-style path glob that can be used to list compacted node metrics files in a
   * given directory with the specified compaction type
   */
  static Path getGlob(Path baseDirectory, CompactionType compactionType) {
    Preconditions.checkArgument(compactionType != CompactionType.Uncompacted);
    Character prefix = COMPACTION_FILE_PREFIX.get(compactionType);
    return baseDirectory.resolve(
        String.format("%s_", prefix) + DATETIME_FORMATTER.getDateTimeGlob() + ".csv");
  }

  /**
   * Returns the timestamp denoted through the filename that the node metrics file represents. Note
   * for these compacted files, the timestamp corresponds to the time that the compacted file was
   * written.
   */
  @Override
  public ZonedDateTime getWriteTimestamp() {
    return getWriteTimestamp(name);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public CompactionType getCompactionType() {
    return compactionType;
  }

  /**
   * Returns true if the specified file path may represent a compacted node metrics file; otherwise,
   * false
   */
  public static boolean isValid(String fileName) {
    return getCompactionType(fileName) != CompactionType.Uncompacted
        && getWriteTimestamp(fileName) != null;
  }

  private static CompactionType getCompactionType(String fileName) {
    if (fileName.length() < 2 || fileName.charAt(1) != '_') {
      return null;
    }
    char prefix = fileName.charAt(0);
    return COMPACTION_FILE_PREFIX.entrySet().stream()
        .filter(entry -> prefix == entry.getValue())
        .findFirst()
        .map(Map.Entry::getKey)
        .orElse(null);
  }

  private static ZonedDateTime getWriteTimestamp(String fileName) {
    int fileExtensionIndex = fileName.lastIndexOf(".");
    if (fileExtensionIndex < 2) {
      return null;
    }
    try {
      String datetimeString = fileName.substring(2, fileExtensionIndex);
      return DATETIME_FORMATTER.parseDateTime(datetimeString);
    } catch (DateTimeParseException e) {
      return null;
    }
  }

  /**
   * @return new unique filename named based on the current timestamp
   */
  private static String createNewFilename(CompactionType compactionType) {
    String fileStem = DATETIME_FORMATTER.formatDateTime(Instant.now());
    Character prefix = COMPACTION_FILE_PREFIX.get(compactionType);
    return String.format("%s_%s.csv", prefix, fileStem);
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if (!(other instanceof NodeMetricsCompactedFile)) {
      return false;
    }

    return name.equals(((NodeMetricsCompactedFile) other).name)
        && compactionType.equals(((NodeMetricsCompactedFile) other).compactionType);
  }

  @Override
  public final int hashCode() {
    return Objects.hash(name, compactionType);
  }

  @Override
  public int compareTo(@NotNull NodeMetricsFile o) {
    return Integer.compare(this.hashCode(), o.hashCode());
  }
}
