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
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.Objects;
import org.apache.commons.io.FilenameUtils;
import org.jetbrains.annotations.NotNull;

/**
 * An abstract representation of an uncompacted node metrics file, i.e. that represents node metrics
 * at a single point in time
 */
final class NodeMetricsPointFile implements NodeMetricsFile {
  private static final NodeMetricsDateTimeFormatter DATETIME_FORMATTER =
      new SecondPrecisionDateTimeFormatter();

  private final String name;

  private NodeMetricsPointFile(String name) {
    this.name = name;
  }

  /** Instantiate from a specified file path */
  static NodeMetricsFile from(String name) {
    Preconditions.checkArgument(isValid(name));
    return new NodeMetricsPointFile(name);
  }

  /** Instantiate with a generated file name given a specified directory */
  static NodeMetricsFile newInstance() {
    String name = createNewFilename();
    return new NodeMetricsPointFile(name);
  }

  /**
   * Returns a FileSystem-style path glob that can be used to list uncompacted node metrics files in
   * a given directory
   */
  static Path getGlob(Path baseDirectory) {
    return baseDirectory.resolve(DATETIME_FORMATTER.getDateTimeGlob() + ".csv");
  }

  @Override
  public String getName() {
    return name;
  }

  /**
   * @return datetime of the file (if persisted) at second precision
   */
  @Override
  public ZonedDateTime getWriteTimestamp() {
    return getWriteTimestamp(name);
  }

  @Override
  public CompactionType getCompactionType() {
    return CompactionType.Uncompacted;
  }

  /**
   * Returns true if the specified file name may represent an uncompacted node metrics file;
   * otherwise, false
   */
  public static boolean isValid(String name) {
    return getWriteTimestamp(name) != null;
  }

  private static ZonedDateTime getWriteTimestamp(String fileName) {
    try {
      String datetimeString = FilenameUtils.getBaseName(fileName);
      return DATETIME_FORMATTER.parseDateTime(datetimeString);
    } catch (DateTimeParseException e) {
      return null;
    }
  }

  /**
   * @return new unique filename named based on the current timestamp
   */
  private static String createNewFilename() {
    String fileStem = DATETIME_FORMATTER.formatDateTime(Instant.now());
    return String.format("%s.csv", fileStem);
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if (!(other instanceof NodeMetricsPointFile)) {
      return false;
    }

    return name.equals(((NodeMetricsPointFile) other).name);
  }

  @Override
  public final int hashCode() {
    return Objects.hashCode(name);
  }

  @Override
  public int compareTo(@NotNull NodeMetricsFile o) {
    return Integer.compare(this.hashCode(), o.hashCode());
  }
}
