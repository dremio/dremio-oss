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
package com.dremio.exec.physical.base;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.Objects;
import javax.annotation.Nullable;
import org.immutables.value.Value;

@JsonDeserialize(builder = ImmutableTableFormatWriterOptions.Builder.class)
@Value.Immutable
public interface TableFormatWriterOptions {
  enum TableFormatOperation {
    NONE,
    CREATE,
    DELETE,
    INSERT,
    MERGE,
    UPDATE,
    OPTIMIZE,
    VACUUM
  }

  /** Iceberg operation type */
  @Value.Default
  default TableFormatOperation getOperation() {
    return TableFormatOperation.NONE;
  }

  /**
   * Minimum input files to consider for a replacing non-optimized data files with optimized ones
   * (replace). Ignored if not a positive value
   */
  @Nullable
  Long getMinInputFilesBeforeOptimize();

  /**
   * SnapshotId for operation. Consuming operation: Starting snapshot ID for conflict resolution
   * with delete files in optimization.
   */
  @Nullable
  Long getSnapshotId();

  /**
   * Target file size to be used by writers. System defaults to be used in case the value is null.
   * Currently used only by Parquet writer.
   */
  @Nullable
  Long getTargetFileSize();

  @Value.Default
  default IcebergWriterOptions getIcebergSpecificOptions() {
    return IcebergWriterOptions.makeDefault();
  }

  static TableFormatWriterOptions makeDefault() {
    return new ImmutableTableFormatWriterOptions.Builder().build();
  }

  @JsonIgnore
  default boolean isTableFormatWriter() {
    return Objects.nonNull(getOperation()) && getOperation() != TableFormatOperation.NONE;
  }
}
