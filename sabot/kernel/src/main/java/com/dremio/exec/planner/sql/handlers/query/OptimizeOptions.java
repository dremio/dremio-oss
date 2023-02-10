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

package com.dremio.exec.planner.sql.handlers.query;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.sql.parser.SqlOptimize;
import com.dremio.options.OptionManager;
import com.google.common.base.Preconditions;

/**
 * A rewrite strategy for data files which determines which files to rewrite based on their size. If
 * files are either smaller than the {@link #OptimizeOptions#minFileSizeBytes} threshold or larger than the
 * {@link #OptimizeOptions#maxFileSizeBytes} threshold, they are considered targets for being rewritten.
 * Groups will be considered for rewriting if they
 * contain more files than {@link #OptimizeOptions#minInputFiles} or would produce at least one file of {@link
 * OptimizeOptions#targetFileSizeBytes}.
 */
public final class OptimizeOptions {

  private final Long targetFileSizeBytes;
  private final Long maxFileSizeBytes;
  private final Long minFileSizeBytes;
  private final Long minInputFiles;
  /**
   * If a dataset is non-partitioned, it should use a single parquet writer.
   * It will avoid different blocks of the same parquet to be written by different fragments.
   * It should use a single parquet writer to ensure the writer rolls to the next file only after completing the current one.
   * This avoids creation of small files when execution planning over parallelisms.
   */
  private final boolean isSingleDataWriter;

  public static OptimizeOptions DEFAULT = new OptimizeOptions();


  private OptimizeOptions() {
    this.targetFileSizeBytes = ExecConstants.OPTIMIZE_TARGET_FILE_SIZE_MB.getDefault().getNumVal();
    this.maxFileSizeBytes = (long) (ExecConstants.OPTIMIZE_MAXIMUM_FILE_SIZE_DEFAULT_RATIO.getDefault().getFloatVal() * targetFileSizeBytes);
    this.minFileSizeBytes = (long) (ExecConstants.OPTIMIZE_MINIMUM_FILE_SIZE_DEFAULT_RATIO.getDefault().getFloatVal() * targetFileSizeBytes);
    this.minInputFiles = ExecConstants.OPTIMIZE_MINIMUM_INPUT_FILES.getDefault().getNumVal();
    this.isSingleDataWriter = false;
  }

  public OptimizeOptions(OptionManager optionManager, SqlOptimize call, boolean isSingleDataWriter) {
    this.isSingleDataWriter = isSingleDataWriter;
    Long targetFileSizeMB, maxFileSizeMB, minFileSizeMB;
    targetFileSizeMB = call.getTargetFileSize().orElse(optionManager.getOption(ExecConstants.OPTIMIZE_TARGET_FILE_SIZE_MB));
    maxFileSizeMB = call
      .getMaxFileSize()
      .orElse(
        (long) (optionManager.getOption(ExecConstants.OPTIMIZE_MAXIMUM_FILE_SIZE_DEFAULT_RATIO)*targetFileSizeMB));
    minFileSizeMB = call
      .getMinFileSize()
      .orElse(
        (long) (optionManager.getOption(ExecConstants.OPTIMIZE_MINIMUM_FILE_SIZE_DEFAULT_RATIO)*targetFileSizeMB));
    minInputFiles = call.getMinInputFiles().orElse(optionManager.getOption(ExecConstants.OPTIMIZE_MINIMUM_INPUT_FILES));
    validateOptions(targetFileSizeMB, minFileSizeMB, maxFileSizeMB, minInputFiles);
    targetFileSizeBytes = targetFileSizeMB * 1024L * 1024L;
    maxFileSizeBytes = maxFileSizeMB * 1024L * 1024L;
    minFileSizeBytes = minFileSizeMB * 1024L * 1024L;
  }

  public static void validateOptions(Long targetFileSizeMB, Long minFileSizeMB, Long maxFileSizeMB, Long minInputFiles) {
    Preconditions.checkArgument(
      targetFileSizeMB > 0,
      "TARGET_FILE_SIZE_MB [%s] should be a positive integer value.",
      targetFileSizeMB);

    Preconditions.checkArgument(
      minFileSizeMB >= 0,
      "MIN_FILE_SIZE_MB [%s] should be a non-negative integer value.",
      minFileSizeMB);

    Preconditions.checkArgument(
      maxFileSizeMB > 0,
      "MAX_FILE_SIZE_MB [%s] should be a positive integer value.",
      maxFileSizeMB);

    Preconditions.checkArgument(
      maxFileSizeMB >= minFileSizeMB,
      "Value of MIN_FILE_SIZE_MB [%s] cannot be greater than MAX_FILE_SIZE_MB [%s].",
      minFileSizeMB,
      maxFileSizeMB);

    Preconditions.checkArgument(
      targetFileSizeMB >= minFileSizeMB,
      "Value of TARGET_FILE_SIZE_MB [%s] cannot be less than MIN_FILE_SIZE_MB [%s].",
      targetFileSizeMB,
      minFileSizeMB);

    Preconditions.checkArgument(
      maxFileSizeMB  >= targetFileSizeMB,
      "Value of TARGET_FILE_SIZE_MB [%s] cannot be greater than MAX_FILE_SIZE_MB [%s].",
      targetFileSizeMB,
      maxFileSizeMB);

    Preconditions.checkArgument(
      minInputFiles > 0,
      "Value of MIN_INPUT_FILES [%s] cannot be less than 1.",
      minInputFiles);
  }

  public Long getTargetFileSizeBytes() {
    return targetFileSizeBytes;
  }

  public Long getMaxFileSizeBytes() {
    return maxFileSizeBytes;
  }

  public Long getMinFileSizeBytes() {
    return minFileSizeBytes;
  }

  public Long getMinInputFiles() {
    return minInputFiles;
  }

  public boolean isSingleDataWriter() {
    return isSingleDataWriter;
  }
}
