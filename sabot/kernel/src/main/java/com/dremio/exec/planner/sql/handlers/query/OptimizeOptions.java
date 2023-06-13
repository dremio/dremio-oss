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

import java.util.Optional;

import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.sql.parser.SqlOptimize;
import com.dremio.options.OptionManager;
import com.fasterxml.jackson.annotation.JsonIgnore;
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
  public static OptimizeOptions DEFAULT = new Builder().build();
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
  private final boolean optimizeDataFiles;
  private final boolean optimizeManifestFiles;


  private OptimizeOptions(Long targetFileSizeBytes, Long maxFileSizeBytes, Long minFileSizeBytes, Long minInputFiles,
                          boolean isSingleDataWriter, boolean optimizeDataFiles, boolean optimizeManifestFiles) {
    this.targetFileSizeBytes = targetFileSizeBytes;
    this.maxFileSizeBytes = maxFileSizeBytes;
    this.minFileSizeBytes = minFileSizeBytes;
    this.minInputFiles = minInputFiles;
    this.isSingleDataWriter = isSingleDataWriter;
    this.optimizeDataFiles = optimizeDataFiles;
    this.optimizeManifestFiles = optimizeManifestFiles;
  }

  public static OptimizeOptions createInstance(OptionManager optionManager, SqlOptimize call, boolean isSingleDataWriter) {
    Builder instanceBuilder = new Builder();

    instanceBuilder.setSingleWriter(isSingleDataWriter);
    instanceBuilder.setTargetFileSizeMB(call.getTargetFileSize().orElse(optionManager.getOption(ExecConstants.OPTIMIZE_TARGET_FILE_SIZE_MB)));
    instanceBuilder.setMaxFileSizeRatio(optionManager.getOption(ExecConstants.OPTIMIZE_MAXIMUM_FILE_SIZE_DEFAULT_RATIO));
    instanceBuilder.setMinFileSizeRatio(optionManager.getOption(ExecConstants.OPTIMIZE_MINIMUM_FILE_SIZE_DEFAULT_RATIO));

    instanceBuilder.setMaxFileSizeMB(call.getMaxFileSize()); // computed from ratio if not set
    instanceBuilder.setMinFileSizeMB(call.getMinFileSize());// computed from ratio if not set
    Long minInputFiles = call.getMinInputFiles().orElse(optionManager.getOption(ExecConstants.OPTIMIZE_MINIMUM_INPUT_FILES));
    instanceBuilder.setMinInputFiles(Optional.of(minInputFiles));

    instanceBuilder.setOptimizeDataFiles(call.getRewriteDataFiles().booleanValue());
    instanceBuilder.setOptimizeManifestFiles(call.getRewriteManifests().booleanValue());

    return instanceBuilder.build();
  }

  public static OptimizeOptions createInstance(SqlOptimize call) {
    Builder instanceBuilder = new Builder();

    call.getTargetFileSize().ifPresent(instanceBuilder::setTargetFileSizeMB);
    instanceBuilder.setMaxFileSizeMB(call.getMaxFileSize());
    instanceBuilder.setMinFileSizeMB(call.getMinFileSize());
    instanceBuilder.setMinInputFiles(call.getMinInputFiles());

    instanceBuilder.setOptimizeDataFiles(call.getRewriteDataFiles().booleanValue());
    instanceBuilder.setOptimizeManifestFiles(call.getRewriteManifests().booleanValue());

    return instanceBuilder.build();
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
      maxFileSizeMB >= targetFileSizeMB,
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

  public boolean isOptimizeDataFiles() {
    return optimizeDataFiles;
  }

  public boolean isOptimizeManifestFiles() {
    return optimizeManifestFiles;
  }

  @JsonIgnore
  public boolean isOptimizeManifestsOnly() {
    return isOptimizeManifestFiles() && !isOptimizeDataFiles();
  }

  private static class Builder {
    private Long targetFileSizeMB = ExecConstants.OPTIMIZE_TARGET_FILE_SIZE_MB.getDefault().getNumVal();
    private Double maxFileSizeRatio = ExecConstants.OPTIMIZE_MAXIMUM_FILE_SIZE_DEFAULT_RATIO.getDefault().getFloatVal();
    private Double minFileSizeRatio = ExecConstants.OPTIMIZE_MINIMUM_FILE_SIZE_DEFAULT_RATIO.getDefault().getFloatVal();
    private Optional<Long> maxFileSizeMB = Optional.empty();
    private Optional<Long> minFileSizeMB = Optional.empty();
    private long minInputFiles = ExecConstants.OPTIMIZE_MINIMUM_INPUT_FILES.getDefault().getNumVal();
    private boolean optimizeDataFiles = true;
    private boolean optimizeManifestFiles = true;
    private boolean isSingleWriter = false;

    private Builder() {}

    private void setMaxFileSizeRatio(Double maxFileSizeRatio) {
      this.maxFileSizeRatio = maxFileSizeRatio;
    }

    private void setMinFileSizeRatio(Double minFileSizeRatio) {
      this.minFileSizeRatio = minFileSizeRatio;
    }

    private void setMinInputFiles(Optional<Long> minInputFiles) {
      minInputFiles.ifPresent(val -> this.minInputFiles = val);
    }

    private void setMaxFileSizeMB(Optional<Long> maxFileSizeMB) {
      this.maxFileSizeMB = maxFileSizeMB;
    }

    private void setMinFileSizeMB(Optional<Long> minFileSizeMB) {
      this.minFileSizeMB = minFileSizeMB;
    }

    private void setTargetFileSizeMB(Long targetFileSizeMB) {
      this.targetFileSizeMB = targetFileSizeMB;
    }

    private void setOptimizeDataFiles(boolean optimizeDataFiles) {
      this.optimizeDataFiles = optimizeDataFiles;
    }

    private void setOptimizeManifestFiles(boolean optimizeManifestFiles) {
      this.optimizeManifestFiles = optimizeManifestFiles;
    }

    public void setSingleWriter(boolean singleWriter) {
      isSingleWriter = singleWriter;
    }

    private long mbToBytes(long sizeMB) {
      return sizeMB * 1024 * 1024;
    }

    private OptimizeOptions build() {
      long maxFileSizeMbVal = this.maxFileSizeMB.orElse((long) (this.targetFileSizeMB * maxFileSizeRatio));
      long minFileSizeMbVal = this.minFileSizeMB.orElse((long) (this.targetFileSizeMB * minFileSizeRatio));

      validateOptions(targetFileSizeMB, minFileSizeMbVal, maxFileSizeMbVal, minInputFiles);

      long targetFileSizeBytes = mbToBytes(targetFileSizeMB);
      long maxFileSizeBytes = mbToBytes(maxFileSizeMbVal);
      long minFileSizeBytes = mbToBytes(minFileSizeMbVal);

      return new OptimizeOptions(targetFileSizeBytes, maxFileSizeBytes, minFileSizeBytes, minInputFiles, isSingleWriter, optimizeDataFiles, optimizeManifestFiles);
    }
  }
}
