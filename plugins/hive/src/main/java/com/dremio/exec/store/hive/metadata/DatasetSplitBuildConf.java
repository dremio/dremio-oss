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
package com.dremio.exec.store.hive.metadata;

import java.util.Objects;

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;

/**
 * Helper class to hold elements needed to construct Dremio DatasetSplit objects.
 */
public class DatasetSplitBuildConf {

  private final JobConf job;
  private final HiveDatasetStats metastoreStats;
  private final InputFormat<?, ?> format;

  private DatasetSplitBuildConf(final JobConf job, final HiveDatasetStats metastoreStats, final InputFormat<?, ?> format) {
    this.job = job;
    this.metastoreStats = metastoreStats;
    this.format = format;
  }

  public JobConf getJob() {
    return job;
  }

  public HiveDatasetStats getMetastoreStats() {
    return metastoreStats;
  }

  public InputFormat<?, ?> getFormat() {
    return format;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder {
    private JobConf job;
    private HiveDatasetStats metastoreStats;
    private InputFormat<?, ?> format;

    private Builder() {
    }

    public Builder job(JobConf job) {
      this.job = job;
      return this;
    }

    public Builder metastoreStats(HiveDatasetStats metastoreStats) {
      this.metastoreStats = metastoreStats;
      return this;
    }

    public Builder format(InputFormat<?, ?> format) {
      this.format = format;
      return this;
    }

    public DatasetSplitBuildConf build() {

      Objects.requireNonNull(job, "job is required");
      Objects.requireNonNull(metastoreStats, "metastore stats is required");
      Objects.requireNonNull(format, "format is required");

      return new DatasetSplitBuildConf(job, metastoreStats, format);
    }
  }
}
