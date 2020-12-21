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
package com.dremio.dac.model.info;

import com.dremio.dac.api.JsonISODateTime;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Description of version information.
 */
public class VersionInfo {
  private final String version;
  @JsonISODateTime
  private final long buildTime;
  private final CommitInfo commit;
  private final String clusterType;

  @JsonCreator
  public VersionInfo(
      @JsonProperty("version") String version,
      @JsonProperty("buildTime") long buildTime,
      @JsonProperty("commit") CommitInfo commit,
      @JsonProperty("clusterType") String clusterType) {
    super();
    this.version = version;
    this.buildTime = buildTime;
    this.commit = commit;
    this.clusterType = clusterType;
  }

  public String getVersion() {
    return version;
  }

  public long getBuildTime() {
    return buildTime;
  }

  public CommitInfo getCommit() {
    return commit;
  }

  public String getClusterType() {
    return clusterType;
  }
}
