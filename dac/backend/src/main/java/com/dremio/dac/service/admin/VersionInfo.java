/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.dac.service.admin;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Description of version information.
 */
public class VersionInfo {
  private final String version;
  private final long buildtime;
  private final CommitInfo commit;

  @JsonCreator
  public VersionInfo(
      @JsonProperty("version") String version,
      @JsonProperty("buildtime") long buildtime,
      @JsonProperty("commit") CommitInfo commit) {
    super();
    this.version = version;
    this.buildtime = buildtime;
    this.commit = commit;
  }

  public String getVersion() {
    return version;
  }

  public long getBuildtime() {
    return buildtime;
  }

  public CommitInfo getCommit() {
    return commit;
  }
}
