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
package com.dremio.dac.service.admin;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Description of commit information.
 */
public class CommitInfo {
  private final String hash;
  private final String builder;
  private final long time;
  private final String message;

  @JsonCreator
  public CommitInfo(
      @JsonProperty("hash") String hash,
      @JsonProperty("builder") String builder,
      @JsonProperty("time") long time,
      @JsonProperty("message") String message) {
    super();
    this.hash = hash;
    this.builder = builder;
    this.time = time;
    this.message = message;
  }
  public String getHash() {
    return hash;
  }
  public String getBuilder() {
    return builder;
  }
  public long getTime() {
    return time;
  }
  public String getMessage() {
    return message;
  }
}
