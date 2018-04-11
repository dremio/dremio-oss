/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.dac.support;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The result of requesting a support upload.
 */
public class SupportResponse {
  private final boolean success;
  private final boolean includesLogs;
  private final String url;

  @JsonCreator
  public SupportResponse(
      @JsonProperty("success") boolean success,
      @JsonProperty("includesLogs") boolean includesLogs,
      @JsonProperty("url") String url) {
    super();
    this.success = success;
    this.includesLogs = includesLogs;
    this.url = url;
  }

  public String getUrl() {
    return url;
  }

  public boolean isSuccess() {
    return success;
  }

  public boolean isIncludesLogs() {
    return includesLogs;
  }

}
