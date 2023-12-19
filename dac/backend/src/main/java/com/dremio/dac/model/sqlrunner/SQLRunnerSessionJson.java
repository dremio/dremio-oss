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
package com.dremio.dac.model.sqlrunner;

import java.util.List;

import com.dremio.service.sqlrunner.SQLRunnerSession;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SQLRunnerSessionJson {

  private final String userId;
  private final List<String> scriptIds;
  private final String currentScriptId;

  @JsonCreator
  public SQLRunnerSessionJson(
    @JsonProperty("userId") String userId,
    @JsonProperty("scriptIds") List<String> scriptIds,
    @JsonProperty("currentScriptId") String currentScriptId
  ) {
    this.userId = userId;
    this.scriptIds = scriptIds;
    this.currentScriptId = currentScriptId;
  }

  public SQLRunnerSessionJson(SQLRunnerSession session) {
    this(session.getUserId(),
      session.getScriptIds(),
      session.getCurrentScriptId());
  }

  public String getUserId() {
    return userId;
  }

  public List<String> getScriptIds() {
    return scriptIds;
  }

  public String getCurrentScriptId() {
    return currentScriptId;
  }
}
