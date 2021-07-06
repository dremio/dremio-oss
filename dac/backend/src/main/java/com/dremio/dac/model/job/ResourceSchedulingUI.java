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
package com.dremio.dac.model.job;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * ResourceSchedulingUI
 */
public class ResourceSchedulingUI {

  private final String queueId;
  private final String queueName;
  private final String ruleId;
  private final String ruleName;
  private final String ruleContent;
  private final String engineName;

  @JsonCreator
  public ResourceSchedulingUI(
    @JsonProperty("queueId") String queueId,
    @JsonProperty("queueName")String queueName,
    @JsonProperty("ruleId")String ruleId,
    @JsonProperty("ruleName")String ruleName,
    @JsonProperty("ruleContent")String ruleContent,
    @JsonProperty("engineName")String engineName) {
    this.queueId = queueId;
    this.queueName = queueName;
    this.ruleId = ruleId;
    this.ruleName = ruleName;
    this.ruleContent = ruleContent;
    this.engineName = engineName;
  }

  public String getQueueId() {
    return queueId;
  }

  public String getQueueName() {
    return queueName;
  }

  public String getRuleId() {
    return ruleId;
  }

  public String getRuleName() {
    return ruleName;
  }

  public String getRuleContent() {
    return ruleContent;
  }

  public String getEngineName() {
    return engineName;
  }
}
