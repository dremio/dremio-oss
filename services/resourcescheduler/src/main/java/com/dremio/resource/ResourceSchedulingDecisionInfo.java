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
package com.dremio.resource;

import com.dremio.exec.enginemanagement.proto.EngineManagementProtos.EngineId;
import com.dremio.exec.enginemanagement.proto.EngineManagementProtos.SubEngineId;
import com.dremio.exec.proto.UserBitShared;

/**
 * To Keep Information regarding some decisions made during
 * resource allocations
 */
public class ResourceSchedulingDecisionInfo {
  private String queueName;
  private String queueId;
  private String ruleContent;
  private String ruleId;
  private String ruleName;
  private String ruleAction;
  // to overwrite WorkloadClass set on PlanFragment level
  private UserBitShared.WorkloadClass workloadClass;
  private long schedulingStartTimeMs;  // Time when resource allocation started, in ms
  private long schedulingEndTimeMs;    // Time when resources were fully allocated, in ms
  private String queueTag;
  private String engineName;
  private EngineId engineId;
  private SubEngineId subEngineId;

  private ResourceSchedulingProperties resourceSchedulingProperties;

  private byte[] extraInfo;

  public ResourceSchedulingDecisionInfo() {
    this.schedulingStartTimeMs = 0;
    this.schedulingEndTimeMs = 0;
  }

  public String getQueueName() {
    return queueName;
  }

  public void setQueueName(String queueName) {
    this.queueName = queueName;
  }

  public String getQueueId() {
    return queueId;
  }

  public void setQueueId(String queueId) {
    this.queueId = queueId;
  }

  public String getRuleContent() {
    return ruleContent;
  }

  public void setRuleContent(String ruleContent) {
    this.ruleContent = ruleContent;
  }

  public String getRuleId() {
    return ruleId;
  }

  public void setRuleId(String ruleId) {
    this.ruleId = ruleId;
  }

  public String getRuleName() {
    return ruleName;
  }

  public void setRuleName(String ruleName) {
    this.ruleName = ruleName;
  }

  public byte[] getExtraInfo() {
    return extraInfo;
  }

  public void setExtraInfo(byte[] extraInfo) {
    this.extraInfo = extraInfo;
  }

  public String getRuleAction() {
    return ruleAction;
  }

  public void setRuleAction(String ruleAction) {
    this.ruleAction = ruleAction;
  }

  public ResourceSchedulingProperties getResourceSchedulingProperties() {
    return resourceSchedulingProperties;
  }

  public void setResourceSchedulingProperties(ResourceSchedulingProperties resourceSchedulingProperties) {
    this.resourceSchedulingProperties = resourceSchedulingProperties;
  }

  public UserBitShared.WorkloadClass getWorkloadClass() {
    return workloadClass;
  }

  public void setWorkloadClass(UserBitShared.WorkloadClass workloadClass) {
    this.workloadClass = workloadClass;
  }

  public long getSchedulingStartTimeMs() {
    return schedulingStartTimeMs;
  }

  public void setSchedulingStartTimeMs(long schedulingStartTimeMs) {
    this.schedulingStartTimeMs = schedulingStartTimeMs;
  }

  public long getSchedulingEndTimeMs() {
    return schedulingEndTimeMs;
  }

  public void setSchedulingEndTimeMs(long schedulingEndTimeMs) {
    this.schedulingEndTimeMs = schedulingEndTimeMs;
  }

  public String getQueueTag() {
    return queueTag;
  }

  public void setQueueTag(String queueTag) {
    this.queueTag = queueTag;
  }

  public String getEngineName() {
    return engineName;
  }

  public void setEngineName(String engineName) {
    this.engineName = engineName;
  }

  public EngineId getEngineId() {
    return engineId;
  }

  public void setEngineId(EngineId engineId) {
    this.engineId = engineId;
  }

  public SubEngineId getSubEngineId() {
    return subEngineId;
  }

  public void setSubEngineId(SubEngineId subEngineId) {
    this.subEngineId = subEngineId;
  }
}
