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
package com.dremio.resource;

import java.util.List;
import java.util.Map;

import com.dremio.exec.proto.CoordinationProtos;

/**
 * Basic ResourceScheduling Properties
 */
public class ResourceSchedulingProperties {

  private String user;
  private String userInfo;
  private Double queryCost;
  private String clientType;
  private String workloadType;
  private String queueName;
  private List<String> tags;
  private Map<Integer, Map<CoordinationProtos.NodeEndpoint, Integer>> resourceData;

  public ResourceSchedulingProperties() {

  }

  public String getUser() {
    return user;
  }

  public ResourceSchedulingProperties setUser(String user) {
    this.user = user;
    return this;
  }

  public String getUserInfo() {
    return userInfo;
  }

  public ResourceSchedulingProperties setUserInfo(String userInfo) {
    this.userInfo = userInfo;
    return this;
  }

  public Double getQueryCost() {
    return queryCost;
  }

  public ResourceSchedulingProperties setQueryCost(Double queryCost) {
    this.queryCost = queryCost;
    return this;
  }

  public String getClientType() {
    return clientType;
  }

  public ResourceSchedulingProperties setClientType(String clientType) {
    this.clientType = clientType;
    return this;
  }

  public String getWorkloadType() {
    return workloadType;
  }

  public ResourceSchedulingProperties setWorkloadType(String workloadType) {
    this.workloadType = workloadType;
    return this;
  }

  public String getQueueName() {
    return queueName;
  }

  public ResourceSchedulingProperties setQueueName(String queueName) {
    this.queueName = queueName;
    return this;
  }

  public List<String> getTags() {
    return tags;
  }

  public ResourceSchedulingProperties setTags(List<String> tags) {
    this.tags = tags;
    return this;
  }

  public Map<Integer, Map<CoordinationProtos.NodeEndpoint, Integer>> getResourceData() {
    return resourceData;
  }

  public ResourceSchedulingProperties setResourceData(Map<Integer, Map<CoordinationProtos.NodeEndpoint, Integer>> resourceData) {
    this.resourceData = resourceData;
    return this;
  }
}
